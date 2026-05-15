defmodule IntentLedger.Runtime.ExecutionTest do
  use IntentLedger.TestCase, async: false

  test "handler execution updates Intent lifecycle state" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123, test_pid: self()})

    queue_payload = queue_payload(TestIntents, intent.id)

    assert {:ok, %{sent: true}} =
             result =
             SendInvoice.perform(queue_payload, %{
               topic: "invoice.send",
               queue_id: "default",
               item_id: intent.id,
               attempt: 1
             })

    intent_id = intent.id
    assert_receive {:handled, 123, ^intent_id, 1}

    assert {:ok, started} = TestIntents.fetch(intent.id)
    assert started.status == :started
    assert started.attempt == 1

    finalize_perform(TestIntents, intent, result)

    assert {:ok, completed} = TestIntents.fetch(intent.id)
    assert completed.status == :completed
    assert completed.result == %{sent: true}

    assert_history(TestIntents, intent, ["intent.enqueued", "intent.started", "intent.completed"])
  end

  test "handler execution emits stop telemetry" do
    attach_telemetry(:handler, self())

    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123, test_pid: self()})

    queue_payload = queue_payload(TestIntents, intent.id)

    assert {:ok, %{sent: true}} =
             SendInvoice.perform(queue_payload, %{
               topic: "invoice.send",
               queue_id: "default",
               item_id: intent.id,
               attempt: 1
             })

    assert_receive {:telemetry, [:intent_ledger, :handler, :stop], measurements, metadata}
    assert is_integer(measurements.duration)
    assert measurements.count == 1
    assert metadata.ledger == TestIntents
    assert metadata.handler == SendInvoice
    assert metadata.intent_id == intent.id
    assert metadata.topic == "invoice.send"
    assert metadata.queue == "default"
    assert metadata.item_id == intent.id
    assert metadata.attempt == 1
    assert metadata.status == :ok
    refute Map.has_key?(metadata, :payload)
  end

  test "handler :ok return completes without a result" do
    attach_telemetry(:handler, self())

    assert {:ok, intent} = enqueue_edge(:ok)

    assert :ok = perform_edge(intent, attempt: 1)
    assert_receive {:edge_handled, :ok, intent_id, 1}
    assert intent_id == intent.id

    assert {:ok, completed} = EdgeIntents.fetch(intent.id)
    assert completed.status == :completed
    assert completed.result == nil
    assert_history(EdgeIntents, intent, ["intent.enqueued", "intent.started", "intent.completed"])
    assert_handler_telemetry(:ok, intent)
  end

  test "handler {:ok, result} validates result and completes" do
    attach_telemetry(:handler, self())

    assert {:ok, intent} = enqueue_edge(:result)

    assert {:ok, %{handled: true}} = perform_edge(intent, attempt: 1)
    assert_receive {:edge_handled, :result, intent_id, 1}
    assert intent_id == intent.id

    assert {:ok, completed} = EdgeIntents.fetch(intent.id)
    assert completed.status == :completed
    assert completed.result == %{handled: true}
    assert_history(EdgeIntents, intent, ["intent.enqueued", "intent.started", "intent.completed"])
    assert_handler_telemetry(:ok, intent)
  end

  test "handler {:error, reason} retries before max attempts and fails at max attempts" do
    attach_telemetry(:handler, self())

    assert {:ok, retry_intent} = enqueue_edge(:error, max_attempts: 2)
    assert {:error, :boom} = perform_edge(retry_intent, attempt: 1)

    assert {:ok, retrying} = EdgeIntents.fetch(retry_intent.id)
    assert retrying.status == :retry_scheduled
    assert retrying.error == :boom

    assert {:ok, fail_intent} = enqueue_edge(:error, max_attempts: 1)
    assert {:error, :boom} = perform_edge(fail_intent, attempt: 1, queue_result: {:ok, :dead_lettered})

    assert {:ok, failed} = EdgeIntents.fetch(fail_intent.id)
    assert failed.status == :failed
    assert failed.error == :boom
  end

  test "handler {:discard, reason} discards the Intent" do
    attach_telemetry(:handler, self())

    assert {:ok, intent} = enqueue_edge(:discard)

    assert {:discard, :not_useful} = perform_edge(intent, attempt: 1)
    assert_receive {:edge_handled, :discard, intent_id, 1}
    assert intent_id == intent.id

    assert {:ok, discarded} = EdgeIntents.fetch(intent.id)
    assert discarded.status == :discarded
    assert discarded.error == :not_useful
    assert_history(EdgeIntents, intent, ["intent.enqueued", "intent.started", "intent.discarded"])
    assert_handler_telemetry(:discard, intent, error_kind: :not_useful)
  end

  test "handler {:snooze, delay_ms} schedules retry lifecycle" do
    attach_telemetry(:handler, self())

    assert {:ok, intent} = enqueue_edge(:snooze)

    assert {:snooze, 5_000} = perform_edge(intent, attempt: 1)
    assert_receive {:edge_handled, :snooze, intent_id, 1}
    assert intent_id == intent.id

    assert {:ok, snoozed} = EdgeIntents.fetch(intent.id)
    assert snoozed.status == :retry_scheduled
    assert snoozed.error == {:snooze, 5_000}
    assert_history(EdgeIntents, intent, ["intent.enqueued", "intent.started", "intent.retry_scheduled"])
    assert_handler_telemetry(:snooze, intent)
  end

  test "invalid handler returns are discarded" do
    attach_telemetry(:handler, self())

    assert {:ok, intent} = enqueue_edge(:invalid_return)

    assert {:discard, {:invalid_handler_return, {:unexpected, :shape}}} = perform_edge(intent, attempt: 1)
    assert_receive {:edge_handled, :invalid_return, intent_id, 1}
    assert intent_id == intent.id

    assert {:ok, discarded} = EdgeIntents.fetch(intent.id)
    assert discarded.status == :discarded
    assert discarded.error == {:invalid_handler_return, {:unexpected, :shape}}
    assert_history(EdgeIntents, intent, ["intent.enqueued", "intent.started", "intent.discarded"])
    assert_handler_telemetry(:discard, intent, error_kind: :invalid_handler_return)
  end

  test "payload validation failures discard before handler execution" do
    attach_telemetry(:handler, self())

    assert {:ok, intent} = EdgeIntents.enqueue("intent.edge", :not_a_map)

    assert {:discard, {:invalid_payload, _errors}} = perform_edge(intent, attempt: 1)

    refute_receive {:edge_handled, _, _, _}
    assert {:ok, discarded} = EdgeIntents.fetch(intent.id)
    assert discarded.status == :discarded
    assert match?({:invalid_payload, _errors}, discarded.error)
    assert_history(EdgeIntents, intent, ["intent.enqueued", "intent.started", "intent.discarded"])
    assert_handler_telemetry(:discard, intent, error_kind: :invalid_payload)
  end

  test "result validation failures discard after handler execution" do
    attach_telemetry(:handler, self())

    assert {:ok, intent} = enqueue_edge(:invalid_result)

    assert {:discard, {:invalid_result, _errors}} = perform_edge(intent, attempt: 1)
    assert_receive {:edge_handled, :invalid_result, intent_id, 1}
    assert intent_id == intent.id

    assert {:ok, discarded} = EdgeIntents.fetch(intent.id)
    assert discarded.status == :discarded
    assert match?({:invalid_result, _errors}, discarded.error)
    assert_history(EdgeIntents, intent, ["intent.enqueued", "intent.started", "intent.discarded"])
    assert_handler_telemetry(:discard, intent, error_kind: :invalid_result)
  end

  test "handler exceptions and throws become retryable handler errors" do
    attach_telemetry(:handler, self())

    assert {:ok, exception_intent} = enqueue_edge(:exception, max_attempts: 1)

    assert {:error, {:exception, %RuntimeError{message: "handler exploded"}}} =
             perform_edge(exception_intent, queue_result: {:ok, :dead_lettered})

    assert {:ok, failed_exception} = EdgeIntents.fetch(exception_intent.id)
    assert failed_exception.status == :failed
    assert failed_exception.error == {:exception, %{type: :exception, module: "RuntimeError", redacted: true}}
    assert_handler_telemetry(:error, exception_intent, error_kind: :exception)

    assert {:ok, throw_intent} = enqueue_edge(:throw, max_attempts: 1)
    assert {:error, {:throw, :handler_threw}} = perform_edge(throw_intent, queue_result: {:ok, :dead_lettered})

    assert {:ok, failed_throw} = EdgeIntents.fetch(throw_intent.id)
    assert failed_throw.status == :failed
    assert failed_throw.error == {:throw, :handler_threw}
    assert_handler_telemetry(:error, throw_intent, error_kind: :throw)
  end
end
