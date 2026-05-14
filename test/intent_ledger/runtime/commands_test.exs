defmodule IntentLedger.Runtime.CommandsTest do
  use IntentLedger.TestCase, async: false

  test "configured modules enqueue Intents and persist lifecycle history" do
    assert {:ok, intent} =
             TestIntents.enqueue("invoice.send", %{invoice_id: 123, test_pid: self()},
               key: "invoice:123:send",
               queue: "tenant:acme",
               priority: 50
             )

    assert intent.topic == "invoice.send"
    assert intent.queue == "tenant:acme"
    assert intent.status == :enqueued

    assert {:ok, ^intent} = TestIntents.fetch(intent.id)
    assert {:ok, [signal]} = TestIntents.history(intent.id)
    assert signal.type == "intent.enqueued"
    assert {:ok, [%{cursor: 1, signal: ^signal}]} = TestIntents.inspect(:outbox)

    assert {:ok, %{"tenant:acme" => %{pending_count: 1, processing_count: 0}}} =
             TestIntents.stats(queue: "tenant:acme")
  end

  test "keys are idempotent at the Intent boundary" do
    assert {:ok, first} = TestIntents.enqueue("invoice.send", %{invoice_id: 123}, key: "invoice:123:send")
    assert {:ok, second} = TestIntents.enqueue("invoice.send", %{invoice_id: 123}, key: "invoice:123:send")

    assert second.id == first.id
    assert {:ok, %{"default" => %{pending_count: 1, processing_count: 0}}} = TestIntents.stats(queue: "default")
  end

  test "idempotency keys return the original Intent across payload topic and queue drift" do
    assert {:ok, first} =
             TestIntents.enqueue("invoice.send", %{invoice_id: 123},
               key: "invoice:123:send",
               queue: "tenant:acme"
             )

    assert {:ok, second} =
             TestIntents.enqueue("invoice.fail", %{invoice_id: 999},
               key: "invoice:123:send",
               queue: "tenant:beta"
             )

    assert second.id == first.id
    assert second.topic == "invoice.send"
    assert second.payload == %{invoice_id: 123}
    assert second.queue == "tenant:acme"

    assert {:ok, queues} = TestIntents.stats()
    assert queues["tenant:acme"].pending_count == 1
    assert queues["tenant:beta"].pending_count == 0
    assert queues["default"].pending_count == 0
  end

  test "idempotency keys collapse concurrent enqueue attempts" do
    results =
      1..20
      |> Task.async_stream(
        fn invoice_id ->
          TestIntents.enqueue("invoice.send", %{invoice_id: invoice_id}, key: "invoice:concurrent:send")
        end,
        max_concurrency: 20,
        timeout: 5_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.all?(results, &match?({:ok, _intent}, &1))

    intent_ids =
      results
      |> Enum.map(fn {:ok, intent} -> intent.id end)
      |> Enum.uniq()

    assert [_intent_id] = intent_ids
    assert {:ok, %{"default" => %{pending_count: 1, processing_count: 0}}} = TestIntents.stats(queue: "default")
  end

  test "command_signal builds signal-native enqueue commands" do
    assert {:ok, signal} =
             TestIntents.command_signal(:enqueue,
               topic: "invoice.send",
               payload: %{invoice_id: 123},
               key: "invoice:signal:send"
             )

    assert signal.type == "intent.command.enqueue"
    assert signal.source == "/intent_ledger/IntentLedger.TestFixtures.TestIntents"
    assert signal.datacontenttype == "application/x-erlang-term"
    assert signal.data.topic == "invoice.send"
    assert signal.data.payload == %{invoice_id: 123}
  end

  test "submit accepts signal-native enqueue commands and redelivery is idempotent" do
    assert {:ok, signal} =
             TestIntents.command_signal(:enqueue,
               topic: "invoice.send",
               payload: %{invoice_id: 123},
               queue: "tenant:acme"
             )

    attach_telemetry(:enqueue, self())

    assert {:ok, first} = TestIntents.submit(signal)
    assert {:ok, second} = TestIntents.submit(signal)

    assert second.id == first.id
    assert first.key == "signal:#{signal.id}"
    assert first.queue == "tenant:acme"
    assert first.causation_id == signal.id
    assert first.metadata.command_signal_id == signal.id
    assert first.metadata.command_signal_type == "intent.command.enqueue"

    assert_receive {:telemetry, [:intent_ledger, :enqueue, :stop], _measurements, metadata}
    assert metadata.command_signal_id == signal.id
    assert metadata.command_signal_type == "intent.command.enqueue"

    assert {:ok, [enqueued]} = TestIntents.history(first.id)
    assert enqueued.extensions.causation_id == signal.id
    assert {:ok, %{"tenant:acme" => %{pending_count: 1, processing_count: 0}}} = TestIntents.stats(queue: "tenant:acme")
  end

  test "submit accepts JSON-shaped signal data" do
    assert {:ok, signal} =
             Jido.Signal.new("intent.command.enqueue", %{
               "topic" => "invoice.send",
               "payload" => %{"invoice_id" => 123},
               "key" => "invoice:json-signal:send",
               "queue" => "tenant:beta"
             })

    assert {:ok, intent} = TestIntents.submit(signal)

    assert intent.topic == "invoice.send"
    assert intent.payload == %{"invoice_id" => 123}
    assert intent.key == "invoice:json-signal:send"
    assert intent.queue == "tenant:beta"
  end

  test "submit normalizes unsupported command signals through public errors" do
    assert {:ok, signal} = Jido.Signal.new("intent.command.missing", %{intent_id: "int-123"})

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :type, value: "intent.command.missing"}} =
             TestIntents.submit(signal)
  end

  test "submit command metadata appears on lifecycle data and telemetry" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123})

    assert {:ok, signal} =
             TestIntents.command_signal(:cancel,
               intent_id: intent.id,
               reason: :signal_cancel
             )

    attach_telemetry(:command, self())

    assert {:ok, canceled} = TestIntents.submit(signal)
    assert canceled.status == :canceled

    assert_receive {:telemetry, [:intent_ledger, :command, :stop], _measurements, metadata}
    assert metadata.command == :cancel
    assert metadata.command_signal_id == signal.id
    assert metadata.command_signal_type == "intent.command.cancel"

    assert {:ok, [_enqueued, canceled_signal]} = TestIntents.history(intent.id)
    assert canceled_signal.data.reason == :signal_cancel
    assert canceled_signal.data.command_signal_id == signal.id
    assert canceled_signal.data.command_signal_type == "intent.command.cancel"
  end

  test "enqueue_many accepts string-keyed map entries" do
    assert {:ok, [intent]} =
             TestIntents.enqueue_many([
               %{
                 "topic" => "invoice.send",
                 "payload" => %{invoice_id: 123},
                 "key" => "invoice:123:send",
                 "queue" => "tenant:beta"
               }
             ])

    assert intent.key == "invoice:123:send"
    assert intent.queue == "tenant:beta"
  end

  test "configured queues define the default queue and reject unknown queues" do
    assert {:ok, intent} = CriticalIntents.enqueue("invoice.send", %{invoice_id: 123})
    assert intent.queue == "critical"

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :queue, value: "tenant:missing"}} =
             TestIntents.enqueue("invoice.send", %{invoice_id: 123}, queue: "tenant:missing")
  end

  test "enqueue emits stop telemetry without payload data" do
    attach_telemetry(:enqueue, self())

    assert {:ok, _intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123})

    assert_receive {:telemetry, [:intent_ledger, :enqueue, :stop], measurements, metadata}
    assert is_integer(measurements.duration)
    assert measurements.count == 1
    assert metadata.ledger == TestIntents
    assert metadata.status == :ok
    refute Map.has_key?(metadata, :payload)
  end

  test "cancel is idempotent once canceled and rejected after completion" do
    assert {:ok, canceled} = TestIntents.enqueue("invoice.send", %{invoice_id: 123})
    assert {:ok, canceled_once} = TestIntents.cancel(canceled.id, :not_needed)
    assert canceled_once.status == :canceled
    assert {:ok, %{"default" => %{pending_count: 0, processing_count: 0}}} = TestIntents.stats(queue: "default")

    assert {:ok, canceled_twice} = TestIntents.cancel(canceled.id, :still_not_needed)
    assert canceled_twice.status == :canceled
    assert canceled_twice.cancel_reason == :not_needed
    assert_history(TestIntents, canceled, ["intent.enqueued", "intent.canceled"])

    assert {:ok, completed} = TestIntents.enqueue("invoice.send", %{invoice_id: 456, test_pid: self()})
    queue_payload = queue_payload(TestIntents, completed.id)

    assert {:ok, %{sent: true}} =
             result =
             SendInvoice.perform(queue_payload, %{
               topic: "invoice.send",
               queue_id: "default",
               item_id: completed.id,
               attempt: 1
             })

    finalize_perform(TestIntents, completed, result)

    assert {:error, %IntentLedger.Error.ConflictError{reason: :not_cancelable, details: %{status: :completed}}} =
             TestIntents.cancel(completed.id, :too_late)
  end

  test "mark_ambiguous is idempotent once ambiguous and rejected after completion" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123})
    assert {:ok, ambiguous_once} = TestIntents.mark_ambiguous(intent.id, :manual_review)
    assert ambiguous_once.status == :ambiguous

    assert {:ok, ambiguous_twice} = TestIntents.mark_ambiguous(intent.id, :still_manual_review)
    assert ambiguous_twice.status == :ambiguous
    assert ambiguous_twice.error == :manual_review
    assert_history(TestIntents, intent, ["intent.enqueued", "intent.ambiguous"])
  end

  test "manual requeue only accepts failed intents" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.fail", %{}, max_attempts: 1)

    assert {:error, %IntentLedger.Error.ConflictError{reason: :not_requeueable, details: %{status: :enqueued}}} =
             TestIntents.requeue(intent.id)

    queue_payload = queue_payload(TestIntents, intent.id)

    assert {:error, :boom} =
             result =
             FailingIntent.perform(queue_payload, %{
               topic: "invoice.fail",
               queue_id: "default",
               item_id: intent.id,
               attempt: 1
             })

    finalize_perform(TestIntents, intent, result, action: :requeue, queue_result: {:ok, :dead_lettered})

    assert {:ok, failed} = TestIntents.fetch(intent.id)
    assert failed.status == :failed

    assert {:ok, requeued} = TestIntents.requeue(failed.id)
    assert requeued.status == :retry_scheduled

    assert {:error, %IntentLedger.Error.ConflictError{reason: :not_requeueable, details: %{status: :retry_scheduled}}} =
             TestIntents.requeue(failed.id)
  end

  test "lifecycle commands emit stop telemetry for failures" do
    attach_telemetry(:command, self())

    assert {:ok, intent} = TestIntents.enqueue("invoice.fail", %{}, max_attempts: 1)

    assert {:error, %IntentLedger.Error.ConflictError{reason: :not_requeueable}} =
             TestIntents.requeue(intent.id)

    assert_receive {:telemetry, [:intent_ledger, :command, :stop], measurements, metadata}
    assert is_integer(measurements.duration)
    assert metadata.ledger == TestIntents
    assert metadata.command == :requeue
    assert metadata.intent_id == intent.id
    assert metadata.status == :error
    assert metadata.error_kind == :not_requeueable
  end
end
