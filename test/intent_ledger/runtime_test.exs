defmodule IntentLedger.RuntimeTest do
  use ExUnit.Case, async: false

  defmodule SendInvoice do
    use IntentLedger.Handler,
      topic: "invoice.send",
      payload_schema: Zoi.map(),
      result_schema: Zoi.map(),
      timeout: 1_000

    @impl true
    def handle(%{invoice_id: invoice_id, test_pid: test_pid}, ctx) do
      send(test_pid, {:handled, invoice_id, ctx.intent.id, ctx.attempt})
      {:ok, %{sent: true}}
    end
  end

  defmodule FailingIntent do
    use IntentLedger.Handler, topic: "invoice.fail"

    @impl true
    def handle(_payload, _ctx), do: {:error, :boom}
  end

  defmodule EdgeIntent do
    use IntentLedger.Handler,
      topic: "intent.edge",
      payload_schema: Zoi.map(),
      result_schema: Zoi.map()

    @impl true
    def handle(%{mode: mode, test_pid: test_pid}, ctx) do
      send(test_pid, {:edge_handled, mode, ctx.intent.id, ctx.attempt})

      case mode do
        :ok -> :ok
        :result -> {:ok, %{handled: true}}
        :error -> {:error, :boom}
        :discard -> {:discard, :not_useful}
        :snooze -> {:snooze, 5_000}
        :invalid_result -> {:ok, :not_a_map}
        :invalid_return -> {:unexpected, :shape}
        :exception -> raise RuntimeError, "handler exploded"
        :throw -> throw(:handler_threw)
      end
    end
  end

  defmodule EdgeIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.FakeRepo,
      intents: %{
        "intent.edge" => [handler: EdgeIntent, queue: "default"]
      }
  end

  defmodule StatusProjection do
  end

  defmodule TestIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.FakeRepo,
      queues: ["tenant:acme", "tenant:beta"],
      intents: %{
        "invoice.send" => [handler: SendInvoice, queue: "default"],
        "invoice.fail" => [handler: FailingIntent, queue: "default"]
      }
  end

  defmodule CriticalIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.FakeRepo,
      intents: %{
        "invoice.send" => [handler: SendInvoice, queue: :critical]
      }
  end

  setup do
    IntentLedger.FakeRepo.reset!()
    :ok
  end

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
      Enum.map(results, fn {:ok, intent} -> intent.id end)
      |> Enum.uniq()

    assert [_intent_id] = intent_ids
    assert {:ok, %{"default" => %{pending_count: 1, processing_count: 0}}} = TestIntents.stats(queue: "default")
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

  test "queue stats default to all configured queues" do
    assert {:ok, _intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123})

    assert {:ok, queues} = TestIntents.stats()

    assert queues |> Map.keys() |> Enum.sort() == ["default", "tenant:acme", "tenant:beta"]
    assert queues["default"].pending_count == 1
    assert queues["tenant:acme"].pending_count == 0
    assert queues["tenant:beta"].pending_count == 0
  end

  test "enqueue emits stop telemetry without payload data" do
    attach_telemetry(:enqueue)

    assert {:ok, _intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123})

    assert_receive {:telemetry, [:intent_ledger, :enqueue, :stop], measurements, metadata}
    assert is_integer(measurements.duration)
    assert measurements.count == 1
    assert metadata.ledger == TestIntents
    assert metadata.status == :ok
    refute Map.has_key?(metadata, :payload)
  end

  test "ledger replay starts from the target stream cursor, not the global stream keyspace" do
    for invoice_id <- 1..12 do
      assert {:ok, _intent} = TestIntents.enqueue("invoice.send", %{invoice_id: invoice_id})
    end

    assert {:ok, [first, second]} = TestIntents.replay(:ledger, cursor: 10, limit: 2)
    assert first.type == "intent.enqueued"
    assert second.type == "intent.enqueued"
  end

  test "outbox replay uses the outbox cursor window" do
    assert {:ok, first} = TestIntents.enqueue("invoice.send", %{invoice_id: 1})
    assert {:ok, second} = TestIntents.enqueue("invoice.send", %{invoice_id: 2})
    assert {:ok, third} = TestIntents.enqueue("invoice.send", %{invoice_id: 3})

    assert {:ok, entries} = TestIntents.inspect(:outbox, cursor: 0, limit: 3)
    assert Enum.map(entries, & &1.cursor) == [1, 2, 3]
    assert Enum.map(entries, & &1.signal.subject) == [first.id, second.id, third.id]

    assert {:ok, [signal]} = TestIntents.replay(:outbox, cursor: 1, limit: 1)
    assert signal.subject == second.id
    assert signal.type == "intent.enqueued"

    assert {:ok, []} = TestIntents.replay(:outbox, cursor: 3, limit: 1)
  end

  test "outbox consumers read and ack durable cursors monotonically" do
    assert {:ok, first} = TestIntents.enqueue("invoice.send", %{invoice_id: 1})
    assert {:ok, second} = TestIntents.enqueue("invoice.send", %{invoice_id: 2})
    assert {:ok, third} = TestIntents.enqueue("invoice.send", %{invoice_id: 3})

    assert {:ok, nil} = TestIntents.outbox_cursor("webhook-dispatcher")

    attach_telemetry(:outbox)

    assert {:ok, batch} = TestIntents.read_outbox("webhook-dispatcher", limit: 2)
    assert batch.consumer == "name:webhook-dispatcher"
    assert batch.acked_cursor == 0
    assert batch.next_cursor == 2
    assert batch.head_cursor == 3
    assert batch.lag == 1
    assert Enum.map(batch.entries, & &1.signal.subject) == [first.id, second.id]

    assert_receive {:telemetry, [:intent_ledger, :outbox, :stop], measurements, metadata}
    assert is_integer(measurements.duration)
    assert measurements.count == 2
    assert metadata.operation == :read
    assert metadata.consumer == "webhook-dispatcher"

    assert {:ok, ack} = TestIntents.ack_outbox("webhook-dispatcher", batch.next_cursor)
    assert ack.cursor == 2
    assert {:ok, 2} = TestIntents.outbox_cursor("webhook-dispatcher")

    assert {:ok, next_batch} = TestIntents.read_outbox("webhook-dispatcher", limit: 10)
    assert next_batch.acked_cursor == 2
    assert next_batch.next_cursor == 3
    assert next_batch.lag == 0
    assert Enum.map(next_batch.entries, & &1.signal.subject) == [third.id]

    assert {:error, %IntentLedger.Error.ConflictError{reason: :stale_outbox_ack}} =
             TestIntents.ack_outbox("webhook-dispatcher", 1)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :cursor, value: 4}} =
             TestIntents.ack_outbox("webhook-dispatcher", 4)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :consumer, value: ""}} =
             TestIntents.read_outbox("")
  end

  test "unsupported replay sources return public invalid input errors" do
    assert {:error, %IntentLedger.Error.InvalidInputError{field: :source, value: :unknown}} =
             TestIntents.replay(:unknown)
  end

  test "projection cursors are durable per configured ledger" do
    assert {:ok, _first} = TestIntents.enqueue("invoice.send", %{invoice_id: 1})
    assert {:ok, _second} = TestIntents.enqueue("invoice.send", %{invoice_id: 2})

    assert {:ok, nil} = TestIntents.projection_cursor(StatusProjection)

    assert :ok = TestIntents.put_projection_cursor(StatusProjection, 1)
    assert {:ok, 1} = TestIntents.projection_cursor(StatusProjection)

    assert {:error, %IntentLedger.Error.ConflictError{reason: :stale_projection_cursor}} =
             TestIntents.put_projection_cursor(StatusProjection, 0)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :cursor, value: 3}} =
             TestIntents.put_projection_cursor(StatusProjection, 3)

    assert :ok = TestIntents.put_projection_cursor(StatusProjection, 0, force: true)
    assert {:ok, 0} = TestIntents.projection_cursor(StatusProjection)

    assert :ok = TestIntents.put_projection_cursor("external-status", 2)
    assert {:ok, 2} = TestIntents.projection_cursor("external-status")

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :cursor, value: -1}} =
             TestIntents.put_projection_cursor(StatusProjection, -1)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :projection, value: ""}} =
             TestIntents.projection_cursor("")
  end

  test "inspection views expose intents retries ambiguous outbox and projections" do
    assert {:ok, enqueued} =
             TestIntents.enqueue("invoice.send", %{invoice_id: 1},
               key: "inspect:enqueued",
               queue: "tenant:acme"
             )

    assert {:ok, retrying} = TestIntents.enqueue("invoice.fail", %{invoice_id: 2}, max_attempts: 3)
    retry_payload = queue_payload(TestIntents, retrying.id)

    assert {:error, :boom} =
             retry_result =
             FailingIntent.perform(retry_payload, %{
               topic: "invoice.fail",
               queue_id: "default",
               item_id: retrying.id,
               attempt: 1
             })

    finalize_perform(TestIntents, retrying, retry_result, action: :requeue, queue_result: {:ok, :requeued})

    assert {:ok, ambiguous} = TestIntents.enqueue("invoice.send", %{invoice_id: 3})
    assert {:ok, _ambiguous} = TestIntents.mark_ambiguous(ambiguous.id, :manual_review)
    assert :ok = TestIntents.put_projection_cursor("ops-status", 5)

    assert {:ok, intents} = TestIntents.inspect(:intents, limit: 10)
    intent_ids = intents |> Enum.map(& &1.id) |> MapSet.new()
    assert MapSet.subset?(MapSet.new([enqueued.id, retrying.id, ambiguous.id]), intent_ids)

    assert {:ok, [filtered]} =
             TestIntents.inspect(:intents,
               queue: "tenant:acme",
               topic: "invoice.send",
               status: "enqueued"
             )

    assert filtered.id == enqueued.id

    assert {:ok, [retry_view]} = TestIntents.inspect(:retries)
    assert retry_view.id == retrying.id
    assert retry_view.status == :retry_scheduled

    assert {:ok, [ambiguous_view]} = TestIntents.inspect(:ambiguous)
    assert ambiguous_view.id == ambiguous.id
    assert ambiguous_view.status == :ambiguous

    assert {:ok, outbox} = TestIntents.inspect(:outbox, limit: 10)
    assert Enum.any?(outbox, &(&1.signal.type == "intent.retry_scheduled"))
    assert Enum.any?(outbox, &(&1.signal.type == "intent.ambiguous"))

    assert {:ok, projections} = TestIntents.inspect(:projections)

    assert %{projection: "name:ops-status", cursor: 5, head_cursor: 6, lag: 1, updated_at: %DateTime{}} =
             Enum.find(projections, &(&1.projection == "name:ops-status"))
  end

  test "inspection views normalize invalid options through public errors" do
    assert {:error, %IntentLedger.Error.InvalidInputError{field: :view, value: :missing}} =
             TestIntents.inspect(:missing)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :cursor, value: -1}} =
             TestIntents.inspect(:outbox, cursor: -1)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :limit, value: 0}} =
             TestIntents.inspect(:intents, limit: 0)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :status, value: :unknown}} =
             TestIntents.inspect(:intents, status: :unknown)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :queue, value: ""}} =
             TestIntents.inspect(:intents, queue: "")
  end

  test "handler execution updates Intent lifecycle state" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123, test_pid: self()})

    queue_payload = %{raw: :erlang.term_to_binary(%{ledger: TestIntents, intent_id: intent.id})}

    assert {:ok, %{sent: true}} =
             result =
             SendInvoice.perform(queue_payload, %{
               topic: "invoice.send",
               queue_id: "default",
               item_id: intent.id,
               attempt: 1
             })

    finalize_perform(TestIntents, intent, result)

    intent_id = intent.id
    assert_receive {:handled, 123, ^intent_id, 1}

    assert {:ok, completed} = TestIntents.fetch(intent.id)
    assert completed.status == :completed
    assert completed.result == %{sent: true}

    assert {:ok, signals} = TestIntents.history(intent.id)
    assert Enum.map(signals, & &1.type) == ["intent.enqueued", "intent.started", "intent.completed"]
  end

  test "queue action hooks keep raw bedrock_job_queue return values" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123})

    assert {:error, :queue_failed} =
             IntentLedger.Runtime.apply_queue_action(
               TestIntents,
               IntentLedger.FakeRepo,
               lease_for(intent),
               :complete,
               :ok,
               {:error, :queue_failed}
             )

    assert {:ok, fresh} = TestIntents.fetch(intent.id)
    assert fresh.status == :enqueued
  end

  test "handler execution emits stop telemetry" do
    attach_telemetry(:handler)

    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123, test_pid: self()})

    queue_payload = %{raw: :erlang.term_to_binary(%{ledger: TestIntents, intent_id: intent.id})}

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
    assert metadata.attempt == 1
    assert metadata.status == :ok
    refute Map.has_key?(metadata, :payload)
  end

  test "handler :ok return completes without a result" do
    attach_telemetry(:handler)

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
    assert {:ok, intent} = enqueue_edge(:result)

    assert {:ok, %{handled: true}} = perform_edge(intent, attempt: 1)

    assert_receive {:edge_handled, :result, intent_id, 1}
    assert intent_id == intent.id

    assert {:ok, completed} = EdgeIntents.fetch(intent.id)
    assert completed.status == :completed
    assert completed.result == %{handled: true}
    assert_history(EdgeIntents, intent, ["intent.enqueued", "intent.started", "intent.completed"])
  end

  test "handler {:error, reason} retries before max attempts and fails at max attempts" do
    attach_telemetry(:handler)

    assert {:ok, intent} = enqueue_edge(:error, max_attempts: 2)

    assert {:error, :boom} = perform_edge(intent, attempt: 1)
    assert {:ok, retrying} = EdgeIntents.fetch(intent.id)
    assert retrying.status == :retry_scheduled
    assert retrying.error == :boom
    assert_handler_telemetry(:error, intent, error_kind: :boom, attempt: 1)

    assert {:error, :boom} = perform_edge(intent, attempt: 2, queue_result: {:ok, :dead_lettered})
    assert {:ok, failed} = EdgeIntents.fetch(intent.id)
    assert failed.status == :failed
    assert failed.error == :boom
    assert_handler_telemetry(:error, intent, error_kind: :boom, attempt: 2)

    assert_history(EdgeIntents, intent, [
      "intent.enqueued",
      "intent.started",
      "intent.retry_scheduled",
      "intent.started",
      "intent.failed"
    ])
  end

  test "handler {:discard, reason} discards the Intent" do
    attach_telemetry(:handler)

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
    attach_telemetry(:handler)

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
    attach_telemetry(:handler)

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
    attach_telemetry(:handler)

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
    attach_telemetry(:handler)

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
    attach_telemetry(:handler)

    assert {:ok, exception_intent} = enqueue_edge(:exception, max_attempts: 1)

    assert {:error, {:exception, %RuntimeError{message: "handler exploded"}}} =
             perform_edge(exception_intent, queue_result: {:ok, :dead_lettered})

    assert {:ok, failed_exception} = EdgeIntents.fetch(exception_intent.id)
    assert failed_exception.status == :failed
    assert match?({:exception, %RuntimeError{}}, failed_exception.error)
    assert_handler_telemetry(:error, exception_intent, error_kind: :exception)

    assert {:ok, throw_intent} = enqueue_edge(:throw, max_attempts: 1)
    assert {:error, {:throw, :handler_threw}} = perform_edge(throw_intent, queue_result: {:ok, :dead_lettered})

    assert {:ok, failed_throw} = EdgeIntents.fetch(throw_intent.id)
    assert failed_throw.status == :failed
    assert failed_throw.error == {:throw, :handler_threw}
    assert_handler_telemetry(:error, throw_intent, error_kind: :throw)
  end

  test "failed handlers schedule retry until max attempts are exhausted" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.fail", %{}, max_attempts: 2)

    queue_payload = %{raw: :erlang.term_to_binary(%{ledger: TestIntents, intent_id: intent.id})}

    assert {:error, :boom} =
             result =
             FailingIntent.perform(queue_payload, %{
               topic: "invoice.fail",
               queue_id: "default",
               item_id: intent.id,
               attempt: 1
             })

    finalize_perform(TestIntents, intent, result, action: :requeue, queue_result: {:ok, :requeued})

    assert {:ok, retrying} = TestIntents.fetch(intent.id)
    assert retrying.status == :retry_scheduled

    assert {:error, :boom} =
             result =
             FailingIntent.perform(queue_payload, %{
               topic: "invoice.fail",
               queue_id: "default",
               item_id: intent.id,
               attempt: 2
             })

    finalize_perform(TestIntents, intent, result, action: :requeue, queue_result: {:ok, :dead_lettered})

    assert {:ok, failed} = TestIntents.fetch(intent.id)
    assert failed.status == :failed
  end

  test "ambiguous intents are parked and not handed to handlers" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123, test_pid: self()})
    assert {:ok, ambiguous} = TestIntents.mark_ambiguous(intent.id, :manual_review)
    assert ambiguous.status == :ambiguous
    assert {:ok, %{"default" => %{pending_count: 0, processing_count: 0}}} = TestIntents.stats(queue: "default")

    queue_payload = %{raw: :erlang.term_to_binary(%{ledger: TestIntents, intent_id: intent.id})}

    assert :ok =
             SendInvoice.perform(queue_payload, %{
               topic: "invoice.send",
               queue_id: "default",
               item_id: intent.id,
               attempt: 1
             })

    refute_receive {:handled, _, _, _}
    assert {:ok, still_ambiguous} = TestIntents.fetch(intent.id)
    assert still_ambiguous.status == :ambiguous
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

    assert {:ok, ambiguous} = TestIntents.enqueue("invoice.send", %{invoice_id: 321})
    assert {:ok, _parked} = TestIntents.mark_ambiguous(ambiguous.id, :manual_review)
    assert {:ok, canceled_ambiguous} = TestIntents.cancel(ambiguous.id, :resolved_by_cancel)
    assert canceled_ambiguous.status == :canceled
    assert_history(TestIntents, ambiguous, ["intent.enqueued", "intent.ambiguous", "intent.canceled"])

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

    assert {:error, %IntentLedger.Error.ConflictError{reason: :not_ambiguousable, details: %{status: :completed}}} =
             TestIntents.mark_ambiguous(completed.id, :too_late)
  end

  test "manual requeue only accepts failed intents" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.fail", %{}, max_attempts: 1)

    assert {:error, %IntentLedger.Error.ConflictError{reason: :not_requeueable, details: %{status: :enqueued}}} =
             TestIntents.requeue(intent.id)

    queue_payload = %{raw: :erlang.term_to_binary(%{ledger: TestIntents, intent_id: intent.id})}

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
    attach_telemetry(:command)

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

  test "health exposes the configured runtime" do
    attach_telemetry(:health)

    assert {:ok, _intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123})
    assert {:ok, health} = TestIntents.health()

    assert health.status == :ok
    assert health.repo == IntentLedger.FakeRepo
    assert health.queues == ["default", "tenant:acme", "tenant:beta"]
    assert health.default_queue == "default"
    assert health.topics == ["invoice.fail", "invoice.send"]
    assert health.queue_stats["default"].pending_count == 1
    assert health.cursors == %{ledger: 1, outbox: 1}
    assert health.errors == []

    assert_receive {:telemetry, [:intent_ledger, :health, :stop], measurements, metadata}
    assert is_integer(measurements.duration)
    assert metadata.ledger == TestIntents
    assert metadata.status == :ok
  end

  defp attach_telemetry(event) do
    id = {__MODULE__, event, make_ref()}

    :telemetry.attach(
      id,
      [:intent_ledger, event, :stop],
      &__MODULE__.handle_telemetry/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  def handle_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry, event, measurements, metadata})
  end

  defp enqueue_edge(mode, opts \\ []) do
    EdgeIntents.enqueue("intent.edge", %{mode: mode, test_pid: self()}, opts)
  end

  defp perform_edge(intent, meta) do
    result =
      EdgeIntent.perform(queue_payload(EdgeIntents, intent.id), %{
        topic: "intent.edge",
        queue_id: "default",
        item_id: intent.id,
        attempt: Keyword.get(meta, :attempt, 1)
      })

    if Keyword.get(meta, :finalize, true) do
      finalize_perform(EdgeIntents, intent, result, meta)
    end

    result
  end

  defp queue_payload(ledger, intent_id), do: %{raw: :erlang.term_to_binary(%{ledger: ledger, intent_id: intent_id})}

  defp finalize_perform(ledger, intent, handler_result, opts \\ []) do
    action = Keyword.get(opts, :action, action_for_result(handler_result))
    queue_result = Keyword.get(opts, :queue_result, queue_result_for_action(action))

    assert :ok =
             IntentLedger.Runtime.apply_queue_action(
               ledger,
               IntentLedger.FakeRepo,
               lease_for(intent),
               action,
               handler_result,
               queue_result
             )
  end

  defp lease_for(intent) do
    %Bedrock.JobQueue.Lease{
      id: "test-lease",
      item_id: intent.id,
      queue_id: intent.queue,
      holder: "test",
      obtained_at: 0,
      expires_at: 1,
      item_key: nil
    }
  end

  defp action_for_result(:ok), do: :complete
  defp action_for_result({:ok, _result}), do: :complete
  defp action_for_result({:discard, _reason}), do: :complete
  defp action_for_result({:error, _reason}), do: :requeue
  defp action_for_result({:snooze, delay_ms}), do: {:snooze, delay_ms}

  defp queue_result_for_action(:complete), do: :ok
  defp queue_result_for_action(:requeue), do: {:ok, :requeued}
  defp queue_result_for_action({:snooze, _delay_ms}), do: {:ok, :requeued}

  defp assert_history(ledger, intent, expected_types) do
    assert {:ok, signals} = ledger.history(intent.id)
    assert Enum.map(signals, & &1.type) == expected_types
  end

  defp assert_handler_telemetry(status, intent, opts \\ []) do
    expected_attempt = Keyword.get(opts, :attempt, 1)

    assert_receive {:telemetry, [:intent_ledger, :handler, :stop], measurements, metadata}
    assert is_integer(measurements.duration)
    assert measurements.count == 1
    assert metadata.ledger == EdgeIntents
    assert metadata.handler == EdgeIntent
    assert metadata.intent_id == intent.id
    assert metadata.topic == "intent.edge"
    assert metadata.queue == "default"
    assert metadata.item_id == intent.id
    assert metadata.attempt == expected_attempt
    assert metadata.status == status
    refute Map.has_key?(metadata, :payload)

    case Keyword.fetch(opts, :error_kind) do
      {:ok, error_kind} -> assert metadata.error_kind == error_kind
      :error -> refute Map.has_key?(metadata, :error_kind)
    end
  end
end
