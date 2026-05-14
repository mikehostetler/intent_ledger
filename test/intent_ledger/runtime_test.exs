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

  test "unsupported replay sources return public invalid input errors" do
    assert {:error, %IntentLedger.Error.InvalidInputError{field: :source, value: :unknown}} =
             TestIntents.replay(:unknown)
  end

  test "projection cursors are durable per configured ledger" do
    assert {:ok, nil} = TestIntents.projection_cursor(StatusProjection)

    assert :ok = TestIntents.put_projection_cursor(StatusProjection, 42)
    assert {:ok, 42} = TestIntents.projection_cursor(StatusProjection)

    assert :ok = TestIntents.put_projection_cursor("external-status", 7)
    assert {:ok, 7} = TestIntents.projection_cursor("external-status")

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :cursor, value: -1}} =
             TestIntents.put_projection_cursor(StatusProjection, -1)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :projection, value: ""}} =
             TestIntents.projection_cursor("")
  end

  test "handler execution updates Intent lifecycle state" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123, test_pid: self()})

    queue_payload = %{raw: :erlang.term_to_binary(%{ledger: TestIntents, intent_id: intent.id})}

    assert {:ok, %{sent: true}} =
             SendInvoice.perform(queue_payload, %{
               topic: "invoice.send",
               queue_id: "default",
               item_id: intent.id,
               attempt: 1
             })

    intent_id = intent.id
    assert_receive {:handled, 123, ^intent_id, 1}

    assert {:ok, completed} = TestIntents.fetch(intent.id)
    assert completed.status == :completed
    assert completed.result == %{sent: true}

    assert {:ok, signals} = TestIntents.history(intent.id)
    assert Enum.map(signals, & &1.type) == ["intent.enqueued", "intent.started", "intent.completed"]
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

    assert {:error, :boom} = perform_edge(intent, attempt: 2)
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
    assert {:error, {:exception, %RuntimeError{message: "handler exploded"}}} = perform_edge(exception_intent)

    assert {:ok, failed_exception} = EdgeIntents.fetch(exception_intent.id)
    assert failed_exception.status == :failed
    assert match?({:exception, %RuntimeError{}}, failed_exception.error)
    assert_handler_telemetry(:error, exception_intent, error_kind: :exception)

    assert {:ok, throw_intent} = enqueue_edge(:throw, max_attempts: 1)
    assert {:error, {:throw, :handler_threw}} = perform_edge(throw_intent)

    assert {:ok, failed_throw} = EdgeIntents.fetch(throw_intent.id)
    assert failed_throw.status == :failed
    assert failed_throw.error == {:throw, :handler_threw}
    assert_handler_telemetry(:error, throw_intent, error_kind: :throw)
  end

  test "failed handlers schedule retry until max attempts are exhausted" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.fail", %{}, max_attempts: 2)

    queue_payload = %{raw: :erlang.term_to_binary(%{ledger: TestIntents, intent_id: intent.id})}

    assert {:error, :boom} =
             FailingIntent.perform(queue_payload, %{
               topic: "invoice.fail",
               queue_id: "default",
               item_id: intent.id,
               attempt: 1
             })

    assert {:ok, retrying} = TestIntents.fetch(intent.id)
    assert retrying.status == :retry_scheduled

    assert {:error, :boom} =
             FailingIntent.perform(queue_payload, %{
               topic: "invoice.fail",
               queue_id: "default",
               item_id: intent.id,
               attempt: 2
             })

    assert {:ok, failed} = TestIntents.fetch(intent.id)
    assert failed.status == :failed
  end

  test "ambiguous intents are parked and not handed to handlers" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123, test_pid: self()})
    assert {:ok, ambiguous} = TestIntents.mark_ambiguous(intent.id, :manual_review)
    assert ambiguous.status == :ambiguous

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

  test "manual requeue only accepts failed intents" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.fail", %{}, max_attempts: 1)

    assert {:error, %IntentLedger.Error.ConflictError{reason: :not_requeueable, details: %{status: :enqueued}}} =
             TestIntents.requeue(intent.id)

    queue_payload = %{raw: :erlang.term_to_binary(%{ledger: TestIntents, intent_id: intent.id})}

    assert {:error, :boom} =
             FailingIntent.perform(queue_payload, %{
               topic: "invoice.fail",
               queue_id: "default",
               item_id: intent.id,
               attempt: 1
             })

    assert {:ok, failed} = TestIntents.fetch(intent.id)
    assert failed.status == :failed

    assert {:ok, requeued} = TestIntents.requeue(failed.id)
    assert requeued.status == :retry_scheduled
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
    assert {:ok, health} = TestIntents.health()

    assert health.status == :ok
    assert health.repo == IntentLedger.FakeRepo
    assert health.queues == ["default", "tenant:acme", "tenant:beta"]
    assert health.default_queue == "default"
    assert health.topics == ["invoice.fail", "invoice.send"]
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

  defp perform_edge(intent, meta \\ []) do
    EdgeIntent.perform(queue_payload(EdgeIntents, intent.id), %{
      topic: "intent.edge",
      queue_id: "default",
      item_id: intent.id,
      attempt: Keyword.get(meta, :attempt, 1)
    })
  end

  defp queue_payload(ledger, intent_id), do: %{raw: :erlang.term_to_binary(%{ledger: ledger, intent_id: intent_id})}

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
