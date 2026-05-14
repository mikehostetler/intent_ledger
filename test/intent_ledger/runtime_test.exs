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
end
