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
      handlers: %{
        "invoice.send" => SendInvoice,
        "invoice.fail" => FailingIntent
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

  test "health exposes the configured runtime" do
    assert {:ok, health} = TestIntents.health()

    assert health.status == :ok
    assert health.repo == IntentLedger.FakeRepo
    assert health.topics == ["invoice.fail", "invoice.send"]
  end
end
