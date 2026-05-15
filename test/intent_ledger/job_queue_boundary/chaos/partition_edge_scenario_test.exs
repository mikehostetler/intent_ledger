defmodule IntentLedger.Chaos.PartitionEdgeScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :chaos
  @moduletag :job_queue_boundary

  alias Bedrock.JobQueue.Consumer.Worker
  alias Bedrock.JobQueue.Internal
  alias Bedrock.JobQueue.Store

  defmodule ChargePayment do
    use IntentLedger.Handler, topic: "payment.charge"

    @impl true
    def handle(%{test_pid: test_pid}, ctx) do
      send(test_pid, {:charged, ctx.intent.id, ctx.attempt})
      {:ok, %{charged: true}}
    end
  end

  defmodule ChaosIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{"payment.charge" => [handler: ChargePayment]}
  end

  setup do
    IntentLedger.RealBedrock.setup!()
  end

  test "signal command redelivery across isolated subscribers converges to one visible Intent" do
    assert {:ok, signal} =
             ChaosIntents.command_signal(:enqueue,
               topic: "payment.charge",
               payload: %{payment_id: "pay_123", test_pid: self()}
             )

    results =
      1..16
      |> Task.async_stream(fn _subscriber -> ChaosIntents.submit(signal) end,
        max_concurrency: 16,
        timeout: 5_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.all?(results, &match?({:ok, _intent}, &1))

    intent_ids =
      results
      |> Enum.map(fn {:ok, intent} -> intent.id end)
      |> Enum.uniq()

    assert [intent_id] = intent_ids
    assert {:ok, intent} = ChaosIntents.fetch(intent_id)
    assert intent.key == "signal:#{signal.id}"

    assert {:ok, %{"default" => %{pending_count: 1, processing_count: 0}}} =
             ChaosIntents.stats(queue: "default")

    assert_history(intent_id, ["intent.enqueued"])
  end

  test "duplicate stale worker completions after a simulated partition do not duplicate terminal facts" do
    assert {:ok, intent} =
             ChaosIntents.enqueue("payment.charge", %{payment_id: "pay_split", test_pid: self()})

    [item] = visible_jobs()
    assert {:ok, lease} = obtain_lease(item, "node-a")

    assert {:ok, %{charged: true}} = first_result = Worker.execute(item, workers())
    assert_receive {:charged, intent_id, 1}
    assert intent_id == intent.id

    assert {:ok, %{charged: true}} = stale_result = Worker.execute(item, workers())
    assert_receive {:charged, ^intent_id, 1}

    assert :ok = complete_with_hook(lease, :complete, first_result)

    assert {:error, :lease_not_found} =
             repo().transact(fn ->
               queue_result = Store.complete(repo(), queue_root(), lease)

               IntentLedger.Runtime.QueueLifecycle.apply(
                 repo(),
                 queue_root(),
                 lease,
                 :complete,
                 stale_result,
                 queue_result,
                 ChaosIntents
               )
             end)

    assert {:ok, completed} = ChaosIntents.fetch(intent.id)
    assert completed.status == :completed
    assert completed.result == %{charged: true}

    assert_history(intent.id, [
      "intent.enqueued",
      "intent.started",
      "intent.started",
      "intent.completed"
    ])

    assert {:ok, []} = ChaosIntents.view(:intents, status: :failed)

    assert {:ok, %{"default" => %{pending_count: 0, processing_count: 0}}} =
             ChaosIntents.stats(queue: "default")
  end

  test "cancel while leased parks the Intent and lets the stale worker only clear queue state" do
    assert {:ok, intent} =
             ChaosIntents.enqueue("payment.charge", %{payment_id: "pay_cancel", test_pid: self()})

    [item] = visible_jobs()
    assert {:ok, lease} = obtain_lease(item, "node-a")

    assert {:ok, canceled} = ChaosIntents.cancel(intent.id, :customer_canceled)
    assert canceled.status == :canceled
    assert canceled.cancel_reason == :customer_canceled

    assert {:ok, %{"default" => %{pending_count: 0, processing_count: 1}}} =
             ChaosIntents.stats(queue: "default")

    assert :ok = Worker.execute(item, workers())
    refute_receive {:charged, _, _}, 50

    assert :ok = complete_with_hook(lease, :complete, :ok)

    assert {:ok, still_canceled} = ChaosIntents.fetch(intent.id)
    assert still_canceled.status == :canceled
    assert still_canceled.result == nil

    assert {:ok, [_enqueued, canceled_signal]} = ChaosIntents.history(intent.id)
    assert canceled_signal.type == "intent.canceled"
    assert canceled_signal.data.queue_neutralization == :leased

    assert {:ok, %{"default" => %{pending_count: 0, processing_count: 0}}} =
             ChaosIntents.stats(queue: "default")
  end

  defp queue_root, do: Internal.root_keyspace(ChaosIntents.JobQueue)

  defp visible_jobs do
    repo().transact(fn -> Store.peek(repo(), queue_root(), "default", limit: 1) end)
  end

  defp obtain_lease(item, holder) do
    repo().transact(fn -> Store.obtain_lease(repo(), queue_root(), item, holder, 30_000) end)
  end

  defp complete_with_hook(lease, action, handler_result) do
    repo().transact(fn ->
      queue_result = Store.complete(repo(), queue_root(), lease)

      IntentLedger.Runtime.QueueLifecycle.apply(
        repo(),
        queue_root(),
        lease,
        action,
        handler_result,
        queue_result,
        ChaosIntents
      )
    end)
  end

  defp workers, do: %{"payment.charge" => ChargePayment}

  defp repo, do: IntentLedger.RealBedrock.Repo

  defp assert_history(intent_id, expected_types) do
    assert {:ok, signals} = ChaosIntents.history(intent_id)
    assert Enum.map(signals, & &1.type) == expected_types
  end
end
