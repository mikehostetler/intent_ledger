defmodule IntentLedger.MultiNode.NetSplitScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :chaos
  @moduletag :job_queue_boundary

  alias Bedrock.JobQueue.Consumer.Worker
  alias Bedrock.JobQueue.Internal
  alias Bedrock.JobQueue.Store

  defmodule PartitionedHandler do
    use IntentLedger.Handler, topic: "partition.invoice"

    @impl true
    def handle(%{test_pid: test_pid}, ctx) do
      send(test_pid, {:partition_side_effect, ctx.intent.id, ctx.attempt})
      {:ok, %{attempt: ctx.attempt}}
    end
  end

  defmodule PartitionIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{"partition.invoice" => [handler: PartitionedHandler]}
  end

  setup do
    IntentLedger.RealBedrock.setup!()
  end

  test "partitioned worker cannot mutate terminal state after the cluster heals" do
    assert {:ok, signal} =
             PartitionIntents.command_signal(:enqueue,
               topic: "partition.invoice",
               payload: %{invoice_id: "split-1", test_pid: self()}
             )

    assert {:ok, intent} = PartitionIntents.submit(signal)
    now = System.system_time(:millisecond) + 1

    [node_b_item] = repo().transact(fn -> Store.peek(repo(), queue_root(), "default", limit: 1, now: now) end)

    assert {:ok, node_b_lease} =
             repo().transact(fn -> Store.obtain_lease(repo(), queue_root(), node_b_item, "node-b", 10, now: now) end)

    assert {:ok, %{attempt: 1}} = node_b_result = Worker.execute(node_b_item, workers())
    assert_receive {:partition_side_effect, intent_id, 1}
    assert intent_id == intent.id

    assert {:ok, started} = PartitionIntents.fetch(intent.id)
    assert started.status == :started

    [node_c_item] = repo().transact(fn -> Store.peek(repo(), queue_root(), "default", limit: 1, now: now + 11) end)
    assert node_c_item.id == intent.id

    assert {:ok, node_c_lease} =
             repo().transact(fn ->
               Store.obtain_lease(repo(), queue_root(), node_c_item, "node-c", 10, now: now + 11)
             end)

    assert {:ok, %{attempt: 1}} = node_c_result = Worker.execute(node_c_item, workers())
    assert_receive {:partition_side_effect, ^intent_id, 1}

    assert :ok = complete_with_hook(node_c_lease, node_c_result)

    assert {:ok, completed} = PartitionIntents.fetch(intent.id)
    assert completed.status == :completed
    assert completed.result == %{attempt: 1}

    assert {:error, :lease_not_found} =
             repo().transact(fn ->
               queue_result = Store.complete(repo(), queue_root(), node_b_lease)

               IntentLedger.Runtime.QueueLifecycle.apply(
                 repo(),
                 queue_root(),
                 node_b_lease,
                 :complete,
                 node_b_result,
                 queue_result,
                 PartitionIntents
               )
             end)

    assert {:ok, still_completed} = PartitionIntents.fetch(intent.id)
    assert still_completed.status == :completed
    assert still_completed.result == %{attempt: 1}

    assert {:ok, history} = PartitionIntents.history(intent.id)

    assert Enum.map(history, & &1.type) == [
             "intent.enqueued",
             "intent.started",
             "intent.started",
             "intent.completed"
           ]

    assert {:ok, %{"default" => %{pending_count: 0, processing_count: 0}}} =
             PartitionIntents.stats(queue: "default")
  end

  defp complete_with_hook(lease, result) do
    repo().transact(fn ->
      queue_result = Store.complete(repo(), queue_root(), lease)

      IntentLedger.Runtime.QueueLifecycle.apply(
        repo(),
        queue_root(),
        lease,
        :complete,
        result,
        queue_result,
        PartitionIntents
      )
    end)
  end

  defp queue_root, do: Internal.root_keyspace(PartitionIntents.JobQueue)

  defp workers, do: %{"partition.invoice" => PartitionedHandler}

  defp repo, do: IntentLedger.RealBedrock.Repo
end
