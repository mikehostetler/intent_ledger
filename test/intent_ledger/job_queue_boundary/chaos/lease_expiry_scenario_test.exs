defmodule IntentLedger.Chaos.LeaseExpiryScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :chaos
  @moduletag :job_queue_boundary

  alias Bedrock.JobQueue.Consumer.Worker
  alias Bedrock.JobQueue.Internal
  alias Bedrock.JobQueue.Store

  defmodule CrashWindowHandler do
    use IntentLedger.Handler, topic: "lease.expiry.crash_window"

    @impl true
    def handle(%{test_pid: test_pid}, ctx) do
      send(test_pid, {:external_side_effect, ctx.intent.id, ctx.attempt})
      {:ok, %{attempt: ctx.attempt}}
    end
  end

  defmodule LeaseIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{"lease.expiry.crash_window" => [handler: CrashWindowHandler]}
  end

  setup do
    IntentLedger.RealBedrock.setup!()
  end

  test "expired item lease becomes visible to another worker without manual repair" do
    assert {:ok, intent} = LeaseIntents.enqueue("lease.expiry.crash_window", %{test_pid: self()})
    now = System.system_time(:millisecond) + 1
    [item] = repo().transact(fn -> Store.peek(repo(), queue_root(), "default", limit: 1, now: now) end)

    assert {:ok, _lease} =
             repo().transact(fn -> Store.obtain_lease(repo(), queue_root(), item, "node-a", 10, now: now) end)

    assert [] = repo().transact(fn -> Store.peek(repo(), queue_root(), "default", limit: 1, now: now + 5) end)

    assert [visible_after_expiry] =
             repo().transact(fn -> Store.peek(repo(), queue_root(), "default", limit: 1, now: now + 11) end)

    assert visible_after_expiry.id == intent.id
  end

  test "worker crash after side effect but before queue commit is recoverable through lease expiry" do
    assert {:ok, intent} = LeaseIntents.enqueue("lease.expiry.crash_window", %{test_pid: self()})
    now = System.system_time(:millisecond) + 1
    [item] = repo().transact(fn -> Store.peek(repo(), queue_root(), "default", limit: 1, now: now) end)

    assert {:ok, _node_a_lease} =
             repo().transact(fn -> Store.obtain_lease(repo(), queue_root(), item, "node-a", 10, now: now) end)

    assert {:ok, %{attempt: 1}} = Worker.execute(item, %{"lease.expiry.crash_window" => CrashWindowHandler})
    assert_receive {:external_side_effect, intent_id, 1}
    assert intent_id == intent.id

    assert {:ok, started} = LeaseIntents.fetch(intent.id)
    assert started.status == :started
    assert started.attempt == 1

    assert {:ok, [node_b_lease]} =
             repo().transact(fn ->
               Store.dequeue(repo(), queue_root(), "default", "node-b",
                 limit: 1,
                 lease_duration: 10,
                 now: now + 11
               )
             end)

    assert node_b_lease.item_id == intent.id
  end

  defp queue_root, do: Internal.root_keyspace(LeaseIntents.JobQueue)

  defp repo, do: IntentLedger.RealBedrock.Repo
end
