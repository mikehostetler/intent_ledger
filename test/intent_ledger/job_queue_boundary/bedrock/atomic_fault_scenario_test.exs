defmodule IntentLedger.Bedrock.AtomicFaultScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :bedrock
  @moduletag :job_queue_boundary

  alias Bedrock.JobQueue.Internal
  alias Bedrock.JobQueue.Store

  defmodule AtomicHandler do
    use IntentLedger.Handler, topic: "atomic.invoice"

    @impl true
    def handle(_payload, _ctx), do: :ok
  end

  defmodule AtomicIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{"atomic.invoice" => [handler: AtomicHandler]}
  end

  setup do
    IntentLedger.RealBedrock.setup!()
  end

  test "queue action and lifecycle hook failure roll back as one atomic boundary" do
    assert {:ok, intent} = AtomicIntents.enqueue("atomic.invoice", %{invoice_id: 123})
    lease = lease_intent(intent)

    assert {:error, {:unexpected_queue_action, :requeue, :ok, :ok}} =
             repo().transact(fn ->
               queue_result = Store.complete(repo(), queue_root(), lease)

               case IntentLedger.Runtime.QueueLifecycle.apply(
                      repo(),
                      queue_root(),
                      lease,
                      :requeue,
                      :ok,
                      queue_result,
                      AtomicIntents
                    ) do
                 :ok -> queue_result
                 {:error, reason} -> repo().rollback(reason)
               end
             end)

    assert {:ok, still_enqueued} = AtomicIntents.fetch(intent.id)
    assert still_enqueued.status == :enqueued

    assert {:ok, %{"default" => %{pending_count: 0, processing_count: 1}}} =
             AtomicIntents.stats(queue: "default")
  end

  test "job_queue manager action hook failures roll back queue state" do
    assert {:ok, intent} = AtomicIntents.enqueue("atomic.invoice", %{invoice_id: 789})
    lease = lease_intent(intent)

    assert {:error, {:intent_lifecycle_update_failed, :boom}} =
             repo().transact(fn ->
               queue_result = Store.complete(repo(), queue_root(), lease)

               {:error, reason} =
                 failing_action_hook(
                   repo(),
                   queue_root(),
                   lease,
                   :complete,
                   :ok,
                   queue_result
                 )

               repo().rollback(reason)
             end)

    assert {:ok, still_enqueued} = AtomicIntents.fetch(intent.id)
    assert still_enqueued.status == :enqueued

    assert {:ok, %{"default" => %{pending_count: 0, processing_count: 1}}} =
             AtomicIntents.stats(queue: "default")
  end

  test "failed queue action does not mutate Intent lifecycle facts" do
    assert {:ok, intent} = AtomicIntents.enqueue("atomic.invoice", %{invoice_id: 456})
    lease = lease_intent(intent)

    assert :ok =
             repo().transact(fn ->
               Store.complete(repo(), queue_root(), lease)
             end)

    assert {:error, :lease_not_found} =
             IntentLedger.Runtime.QueueLifecycle.apply(
               repo(),
               queue_root(),
               lease,
               :complete,
               :ok,
               {:error, :lease_not_found},
               AtomicIntents
             )

    assert {:ok, still_enqueued} = AtomicIntents.fetch(intent.id)
    assert still_enqueued.status == :enqueued
    assert {:ok, history} = AtomicIntents.history(intent.id)
    assert Enum.map(history, & &1.type) == ["intent.enqueued"]
  end

  defp queue_root, do: Internal.root_keyspace(AtomicIntents.JobQueue)

  defp failing_action_hook(_repo, _queue_root, _lease, _action, _handler_result, _queue_result) do
    {:error, {:intent_lifecycle_update_failed, :boom}}
  end

  defp lease_intent(intent) do
    [item] = repo().transact(fn -> Store.peek(repo(), queue_root(), intent.queue, limit: 1) end)

    assert {:ok, lease} =
             repo().transact(fn -> Store.obtain_lease(repo(), queue_root(), item, "atomic-test", 30_000) end)

    lease
  end

  defp repo, do: IntentLedger.RealBedrock.Repo
end
