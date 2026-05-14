defmodule IntentLedger.MultiNodeScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :multi_node

  alias Bedrock.JobQueue.Consumer.Worker
  alias Bedrock.JobQueue.Internal
  alias Bedrock.JobQueue.Store

  defmodule ProcessInvoice do
    use IntentLedger.Handler, topic: "invoice.multi_node"

    @impl true
    def handle(%{test_pid: test_pid}, ctx) do
      send(test_pid, {:node_b_handled, ctx.intent.id, ctx.attempt})
      {:ok, %{handled_by: :node_b}}
    end
  end

  defmodule ClusterIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.FakeRepo,
      intents: %{"invoice.multi_node" => ProcessInvoice}
  end

  setup do
    IntentLedger.FakeRepo.reset!()
    :ok
  end

  test "node A enqueues, node B executes, and node C inspects and replays after restart" do
    test_pid = self()

    results =
      1..12
      |> Task.async_stream(
        fn invoice_id ->
          ClusterIntents.enqueue(
            "invoice.multi_node",
            %{invoice_id: invoice_id, test_pid: test_pid},
            key: "invoice:multi-node:process"
          )
        end,
        max_concurrency: 12,
        timeout: 5_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.all?(results, &match?({:ok, _intent}, &1))

    intent_ids =
      results
      |> Enum.map(fn {:ok, intent} -> intent.id end)
      |> Enum.uniq()

    assert [intent_id] = intent_ids
    assert {:ok, %{"default" => %{pending_count: 1, processing_count: 0}}} = ClusterIntents.inspect(:queues)

    assert {:ok, %{handled_by: :node_b}} = node_b_execute()
    assert_receive {:node_b_handled, ^intent_id, 1}

    assert {:ok, completed} = ClusterIntents.fetch(intent_id)
    assert completed.status == :completed
    assert completed.result == %{handled_by: :node_b}

    assert {:ok, [intent_view]} = ClusterIntents.inspect(:intents, status: :completed)
    assert intent_view.id == intent_id

    assert {:ok, %{"default" => %{pending_count: 0, processing_count: 0}}} = ClusterIntents.inspect(:queues)

    assert {:ok, before_restart} = ClusterIntents.replay(:outbox, limit: 100)
    before_facts = Enum.map(before_restart, &{&1.type, &1.subject})

    assert [
             {"intent.enqueued", ^intent_id},
             {"intent.started", ^intent_id},
             {"intent.completed", ^intent_id}
           ] = before_facts

    IntentLedger.FakeRepo.snapshot!()
    |> IntentLedger.FakeRepo.restart!()

    assert {:ok, after_restart} = ClusterIntents.replay(:outbox, limit: 100)
    assert Enum.map(after_restart, &{&1.type, &1.subject}) == before_facts

    assert {:ok, [replayed_view]} = ClusterIntents.inspect(:intents, status: :completed)
    assert replayed_view.id == intent_id
  end

  defp node_b_execute do
    queue_root = Internal.root_keyspace(ClusterIntents.JobQueue)
    [item] = Store.peek(IntentLedger.FakeRepo, queue_root, "default", limit: 1)

    assert item.topic == "invoice.multi_node"
    assert :erlang.binary_to_term(item.payload) == %{ledger: ClusterIntents, intent_id: item.id}

    assert {:ok, lease} =
             Store.obtain_lease(IntentLedger.FakeRepo, queue_root, item, "node-b", 30_000)

    result = Worker.execute(item, %{"invoice.multi_node" => ProcessInvoice})

    assert :ok =
             IntentLedger.FakeRepo.transact(fn ->
               queue_result = Store.complete(IntentLedger.FakeRepo, queue_root, lease)

               IntentLedger.JobQueueHook.apply(
                 IntentLedger.FakeRepo,
                 queue_root,
                 lease,
                 :complete,
                 result,
                 queue_result,
                 ClusterIntents
               )
             end)

    result
  end
end
