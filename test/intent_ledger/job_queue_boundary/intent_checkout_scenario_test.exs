defmodule IntentLedger.IntentCheckoutScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :bedrock
  @moduletag :job_queue_boundary

  defmodule ReserveInventory do
    use IntentLedger.Handler,
      topic: "inventory.reserve",
      payload_schema: Zoi.map(),
      result_schema: Zoi.map()

    @impl true
    def handle(%{order_id: order_id, workflow_pid: workflow_pid}, ctx) do
      send(workflow_pid, {:reserved_inventory, ctx.intent.id, order_id, ctx.attempt})
      {:ok, %{reserved: true, order_id: order_id}}
    end
  end

  defmodule WorkflowIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{
        "inventory.reserve" => [handler: ReserveInventory, queue: "workflow"]
      }
  end

  defmodule WorkflowSystem do
    alias Bedrock.JobQueue.Consumer.Worker
    alias Bedrock.JobQueue.Internal
    alias Bedrock.JobQueue.Store

    def checkout_next(ledger, queue_id, opts \\ []) do
      holder = Keyword.get(opts, :holder, "workflow-system")
      lease_ms = Keyword.get(opts, :lease_ms, 30_000)
      repo = ledger.__intent_ledger__().repo
      queue_root = Internal.root_keyspace(ledger.__intent_ledger__().job_queue)

      with {:ok, item, lease, pointer} <-
             repo.transact(fn ->
               case Store.peek(repo, queue_root, queue_id, limit: 1) do
                 [] ->
                   {:error, :empty}

                 [item] ->
                   with {:ok, pointer} <- decode_pointer(item, ledger),
                        {:ok, lease} <- Store.obtain_lease(repo, queue_root, item, holder, lease_ms) do
                     {:ok, item, lease, pointer}
                   end
               end
             end),
           {:ok, intent} <- ledger.fetch(pointer.intent_id) do
        {:ok,
         %{
           intent: intent,
           item: item,
           lease: lease,
           ledger: ledger,
           pointer: pointer,
           repo: repo,
           queue_root: queue_root,
           workers: ledger.__intent_ledger__().handlers
         }}
      end
    end

    def perform(%{item: item, workers: workers}), do: Worker.execute(item, workers)

    def commit(%{lease: lease, ledger: ledger, queue_root: queue_root, repo: repo}, result) do
      action = action_for(result)

      repo.transact(fn ->
        queue_result = apply_queue_action(repo, queue_root, lease, action)

        IntentLedger.Runtime.QueueLifecycle.apply(
          repo,
          queue_root,
          lease,
          action,
          result,
          queue_result,
          ledger
        )
      end)
    end

    defp decode_pointer(item, ledger) do
      case :erlang.binary_to_term(item.payload) do
        %{ledger: ^ledger, intent_id: intent_id} -> {:ok, %{ledger: ledger, intent_id: intent_id}}
        other -> {:error, {:unexpected_queue_pointer, other}}
      end
    end

    defp action_for(:ok), do: :complete
    defp action_for({:ok, _result}), do: :complete
    defp action_for({:discard, _reason}), do: :complete
    defp action_for({:error, _reason}), do: :requeue
    defp action_for({:snooze, delay_ms}), do: {:snooze, delay_ms}

    defp apply_queue_action(repo, queue_root, lease, :complete), do: Store.complete(repo, queue_root, lease)

    defp apply_queue_action(repo, queue_root, lease, :requeue),
      do: Store.requeue(repo, queue_root, lease, backoff_fn: fn _attempt -> 60_000 end)

    defp apply_queue_action(repo, queue_root, lease, {:snooze, delay_ms}) do
      Store.requeue(repo, queue_root, lease,
        base_delay: delay_ms,
        max_delay: delay_ms
      )
    end
  end

  setup do
    IntentLedger.RealBedrock.setup!()
  end

  @tag :intent_checkout
  test "workflow system checks out an Intent pointer and commits completed work" do
    payload = %{order_id: "order-123", workflow_pid: self()}

    assert {:ok, intent} =
             WorkflowIntents.enqueue("inventory.reserve", payload,
               key: "inventory:reserve:order-123",
               priority: 10
             )

    assert intent.status == :enqueued
    assert {:ok, %{"workflow" => %{pending_count: 1, processing_count: 0}}} = WorkflowIntents.stats()

    assert {:ok, checkout} = WorkflowSystem.checkout_next(WorkflowIntents, "workflow", holder: "workflow-a")

    assert checkout.intent.id == intent.id
    assert checkout.intent.status == :enqueued
    assert checkout.intent.payload == payload
    assert checkout.item.id == intent.id
    assert checkout.item.topic == "inventory.reserve"
    assert checkout.pointer == %{ledger: WorkflowIntents, intent_id: intent.id}

    assert {:ok, %{"workflow" => %{pending_count: 0, processing_count: 1}}} = WorkflowIntents.stats()

    assert {:ok, %{reserved: true, order_id: "order-123"}} =
             result =
             WorkflowSystem.perform(checkout)

    assert_receive {:reserved_inventory, intent_id, "order-123", 1}
    assert intent_id == intent.id

    assert {:ok, started} = WorkflowIntents.fetch(intent.id)
    assert started.status == :started
    assert started.attempt == 1

    assert :ok = WorkflowSystem.commit(checkout, result)

    assert {:ok, completed} = WorkflowIntents.fetch(intent.id)
    assert completed.status == :completed
    assert completed.result == %{reserved: true, order_id: "order-123"}

    assert {:ok, %{"workflow" => %{pending_count: 0, processing_count: 0}}} = WorkflowIntents.stats()
    assert {:ok, history} = WorkflowIntents.history(intent.id)
    assert Enum.map(history, & &1.type) == ["intent.enqueued", "intent.started", "intent.completed"]

    assert {:ok, outbox} = WorkflowIntents.read_outbox("workflow-demo", limit: 10)
    assert Enum.map(outbox.entries, & &1.signal.type) == ["intent.enqueued", "intent.started", "intent.completed"]
    assert {:ok, %{cursor: 3}} = WorkflowIntents.ack_outbox("workflow-demo", outbox.next_cursor)
  end
end
