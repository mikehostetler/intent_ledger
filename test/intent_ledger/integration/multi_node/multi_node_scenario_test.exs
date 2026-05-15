defmodule IntentLedger.MultiNodeScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :multi_node
  @moduletag timeout: 45_000

  alias IntentLedger.PeerFixtures.Runtime

  setup do
    IntentLedger.RealBedrock.setup!()

    peers =
      [:intent_ledger_node_a, :intent_ledger_node_b, :intent_ledger_node_c]
      |> Enum.map(&IntentLedger.RealBedrock.start_peer!/1)

    on_exit(fn ->
      Enum.each(peers, fn {peer, _node} ->
        try do
          :peer.stop(peer)
        catch
          :exit, _reason -> :ok
        end
      end)
    end)

    {:ok, peers: peers}
  end

  test "peer node A enqueues, peer node B executes, and peer node C inspects after B restarts", %{
    peers: [{_peer_a, node_a}, {_peer_b, node_b}, {_peer_c, node_c}]
  } do
    coordinator_node = Node.self()

    assert {:ok, state_a} = rpc(node_a, Runtime, :start_cluster!, [coordinator_node])
    assert {:ok, state_b} = rpc(node_b, Runtime, :start_cluster_and_runtime!, [[coordinator_node: coordinator_node]])
    assert {:ok, state_c} = rpc(node_c, Runtime, :start_cluster!, [coordinator_node])

    assert {:ok, intent} =
             rpc(node_a, Runtime, :enqueue_from_peer, [
               "peer.invoice",
               %{invoice_id: "multi-node-1", node_label: :node_b, test_pid: self()},
               [key: "peer:invoice:multi-node-1"]
             ])

    intent_id = intent.id
    assert_receive {:peer_handled, :node_b, ^node_b, ^intent_id, 1}, 10_000

    assert_eventually(fn ->
      case rpc(node_c, Runtime, :fetch, [intent_id]) do
        {:ok, completed} ->
          completed.status == :completed and completed.result == %{handled_by: :node_b, node: node_b}

        _other ->
          false
      end
    end)

    assert {:ok, before_restart} = rpc(node_c, Runtime, :replay, [:outbox, [limit: 100]])
    assert Enum.map(before_restart, & &1.type) == ["intent.enqueued", "intent.started", "intent.completed"]

    assert :ok = rpc(node_b, Runtime, :stop!, [state_b])

    assert {:ok, restarted_b} =
             rpc(node_b, Runtime, :start_cluster_and_runtime!, [[coordinator_node: coordinator_node]])

    assert {:ok, second} =
             rpc(node_a, Runtime, :enqueue_from_peer, [
               "peer.invoice",
               %{invoice_id: "multi-node-2", node_label: :node_b, test_pid: self()},
               [key: "peer:invoice:multi-node-2"]
             ])

    second_id = second.id
    assert_receive {:peer_handled, :node_b, ^node_b, ^second_id, 1}, 10_000

    assert_eventually(fn ->
      case rpc(node_c, Runtime, :stats, [[queue: "default"]]) do
        {:ok, %{"default" => %{pending_count: 0, processing_count: 0}}} -> true
        _other -> false
      end
    end)

    assert {:ok, after_restart} = rpc(node_c, Runtime, :replay, [:outbox, [limit: 100]])

    assert Enum.map(after_restart, & &1.type) == [
             "intent.enqueued",
             "intent.started",
             "intent.completed",
             "intent.enqueued",
             "intent.started",
             "intent.completed"
           ]

    rpc(node_b, Runtime, :stop!, [restarted_b])
    rpc(node_a, Runtime, :stop!, [state_a])
    rpc(node_c, Runtime, :stop!, [state_c])
  end

  defp rpc(node, module, function, args) do
    case :rpc.call(node, module, function, args, 15_000) do
      {:badrpc, reason} -> flunk("RPC #{inspect(module)}.#{function}/#{length(args)} failed: #{inspect(reason)}")
      result -> result
    end
  end

  defp assert_eventually(fun, timeout \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(fun, deadline)
  end

  defp do_assert_eventually(fun, deadline) do
    if fun.() do
      assert true
    else
      if System.monotonic_time(:millisecond) >= deadline do
        assert fun.()
      else
        Process.sleep(50)
        do_assert_eventually(fun, deadline)
      end
    end
  end
end
