defmodule IntentLedger.PeerNodesTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :multi_node

  alias IntentLedger.PeerNodes

  test "starts peers with project code paths and tears them down" do
    peers = PeerNodes.start_peers!(2, prefix: :intent_ledger_peer_start)

    assert length(peers) == 2
    assert PeerNodes.nodes(peers) == Enum.uniq(PeerNodes.nodes(peers))

    assert Enum.all?(peers, fn peer ->
             PeerNodes.call(peer, Node, :alive?, []) and
               PeerNodes.call(peer, Code, :ensure_loaded?, [IntentLedger])
           end)

    PeerNodes.stop_peers(peers)
    refute Enum.any?(peers, &Process.alive?(&1.pid))
  end

  test "connects peers to each other over loopback distribution" do
    [first, second] = peers = PeerNodes.start_peers!(2, prefix: :intent_ledger_peer_connect)

    assert :ok = PeerNodes.connect_all(peers)

    assert second.node in PeerNodes.call(first, Node, :list, [])
    assert first.node in PeerNodes.call(second, Node, :list, [])
  end
end
