defmodule IntentLedger.BedrockMultiNodeShardOwnerDeathTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :bedrock
  @moduletag :multi_node
  @moduletag :bedrock_multi_node

  alias IntentLedger.Store.Conflict
  alias IntentLedger.{BedrockClusterSetup, CrossNodeStore, PeerNodes}

  @now ~U[2026-01-01 00:00:00Z]
  @lease_until ~U[2026-01-01 00:00:10Z]
  @after_expiry ~U[2026-01-01 00:00:11Z]
  @takeover_until ~U[2026-01-01 00:00:41Z]

  test "a surviving node takes over an expired shard lease after owner death" do
    cluster = BedrockClusterSetup.start_cluster!(3, peer_opts: [prefix: :intent_ledger_shard_owner_death])
    store = CrossNodeStore.start!(cluster)
    [node_a, node_b, node_c] = cluster.peers

    assert {:ok, acquired} =
             CrossNodeStore.shard_lease(node_b, store, :acquire,
               queue: "default",
               shard: 0,
               owner_id: "node-b",
               now: @now,
               lease_until: @lease_until
             )

    assert acquired.owner_id == "node-b"
    assert acquired.lease_until == @lease_until

    PeerNodes.stop_peer(node_b)

    assert {:ok, takeover} =
             CrossNodeStore.shard_lease(node_c, store, :takeover,
               queue: "default",
               shard: 0,
               owner_id: "node-c",
               now: @after_expiry,
               lease_until: @takeover_until
             )

    assert takeover.owner_id == "node-c"
    assert takeover.lease_until == @takeover_until

    assert {:error, %Conflict{type: :shard_lease}} =
             CrossNodeStore.shard_lease(node_a, store, :renew,
               queue: "default",
               shard: 0,
               owner_id: "node-b",
               now: @after_expiry,
               lease_until: DateTime.add(@after_expiry, 60, :second)
             )
  end
end
