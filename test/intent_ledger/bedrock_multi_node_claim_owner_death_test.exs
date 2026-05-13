defmodule IntentLedger.BedrockMultiNodeClaimOwnerDeathTest do
  use ExUnit.Case, async: false

  alias IntentLedger.{BedrockClusterSetup, CrossNodeStore, PeerNodes}

  @now ~U[2026-01-01 00:00:00Z]
  @lease_until ~U[2026-01-01 00:00:10Z]
  @after_expiry ~U[2026-01-01 00:00:11Z]

  @tag :bedrock_multi_node
  test "a surviving node recovers work after the claim owner node dies" do
    cluster = BedrockClusterSetup.start_cluster!(3, peer_opts: [prefix: :intent_ledger_owner_death])
    store = CrossNodeStore.start!(cluster)
    [node_a, node_b, node_c] = cluster.peers

    assert {:ok, _submitted} =
             CrossNodeStore.submit(node_a, store,
               intent_id: "int_owner_death",
               key: "job:owner-death",
               now: @now,
               command_id: "cmd:submit:owner-death"
             )

    assert {:ok, dead_owner_claim} =
             CrossNodeStore.claim(node_b, store,
               owner_id: "node-b",
               now: @now,
               lease_until: @lease_until,
               command_id: "cmd:claim:owner-death"
             )

    assert dead_owner_claim.result.status == :claimed

    PeerNodes.stop_peer(node_b)

    assert {:ok, recovered} =
             CrossNodeStore.recover(node_c, store,
               queue: "default",
               now: @after_expiry,
               command_id: "cmd:recover:owner-death"
             )

    assert recovered.result.count == 1
    assert recovered.result.intent_ids == ["int_owner_death"]

    assert {:ok, survivor_claim} =
             CrossNodeStore.claim(node_a, store,
               owner_id: "node-a",
               now: @after_expiry,
               lease_until: DateTime.add(@after_expiry, 30, :second),
               command_id: "cmd:claim:owner-death-survivor"
             )

    assert survivor_claim.result.intent_id == dead_owner_claim.result.intent_id
    assert survivor_claim.result.owner_id == "node-a"
    refute survivor_claim.result.claim_id == dead_owner_claim.result.claim_id
  end
end
