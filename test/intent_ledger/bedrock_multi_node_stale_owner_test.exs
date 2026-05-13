defmodule IntentLedger.BedrockMultiNodeStaleOwnerTest do
  use ExUnit.Case, async: false

  alias IntentLedger.Store.Conflict
  alias IntentLedger.{BedrockClusterSetup, CrossNodeStore}

  @now ~U[2026-01-01 00:00:00Z]
  @lease_until ~U[2026-01-01 00:00:30Z]
  @after_release ~U[2026-01-01 00:00:01Z]
  @after_expiry ~U[2026-01-01 00:00:31Z]

  @tag :bedrock_multi_node
  test "stale claim owner is rejected after release and after lease expiry" do
    cluster = BedrockClusterSetup.start_cluster!(3, peer_opts: [prefix: :intent_ledger_stale_owner])
    store = CrossNodeStore.start!(cluster)
    [node_a, node_b, node_c] = cluster.peers

    assert {:ok, _submitted} =
             CrossNodeStore.submit(node_a, store,
               intent_id: "int_release_stale",
               key: "job:release-stale",
               now: @now,
               command_id: "cmd:submit:release-stale"
             )

    assert {:ok, released_claim} =
             CrossNodeStore.claim(node_b, store,
               owner_id: "node-b",
               now: @now,
               lease_until: @lease_until,
               command_id: "cmd:claim:release-stale"
             )

    assert {:ok, released} =
             CrossNodeStore.release(node_c, store,
               intent_id: released_claim.result.intent_id,
               claim_id: released_claim.result.claim_id,
               token: released_claim.result.token,
               now: @after_release,
               command_id: "cmd:release:release-stale"
             )

    assert released.result.status == :available

    assert {:error, %Conflict{type: :claim_fence}} =
             CrossNodeStore.complete(node_b, store,
               intent_id: released_claim.result.intent_id,
               claim_id: released_claim.result.claim_id,
               token: released_claim.result.token,
               result: :too_late,
               now: @after_release,
               command_id: "cmd:complete:after-release"
             )

    assert {:ok, _submitted} =
             CrossNodeStore.submit(node_a, store,
               intent_id: "int_expired_stale",
               key: "job:expired-stale",
               now: @now,
               command_id: "cmd:submit:expired-stale"
             )

    assert {:ok, expired_claim} =
             CrossNodeStore.claim(node_b, store,
               owner_id: "node-b",
               now: @now,
               lease_until: @lease_until,
               command_id: "cmd:claim:expired-stale"
             )

    assert {:error, %Conflict{type: :claim_fence}} =
             CrossNodeStore.fail(node_c, store,
               intent_id: expired_claim.result.intent_id,
               claim_id: expired_claim.result.claim_id,
               token: expired_claim.result.token,
               error: :expired_owner,
               now: @after_expiry,
               command_id: "cmd:fail:after-expiry"
             )
  end
end
