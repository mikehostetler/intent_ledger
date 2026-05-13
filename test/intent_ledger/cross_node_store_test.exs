defmodule IntentLedger.CrossNodeStoreTest do
  use ExUnit.Case, async: false

  alias IntentLedger.{BedrockClusterSetup, CrossNodeStore}

  @now ~U[2026-01-01 00:00:00Z]
  @lease_until ~U[2026-01-01 00:00:30Z]
  @expired_at ~U[2026-01-01 00:00:31Z]

  @tag :bedrock_cluster
  test "runs submit claim complete and replay helpers on separate nodes" do
    cluster = BedrockClusterSetup.start_cluster!(3, peer_opts: [prefix: :intent_ledger_cross_node])
    store = CrossNodeStore.start!(cluster)
    [node_a, node_b, node_c] = cluster.peers

    assert {:ok, submit} =
             CrossNodeStore.submit(node_a, store,
               intent_id: "int_cross_complete",
               key: "job:complete",
               now: @now
             )

    assert submit.result.status == :submitted

    assert {:ok, claim} =
             CrossNodeStore.claim(node_b, store,
               owner_id: "node-b",
               now: @now,
               lease_until: @lease_until,
               command_id: "cmd:claim:cross-complete"
             )

    assert claim.result.intent_id == "int_cross_complete"
    assert claim.result.status == :claimed

    assert {:ok, complete} =
             CrossNodeStore.complete(node_c, store,
               intent_id: claim.result.intent_id,
               claim_id: claim.result.claim_id,
               token: claim.result.token,
               result: %{ok: true},
               now: @now,
               command_id: "cmd:complete:cross-complete"
             )

    assert complete.result.status == :completed

    assert {:ok, replayed} =
             CrossNodeStore.replay(
               node_a,
               store,
               :complete,
               complete.command_id,
               complete.result.command
             )

    assert replayed.replayed
    assert replayed.result == complete.result
  end

  @tag :bedrock_cluster
  test "runs recover and fail helpers across nodes" do
    cluster = BedrockClusterSetup.start_cluster!(3, peer_opts: [prefix: :intent_ledger_cross_node_recover])
    store = CrossNodeStore.start!(cluster)
    [node_a, node_b, node_c] = cluster.peers

    assert {:ok, _submit} =
             CrossNodeStore.submit(node_a, store,
               intent_id: "int_cross_recover",
               key: "job:recover",
               now: @now
             )

    assert {:ok, expired_claim} =
             CrossNodeStore.claim(node_b, store,
               owner_id: "node-b",
               now: @now,
               lease_until: @lease_until,
               command_id: "cmd:claim:cross-recover"
             )

    assert {:ok, recovered} =
             CrossNodeStore.recover(node_c, store,
               queue: "default",
               now: @expired_at,
               command_id: "cmd:recover:cross-recover"
             )

    assert recovered.result.count == 1
    assert recovered.result.intent_ids == ["int_cross_recover"]

    assert {:ok, retry_claim} =
             CrossNodeStore.claim(node_a, store,
               owner_id: "node-a",
               now: @expired_at,
               lease_until: DateTime.add(@expired_at, 30, :second),
               command_id: "cmd:claim:cross-retry"
             )

    refute retry_claim.result.claim_id == expired_claim.result.claim_id

    assert {:ok, failed} =
             CrossNodeStore.fail(node_b, store,
               intent_id: retry_claim.result.intent_id,
               claim_id: retry_claim.result.claim_id,
               token: retry_claim.result.token,
               error: %{reason: "boom"},
               now: @expired_at,
               command_id: "cmd:fail:cross-retry"
             )

    assert failed.result.status == :failed
  end
end
