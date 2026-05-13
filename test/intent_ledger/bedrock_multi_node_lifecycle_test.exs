defmodule IntentLedger.BedrockMultiNodeLifecycleTest do
  use ExUnit.Case, async: false

  alias IntentLedger.Store.Outbox
  alias IntentLedger.{BedrockClusterSetup, CrossNodeStore}

  @now ~U[2026-01-01 00:00:00Z]
  @lease_until ~U[2026-01-01 00:00:30Z]

  @tag :bedrock_multi_node
  test "node A submits node B claims and node C completes one intent" do
    cluster = BedrockClusterSetup.start_cluster!(3, peer_opts: [prefix: :intent_ledger_lifecycle])
    store = CrossNodeStore.start!(cluster)
    [node_a, node_b, node_c] = cluster.peers

    assert {:ok, submitted} =
             CrossNodeStore.submit(node_a, store,
               intent_id: "int_multi_lifecycle",
               key: "job:multi-lifecycle",
               now: @now,
               command_id: "cmd:submit:multi-lifecycle"
             )

    assert submitted.result.status == :submitted

    assert {:ok, claimed} =
             CrossNodeStore.claim(node_b, store,
               owner_id: "node-b",
               now: @now,
               lease_until: @lease_until,
               command_id: "cmd:claim:multi-lifecycle"
             )

    assert claimed.result.intent_id == submitted.result.intent_id
    assert claimed.result.status == :claimed

    assert {:ok, completed} =
             CrossNodeStore.complete(node_c, store,
               intent_id: claimed.result.intent_id,
               claim_id: claimed.result.claim_id,
               token: claimed.result.token,
               result: %{delivered: true},
               now: @now,
               command_id: "cmd:complete:multi-lifecycle"
             )

    assert completed.result.status == :completed

    assert {:ok, stream} = CrossNodeStore.read_stream(node_a, store, "intent:int_multi_lifecycle")
    assert stream.version == 3

    assert Enum.map(stream.signals, & &1.type) == [
             "intent_ledger.intent.submitted",
             "intent_ledger.intent.claimed",
             "intent_ledger.intent.completed"
           ]

    assert {:ok, outbox_entries} = CrossNodeStore.outbox(node_b, store, Outbox.replay(cursor: 0, limit: 10))
    assert Enum.map(outbox_entries, & &1.stream) == ["intent:int_multi_lifecycle", "intent:int_multi_lifecycle"]

    assert Enum.map(outbox_entries, & &1.signal.type) == [
             "intent_ledger.intent.submitted",
             "intent_ledger.intent.completed"
           ]
  end
end
