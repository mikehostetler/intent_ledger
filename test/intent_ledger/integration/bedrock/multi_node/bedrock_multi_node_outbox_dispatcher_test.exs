defmodule IntentLedger.BedrockMultiNodeOutboxDispatcherTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :bedrock
  @moduletag :multi_node
  @moduletag :bedrock_multi_node

  alias IntentLedger.Store.Outbox
  alias IntentLedger.{BedrockClusterSetup, CrossNodeStore}

  @now ~U[2026-01-01 00:00:00Z]
  @lease_until ~U[2026-01-01 00:00:30Z]
  @acked_at ~U[2026-01-01 00:00:45Z]

  test "interrupted outbox dispatch resumes from unacked entries on another node" do
    cluster = BedrockClusterSetup.start_cluster!(3, peer_opts: [prefix: :intent_ledger_outbox_dispatch])
    store = CrossNodeStore.start!(cluster)
    [node_a, node_b, node_c] = cluster.peers

    assert {:ok, submitted} =
             CrossNodeStore.submit(node_a, store,
               intent_id: "int_outbox_dispatch",
               key: "job:outbox-dispatch",
               now: @now,
               command_id: "cmd:submit:outbox-dispatch"
             )

    assert {:ok, claimed} =
             CrossNodeStore.claim(node_b, store,
               owner_id: "node-b",
               now: @now,
               lease_until: @lease_until,
               command_id: "cmd:claim:outbox-dispatch"
             )

    assert {:ok, completed} =
             CrossNodeStore.complete(node_c, store,
               intent_id: submitted.result.intent_id,
               claim_id: claimed.result.claim_id,
               token: claimed.result.token,
               result: %{delivered: true},
               now: @now,
               command_id: "cmd:complete:outbox-dispatch"
             )

    assert {:ok, [submitted_entry, completed_entry]} =
             CrossNodeStore.outbox(node_a, store, Outbox.read("dispatcher", cursor: 0, limit: 10))

    assert Enum.map([submitted_entry, completed_entry], & &1.key) == [
             "out:#{submitted.command_id}",
             "out:#{completed.command_id}"
           ]

    assert {:ok, acked_submitted} =
             CrossNodeStore.outbox(
               node_b,
               store,
               Outbox.ack(submitted_entry.key, "dispatcher", metadata: %{acked_at: @acked_at})
             )

    assert acked_submitted.acked_at == @acked_at
    assert acked_submitted.consumer == "dispatcher"

    assert {:ok, [resumed_entry]} =
             CrossNodeStore.outbox(node_c, store, Outbox.read("dispatcher", cursor: 0, limit: 10))

    assert resumed_entry.key == completed_entry.key
    assert resumed_entry.sequence == completed_entry.sequence
    assert resumed_entry.acked_at == nil

    assert {:ok, acked_completed} =
             CrossNodeStore.outbox(
               node_c,
               store,
               Outbox.ack(resumed_entry.key, "dispatcher", metadata: %{acked_at: @acked_at})
             )

    assert acked_completed.acked_at == @acked_at

    assert {:ok, []} = CrossNodeStore.outbox(node_a, store, Outbox.read("dispatcher", cursor: 0, limit: 10))

    assert {:ok, replayed} = CrossNodeStore.outbox(node_b, store, Outbox.replay(cursor: 0, limit: 10))
    assert Enum.map(replayed, & &1.key) == [submitted_entry.key, completed_entry.key]
    assert Enum.all?(replayed, &(&1.acked_at == @acked_at))
  end
end
