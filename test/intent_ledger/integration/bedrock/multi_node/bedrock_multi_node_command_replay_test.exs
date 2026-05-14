defmodule IntentLedger.BedrockMultiNodeCommandReplayTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :bedrock
  @moduletag :multi_node
  @moduletag :bedrock_multi_node

  alias IntentLedger.Store.{Conflict, Outbox}
  alias IntentLedger.{BedrockClusterSetup, CrossNodeStore}

  @now ~U[2026-01-01 00:00:00Z]

  test "duplicate command replay returns the original result across nodes" do
    cluster = BedrockClusterSetup.start_cluster!(3, peer_opts: [prefix: :intent_ledger_command_replay])
    store = CrossNodeStore.start!(cluster)
    [node_a, node_b, node_c] = cluster.peers

    assert {:ok, submitted} =
             CrossNodeStore.submit(node_a, store,
               intent_id: "int_command_replay",
               key: "job:command-replay",
               now: @now,
               command_id: "cmd:submit:command-replay"
             )

    assert {:ok, replayed} =
             CrossNodeStore.replay(
               node_b,
               store,
               :submit,
               submitted.command_id,
               submitted.result.command
             )

    assert replayed.replayed
    assert replayed.replay_of == submitted.command_id
    assert replayed.result == submitted.result

    assert {:error, %Conflict{type: :command_conflict}} =
             CrossNodeStore.replay(
               node_c,
               store,
               :submit,
               submitted.command_id,
               %{intent_id: "int_command_replay", key: "different"}
             )

    assert {:ok, stream} = CrossNodeStore.read_stream(node_c, store, "intent:int_command_replay")
    assert stream.version == 1
    assert Enum.map(stream.signals, & &1.type) == ["intent_ledger.intent.submitted"]

    assert {:ok, outbox_entries} = CrossNodeStore.outbox(node_b, store, Outbox.replay(cursor: 0, limit: 10))
    assert Enum.map(outbox_entries, & &1.key) == ["out:cmd:submit:command-replay"]
  end
end
