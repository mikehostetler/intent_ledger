defmodule IntentLedger.BedrockScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :bedrock
  @moduletag :bedrock_scenario

  alias IntentLedger.Store.Outbox
  alias IntentLedger.{BedrockClusterSetup, CrossNodeStore}

  @now ~U[2026-01-01 00:00:00Z]
  @intent_id "int_bedrock_scenario_submit"
  @command_id "cmd:scenario:bedrock:submit"
  @stream "intent:#{@intent_id}"

  test "single Bedrock-backed node commits and replays one submitted intent" do
    cluster = BedrockClusterSetup.start_cluster!(1, peer_opts: [prefix: :intent_ledger_bedrock_scenario])
    store = CrossNodeStore.start!(cluster)
    [node] = cluster.peers

    assert {:ok, submitted} =
             CrossNodeStore.submit(node, store,
               intent_id: @intent_id,
               key: "job:bedrock-scenario-submit",
               now: @now,
               command_id: @command_id
             )

    assert submitted.command_id == @command_id

    assert submitted.result == %{
             intent_id: @intent_id,
             status: :submitted,
             command_id: @command_id,
             command: %{intent_id: @intent_id, key: "job:bedrock-scenario-submit"}
           }

    assert {:ok, replayed} =
             CrossNodeStore.replay(
               node,
               store,
               :submit,
               @command_id,
               submitted.result.command
             )

    assert replayed.replayed
    assert replayed.replay_of == @command_id
    assert replayed.result == submitted.result

    assert {:ok, stream} = CrossNodeStore.read_stream(node, store, @stream)
    assert stream.version == 1

    assert [%{type: "intent_ledger.intent.submitted", subject: @intent_id} = signal] = stream.signals
    assert signal.metadata == %{}

    assert {:ok, outbox_entries} = CrossNodeStore.outbox(node, store, Outbox.replay(cursor: 0, limit: 10))

    assert [
             %{
               key: "out:" <> @command_id,
               stream: @stream,
               signal: ^signal
             }
           ] = outbox_entries
  end
end
