defmodule IntentLedger.BedrockMultiNodeLifecycleReplayTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :bedrock
  @moduletag :multi_node
  @moduletag :bedrock_multi_node

  alias IntentLedger.{BedrockClusterSetup, CrossNodeStore}

  @now ~U[2026-01-01 00:00:00Z]
  @lease_until ~U[2026-01-01 00:00:30Z]

  test "lifecycle replay rebuilds a dropped projection from another node" do
    cluster = BedrockClusterSetup.start_cluster!(3, peer_opts: [prefix: :intent_ledger_lifecycle_replay])
    store = CrossNodeStore.start!(cluster)
    [node_a, node_b, node_c] = cluster.peers

    assert {:ok, submitted} =
             CrossNodeStore.submit(node_a, store,
               intent_id: "int_lifecycle_replay",
               key: "job:lifecycle-replay",
               now: @now,
               command_id: "cmd:submit:lifecycle-replay"
             )

    assert {:ok, claimed} =
             CrossNodeStore.claim(node_b, store,
               owner_id: "node-b",
               now: @now,
               lease_until: @lease_until,
               command_id: "cmd:claim:lifecycle-replay"
             )

    assert {:ok, completed} =
             CrossNodeStore.complete(node_c, store,
               intent_id: submitted.result.intent_id,
               claim_id: claimed.result.claim_id,
               token: claimed.result.token,
               result: %{delivered: true, node: "node-c"},
               now: @now,
               command_id: "cmd:complete:lifecycle-replay"
             )

    assert completed.result.status == :completed

    assert {:ok, node_a_stream} = CrossNodeStore.read_stream(node_a, store, "intent:int_lifecycle_replay")
    assert {:ok, node_c_stream} = CrossNodeStore.read_stream(node_c, store, "intent:int_lifecycle_replay")
    assert node_a_stream == node_c_stream

    partial_projection =
      node_a_stream.signals
      |> Enum.take(1)
      |> rebuild_projection()

    assert partial_projection.status == :submitted
    assert partial_projection.version == 1

    caught_up_projection =
      node_c_stream.signals
      |> Enum.drop(partial_projection.version)
      |> Enum.reduce(partial_projection, &apply_signal/2)

    rebuilt_projection =
      node_c_stream.signals
      |> rebuild_projection()

    assert caught_up_projection == rebuilt_projection

    assert rebuilt_projection == %{
             intent_id: "int_lifecycle_replay",
             version: 3,
             status: :completed,
             transitions: [:submitted, :claimed, :completed],
             claim_id: claimed.result.claim_id,
             owner_id: "node-b",
             result: %{delivered: true, node: "node-c"}
           }
  end

  defp rebuild_projection(signals) do
    Enum.reduce(signals, new_projection(), &apply_signal/2)
  end

  defp new_projection do
    %{
      intent_id: nil,
      version: 0,
      status: nil,
      transitions: [],
      claim_id: nil,
      owner_id: nil,
      result: nil
    }
  end

  defp apply_signal(%{type: "intent_ledger.intent.submitted", subject: intent_id}, projection) do
    advance(projection,
      intent_id: intent_id,
      status: :submitted,
      transitions: projection.transitions ++ [:submitted]
    )
  end

  defp apply_signal(%{type: "intent_ledger.intent.claimed", metadata: metadata}, projection) do
    advance(projection,
      status: :claimed,
      transitions: projection.transitions ++ [:claimed],
      claim_id: Map.fetch!(metadata, :claim_id),
      owner_id: Map.fetch!(metadata, :owner_id)
    )
  end

  defp apply_signal(%{type: "intent_ledger.intent.completed", metadata: metadata}, projection) do
    advance(projection,
      status: :completed,
      transitions: projection.transitions ++ [:completed],
      result: Map.fetch!(metadata, :result)
    )
  end

  defp advance(projection, attrs) do
    projection
    |> Map.merge(Map.new(attrs))
    |> Map.update!(:version, &(&1 + 1))
  end
end
