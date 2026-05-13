defmodule IntentLedger.BedrockMultiNodeClaimRaceTest do
  use ExUnit.Case, async: false

  alias IntentLedger.Store.Conflict
  alias IntentLedger.{BedrockClusterSetup, CrossNodeStore}

  @now ~U[2026-01-01 00:00:00Z]
  @lease_until ~U[2026-01-01 00:00:30Z]

  @tag :bedrock_multi_node
  test "only one of three nodes can claim the same available intent" do
    cluster = BedrockClusterSetup.start_cluster!(3, peer_opts: [prefix: :intent_ledger_claim_race])
    store = CrossNodeStore.start!(cluster)
    [node_a, node_b, node_c] = cluster.peers

    assert {:ok, _submitted} =
             CrossNodeStore.submit(node_a, store,
               intent_id: "int_claim_race",
               key: "job:claim-race",
               now: @now,
               command_id: "cmd:submit:claim-race"
             )

    race_results =
      [node_a, node_b, node_c]
      |> Enum.with_index(1)
      |> Enum.map(fn {peer, index} ->
        Task.async(fn ->
          CrossNodeStore.claim_intent(peer, store, "int_claim_race",
            owner_id: "node-#{index}",
            claim_id: "clm_claim_race_#{index}",
            token: "tok_claim_race_#{index}",
            command_id: "cmd:claim:claim-race:#{index}",
            now: @now,
            lease_until: @lease_until
          )
        end)
      end)
      |> Task.await_many(15_000)

    winners = Enum.filter(race_results, &match?({:ok, _commit}, &1))
    losers = race_results -- winners

    assert [{:ok, winner}] = winners
    assert winner.result.status == :claimed
    assert winner.result.intent_id == "int_claim_race"

    assert length(losers) == 2

    assert Enum.all?(losers, fn
             {:error, %Conflict{type: type}} when type in [:stream_version, :intent_status] -> true
             _other -> false
           end)

    assert :empty = CrossNodeStore.claim(node_c, store, owner_id: "node-c", now: @now)

    assert {:ok, stream} = CrossNodeStore.read_stream(node_b, store, "intent:int_claim_race")
    assert stream.version == 2

    assert Enum.map(stream.signals, & &1.type) == [
             "intent_ledger.intent.submitted",
             "intent_ledger.intent.claimed"
           ]
  end
end
