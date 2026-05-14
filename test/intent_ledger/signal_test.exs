defmodule IntentLedger.SignalTest do
  use ExUnit.Case, async: true

  alias IntentLedger.Signal

  @events [
    :intent_submitted,
    :intent_available,
    :intent_claimed,
    :intent_completed,
    :intent_failed,
    :intent_retry_scheduled,
    :intent_cancelled,
    :intent_marked_ambiguous,
    :intent_released,
    :claim_heartbeat,
    :claim_lease_expired
  ]

  test "catalogue defines versioned lifecycle signal schemas" do
    assert Signal.events() == @events

    for event <- @events do
      definition = Signal.fetch!(event)

      assert definition.event == event
      assert definition.type == Signal.type_for(event)
      assert definition.version == 1
      assert is_list(definition.required)
      assert is_list(definition.optional)
      assert Enum.all?(Signal.lineage_fields(), &(&1 in definition.optional))
      assert Signal.fetch!(definition.type) == definition
    end
  end

  test "lifecycle builder emits a versioned Jido signal" do
    signal =
      Signal.lifecycle(:intent_claimed, MyApp.IntentLedger, "intent:int_1", %{
        claim_id: "clm_1",
        owner_id: "worker-1",
        attempt: 1,
        lease_until: ~U[2026-01-01 00:00:00Z],
        correlation_id: "corr_1",
        depth: 2
      })

    assert %Jido.Signal{} = signal
    assert signal.type == "intent_ledger.intent.claimed"
    assert signal.source == "/intent_ledger/MyApp.IntentLedger"
    assert signal.subject == "intent:int_1"
    assert signal.datacontenttype == "application/json"
    assert signal.dataschema == "https://hexdocs.pm/intent_ledger/lifecycle/intent_claimed/v1.json"
    assert signal.data.schema_version == 1
    assert signal.data.lease_until == "2026-01-01T00:00:00Z"
    assert signal.data.correlation_id == "corr_1"
    assert signal.data.depth == 2
  end

  test "lifecycle builder rejects data missing required schema fields" do
    assert_raise ArgumentError, ~r/missing lifecycle signal field :claim_id/, fn ->
      Signal.lifecycle(:intent_completed, MyApp.IntentLedger, "intent:int_1", %{result: :ok})
    end
  end
end
