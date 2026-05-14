defmodule IntentLedger.StoreBedrockValueTest do
  use ExUnit.Case, async: true

  @moduletag :integration
  @moduletag :bedrock

  alias IntentLedger.{Claim, Intent, IntentState, Signal}
  alias IntentLedger.Store.Bedrock.Value

  @ledger MyApp.IntentLedger
  @now ~U[2026-01-01 00:00:00Z]

  test "round trips typed Store V1 values" do
    {:ok, intent} =
      Intent.new(%{
        id: "int_1",
        key: "job:1",
        kind: "job.run",
        queue: "default",
        visible_at: @now,
        payload: %{work: true}
      })

    state = IntentState.new(intent, @now)
    signal = Signal.lifecycle(:intent_available, @ledger, intent.id, %{visible_at: @now})
    claim = %{intent_id: intent.id, token_hash: Claim.token_hash("tok_1"), lease_until: @now}
    lease = %{queue: "default", shard: 0, owner_id: "node-a", lease_until: @now}
    command = %{signature: {:submit, %{key: intent.key}}, result: %{intent_id: intent.id}}
    outbox = %{key: "out_1", sequence: 1, stream: "intent:int_1", signal: signal, acked_at: nil}

    assert {:ok, ^intent} = intent |> Value.pack_intent() |> Value.unpack_intent()
    assert {:ok, ^state} = state |> Value.pack_state() |> Value.unpack_state()
    assert {:ok, ^signal} = signal |> Value.pack_signal() |> Value.unpack_signal()
    assert {:ok, ^claim} = claim |> Value.pack_claim() |> Value.unpack_claim()
    assert {:ok, ^lease} = lease |> Value.pack_shard_lease() |> Value.unpack_shard_lease()
    assert {:ok, ^command} = command |> Value.pack_command() |> Value.unpack_command()
    assert {:ok, ^outbox} = outbox |> Value.pack_outbox() |> Value.unpack_outbox()
  end

  test "uses deterministic versioned envelopes" do
    value = %{queue: "default", shard: 0, owner_id: "node-a", lease_until: @now}

    encoded = Value.pack_shard_lease(value)

    assert encoded == Value.pack_shard_lease(value)
    assert {:ok, {:shard_lease, ^value}} = Value.unpack(encoded)
    assert Value.schema_version() == 1
    assert :shard_lease in Value.types()
  end

  test "rejects type mismatches before returning values" do
    encoded = Value.pack_command(%{signature: {:submit, %{}}, result: :ok})

    assert {:error, {:unexpected_bedrock_value_type, %{expected: :outbox, actual: :command}}} =
             Value.unpack_outbox(encoded)
  end

  test "rejects malformed and unsupported value envelopes" do
    assert {:error, {:invalid_bedrock_value, :malformed_binary}} = Value.unpack("not external term")

    unsupported =
      %{schema_version: 999, type: "intent", value: %{}}
      |> :erlang.term_to_binary([:deterministic])

    assert {:error, {:unsupported_bedrock_value_version, 999}} = Value.unpack(unsupported)

    unknown_type =
      %{schema_version: Value.schema_version(), type: "unknown", value: %{}}
      |> :erlang.term_to_binary([:deterministic])

    assert {:error, {:invalid_bedrock_value, {:unknown_type, "unknown"}}} = Value.unpack(unknown_type)
  end

  test "raises for unsupported typed values at encode time" do
    assert_raise ArgumentError, ~r/invalid intent Bedrock value/, fn ->
      Value.pack(:intent, %{id: "int_1"})
    end
  end
end
