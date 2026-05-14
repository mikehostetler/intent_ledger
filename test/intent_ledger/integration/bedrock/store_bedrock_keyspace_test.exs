defmodule IntentLedger.StoreBedrockKeyspaceTest do
  use ExUnit.Case, async: true

  @moduletag :integration
  @moduletag :bedrock

  alias IntentLedger.Store.Bedrock.Keyspace

  @ledger MyApp.IntentLedger
  @other_ledger OtherApp.IntentLedger
  @now ~U[2026-01-01 00:00:00Z]

  test "embeds a schema version in ledger-scoped ranges" do
    assert Keyspace.schema_version() == 1

    intent_key = Keyspace.intent(@ledger, "int_1")

    assert Keyspace.contains?(Keyspace.ledger_range(@ledger), intent_key)
    assert Keyspace.contains?(Keyspace.table_range(@ledger, :intent), intent_key)
    refute Keyspace.contains?(Keyspace.ledger_range(@other_ledger), intent_key)
    refute Keyspace.contains?(Keyspace.table_range(@ledger, :state), intent_key)
  end

  test "encodes each store family under a distinct table range" do
    keys = [
      intent: Keyspace.intent(@ledger, "int_1"),
      state: Keyspace.state(@ledger, "int_1"),
      command: Keyspace.command(@ledger, "cmd_1"),
      stream: Keyspace.stream(@ledger, "intent:int_1", 1),
      queue: Keyspace.queue(@ledger, :default, 0, @now, 10, "int_1"),
      claim: Keyspace.claim(@ledger, "claim_1"),
      shard: Keyspace.shard_lease(@ledger, :default, 0),
      outbox: Keyspace.outbox(@ledger, 1),
      projection: Keyspace.projection(@ledger, :dispatcher)
    ]

    assert keys |> Keyword.values() |> Enum.uniq() |> length() == length(keys)

    assert Enum.all?(keys, fn {table, key} ->
             Keyspace.contains?(Keyspace.table_range(@ledger, table), key)
           end)
  end

  test "orders stream versions and outbox sequences numerically" do
    assert Keyspace.stream(@ledger, "intent:int_1", 1) < Keyspace.stream(@ledger, "intent:int_1", 2)
    assert Keyspace.outbox(@ledger, 1) < Keyspace.outbox(@ledger, 2)

    assert Keyspace.contains?(
             Keyspace.stream_range(@ledger, "intent:int_1"),
             Keyspace.stream(@ledger, "intent:int_1", 2)
           )

    refute Keyspace.contains?(
             Keyspace.stream_range(@ledger, "intent:int_2"),
             Keyspace.stream(@ledger, "intent:int_1", 2)
           )

    assert Keyspace.contains?(Keyspace.outbox_range(@ledger), Keyspace.outbox(@ledger, 2))
  end

  test "orders due-intent queue keys for ascending range scans" do
    high_priority = Keyspace.queue(@ledger, :default, 0, @now, 10, "int_high")
    low_priority = Keyspace.queue(@ledger, :default, 0, @now, 1, "int_low")
    later = Keyspace.queue(@ledger, :default, 0, DateTime.add(@now, 1, :second), 10, "int_later")
    shard_one = Keyspace.queue(@ledger, :default, 1, @now, 10, "int_shard_1")

    assert high_priority < low_priority
    assert high_priority < later

    assert Keyspace.contains?(Keyspace.queue_range(@ledger, :default), shard_one)
    assert Keyspace.contains?(Keyspace.queue_range(@ledger, :default, 0), high_priority)
    refute Keyspace.contains?(Keyspace.queue_range(@ledger, :default, 0), shard_one)
  end

  test "keeps shard leases scoped by queue and shard" do
    default_shard = Keyspace.shard_lease(@ledger, :default, 0)
    other_queue = Keyspace.shard_lease(@ledger, :critical, 0)

    assert Keyspace.contains?(Keyspace.shard_lease_range(@ledger, :default), default_shard)
    refute Keyspace.contains?(Keyspace.shard_lease_range(@ledger, :default), other_queue)
  end

  test "rejects invalid dynamic key components early" do
    assert_raise ArgumentError, ~r/intent_id must be a non-empty binary/, fn ->
      Keyspace.intent(@ledger, "")
    end

    assert_raise ArgumentError, ~r/shard must be a non-negative integer/, fn ->
      Keyspace.queue(@ledger, :default, -1, @now, 1, "int_1")
    end

    assert_raise ArgumentError, ~r/visible_at must be a DateTime/, fn ->
      Keyspace.queue(@ledger, :default, 0, nil, 1, "int_1")
    end
  end
end
