defmodule IntentLedger.StoreEctoMigrationTest do
  use ExUnit.Case, async: true

  @moduletag :integration
  @moduletag :postgres

  alias IntentLedger.Store.Ecto.Migration

  test "exposes default table names for Store V1 structures" do
    assert Migration.table_names() == %{
             intents: :intent_ledger_intents,
             states: :intent_ledger_states,
             signals: :intent_ledger_signals,
             streams: :intent_ledger_streams,
             commands: :intent_ledger_commands,
             claims: :intent_ledger_claims,
             shard_leases: :intent_ledger_shard_leases,
             outbox: :intent_ledger_outbox,
             projection_offsets: :intent_ledger_projection_offsets
           }
  end

  test "supports repo prefix and table name options" do
    opts = [
      repo: MyApp.Repo,
      prefix: "tenant_a",
      tables: [intents: :tenant_intents, outbox: :tenant_outbox]
    ]

    assert Migration.repo(opts) == MyApp.Repo
    assert Migration.prefix(opts) == "tenant_a"
    assert Migration.table_name(:intents, opts) == :tenant_intents
    assert Migration.table_name(:outbox, opts) == :tenant_outbox
    assert Migration.table_name(:states, opts) == :intent_ledger_states
  end

  test "provides up down and change migration entrypoints" do
    assert Code.ensure_loaded?(Migration)

    assert function_exported?(Migration, :up, 0)
    assert function_exported?(Migration, :up, 1)
    assert function_exported?(Migration, :down, 0)
    assert function_exported?(Migration, :down, 1)
    assert function_exported?(Migration, :change, 0)
    assert function_exported?(Migration, :change, 1)
  end
end
