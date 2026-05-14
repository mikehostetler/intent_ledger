defmodule IntentLedger.StoreEctoSchemaTest do
  use ExUnit.Case, async: true

  @moduletag :integration
  @moduletag :postgres

  alias IntentLedger.Store.Ecto.Schema

  test "maps logical tables to Ecto schema modules" do
    assert Schema.module_for(:intents) == IntentLedger.Store.Ecto.Schema.Intent
    assert Schema.module_for(:states) == IntentLedger.Store.Ecto.Schema.State
    assert Schema.module_for(:signals) == IntentLedger.Store.Ecto.Schema.Signal
    assert Schema.module_for(:outbox) == IntentLedger.Store.Ecto.Schema.OutboxEntry
  end

  test "builds source tuples with table-name overrides" do
    assert Schema.source(:intents) == {"intent_ledger_intents", IntentLedger.Store.Ecto.Schema.Intent}

    assert Schema.source(:outbox, tables: [outbox: :custom_outbox]) ==
             {"custom_outbox", IntentLedger.Store.Ecto.Schema.OutboxEntry}
  end

  test "schema modules expose expected row sources and fields" do
    assert IntentLedger.Store.Ecto.Schema.Intent.__schema__(:source) == "intent_ledger_intents"
    assert IntentLedger.Store.Ecto.Schema.State.__schema__(:source) == "intent_ledger_states"
    assert IntentLedger.Store.Ecto.Schema.OutboxEntry.__schema__(:source) == "intent_ledger_outbox"

    assert :intent_id in IntentLedger.Store.Ecto.Schema.Intent.__schema__(:fields)
    assert :lease_until in IntentLedger.Store.Ecto.Schema.Claim.__schema__(:fields)
    assert :sequence in IntentLedger.Store.Ecto.Schema.OutboxEntry.__schema__(:fields)
    assert :cursor in IntentLedger.Store.Ecto.Schema.ProjectionOffset.__schema__(:fields)
  end
end
