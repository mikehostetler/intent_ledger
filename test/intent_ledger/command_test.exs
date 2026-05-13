defmodule IntentLedger.CommandTest do
  use ExUnit.Case, async: true

  alias IntentLedger.Command

  @mutating_operations [
    :submit,
    :submit_many,
    :claim,
    :heartbeat,
    :complete,
    :fail,
    :release,
    :cancel,
    :requeue,
    :mark_ambiguous,
    :recover
  ]

  test "catalogue covers every mutating public operation" do
    public_functions =
      IntentLedger.__info__(:functions)
      |> Keyword.keys()
      |> MapSet.new()

    assert Command.operations() == @mutating_operations

    for operation <- @mutating_operations do
      assert MapSet.member?(public_functions, operation)
    end

    refute :get in Command.operations()
    refute :history in Command.operations()
  end

  test "catalogue entries use stable versioned command types" do
    for operation <- @mutating_operations do
      definition = Command.fetch!(operation)

      assert definition.operation == operation
      assert definition.type == "intent_ledger.command.#{operation}"
      assert definition.version == 1
      assert Command.type_for(operation) == definition.type
      assert Command.fetch!(definition.type) == definition
      assert Command.operation_for_type(definition.type) == {:ok, operation}
    end
  end

  test "catalogue entries include command metadata fields" do
    common_metadata = Command.common_metadata_fields()

    assert common_metadata == [
             :command_id,
             :idempotency_key,
             :actor,
             :causation_id,
             :correlation_id,
             :root_intent_id,
             :parent_intent_id,
             :depth
           ]

    for definition <- Command.all() do
      assert Enum.all?(common_metadata, &(&1 in definition.optional))
    end
  end

  test "catalogue entries publish operation-specific fields" do
    assert Command.fetch!(:submit).required == [:intent]
    assert Command.fetch!(:submit_many).required == [:intents]
    assert Command.fetch!(:claim).required == [:queue, :owner_id]
    assert Command.fetch!(:heartbeat).required == [:claim_id, :token]
    assert Command.fetch!(:complete).required == [:claim_id, :token, :result]
    assert Command.fetch!(:fail).required == [:claim_id, :token, :error]
    assert Command.fetch!(:release).required == [:claim_id, :token]
    assert Command.fetch!(:cancel).required == [:intent_id, :reason]
    assert Command.fetch!(:requeue).required == [:intent_id]
    assert Command.fetch!(:mark_ambiguous).required == [:intent_id, :reason]
    assert Command.fetch!(:recover).required == [:queue]
  end

  test "generic builder creates a Jido signal envelope from the catalogue" do
    signal =
      Command.new(
        MyApp.IntentLedger,
        :submit,
        %{intent: %{id: "int_1", key: "job:1", kind: "job.run"}, command_id: "cmd_1", actor: "user:1"}
      )

    assert %Jido.Signal{} = signal
    assert signal.id == "cmd_1"
    assert signal.type == Command.type_for(:submit)
    assert signal.source == "/intent_ledger/MyApp.IntentLedger"
    assert signal.subject == "intent:int_1"
    assert signal.datacontenttype == "application/json"
    assert signal.dataschema == "https://hexdocs.pm/intent_ledger/commands/submit/v1.json"
    assert signal.data.schema_version == 1
    assert signal.data.command_id == "cmd_1"
    assert signal.data.actor == "user:1"
  end

  test "operation builders publish command-specific payloads" do
    assert Command.submit(MyApp.IntentLedger, %{key: "job:1", kind: "job.run"}).type ==
             "intent_ledger.command.submit"

    assert Command.submit_many(MyApp.IntentLedger, [%{key: "job:1", kind: "job.run"}]).data.intents == [
             %{key: "job:1", kind: "job.run"}
           ]

    assert Command.claim(MyApp.IntentLedger, :default, "worker-1", limit: 2).data == %{
             queue: :default,
             owner_id: "worker-1",
             limit: 2,
             schema_version: 1
           }

    assert Command.heartbeat(MyApp.IntentLedger, "clm_1", "tok_1").subject == "claim:clm_1"
    assert Command.complete(MyApp.IntentLedger, "clm_1", "tok_1", :ok).data.result == :ok
    assert Command.fail(MyApp.IntentLedger, "clm_1", "tok_1", :boom).data.error == :boom
    assert Command.release(MyApp.IntentLedger, "clm_1", "tok_1").data.token == "tok_1"
    assert Command.cancel(MyApp.IntentLedger, "int_1", :because).subject == "intent:int_1"
    assert Command.requeue(MyApp.IntentLedger, "int_1").type == "intent_ledger.command.requeue"

    assert Command.mark_ambiguous(MyApp.IntentLedger, "int_1", :manual_review).data.reason ==
             :manual_review

    assert Command.recover(MyApp.IntentLedger, :default, limit: 100).subject == "queue:default"
  end

  test "builder normalizes DateTime values for JSON command data" do
    now = ~U[2026-01-01 00:00:00Z]

    signal =
      Command.requeue(MyApp.IntentLedger, "int_1",
        retry_at: now,
        signal_attrs: [subject: "custom:int_1"]
      )

    assert signal.subject == "custom:int_1"
    assert signal.data.retry_at == "2026-01-01T00:00:00Z"
  end
end
