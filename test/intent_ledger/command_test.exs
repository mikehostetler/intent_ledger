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

    assert Command.lineage_fields() == [
             :actor,
             :causation_id,
             :correlation_id,
             :root_intent_id,
             :parent_intent_id,
             :depth
           ]

    assert common_metadata == [:command_id, :idempotency_key | Command.lineage_fields()]

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

  test "normalizes command metadata from a command signal" do
    signal =
      Command.claim(MyApp.IntentLedger, :default, "worker-1",
        command_id: "cmd_claim",
        idempotency_key: "idem_claim",
        actor: :worker,
        causation_id: 123,
        correlation_id: "corr_1",
        root_intent_id: "int_root",
        parent_intent_id: "int_parent",
        depth: "2"
      )

    assert {:ok, normalized} = Command.normalize(signal)
    assert normalized.operation == :claim
    assert normalized.type == "intent_ledger.command.claim"
    assert normalized.schema_version == 1
    assert normalized.command_id == "cmd_claim"
    assert normalized.idempotency_key == "idem_claim"
    assert normalized.actor == "worker"
    assert normalized.causation_id == "123"
    assert normalized.correlation_id == "corr_1"
    assert normalized.root_intent_id == "int_root"
    assert normalized.parent_intent_id == "int_parent"
    assert normalized.depth == 2
    assert normalized.data.command_id == "cmd_claim"
    assert normalized.data.queue == :default
    assert normalized.data.owner_id == "worker-1"
  end

  test "normalizer accepts string-keyed command data and falls back to the signal id" do
    signal =
      Jido.Signal.new!(
        Command.type_for(:recover),
        %{"schema_version" => "1", "queue" => "default"},
        source: "/intent_ledger/MyApp.IntentLedger"
      )

    assert {:ok, normalized} = Command.parse(signal)
    assert normalized.operation == :recover
    assert normalized.command_id == signal.id
    assert normalized.depth == 0
    assert normalized.data.command_id == signal.id
    assert normalized.data.queue == "default"
  end

  test "normalizer rejects invalid command signals" do
    unknown =
      Jido.Signal.new!("intent_ledger.command.unknown", %{schema_version: 1},
        source: "/intent_ledger/MyApp.IntentLedger"
      )

    missing =
      Jido.Signal.new!(Command.type_for(:complete), %{schema_version: 1}, source: "/intent_ledger/MyApp.IntentLedger")

    mismatch =
      Jido.Signal.new!(
        Command.type_for(:claim),
        %{schema_version: 1, command_id: "cmd_1", queue: "default", owner_id: "worker-1"},
        id: "cmd_2",
        source: "/intent_ledger/MyApp.IntentLedger"
      )

    invalid_depth =
      Command.claim(MyApp.IntentLedger, :default, "worker-1", command_id: "cmd_depth", depth: -1)

    assert Command.normalize(unknown) == {:error, {:unknown_command_type, "intent_ledger.command.unknown"}}
    assert Command.normalize(missing) == {:error, {:required, :claim_id}}
    assert Command.normalize(mismatch) == {:error, {:command_id_mismatch, "cmd_2", "cmd_1"}}
    assert Command.normalize(invalid_depth) == {:error, {:invalid_non_negative_integer, :depth, -1}}
  end
end
