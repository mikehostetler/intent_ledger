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
end
