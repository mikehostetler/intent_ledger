defmodule IntentLedger.SerializationTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias IntentLedger.{Command, Signal}

  test "command signals retain their compatibility contract through JSON serialization" do
    signal =
      Command.submit(
        MyApp.IntentLedger,
        %{id: "int_1", key: "job:1", kind: "job.run"},
        command_id: "cmd_1",
        actor: "tester"
      )

    assert {:ok, json} = Jido.Signal.serialize(signal)
    encoded = Jason.decode!(json)

    assert encoded["id"] == "cmd_1"
    assert encoded["type"] == "intent_ledger.command.submit"
    assert encoded["source"] == "/intent_ledger/MyApp.IntentLedger"
    assert encoded["subject"] == "intent:int_1"
    assert encoded["datacontenttype"] == "application/json"
    assert encoded["dataschema"] == "https://hexdocs.pm/intent_ledger/commands/submit/v1.json"
    assert encoded["data"]["schema_version"] == 1
    assert encoded["data"]["command_id"] == "cmd_1"
    assert encoded["data"]["actor"] == "tester"

    assert {{:ok, decoded}, _log} = with_log(fn -> Jido.Signal.deserialize(json) end)
    assert {:ok, normalized} = Command.normalize(decoded)
    assert normalized.operation == :submit
    assert normalized.command_id == "cmd_1"
    assert normalized.actor == "tester"
    assert normalized.data.intent["key"] == "job:1"
  end

  test "lifecycle signals retain their compatibility contract through JSON serialization" do
    signal =
      Signal.lifecycle(:intent_completed, MyApp.IntentLedger, "intent:int_1", %{
        claim_id: "clm_1",
        result: %{"ok" => true}
      })

    assert {:ok, json} = Jido.Signal.serialize(signal)
    encoded = Jason.decode!(json)

    assert encoded["type"] == "intent_ledger.intent.completed"
    assert encoded["source"] == "/intent_ledger/MyApp.IntentLedger"
    assert encoded["subject"] == "intent:int_1"
    assert encoded["datacontenttype"] == "application/json"
    assert encoded["dataschema"] == "https://hexdocs.pm/intent_ledger/lifecycle/intent_completed/v1.json"
    assert encoded["data"]["schema_version"] == 1
    assert encoded["data"]["claim_id"] == "clm_1"
    assert encoded["data"]["result"] == %{"ok" => true}

    assert {{:ok, decoded}, _log} = with_log(fn -> Jido.Signal.deserialize(json) end)
    assert decoded.type == signal.type
    assert decoded.dataschema == signal.dataschema
    assert decoded.data["schema_version"] == 1
  end
end
