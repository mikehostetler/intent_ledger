defmodule IntentLedger.SerializationTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias IntentLedger.{Claim, Claimed, Command, Inspection, Intent, IntentState, Record, ShardState, Signal}

  alias IntentLedger.Store.{
    Commit,
    CommitRequest,
    Conflict,
    Listing,
    Outbox,
    Precondition,
    Write
  }

  test "public schema-backed structs expose schemas and JSON encoders" do
    now = ~U[2026-01-01 00:00:00Z]
    lease_until = DateTime.add(now, 30, :second)

    {:ok, intent} =
      Intent.new(
        %{
          id: "int_json",
          key: "json:key",
          kind: "json.kind",
          shard: 0,
          visible_at: now,
          payload: %{invoice_id: 123}
        },
        now: now
      )

    state = IntentState.new(intent, now)

    claim = %Claim{
      id: "clm_json",
      intent_id: intent.id,
      owner_id: "worker",
      token: "tok_json",
      attempt: 1,
      lease_until: lease_until
    }

    claimed_state = %{
      state
      | status: :claimed,
        claim_id: claim.id,
        claim_token_hash: Claim.token_hash(claim.token),
        lease_until: lease_until
    }

    precondition = Precondition.stream_version("intent:int_json", 1)
    write = Write.put_idempotency("cmd_json", %{status: "ok"})

    structs = [
      {Intent, intent, "id"},
      {IntentState, state, "intent_id"},
      {Claim, claim, "token"},
      {Claimed, %Claimed{intent: intent, state: claimed_state, claim: claim}, "claim"},
      {Record, %Record{intent: intent, state: state}, "state"},
      {ShardState,
       %ShardState{
         queue: "default",
         shard: 0,
         cursor: 10,
         lease_owner: "node-a",
         lease_until: lease_until,
         updated_at: now
       }, "lease_owner"},
      {Inspection, Inspection.queues(queue: :default, at: now), "queue"},
      {Commit,
       Commit.new(
         command_id: "cmd_json",
         result: %{status: "ok"},
         writes: [write]
       ), "writes"},
      {CommitRequest,
       CommitRequest.new(
         command_id: "cmd_json",
         operation: :submit,
         command: %{kind: "json.kind"},
         preconditions: [precondition],
         writes: [write]
       ), "preconditions"},
      {Conflict, Conflict.stream_version("intent:int_json", 1, 2), "message"},
      {Listing, Listing.due_intents(:default, 0, now), "order"},
      {Outbox, Outbox.read("dispatcher", cursor: 0, limit: 10), "consumer"},
      {Precondition, precondition, "type"},
      {Write, write, "type"}
    ]

    for {module, value, expected_field} <- structs do
      assert function_exported?(module, :schema, 0)
      assert {:ok, types} = Code.Typespec.fetch_types(module)
      assert Enum.any?(types, &public_t_type?/1)
      assert {:ok, %{__struct__: ^module}} = Zoi.parse(module.schema(), value)
      assert {:ok, json} = Jason.encode(value)

      assert json
             |> Jason.decode!()
             |> Map.has_key?(expected_field)
    end
  end

  defp public_t_type?({:type, {:t, _type, []}}), do: true
  defp public_t_type?(_type), do: false

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
