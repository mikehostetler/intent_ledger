defmodule IntentLedger.StoreContractTest do
  use ExUnit.Case, async: true

  alias IntentLedger.Store.{Commit, CommitRequest, Conflict, Precondition, Write}

  test "commit request structs compose preconditions and writes" do
    precondition = Precondition.new(:stream_version, stream: "intent:int_1", expected: 1)
    write = Write.new(:append_signal, stream: "intent:int_1", value: %{type: "intent_ledger.intent.completed"})

    request =
      CommitRequest.new(
        ledger: MyApp.IntentLedger,
        command_id: "cmd_1",
        operation: :complete,
        preconditions: [precondition],
        writes: [write],
        metadata: %{actor: "worker-1"}
      )

    assert request.command_id == "cmd_1"
    assert request.operation == :complete
    assert request.preconditions == [precondition]
    assert request.writes == [write]
    assert request.metadata.actor == "worker-1"
  end

  test "commit result and conflict structs expose explicit outcomes" do
    write = Write.new(:put_state, key: "intent:int_1", value: %{status: :completed})
    commit = Commit.new(command_id: "cmd_1", result: :ok, writes: [write], signals: [%{type: "done"}])

    conflict =
      Conflict.new(:stream_version,
        key: "intent:int_1",
        expected: 1,
        actual: 2,
        message: "stream version conflict"
      )

    assert commit.writes == [write]
    assert commit.signals == [%{type: "done"}]
    assert conflict.type == :stream_version
    assert conflict.expected == 1
    assert conflict.actual == 2
  end

  test "store v1 structs expose Zoi schemas" do
    assert %Zoi.Types.Struct{} = CommitRequest.schema()
    assert %Zoi.Types.Struct{} = Commit.schema()
    assert %Zoi.Types.Struct{} = Precondition.schema()
    assert %Zoi.Types.Struct{} = Write.schema()
    assert %Zoi.Types.Struct{} = Conflict.schema()

    assert {:ok, %CommitRequest{}} = Zoi.parse(CommitRequest.schema(), CommitRequest.new())
    assert {:ok, %Commit{}} = Zoi.parse(Commit.schema(), Commit.new())
    assert {:ok, %Precondition{}} = Zoi.parse(Precondition.schema(), Precondition.new(:command_absent))
    assert {:ok, %Write{}} = Zoi.parse(Write.schema(), Write.new(:put_idempotency))
    assert {:ok, %Conflict{}} = Zoi.parse(Conflict.schema(), Conflict.new(:claim_fence))
  end

  test "store v1 structs publish supported semantic kinds" do
    assert :claim_fence in Precondition.kinds()
    assert :put_outbox in Write.kinds()
    assert :command_replay in Conflict.kinds()
  end

  test "store behaviour exposes semantic v1 callbacks" do
    callbacks = IntentLedger.Store.behaviour_info(:callbacks) |> MapSet.new()

    assert MapSet.subset?(
             MapSet.new(child_spec: 1, commit: 4, read: 4, lease: 4, listing: 4, outbox: 4),
             callbacks
           )

    refute MapSet.member?(callbacks, {:submit, 4})
    refute MapSet.member?(callbacks, {:complete, 6})
    refute MapSet.member?(callbacks, {:recover, 4})
  end

  test "memory adapter advertises store v1 callback placeholders" do
    assert {:error, :store_v1_not_implemented} =
             IntentLedger.Store.Memory.commit(:store, MyApp.IntentLedger, CommitRequest.new(), [])

    assert {:error, :store_v1_not_implemented} =
             IntentLedger.Store.Memory.read(:store, MyApp.IntentLedger, {:intent, "int_1"}, [])

    assert {:error, :store_v1_not_implemented} =
             IntentLedger.Store.Memory.lease(:store, MyApp.IntentLedger, {:shard, :acquire, %{}}, [])

    assert {:error, :store_v1_not_implemented} =
             IntentLedger.Store.Memory.listing(:store, MyApp.IntentLedger, {:due_intents, %{}}, [])

    assert {:error, :store_v1_not_implemented} =
             IntentLedger.Store.Memory.outbox(:store, MyApp.IntentLedger, {:read, %{}}, [])
  end
end
