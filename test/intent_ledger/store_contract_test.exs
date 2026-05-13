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
    assert :shard_lease in Precondition.kinds()
    assert :put_shard_lease in Write.kinds()
    assert :put_outbox in Write.kinds()
    assert :command_replay in Conflict.kinds()
  end

  test "stream version helpers describe atomic compare-and-append semantics" do
    precondition = Precondition.stream_version("intent:int_1", 3)
    write = Write.append_signal("intent:int_1", %{type: "intent_ledger.intent.completed"})
    conflict = Conflict.stream_version("intent:int_1", 3, 4)

    assert precondition.type == :stream_version
    assert precondition.stream == "intent:int_1"
    assert precondition.expected == 3
    assert write.type == :append_signal
    assert write.stream == "intent:int_1"
    assert conflict.key == "intent:int_1"
    assert conflict.expected == 3
    assert conflict.actual == 4
  end

  test "command idempotency helpers describe deterministic replay semantics" do
    absent = Precondition.command_absent("cmd_1")
    replay = Precondition.command_replay("cmd_1")
    write = Write.put_idempotency("cmd_1", %{intent_id: "int_1"})
    replayed = Commit.new(command_id: "cmd_1", result: %{intent_id: "int_1"}, replayed: true, replay_of: "cmd_1")
    conflict = Conflict.command_conflict("cmd_1", %{operation: :submit}, %{operation: :complete})

    assert absent.type == :command_absent
    assert replay.type == :command_replay
    assert write.type == :put_idempotency
    assert write.key == "cmd_1"
    assert replayed.replayed
    assert replayed.replay_of == "cmd_1"
    assert conflict.type == :command_conflict
  end

  test "claim fencing helpers describe acquire and owner-checked lifecycle semantics" do
    claim_gate =
      Precondition.intent_status("int_1", [:available, :retry_scheduled],
        metadata: %{operation: :claim, queue: "default", shard: 0}
      )

    claim_write =
      Write.put_claim("clm_1", %{
        intent_id: "int_1",
        owner_id: "worker-1",
        token_hash: "hash_1",
        attempt: 1,
        lease_until: ~U[2026-01-01 00:01:00Z]
      })

    assert claim_gate.type == :intent_status
    assert claim_gate.key == "int_1"
    assert claim_gate.expected == [:available, :retry_scheduled]
    assert claim_gate.metadata.operation == :claim
    assert claim_write.type == :put_claim
    assert claim_write.key == "clm_1"
    assert claim_write.value.token_hash == "hash_1"

    for operation <- [:heartbeat, :complete, :fail, :release] do
      fence =
        Precondition.claim_fence("clm_1", "hash_1",
          metadata: %{operation: operation, intent_id: "int_1", lease_required: true}
        )

      request =
        CommitRequest.new(
          command_id: "cmd_#{operation}",
          operation: operation,
          preconditions: [fence],
          writes: [Write.append_signal("intent:int_1", %{type: "intent_ledger.#{operation}"})]
        )

      assert fence.type == :claim_fence
      assert fence.key == "clm_1"
      assert fence.expected == %{status: :claimed, token_hash: "hash_1"}
      assert fence.metadata.operation == operation
      assert request.preconditions == [fence]
    end

    assert Write.delete_claim("clm_1").type == :delete_claim

    assert Conflict.intent_status("int_1", [:available], :claimed).type == :intent_status
    assert Conflict.claim_fence("clm_1", %{token_hash: "hash_1"}, %{token_hash: "hash_2"}).type == :claim_fence
  end

  test "shard lease helpers describe acquire renew release expiry and takeover semantics" do
    now = ~U[2026-01-01 00:00:00Z]
    lease_until = ~U[2026-01-01 00:00:30Z]

    acquire =
      Precondition.shard_available(:default, 0, now, metadata: %{operation: :acquire, owner_id: "node-a"})

    lease =
      Write.put_shard_lease(:default, 0, %{
        owner_id: "node-a",
        acquired_at: now,
        lease_until: lease_until
      })

    assert acquire.type == :shard_lease
    assert acquire.key == "shard:default:0"
    assert acquire.expected == %{available_at: now}
    assert acquire.metadata.operation == :acquire
    assert lease.type == :put_shard_lease
    assert lease.value.owner_id == "node-a"

    for operation <- [:renew, :release] do
      owner_gate =
        Precondition.shard_lease("default", 0, "node-a", metadata: %{operation: operation, now: now})

      assert owner_gate.type == :shard_lease
      assert owner_gate.expected == %{owner_id: "node-a", status: :current}
      assert owner_gate.metadata.operation == operation
    end

    for operation <- [:expire, :takeover] do
      expired_gate =
        Precondition.shard_expired(:default, 0, now, metadata: %{operation: operation, previous_owner_id: "node-a"})

      assert expired_gate.type == :shard_lease
      assert expired_gate.expected == %{expired_at_or_before: now}
      assert expired_gate.metadata.operation == operation
    end

    assert Write.delete_shard_lease("default", 0).type == :delete_shard_lease

    conflict =
      Conflict.shard_lease(:default, 0, %{owner_id: "node-a", status: :current}, %{owner_id: "node-b"})

    assert conflict.type == :shard_lease
    assert conflict.key == "shard:default:0"
    assert conflict.message == "shard lease conflict"
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
