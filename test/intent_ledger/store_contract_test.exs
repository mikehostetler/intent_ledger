defmodule IntentLedger.StoreContractTest do
  use ExUnit.Case, async: true

  alias IntentLedger.Store.{Commit, CommitRequest, Conflict, Listing, Outbox, Precondition, Write}

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
    assert %Zoi.Types.Struct{} = Listing.schema()
    assert %Zoi.Types.Struct{} = Outbox.schema()

    assert {:ok, %CommitRequest{}} = Zoi.parse(CommitRequest.schema(), CommitRequest.new())
    assert {:ok, %Commit{}} = Zoi.parse(Commit.schema(), Commit.new())
    assert {:ok, %Precondition{}} = Zoi.parse(Precondition.schema(), Precondition.new(:command_absent))
    assert {:ok, %Write{}} = Zoi.parse(Write.schema(), Write.new(:put_idempotency))
    assert {:ok, %Conflict{}} = Zoi.parse(Conflict.schema(), Conflict.new(:claim_fence))
    assert {:ok, %Listing{}} = Zoi.parse(Listing.schema(), Listing.due_intents(:default, 0, ~U[2026-01-01 00:00:00Z]))
    assert {:ok, %Outbox{}} = Zoi.parse(Outbox.schema(), Outbox.read("dispatcher"))
  end

  test "store v1 structs publish supported semantic kinds" do
    assert :read in Outbox.kinds()
    assert :ack in Outbox.kinds()
    assert :due_intents in Listing.kinds()
    assert :expired_claims in Listing.kinds()
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

  test "listing helpers describe due intent and expired claim scans" do
    now = ~U[2026-01-01 00:00:00Z]

    due = Listing.due_intents(:default, 0, now, limit: 25, cursor: "page-1")

    assert due.type == :due_intents
    assert due.queue == "default"
    assert due.shard == 0
    assert due.at == now
    assert due.limit == 25
    assert due.cursor == "page-1"
    assert due.order == [:priority_desc, :visible_at_asc, :intent_id_asc]
    assert Listing.to_request(due) == {:due_intents, Map.delete(Map.from_struct(due), :type)}

    expired = Listing.expired_claims("default", nil, now, metadata: %{recovery_owner: "node-a"})

    assert expired.type == :expired_claims
    assert expired.queue == "default"
    assert expired.shard == nil
    assert expired.at == now
    assert expired.order == [:lease_until_asc, :intent_id_asc]
    assert expired.metadata.recovery_owner == "node-a"
    assert Listing.to_request(expired) == {:expired_claims, Map.delete(Map.from_struct(expired), :type)}
  end

  test "outbox helpers describe insert read ack and replay semantics" do
    now = ~U[2026-01-01 00:00:00Z]
    signal = %{type: "intent_ledger.intent.completed", id: "sig_1"}

    insert = Outbox.insert("intent:int_1", signal, metadata: %{command_id: "cmd_1"})
    write = Write.put_outbox("out_1", %{stream: insert.stream, signal: insert.value, inserted_at: now})

    assert insert.type == :insert
    assert insert.stream == "intent:int_1"
    assert insert.value == signal
    assert insert.metadata.command_id == "cmd_1"
    assert write.type == :put_outbox
    assert write.key == "out_1"

    read = Outbox.read(:dispatcher, cursor: 10, limit: 50)

    assert read.type == :read
    assert read.consumer == "dispatcher"
    assert read.cursor == 10
    assert read.limit == 50
    assert Outbox.to_request(read) == {:read, Map.delete(Map.from_struct(read), :type)}

    ack = Outbox.ack("out_1", :dispatcher, metadata: %{acked_at: now})
    ack_gate = Precondition.outbox_unacked("out_1")
    ack_write = Write.ack_outbox("out_1", metadata: %{acked_at: now, consumer: "dispatcher"})

    assert ack.type == :ack
    assert ack.key == "out_1"
    assert ack.consumer == "dispatcher"
    assert ack_gate.type == :outbox_unacked
    assert ack_gate.expected == :unacked
    assert ack_write.type == :ack_outbox

    replay = Outbox.replay(cursor: 5, limit: 100)
    conflict = Conflict.outbox("out_1", :unacked, :acked)

    assert replay.type == :replay
    assert replay.cursor == 5
    assert conflict.type == :outbox
    assert conflict.message == "outbox conflict"
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

  test "memory adapter routes store v1 callbacks through the GenServer" do
    store = start_supervised!({IntentLedger.Store.Memory, name: :"store_v1_#{System.unique_integer([:positive])}"})

    assert {:ok, %Commit{writes: [], signals: []}} =
             IntentLedger.Store.Memory.commit(store, MyApp.IntentLedger, CommitRequest.new(), [])

    assert {:error, :not_found} =
             IntentLedger.Store.Memory.read(store, MyApp.IntentLedger, {:intent, "int_1"}, [])

    assert {:ok, %{stream: "intent:int_1", version: 0, signals: []}} =
             IntentLedger.Store.Memory.read(store, MyApp.IntentLedger, {:stream, "intent:int_1", []}, [])

    assert {:error, {:unsupported_store_v1_request, :lease, {:shard, :acquire, %{}}}} =
             IntentLedger.Store.Memory.lease(store, MyApp.IntentLedger, {:shard, :acquire, %{}}, [])

    assert {:error, {:unsupported_store_v1_request, :listing, {:due_intents, %{}}}} =
             IntentLedger.Store.Memory.listing(store, MyApp.IntentLedger, {:due_intents, %{}}, [])

    assert {:error, {:unsupported_store_v1_request, :outbox, {:read, %{}}}} =
             IntentLedger.Store.Memory.outbox(store, MyApp.IntentLedger, {:read, %{}}, [])
  end

  test "memory adapter stores v1 command results for deterministic replay" do
    store =
      start_supervised!({IntentLedger.Store.Memory, name: :"store_v1_replay_#{System.unique_integer([:positive])}"})

    result = %{intent_id: "int_1", status: :submitted}

    request =
      CommitRequest.new(
        command_id: "cmd_1",
        operation: :submit,
        command: %{key: "job:1"},
        preconditions: [Precondition.command_absent("cmd_1")],
        writes: [Write.put_idempotency("cmd_1", result)]
      )

    assert {:ok, %Commit{} = commit} = IntentLedger.Store.Memory.commit(store, MyApp.IntentLedger, request, [])
    assert commit.result == result
    refute commit.replayed

    replay =
      CommitRequest.new(
        command_id: "cmd_1",
        operation: :submit,
        command: %{key: "job:1"},
        preconditions: [Precondition.command_replay("cmd_1")]
      )

    assert {:ok, %Commit{} = replayed} = IntentLedger.Store.Memory.commit(store, MyApp.IntentLedger, replay, [])
    assert replayed.result == result
    assert replayed.replayed
    assert replayed.replay_of == "cmd_1"

    conflicting_replay =
      CommitRequest.new(
        command_id: "cmd_1",
        operation: :complete,
        command: %{claim_id: "clm_1"},
        preconditions: [Precondition.command_replay("cmd_1")]
      )

    assert {:error, %Conflict{type: :command_conflict}} =
             IntentLedger.Store.Memory.commit(store, MyApp.IntentLedger, conflicting_replay, [])
  end

  test "memory adapter enforces v1 stream versions and appends lifecycle signals" do
    store =
      start_supervised!({IntentLedger.Store.Memory, name: :"store_v1_stream_#{System.unique_integer([:positive])}"})

    signal = %{type: "intent_ledger.intent.submitted", id: "sig_1"}
    stream = "intent:int_1"

    request =
      CommitRequest.new(
        command_id: "cmd_stream_1",
        operation: :submit,
        preconditions: [Precondition.stream_version(stream, 0)],
        writes: [Write.append_signal(stream, signal)]
      )

    assert {:ok, %Commit{} = commit} = IntentLedger.Store.Memory.commit(store, MyApp.IntentLedger, request, [])
    assert commit.signals == [signal]

    assert {:ok, %{version: 1, signals: [^signal]}} =
             IntentLedger.Store.Memory.read(store, MyApp.IntentLedger, {:stream, stream, []}, [])

    stale =
      CommitRequest.new(
        command_id: "cmd_stream_stale",
        operation: :submit,
        preconditions: [Precondition.stream_version(stream, 0)],
        writes: [Write.append_signal(stream, %{id: "sig_stale"})]
      )

    assert {:error, %Conflict{type: :stream_version, expected: 0, actual: 1}} =
             IntentLedger.Store.Memory.commit(store, MyApp.IntentLedger, stale, [])

    next =
      CommitRequest.new(
        command_id: "cmd_stream_2",
        operation: :submit,
        preconditions: [Precondition.stream_version(stream, 1)],
        writes: [Write.append_signal(stream, %{id: "sig_2"})]
      )

    assert {:ok, %Commit{}} = IntentLedger.Store.Memory.commit(store, MyApp.IntentLedger, next, [])
    assert {:ok, %{version: 2}} = IntentLedger.Store.Memory.read(store, MyApp.IntentLedger, {:stream, stream, []}, [])
  end
end
