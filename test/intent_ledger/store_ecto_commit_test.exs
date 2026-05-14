defmodule IntentLedger.StoreEctoCommitTest do
  use ExUnit.Case, async: true

  @moduletag :integration
  @moduletag :postgres

  alias IntentLedger.Error.AdapterRuntimeError
  alias IntentLedger.{Intent, IntentState, Signal}
  alias IntentLedger.Store.{Commit, CommitRequest, Conflict, Listing, Outbox, Precondition, Write}
  alias IntentLedger.Store.Ecto, as: EctoStore

  defmodule PostgresRepo do
    def __adapter__, do: Elixir.Ecto.Adapters.Postgres

    def transaction(fun, opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      Process.put(:ops, [])
      Process.put(:rows, Keyword.get(opts, :rows, []))
      send(test_pid, {:transaction, opts |> Keyword.delete(:test_pid) |> Keyword.delete(:rows)})

      result = fun.()
      send(test_pid, {:ops, Enum.reverse(Process.get(:ops, []))})

      {:ok, result}
    after
      Process.delete(:ops)
      Process.delete(:rows)
    end

    def one(query) do
      record({:one, query})

      case Process.get(:rows, []) do
        [row | rows] ->
          Process.put(:rows, rows)
          row

        [] ->
          nil
      end
    end

    def all(query) do
      record({:all, query})
      Process.get(:rows, [])
    end

    def insert_all(source, rows, opts) do
      record({:insert_all, source, rows, opts})
      {length(rows), nil}
    end

    def update_all(query, opts) do
      record({:update_all, query, opts})
      {1, nil}
    end

    def delete_all(query, opts) do
      record({:delete_all, query, opts})
      {1, nil}
    end

    def rollback(reason), do: {:error, reason}

    defp record(op) do
      Process.put(:ops, [op | Process.get(:ops, [])])
      :ok
    end
  end

  @ledger MyApp.IntentLedger
  @now ~U[2026-01-01 00:00:00Z]
  @lease_until ~U[2026-01-01 00:01:00Z]

  setup do
    name = :"ecto_store_#{System.unique_integer([:positive])}"
    store = start_supervised!({EctoStore, name: name, repo: PostgresRepo, tables: [intents: :custom_intents]})

    %{store: store}
  end

  test "applies basic Store V1 writes inside a repo transaction", %{store: store} do
    {:ok, intent} = Intent.new(%{id: "int_1", key: "job:1", kind: "job.run", visible_at: @now})
    state = IntentState.new(intent, @now)
    signal = Signal.lifecycle(:intent_available, @ledger, "intent:int_1", %{visible_at: @now})
    result = %{intent_id: intent.id}

    request =
      CommitRequest.new(
        command_id: "cmd_1",
        operation: :submit,
        command: %{key: intent.key},
        preconditions: [Precondition.stream_version("intent:int_1", 0)],
        writes: [
          Write.new(:put_intent, key: intent.id, value: intent),
          Write.new(:put_state, key: intent.id, value: state),
          Write.append_signal("intent:int_1", signal),
          Write.put_idempotency("cmd_1", result)
        ]
      )

    assert {:ok, %Commit{} = commit} = EctoStore.commit(store, @ledger, request, transaction_opts: [test_pid: self()])
    assert commit.command_id == "cmd_1"
    assert commit.result == result
    assert commit.signals == [signal]

    assert_receive {:transaction, []}
    assert_receive {:ops, ops}

    assert [
             {:one, %Ecto.Query{}},
             {:insert_all, {"custom_intents", IntentLedger.Store.Ecto.Schema.Intent}, [intent_row], intent_opts},
             {:insert_all, {"intent_ledger_states", IntentLedger.Store.Ecto.Schema.State}, [state_row], _state_opts},
             {:insert_all, {"intent_ledger_streams", IntentLedger.Store.Ecto.Schema.Stream}, [stream_row],
              _stream_opts},
             {:insert_all, {"intent_ledger_signals", IntentLedger.Store.Ecto.Schema.Signal}, [signal_row],
              _signal_opts},
             {:insert_all, {"intent_ledger_commands", IntentLedger.Store.Ecto.Schema.Command}, [command_row],
              _command_opts}
           ] = ops

    assert intent_opts[:prefix] == nil
    assert intent_row.ledger == "MyApp.IntentLedger"
    assert intent_row.intent_id == "int_1"
    assert intent_row.intent.id == "int_1"

    assert state_row.status == "available"
    assert state_row.queue == "default"
    assert stream_row.version == 1
    assert signal_row.version == 1
    assert signal_row.signal.type == "intent_ledger.intent.available"
    assert command_row.result == result
  end

  test "checks command_absent before writing command replay records", %{store: store} do
    result = %{intent_id: "int_1"}

    request =
      CommitRequest.new(
        command_id: "cmd_1",
        operation: :submit,
        command: %{key: "job:1"},
        preconditions: [Precondition.command_absent("cmd_1")],
        writes: [Write.put_idempotency("cmd_1", result)]
      )

    assert {:ok, %Commit{result: ^result}} =
             EctoStore.commit(store, @ledger, request, transaction_opts: [test_pid: self()])

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}, {:insert_all, {"intent_ledger_commands", _schema}, [row], _opts}]}

    assert row.command_id == "cmd_1"
    assert row.operation == "submit"
    assert row.command == %{key: "job:1"}
    assert row.result == result
  end

  test "returns a command conflict when command_absent finds an existing command", %{store: store} do
    request =
      CommitRequest.new(
        command_id: "cmd_1",
        preconditions: [Precondition.command_absent("cmd_1")]
      )

    existing = command_row(:submit, %{key: "job:1"}, %{intent_id: "int_1"})

    assert {:error, %Conflict{type: :command_conflict, key: "cmd_1"}} =
             EctoStore.commit(store, @ledger, request, transaction_opts: [test_pid: self(), rows: [existing]])

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}]}
  end

  test "replays an existing command without applying writes", %{store: store} do
    result = %{intent_id: "int_1"}
    existing = command_row(:submit, %{key: "job:1"}, result)

    request =
      CommitRequest.new(
        command_id: "cmd_1",
        operation: :submit,
        command: %{key: "job:1"},
        preconditions: [Precondition.command_replay("cmd_1")],
        writes: [Write.put_idempotency("cmd_1", %{intent_id: "other"})]
      )

    assert {:ok, %Commit{result: ^result, replayed: true, replay_of: "cmd_1", writes: []}} =
             EctoStore.commit(store, @ledger, request, transaction_opts: [test_pid: self(), rows: [existing]])

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}]}
  end

  test "rejects command replay when the stored signature differs", %{store: store} do
    existing = command_row(:submit, %{key: "job:1"}, %{intent_id: "int_1"})

    request =
      CommitRequest.new(
        command_id: "cmd_1",
        operation: :submit,
        command: %{key: "other"},
        preconditions: [Precondition.command_replay("cmd_1")]
      )

    assert {:error, %Conflict{type: :command_conflict, key: "cmd_1"}} =
             EctoStore.commit(store, @ledger, request, transaction_opts: [test_pid: self(), rows: [existing]])

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}]}
  end

  test "rejects stale stream version preconditions without appending", %{store: store} do
    request =
      CommitRequest.new(
        preconditions: [Precondition.stream_version("intent:int_1", 1)],
        writes: [Write.append_signal("intent:int_1", %{id: "sig_3"})]
      )

    assert {:error, %Conflict{type: :stream_version, expected: 1, actual: 2}} =
             EctoStore.commit(store, @ledger, request, transaction_opts: [test_pid: self(), rows: [%{version: 2}]])

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}]}
  end

  test "requires append writes to declare a stream version", %{store: store} do
    request =
      CommitRequest.new(writes: [Write.append_signal("intent:int_1", %{id: "sig_1"})])

    assert {:error, %AdapterRuntimeError{} = error} =
             EctoStore.commit(store, @ledger, request, transaction_opts: [test_pid: self()])

    assert error.details.reason == :missing_stream_version

    assert_receive {:transaction, []}
    assert_receive {:ops, []}
  end

  test "checks intent status before claim writes", %{store: store} do
    claim = %{intent_id: "int_1", owner_id: "worker", token_hash: "hash", lease_until: @lease_until}

    request =
      CommitRequest.new(
        preconditions: [Precondition.intent_status("int_1", :available)],
        writes: [Write.put_claim("clm_1", claim)]
      )

    assert {:ok, %Commit{}} =
             EctoStore.commit(
               store,
               @ledger,
               request,
               transaction_opts: [test_pid: self(), rows: [state_row(status: "available")]]
             )

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}, {:insert_all, {"intent_ledger_claims", _schema}, [_row], _opts}]}
  end

  test "checks claim fences against claim and state rows", %{store: store} do
    claim = claim_row(token_hash: "hash", lease_until: @lease_until)
    state = state_row(status: "claimed", claim_id: "clm_1")

    request =
      CommitRequest.new(
        preconditions: [Precondition.claim_fence("clm_1", "hash", metadata: %{now: @now})],
        writes: [Write.delete_claim("clm_1")]
      )

    assert {:ok, %Commit{}} =
             EctoStore.commit(store, @ledger, request, transaction_opts: [test_pid: self(), rows: [claim, state]])

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}, {:one, %Ecto.Query{}}, {:delete_all, %Ecto.Query{}, []}]}
  end

  test "rejects stale claim fences", %{store: store} do
    claim = claim_row(token_hash: "other", lease_until: @lease_until)

    request =
      CommitRequest.new(
        preconditions: [Precondition.claim_fence("clm_1", "hash", metadata: %{now: @now})],
        writes: [Write.delete_claim("clm_1")]
      )

    assert {:error, %Conflict{type: :claim_fence, key: "clm_1"}} =
             EctoStore.commit(store, @ledger, request, transaction_opts: [test_pid: self(), rows: [claim]])

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}]}
  end

  test "checks shard lease preconditions before commit writes", %{store: store} do
    request =
      CommitRequest.new(
        preconditions: [Precondition.shard_available(:default, 0, @now)],
        writes: [Write.put_shard_lease(:default, 0, %{owner_id: "node-a", lease_until: @lease_until})]
      )

    assert {:ok, %Commit{}} = EctoStore.commit(store, @ledger, request, transaction_opts: [test_pid: self()])

    assert_receive {:transaction, []}

    assert_receive {:ops,
                    [
                      {:one, %Ecto.Query{}},
                      {:insert_all, {"intent_ledger_shard_leases", _schema}, [lease], _opts}
                    ]}

    assert lease.owner_id == "node-a"

    duplicate =
      CommitRequest.new(
        preconditions: [Precondition.shard_available(:default, 0, @now)],
        writes: [Write.put_shard_lease(:default, 0, %{owner_id: "node-b", lease_until: @lease_until})]
      )

    assert {:error, %Conflict{type: :shard_lease}} =
             EctoStore.commit(
               store,
               @ledger,
               duplicate,
               transaction_opts: [test_pid: self(), rows: [lease_row(owner_id: "node-a", lease_until: @lease_until)]]
             )

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}]}
  end

  test "applies local shard lease callback operations", %{store: store} do
    renewed_until = ~U[2026-01-01 00:02:00Z]
    expired_at = ~U[2026-01-01 00:01:01Z]

    assert {:ok, %{owner_id: "node-a", lease_until: @lease_until}} =
             EctoStore.lease(
               store,
               @ledger,
               {:shard, :acquire,
                %{queue: :default, shard: 0, owner_id: "node-a", lease_until: @lease_until, now: @now}},
               transaction_opts: [test_pid: self()]
             )

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}, {:insert_all, {"intent_ledger_shard_leases", _schema}, [_], _opts}]}

    assert {:ok, %{owner_id: "node-a", lease_until: ^renewed_until}} =
             EctoStore.lease(
               store,
               @ledger,
               {:shard, :renew,
                %{queue: :default, shard: 0, owner_id: "node-a", lease_until: renewed_until, now: @now}},
               transaction_opts: [
                 test_pid: self(),
                 rows: [lease_row(owner_id: "node-a", lease_until: @lease_until)]
               ]
             )

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}, {:insert_all, {"intent_ledger_shard_leases", _schema}, [_], _opts}]}

    assert {:ok, %{owner_id: "node-a"}} =
             EctoStore.lease(
               store,
               @ledger,
               {:shard, :release, %{queue: :default, shard: 0, owner_id: "node-a", now: @now}},
               transaction_opts: [
                 test_pid: self(),
                 rows: [lease_row(owner_id: "node-a", lease_until: renewed_until)]
               ]
             )

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}, {:delete_all, %Ecto.Query{}, []}]}

    assert {:ok, %{owner_id: "node-b", lease_until: ^renewed_until}} =
             EctoStore.lease(
               store,
               @ledger,
               {:shard, :takeover,
                %{queue: :default, shard: 0, owner_id: "node-b", lease_until: renewed_until, now: expired_at}},
               transaction_opts: [
                 test_pid: self(),
                 rows: [lease_row(owner_id: "node-a", lease_until: @lease_until)]
               ]
             )

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}, {:insert_all, {"intent_ledger_shard_leases", _schema}, [_], _opts}]}
  end

  test "lists due intents with deterministic ordering", %{store: store} do
    future = ~U[2026-01-01 00:01:00Z]

    rows = [
      state_row(intent_id: "int_low", status: "available", visible_at: @now, priority: 1),
      state_row(intent_id: "int_high", status: "available", visible_at: @now, priority: 10),
      state_row(intent_id: "int_future", status: "available", visible_at: future, priority: 20),
      state_row(intent_id: "int_other_shard", status: "available", shard: 1, visible_at: @now, priority: 30),
      state_row(intent_id: "int_claimed", status: "claimed", visible_at: @now, priority: 40)
    ]

    assert {:ok, due} =
             EctoStore.listing(
               store,
               @ledger,
               Listing.due_intents(:default, 0, @now, limit: 2),
               transaction_opts: [test_pid: self(), rows: rows]
             )

    assert Enum.map(due, & &1.intent_id) == ["int_high", "int_low"]

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:all, %Ecto.Query{}}]}
  end

  test "lists expired claims across queue shards", %{store: store} do
    future = ~U[2026-01-01 00:01:00Z]
    earlier = ~U[2025-12-31 23:59:00Z]

    rows = [
      state_row(intent_id: "int_later", status: "claimed", lease_until: @now),
      state_row(intent_id: "int_earlier", status: "claimed", shard: 1, lease_until: earlier),
      state_row(intent_id: "int_future", status: "claimed", lease_until: future),
      state_row(intent_id: "int_available", status: "available", lease_until: earlier)
    ]

    assert {:ok, expired} =
             EctoStore.listing(
               store,
               @ledger,
               {:expired_claims, %{queue: :default, shard: nil, at: @now}},
               transaction_opts: [test_pid: self(), rows: rows]
             )

    assert Enum.map(expired, & &1.intent_id) == ["int_earlier", "int_later"]

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:all, %Ecto.Query{}}]}
  end

  test "inserts outbox entries with allocated sequences", %{store: store} do
    request = Outbox.insert("intent:int_3", %{id: "sig_3", type: "intent_ledger.intent.completed"}, key: "out_3")

    assert {:ok, entry} =
             EctoStore.outbox(
               store,
               @ledger,
               request,
               transaction_opts: [test_pid: self(), rows: [outbox_entry_row(key: "out_2", sequence: 2)]]
             )

    assert entry.key == "out_3"
    assert entry.sequence == 3
    assert entry.stream == "intent:int_3"
    assert entry.signal.id == "sig_3"

    assert_receive {:transaction, []}

    assert_receive {:ops,
                    [
                      {:all, %Ecto.Query{}},
                      {:insert_all, {"intent_ledger_outbox", _schema}, [row], _opts}
                    ]}

    assert row.key == "out_3"
    assert row.sequence == 3
  end

  test "reads acks and replays outbox entries", %{store: store} do
    unacked = outbox_entry_row(key: "out_1", sequence: 1, acked_at: nil)
    acked = outbox_entry_row(key: "out_2", sequence: 2, acked_at: @now, consumer: "dispatcher")

    assert {:ok, [entry]} =
             EctoStore.outbox(
               store,
               @ledger,
               Outbox.read("dispatcher", cursor: 0),
               transaction_opts: [test_pid: self(), rows: [acked, unacked]]
             )

    assert entry.key == "out_1"

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:all, %Ecto.Query{}}]}

    assert {:ok, acked_entry} =
             EctoStore.outbox(
               store,
               @ledger,
               Outbox.ack("out_1", "dispatcher", metadata: %{acked_at: @now}),
               transaction_opts: [test_pid: self(), rows: [unacked]]
             )

    assert acked_entry.key == "out_1"
    assert acked_entry.acked_at == @now
    assert acked_entry.consumer == "dispatcher"

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}, {:update_all, %Ecto.Query{}, [set: ack_fields]}]}
    assert Keyword.fetch!(ack_fields, :acked_at) == @now
    assert Keyword.fetch!(ack_fields, :consumer) == "dispatcher"

    assert {:ok, replayed} =
             EctoStore.outbox(
               store,
               @ledger,
               Outbox.replay(cursor: 0, limit: 10),
               transaction_opts: [test_pid: self(), rows: [acked, unacked]]
             )

    assert Enum.map(replayed, & &1.key) == ["out_1", "out_2"]

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:all, %Ecto.Query{}}]}
  end

  test "rejects acked outbox entries", %{store: store} do
    acked = outbox_entry_row(key: "out_1", sequence: 1, acked_at: @now, consumer: "dispatcher")

    assert {:error, %Conflict{type: :outbox, key: "out_1"}} =
             EctoStore.outbox(
               store,
               @ledger,
               Outbox.ack("out_1", "dispatcher", metadata: %{acked_at: @now}),
               transaction_opts: [test_pid: self(), rows: [acked]]
             )

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}]}
  end

  test "checks outbox_unacked preconditions", %{store: store} do
    acked = outbox_entry_row(key: "out_1", sequence: 1, acked_at: @now, consumer: "dispatcher")
    request = CommitRequest.new(preconditions: [Precondition.outbox_unacked("out_1")])

    assert {:error, %Conflict{type: :outbox, key: "out_1"}} =
             EctoStore.commit(store, @ledger, request, transaction_opts: [test_pid: self(), rows: [acked]])

    assert_receive {:transaction, []}
    assert_receive {:ops, [{:one, %Ecto.Query{}}]}
  end

  test "writes claims shard leases outbox and delete operations", %{store: store} do
    claim = %{intent_id: "int_1", owner_id: "worker", token_hash: "hash", lease_until: @now}
    lease = %{queue: "default", shard: 0, owner_id: "node-a", lease_until: @now}
    outbox = %{sequence: 7, stream: "intent:int_1", signal: %{id: "sig_1", type: "intent_ledger.intent.claimed"}}

    request =
      CommitRequest.new(
        writes: [
          Write.put_claim("clm_1", claim),
          Write.put_shard_lease(:default, 0, lease),
          Write.put_outbox("out_1", outbox),
          Write.delete_claim("clm_1"),
          Write.delete_shard_lease(:default, 0),
          Write.ack_outbox("out_1", metadata: %{consumer: "dispatcher", acked_at: @now})
        ]
      )

    assert {:ok, %Commit{}} = EctoStore.commit(store, @ledger, request, transaction_opts: [test_pid: self()])

    assert_receive {:transaction, []}
    assert_receive {:ops, ops}

    assert [
             {:insert_all, {"intent_ledger_claims", IntentLedger.Store.Ecto.Schema.Claim}, [claim_row], _claim_opts},
             {:insert_all, {"intent_ledger_shard_leases", IntentLedger.Store.Ecto.Schema.ShardLease}, [lease_row],
              _lease_opts},
             {:insert_all, {"intent_ledger_outbox", IntentLedger.Store.Ecto.Schema.OutboxEntry}, [outbox_row],
              _outbox_opts},
             {:delete_all, %Ecto.Query{}, []},
             {:delete_all, %Ecto.Query{}, []},
             {:update_all, %Ecto.Query{}, [set: ack_fields]}
           ] = ops

    assert claim_row.claim_id == "clm_1"
    assert claim_row.intent_id == "int_1"
    assert lease_row.queue == "default"
    assert lease_row.shard == 0
    assert outbox_row.key == "out_1"
    assert outbox_row.sequence == 7
    assert outbox_row.signal_id == "sig_1"
    assert Keyword.fetch!(ack_fields, :consumer) == "dispatcher"
    assert Keyword.fetch!(ack_fields, :acked_at) == @now
  end

  defp command_row(operation, command, result) do
    %{operation: to_string(operation), command: command, result: result}
  end

  defp state_row(attrs) do
    attrs = Map.new(attrs)

    Map.merge(
      %{intent_id: "int_1", status: "claimed", queue: "default", shard: 0, state: %{}},
      attrs
    )
  end

  defp claim_row(attrs) do
    attrs = Map.new(attrs)

    Map.merge(
      %{claim_id: "clm_1", intent_id: "int_1", owner_id: "worker", token_hash: "hash", claim: %{}},
      attrs
    )
  end

  defp lease_row(attrs) do
    attrs = Map.new(attrs)

    Map.merge(
      %{queue: "default", shard: 0, owner_id: "node-a", lease_until: @lease_until, lease: %{}},
      attrs
    )
  end

  defp outbox_entry_row(attrs) do
    attrs = Map.new(attrs)
    signal = Map.get(attrs, :signal, %{id: "sig_1", type: "intent_ledger.intent.completed"})

    Map.merge(
      %{
        key: "out_1",
        sequence: 1,
        stream: "intent:int_1",
        signal_id: signal.id,
        signal_type: signal.type,
        subject: "intent:int_1",
        signal: signal,
        entry: %{},
        acked_at: nil,
        consumer: nil,
        metadata: %{}
      },
      attrs
    )
  end
end
