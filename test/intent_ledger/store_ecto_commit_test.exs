defmodule IntentLedger.StoreEctoCommitTest do
  use ExUnit.Case, async: true

  alias IntentLedger.Error.AdapterRuntimeError
  alias IntentLedger.{Intent, IntentState, Signal}
  alias IntentLedger.Store.{Commit, CommitRequest, Conflict, Precondition, Write}
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
end
