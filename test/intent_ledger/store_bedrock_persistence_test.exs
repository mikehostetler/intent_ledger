defmodule IntentLedger.StoreBedrockPersistenceTest.PersistentRepo do
  @moduledoc false

  use Agent

  def start_link(opts) do
    path = Keyword.fetch!(opts, :path)
    Agent.start_link(fn -> %{path: path, values: load_values(path)} end, name: __MODULE__)
  end

  def transact(fun, _opts) do
    Agent.get_and_update(__MODULE__, fn %{path: path, values: values} = state ->
      try do
        Process.put(:values, values)

        result = fun.(__MODULE__)

        case result do
          {:error, _reason} ->
            {result, state}

          _success ->
            next_values = Process.get(:values, values)
            persist_values!(path, next_values)
            {result, %{state | values: next_values}}
        end
      after
        Process.delete(:values)
      end
    end)
  end

  def put(key, value) do
    Process.put(:values, Map.put(Process.get(:values, %{}), key, value))
    :ok
  end

  def clear(key) do
    Process.put(:values, Map.delete(Process.get(:values, %{}), key))
    :ok
  end

  def get(key), do: Map.get(Process.get(:values, %{}), key)

  def get_range({start_key, end_key}) do
    Process.get(:values, %{})
    |> Enum.filter(fn {key, _value} -> key >= start_key and key < end_key end)
    |> Enum.sort_by(fn {key, _value} -> key end)
  end

  def add_read_conflict_key(_key), do: :ok
  def add_write_conflict_range(_range), do: :ok

  defp load_values(path) do
    if File.exists?(path) do
      path
      |> File.read!()
      |> :erlang.binary_to_term()
    else
      %{}
    end
  end

  defp persist_values!(path, values) do
    path |> Path.dirname() |> File.mkdir_p!()
    File.write!(path, :erlang.term_to_binary(values))
  end
end

defmodule IntentLedger.StoreBedrockPersistenceTest do
  use ExUnit.Case, async: false

  alias IntentLedger.Store.{Bedrock, Commit, CommitRequest, Listing, Outbox, Precondition, Write}

  @ledger MyApp.IntentLedger
  @now ~U[2026-01-01 00:00:00Z]
  @lease_until ~U[2026-01-01 00:01:00Z]
  @renewed_until ~U[2026-01-01 00:02:00Z]
  @repo IntentLedger.StoreBedrockPersistenceTest.PersistentRepo

  setup do
    base = Path.join(System.tmp_dir!(), "intent_ledger_bedrock_persistence_#{System.unique_integer([:positive])}")
    path = Path.join(base, "kv.term")
    store_name = :"bedrock_persistence_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      stop_if_alive(store_name)
      stop_if_alive(@repo)
      File.rm_rf!(base)
    end)

    {:ok, repo_pid} = @repo.start_link(path: path)
    {:ok, store} = Bedrock.start_link(name: store_name, repo: @repo)

    %{path: path, repo_pid: repo_pid, store: store, store_name: store_name}
  end

  test "preserves lifecycle and operational state across repo and adapter restarts", context do
    stream = "intent:int_persist"
    signal = %{id: "sig_persist", type: "intent_ledger.intent.submitted"}
    result = %{intent_id: "int_persist", status: :submitted}
    state = %{intent_id: "int_persist", queue: "default", shard: 0, status: :available, visible_at: @now, priority: 5}
    outbox = %{stream: stream, signal: signal, inserted_at: @now}

    seed =
      CommitRequest.new(
        command_id: "cmd_persist_seed",
        operation: :submit,
        command: %{key: "job:persist"},
        preconditions: [Precondition.command_absent("cmd_persist_seed"), Precondition.stream_version(stream, 0)],
        writes: [
          Write.new(:put_state, key: "int_persist", value: state),
          Write.append_signal(stream, signal),
          Write.put_idempotency("cmd_persist_seed", result),
          Write.put_shard_lease(:default, 0, %{owner_id: "node-a", lease_until: @lease_until}),
          Write.put_outbox("out_persist", outbox)
        ]
      )

    assert {:ok, %Commit{result: ^result}} = Bedrock.commit(context.store, @ledger, seed, [])

    assert {:ok, acked} =
             Bedrock.outbox(
               context.store,
               @ledger,
               Outbox.ack("out_persist", "dispatcher", metadata: %{acked_at: @now}),
               []
             )

    assert acked.acked_at == @now

    {store, repo_pid} = restart_store_and_repo(context.store, context.repo_pid, context.path, context.store_name)

    assert {:ok, %Commit{replayed: true, result: ^result}} =
             Bedrock.commit(
               store,
               @ledger,
               CommitRequest.new(
                 command_id: "cmd_persist_seed",
                 operation: :submit,
                 command: %{key: "job:persist"},
                 preconditions: [Precondition.command_replay("cmd_persist_seed")]
               ),
               []
             )

    assert {:ok, %{version: 1, signals: [^signal]}} = Bedrock.read(store, @ledger, {:stream, stream, []}, [])

    assert {:ok, [due]} = Bedrock.listing(store, @ledger, Listing.due_intents(:default, 0, @now), [])
    assert due.intent_id == "int_persist"
    assert due.priority == 5

    assert {:ok, []} = Bedrock.outbox(store, @ledger, Outbox.read("dispatcher"), [])
    assert {:ok, [replayed_outbox]} = Bedrock.outbox(store, @ledger, Outbox.replay(cursor: 0), [])
    assert replayed_outbox.key == "out_persist"
    assert replayed_outbox.acked_at == @now

    assert {:ok, %{lease_until: @renewed_until}} =
             Bedrock.lease(
               store,
               @ledger,
               {:shard, :renew,
                %{queue: :default, shard: 0, owner_id: "node-a", lease_until: @renewed_until, now: @now}},
               []
             )

    {store, repo_pid} = restart_store_and_repo(store, repo_pid, context.path, context.store_name)

    assert {:ok, %{lease_until: @renewed_until}} =
             Bedrock.lease(
               store,
               @ledger,
               {:shard, :release, %{queue: :default, shard: 0, owner_id: "node-a", now: @now}},
               []
             )

    failed =
      CommitRequest.new(
        command_id: "cmd_rollback",
        operation: :submit,
        preconditions: [Precondition.stream_version(stream, 0)],
        writes: [
          Write.append_signal(stream, %{id: "sig_should_not_persist"}),
          Write.put_idempotency("cmd_rollback", %{rolled_back?: false})
        ]
      )

    assert {:error, %{type: :stream_version}} = Bedrock.commit(store, @ledger, failed, [])

    {store, _repo_pid} = restart_store_and_repo(store, repo_pid, context.path, context.store_name)

    assert {:ok, %{version: 1, signals: [^signal]}} = Bedrock.read(store, @ledger, {:stream, stream, []}, [])

    assert {:ok, %Commit{}} =
             Bedrock.commit(
               store,
               @ledger,
               CommitRequest.new(preconditions: [Precondition.command_absent("cmd_rollback")]),
               []
             )

    assert File.exists?(context.path)
  end

  defp restart_store_and_repo(store, repo_pid, path, store_name) do
    GenServer.stop(store)
    GenServer.stop(repo_pid)

    {:ok, next_repo_pid} = @repo.start_link(path: path)
    {:ok, next_store} = Bedrock.start_link(name: store_name, repo: @repo)

    {next_store, next_repo_pid}
  end

  defp stop_if_alive(name) when is_atom(name) do
    case Process.whereis(name) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end
  end
end
