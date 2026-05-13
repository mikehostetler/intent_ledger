defmodule IntentLedger.CrossNodeRepo do
  @moduledoc false

  @app :intent_ledger

  @spec configure(Path.t()) :: :ok
  def configure(path) when is_binary(path) do
    Application.put_env(@app, __MODULE__, path: path)
  end

  def transact(fun, _opts) when is_function(fun, 1) do
    path = path!()

    :global.trans({__MODULE__, path}, fn ->
      values = load_values(path)

      try do
        Process.put(:values, values)

        result = fun.(__MODULE__)

        case result do
          {:error, _reason} ->
            result

          _success ->
            path
            |> persist_values!(Process.get(:values, values))

            result
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

  defp path! do
    @app
    |> Application.fetch_env!(__MODULE__)
    |> Keyword.fetch!(:path)
  end

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
    File.write!(path, :erlang.term_to_binary(values, [:deterministic]))
  end
end

defmodule IntentLedger.CrossNodeStore do
  @moduledoc false

  alias IntentLedger.{Claim, CrossNodeRepo, IntentState, PeerNodes}
  alias IntentLedger.Store.{Bedrock, CommitRequest, Listing, Outbox, Precondition, Write}

  @default_ledger IntentLedger.CrossNodeLedger
  @default_repo CrossNodeRepo
  @default_now ~U[2026-01-01 00:00:00Z]
  @default_lease_ms 30_000
  @default_timeout 10_000

  @type t :: %{
          ledger: atom(),
          peers: [PeerNodes.peer()],
          repo: module(),
          store_name: atom(),
          stores: %{node() => pid()},
          path: Path.t()
        }

  @spec start!(map(), keyword()) :: t()
  def start!(%{base_path: base_path, peers: peers}, opts \\ []) do
    path = Keyword.get(opts, :path, Path.join(base_path, "cross_node_store.term"))
    ledger = Keyword.get(opts, :ledger, @default_ledger)
    repo = Keyword.get(opts, :repo, @default_repo)
    store_name = Keyword.get_lazy(opts, :store_name, &unique_store_name/0)

    context = start_on_peers!(peers, repo, path, ledger, store_name)
    ExUnit.Callbacks.on_exit(fn -> stop(context) end)
    context
  end

  @spec stop(t()) :: :ok
  def stop(%{peers: peers, stores: stores}) do
    Enum.each(peers, fn peer ->
      case Map.fetch(stores, peer.node) do
        {:ok, store} -> safe_call(peer, __MODULE__, :stop_store, [store], 5_000)
        :error -> :ok
      end
    end)

    :ok
  end

  @spec submit(PeerNodes.peer(), t(), map() | keyword()) :: term()
  def submit(peer, context, attrs \\ []) do
    call(peer, __MODULE__, :submit_on_node, [context.store_name, context.ledger, normalize_attrs(attrs)])
  end

  @spec claim(PeerNodes.peer(), t(), map() | keyword()) :: term()
  def claim(peer, context, attrs \\ []) do
    call(peer, __MODULE__, :claim_on_node, [context.store_name, context.ledger, normalize_attrs(attrs)])
  end

  @spec claim_intent(PeerNodes.peer(), t(), String.t(), map() | keyword()) :: term()
  def claim_intent(peer, context, intent_id, attrs \\ []) do
    call(peer, __MODULE__, :claim_intent_on_node, [
      context.store_name,
      context.ledger,
      intent_id,
      normalize_attrs(attrs)
    ])
  end

  @spec complete(PeerNodes.peer(), t(), map() | keyword()) :: term()
  def complete(peer, context, attrs) do
    call(peer, __MODULE__, :complete_on_node, [context.store_name, context.ledger, normalize_attrs(attrs)])
  end

  @spec release(PeerNodes.peer(), t(), map() | keyword()) :: term()
  def release(peer, context, attrs) do
    call(peer, __MODULE__, :release_on_node, [context.store_name, context.ledger, normalize_attrs(attrs)])
  end

  @spec fail(PeerNodes.peer(), t(), map() | keyword()) :: term()
  def fail(peer, context, attrs) do
    call(peer, __MODULE__, :fail_on_node, [context.store_name, context.ledger, normalize_attrs(attrs)])
  end

  @spec recover(PeerNodes.peer(), t(), map() | keyword()) :: term()
  def recover(peer, context, attrs \\ []) do
    call(peer, __MODULE__, :recover_on_node, [context.store_name, context.ledger, normalize_attrs(attrs)])
  end

  @spec shard_lease(PeerNodes.peer(), t(), IntentLedger.Store.shard_lease_operation(), map() | keyword()) :: term()
  def shard_lease(peer, context, operation, attrs) do
    call(peer, __MODULE__, :shard_lease_on_node, [
      context.store_name,
      context.ledger,
      operation,
      normalize_attrs(attrs)
    ])
  end

  @spec replay(PeerNodes.peer(), t(), atom(), String.t(), map()) :: term()
  def replay(peer, context, operation, command_id, command) do
    call(peer, __MODULE__, :replay_on_node, [context.store_name, context.ledger, operation, command_id, command])
  end

  @spec outbox(PeerNodes.peer(), t(), Outbox.t()) :: term()
  def outbox(peer, context, %Outbox{} = request) do
    call(peer, Bedrock, :outbox, [context.store_name, context.ledger, request, []])
  end

  @spec read_stream(PeerNodes.peer(), t(), String.t()) :: term()
  def read_stream(peer, context, stream) do
    call(peer, Bedrock, :read, [context.store_name, context.ledger, {:stream, stream, []}, []])
  end

  @spec configure_and_start_store(module(), Path.t(), atom()) :: {:ok, pid()} | {:error, term()}
  def configure_and_start_store(repo, path, store_name) do
    _ = Application.ensure_all_started(:bedrock)
    :ok = repo.configure(path)

    case Bedrock.start_link(name: store_name, repo: repo) do
      {:ok, pid} ->
        Process.unlink(pid)
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec stop_store(pid()) :: :ok
  def stop_store(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      GenServer.stop(pid)
    end

    :ok
  catch
    :exit, _reason -> :ok
  end

  @spec submit_on_node(GenServer.server(), atom(), map()) :: IntentLedger.Store.commit_result()
  def submit_on_node(store, ledger, attrs) do
    now = Map.get(attrs, :now, @default_now)
    intent_id = Map.get_lazy(attrs, :intent_id, fn -> unique_id("int") end)
    stream = stream(intent_id)
    command_id = Map.get(attrs, :command_id, "cmd:submit:#{intent_id}")
    command = %{intent_id: intent_id, key: Map.get(attrs, :key, intent_id)}
    signal = signal(intent_id, "submitted", now, Map.get(attrs, :signal_metadata, %{}))
    result = %{intent_id: intent_id, status: :submitted, command_id: command_id, command: command}
    state = state(intent_id, attrs, now, :available)

    commit(store, ledger,
      command_id: command_id,
      operation: :submit,
      command: command,
      preconditions: [Precondition.command_absent(command_id), Precondition.stream_version(stream, 0)],
      writes: [
        Write.new(:put_state, key: intent_id, value: state),
        Write.append_signal(stream, signal),
        Write.put_idempotency(command_id, result),
        Write.put_outbox(outbox_key(command_id), %{stream: stream, signal: signal, inserted_at: now})
      ]
    )
  end

  @spec claim_on_node(GenServer.server(), atom(), map()) :: IntentLedger.Store.commit_result() | :empty
  def claim_on_node(store, ledger, attrs) do
    now = Map.get(attrs, :now, @default_now)
    queue = attrs |> Map.get(:queue, "default") |> to_string()
    shard = Map.get(attrs, :shard, 0)

    case Bedrock.listing(store, ledger, Listing.due_intents(queue, shard, now, limit: 1), []) do
      {:ok, [state | _]} -> claim_state(store, ledger, state, attrs, now)
      {:ok, []} -> :empty
      {:error, reason} -> {:error, reason}
    end
  end

  @spec claim_intent_on_node(GenServer.server(), atom(), String.t(), map()) :: IntentLedger.Store.commit_result()
  def claim_intent_on_node(store, ledger, intent_id, attrs) do
    now = Map.get(attrs, :now, @default_now)

    state =
      %IntentState{
        intent_id: intent_id,
        queue: attrs |> Map.get(:queue, "default") |> to_string(),
        shard: Map.get(attrs, :shard, 0),
        status: :available,
        visible_at: Map.get(attrs, :visible_at, now),
        priority: Map.get(attrs, :priority, 0),
        attempt: Map.get(attrs, :attempt, 0),
        max_attempts: Map.get(attrs, :max_attempts, 3),
        updated_at: now
      }

    claim_state(store, ledger, state, attrs, now)
  end

  @spec complete_on_node(GenServer.server(), atom(), map()) :: IntentLedger.Store.commit_result()
  def complete_on_node(store, ledger, attrs) do
    now = Map.get(attrs, :now, @default_now)
    claim_id = Map.fetch!(attrs, :claim_id)
    token = Map.fetch!(attrs, :token)
    token_hash = Claim.token_hash(token)
    intent_id = Map.fetch!(attrs, :intent_id)
    stream = stream(intent_id)
    stream_version = stream_version!(store, ledger, stream)
    command_id = Map.get(attrs, :command_id, "cmd:complete:#{claim_id}")
    command = %{claim_id: claim_id, result: Map.get(attrs, :result)}
    signal = signal(intent_id, "completed", now, %{result: Map.get(attrs, :result)})
    result = %{intent_id: intent_id, status: :completed, command_id: command_id, command: command}

    commit(store, ledger,
      command_id: command_id,
      operation: :complete,
      command: command,
      preconditions: [
        Precondition.command_absent(command_id),
        Precondition.stream_version(stream, stream_version),
        Precondition.claim_fence(claim_id, token_hash, metadata: %{now: now})
      ],
      writes: [
        Write.new(:put_state,
          key: intent_id,
          value: final_state(intent_id, attrs, now, :completed, result: Map.get(attrs, :result))
        ),
        Write.append_signal(stream, signal),
        Write.delete_claim(claim_id),
        Write.put_idempotency(command_id, result),
        Write.put_outbox(outbox_key(command_id), %{stream: stream, signal: signal, inserted_at: now})
      ]
    )
  end

  @spec release_on_node(GenServer.server(), atom(), map()) :: IntentLedger.Store.commit_result()
  def release_on_node(store, ledger, attrs) do
    now = Map.get(attrs, :now, @default_now)
    claim_id = Map.fetch!(attrs, :claim_id)
    token = Map.fetch!(attrs, :token)
    token_hash = Claim.token_hash(token)
    intent_id = Map.fetch!(attrs, :intent_id)
    stream = stream(intent_id)
    stream_version = stream_version!(store, ledger, stream)
    command_id = Map.get(attrs, :command_id, "cmd:release:#{claim_id}")
    command = %{claim_id: claim_id}
    signal = signal(intent_id, "released", now, %{claim_id: claim_id})
    result = %{intent_id: intent_id, status: :available, command_id: command_id, command: command}

    commit(store, ledger,
      command_id: command_id,
      operation: :release,
      command: command,
      preconditions: [
        Precondition.command_absent(command_id),
        Precondition.stream_version(stream, stream_version),
        Precondition.claim_fence(claim_id, token_hash, metadata: %{now: now})
      ],
      writes: [
        Write.new(:put_state,
          key: intent_id,
          value:
            state(intent_id, attrs, now, :available)
            |> merge_state(claim_id: nil, claim_token_hash: nil, lease_until: nil, updated_at: now)
        ),
        Write.append_signal(stream, signal),
        Write.delete_claim(claim_id),
        Write.put_idempotency(command_id, result)
      ]
    )
  end

  @spec fail_on_node(GenServer.server(), atom(), map()) :: IntentLedger.Store.commit_result()
  def fail_on_node(store, ledger, attrs) do
    now = Map.get(attrs, :now, @default_now)
    claim_id = Map.fetch!(attrs, :claim_id)
    token = Map.fetch!(attrs, :token)
    token_hash = Claim.token_hash(token)
    intent_id = Map.fetch!(attrs, :intent_id)
    stream = stream(intent_id)
    stream_version = stream_version!(store, ledger, stream)
    command_id = Map.get(attrs, :command_id, "cmd:fail:#{claim_id}")
    command = %{claim_id: claim_id, error: Map.get(attrs, :error)}
    status = if Map.get(attrs, :retry_at), do: :retry_scheduled, else: :failed
    signal = signal(intent_id, Atom.to_string(status), now, %{error: Map.get(attrs, :error)})
    result = %{intent_id: intent_id, status: status, command_id: command_id, command: command}

    commit(store, ledger,
      command_id: command_id,
      operation: :fail,
      command: command,
      preconditions: [
        Precondition.command_absent(command_id),
        Precondition.stream_version(stream, stream_version),
        Precondition.claim_fence(claim_id, token_hash, metadata: %{now: now})
      ],
      writes: [
        Write.new(:put_state,
          key: intent_id,
          value:
            final_state(intent_id, attrs, now, status,
              error: Map.get(attrs, :error),
              visible_at: Map.get(attrs, :retry_at)
            )
        ),
        Write.append_signal(stream, signal),
        Write.delete_claim(claim_id),
        Write.put_idempotency(command_id, result),
        Write.put_outbox(outbox_key(command_id), %{stream: stream, signal: signal, inserted_at: now})
      ]
    )
  end

  @spec recover_on_node(GenServer.server(), atom(), map()) :: IntentLedger.Store.commit_result()
  def recover_on_node(store, ledger, attrs) do
    now = Map.get(attrs, :now, @default_now)
    queue = attrs |> Map.get(:queue, "default") |> to_string()
    shard = Map.get(attrs, :shard)
    command_id = Map.get(attrs, :command_id, "cmd:recover:#{queue}:#{System.unique_integer([:positive])}")

    with {:ok, expired} <- Bedrock.listing(store, ledger, Listing.expired_claims(queue, shard, now), []) do
      writes =
        Enum.flat_map(expired, fn state ->
          intent_id = state.intent_id
          stream = stream(intent_id)
          signal = signal(intent_id, "recovered", now, %{})

          [
            Write.new(:put_state,
              key: intent_id,
              value:
                merge_state(state,
                  status: :retry_scheduled,
                  visible_at: now,
                  claim_id: nil,
                  claim_token_hash: nil,
                  lease_until: nil,
                  updated_at: now
                )
            ),
            Write.append_signal(stream, signal),
            Write.delete_claim(state.claim_id),
            Write.put_outbox(outbox_key("#{command_id}:#{intent_id}"), %{
              stream: stream,
              signal: signal,
              inserted_at: now
            })
          ]
        end)

      result = %{count: length(expired), intent_ids: Enum.map(expired, & &1.intent_id), status: :recovered}

      commit(store, ledger,
        command_id: command_id,
        operation: :recover,
        command: %{queue: queue, shard: shard},
        preconditions:
          [Precondition.command_absent(command_id)] ++
            Enum.map(expired, &Precondition.intent_status(&1.intent_id, :claimed)) ++
            Enum.map(
              expired,
              &Precondition.stream_version(stream(&1.intent_id), stream_version!(store, ledger, stream(&1.intent_id)))
            ),
        writes: writes ++ [Write.put_idempotency(command_id, result)]
      )
    end
  end

  @spec shard_lease_on_node(GenServer.server(), atom(), IntentLedger.Store.shard_lease_operation(), map()) ::
          IntentLedger.Store.result()
  def shard_lease_on_node(store, ledger, operation, attrs) do
    Bedrock.lease(
      store,
      ledger,
      {:shard, operation,
       %{
         queue: attrs |> Map.get(:queue, "default") |> to_string(),
         shard: Map.get(attrs, :shard, 0),
         owner_id: attrs |> Map.get(:owner_id, "owner") |> to_string(),
         lease_until: Map.get(attrs, :lease_until),
         now: Map.get(attrs, :now, @default_now)
       }},
      []
    )
  end

  @spec replay_on_node(GenServer.server(), atom(), atom(), String.t(), map()) :: IntentLedger.Store.commit_result()
  def replay_on_node(store, ledger, operation, command_id, command) do
    commit(store, ledger,
      command_id: command_id,
      operation: operation,
      command: command,
      preconditions: [Precondition.command_replay(command_id)]
    )
  end

  defp start_on_peers!(peers, repo, path, ledger, store_name) do
    stores =
      Enum.reduce(peers, %{}, fn peer, acc ->
        case call(peer, __MODULE__, :configure_and_start_store, [repo, path, store_name]) do
          {:ok, store} -> Map.put(acc, peer.node, store)
          {:error, reason} -> raise "failed to start cross-node store on #{peer.node}: #{inspect(reason)}"
        end
      end)

    %{ledger: ledger, peers: peers, repo: repo, store_name: store_name, stores: stores, path: path}
  end

  defp claim_state(store, ledger, state, attrs, now) do
    lease_ms = Map.get(attrs, :lease_ms, @default_lease_ms)
    lease_until = Map.get_lazy(attrs, :lease_until, fn -> DateTime.add(now, lease_ms, :millisecond) end)
    owner_id = attrs |> Map.get(:owner_id, "worker") |> to_string()
    claim_id = Map.get_lazy(attrs, :claim_id, fn -> unique_id("clm") end)
    token = Map.get_lazy(attrs, :token, fn -> unique_id("tok") end)
    token_hash = Claim.token_hash(token)
    intent_id = state.intent_id
    stream = stream(intent_id)
    stream_version = stream_version!(store, ledger, stream)
    command_id = Map.get(attrs, :command_id, "cmd:claim:#{claim_id}")
    command = %{intent_id: intent_id, queue: state.queue, owner_id: owner_id}
    signal = signal(intent_id, "claimed", now, %{claim_id: claim_id, owner_id: owner_id})

    result = %{
      intent_id: intent_id,
      status: :claimed,
      claim_id: claim_id,
      token: token,
      token_hash: token_hash,
      owner_id: owner_id,
      lease_until: lease_until,
      command_id: command_id,
      command: command
    }

    commit(store, ledger,
      command_id: command_id,
      operation: :claim,
      command: command,
      preconditions: [
        Precondition.command_absent(command_id),
        Precondition.stream_version(stream, stream_version),
        Precondition.intent_status(intent_id, [:available, :retry_scheduled])
      ],
      writes: [
        Write.new(:put_state,
          key: intent_id,
          value:
            merge_state(state,
              status: :claimed,
              attempt: state.attempt + 1,
              claim_id: claim_id,
              claim_token_hash: token_hash,
              lease_until: lease_until,
              updated_at: now
            )
        ),
        Write.put_claim(claim_id, %{
          intent_id: intent_id,
          owner_id: owner_id,
          token_hash: token_hash,
          lease_until: lease_until
        }),
        Write.append_signal(stream, signal),
        Write.put_idempotency(command_id, result)
      ]
    )
  end

  defp commit(store, ledger, attrs) do
    Bedrock.commit(store, ledger, CommitRequest.new(attrs), [])
  end

  defp stream_version!(store, ledger, stream) do
    case Bedrock.read(store, ledger, {:stream, stream, []}, []) do
      {:ok, %{version: version}} -> version
      {:error, reason} -> raise "failed to read stream #{inspect(stream)}: #{inspect(reason)}"
    end
  end

  defp state(intent_id, attrs, now, status) do
    %IntentState{
      intent_id: intent_id,
      queue: attrs |> Map.get(:queue, "default") |> to_string(),
      shard: Map.get(attrs, :shard, 0),
      status: status,
      visible_at: Map.get(attrs, :visible_at, now),
      priority: Map.get(attrs, :priority, 0),
      attempt: Map.get(attrs, :attempt, 0),
      max_attempts: Map.get(attrs, :max_attempts, 3),
      idempotency_key: Map.get(attrs, :idempotency_key),
      updated_at: now
    }
  end

  defp final_state(intent_id, attrs, now, status, extra) do
    intent_id
    |> state(attrs, now, status)
    |> merge_state(
      Keyword.merge(
        [
          claim_id: nil,
          claim_token_hash: nil,
          lease_until: nil,
          updated_at: now,
          completed_at: if(status == :completed, do: now)
        ],
        extra
      )
    )
  end

  defp merge_state(%IntentState{} = state, attrs), do: struct!(state, attrs)
  defp merge_state(state, attrs) when is_map(state), do: state |> struct!(attrs)

  defp signal(intent_id, transition, now, metadata) do
    %{
      id: unique_id("sig"),
      type: "intent_ledger.intent.#{transition}",
      subject: intent_id,
      time: now,
      metadata: metadata
    }
  end

  defp stream(intent_id), do: "intent:#{intent_id}"
  defp outbox_key(command_id), do: "out:#{command_id}"

  defp call(peer, module, function, args, timeout \\ @default_timeout) do
    PeerNodes.call(peer, module, function, args, timeout)
  end

  defp safe_call(peer, module, function, args, timeout) do
    call(peer, module, function, args, timeout)
  catch
    :exit, _reason -> :ok
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs

  defp unique_id(prefix), do: "#{prefix}_#{System.unique_integer([:positive])}"
  defp unique_store_name, do: :"intent_ledger_cross_node_store_#{System.unique_integer([:positive])}"
end
