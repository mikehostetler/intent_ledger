defmodule IntentLedger.Store.Bedrock do
  @moduledoc """
  Bedrock-backed durable store adapter.

  Bedrock is an optional dependency so Hex consumers can use the core package
  and the memory reference adapter without pulling in a Bedrock runtime. Projects
  that configure this adapter must include `:bedrock`; the adapter checks for
  the dependency at startup and returns a normalized
  `IntentLedger.Error.AdapterRuntimeError` when it is missing.
  """

  @behaviour IntentLedger.Store

  use GenServer

  alias IntentLedger.{Error, Store}
  alias IntentLedger.Store.{Commit, CommitRequest}
  alias IntentLedger.Store.Bedrock.{Keyspace, Value}

  @dependency :bedrock
  @required_modules [Bedrock, Bedrock.Repo]

  @type option ::
          {:name, GenServer.name()}
          | {:repo, term()}
          | {:cluster, term()}
          | {:keyspace, term()}
          | {:ledger, term()}
          | {:transaction_opts, keyword()}

  defstruct repo: nil,
            transaction_opts: []

  @type t :: %__MODULE__{
          repo: module(),
          transaction_opts: keyword()
        }

  @doc false
  @impl true
  @spec child_spec([option()]) :: Supervisor.child_spec()
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5000
    }
  end

  @doc false
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)

    with :ok <- ensure_available(),
         {:ok, repo} <- fetch_repo(opts) do
      GenServer.start_link(__MODULE__, %{repo: repo, transaction_opts: Keyword.get(opts, :transaction_opts, [])},
        name: name
      )
    end
  end

  @doc false
  @impl true
  @spec init(map()) :: {:ok, t()}
  def init(state), do: {:ok, struct!(__MODULE__, state)}

  @doc false
  @impl true
  @spec commit(Store.ref(), atom(), Store.CommitRequest.t(), keyword()) :: Store.commit_result()
  def commit(ref, ledger, %CommitRequest{} = request, opts), do: GenServer.call(ref, {:commit, ledger, request, opts})

  @doc false
  @impl true
  @spec read(Store.ref(), atom(), Store.read_request(), keyword()) :: Store.result()
  def read(_ref, _ledger, _request, _opts), do: unavailable()

  @doc false
  @impl true
  @spec lease(Store.ref(), atom(), Store.lease_request(), keyword()) :: Store.result()
  def lease(_ref, _ledger, _request, _opts), do: unavailable()

  @doc false
  @impl true
  @spec listing(Store.ref(), atom(), Store.listing_request(), keyword()) :: Store.result()
  def listing(_ref, _ledger, _request, _opts), do: unavailable()

  @doc false
  @impl true
  @spec outbox(Store.ref(), atom(), Store.outbox_request(), keyword()) :: Store.result()
  def outbox(_ref, _ledger, _request, _opts), do: unavailable()

  @impl true
  def handle_call({:commit, ledger, %CommitRequest{} = request, opts}, _from, %__MODULE__{} = state) do
    result =
      transact(
        state.repo,
        fn repo ->
          compile_commit(repo, ledger, request)
        end,
        transaction_opts(state, opts)
      )

    {:reply, normalize_transact_result(result), state}
  end

  @doc false
  @spec available?() :: boolean()
  def available?, do: ensure_available() == :ok

  @doc false
  @spec ensure_available([module()]) :: :ok | {:error, Exception.t()}
  def ensure_available(required_modules \\ @required_modules) when is_list(required_modules) do
    case Enum.reject(required_modules, &loaded?/1) do
      [] ->
        :ok

      missing_modules ->
        {:error,
         adapter_error("Bedrock dependency is required to use IntentLedger.Store.Bedrock",
           reason: :missing_dependency,
           dependency: @dependency,
           missing_modules: missing_modules
         )}
    end
  end

  defp unavailable do
    with :ok <- ensure_available() do
      {:error, adapter_error("Bedrock store adapter is not configured yet", reason: :not_configured)}
    end
  end

  defp fetch_repo(opts) do
    case Keyword.fetch(opts, :repo) do
      {:ok, repo} when is_atom(repo) ->
        {:ok, repo}

      {:ok, repo} ->
        {:error, adapter_error("Bedrock store repo must be a module", reason: :invalid_repo, repo: repo)}

      :error ->
        {:error, adapter_error("Bedrock store requires a :repo option", reason: :missing_repo)}
    end
  end

  defp transact(repo, fun, opts), do: apply(repo, :transact, [fun, opts])

  defp transaction_opts(%__MODULE__{} = state, opts) do
    Keyword.merge(state.transaction_opts, Keyword.get(opts, :transaction_opts, []))
  end

  defp normalize_transact_result({:ok, %Commit{} = commit}), do: {:ok, commit}
  defp normalize_transact_result({:error, reason}), do: {:error, reason}
  defp normalize_transact_result(:ok), do: {:ok, Commit.new()}

  defp normalize_transact_result(other),
    do: {:error, adapter_error("Bedrock transaction returned an invalid result", result: other)}

  defp compile_commit(_repo, _ledger, %CommitRequest{preconditions: [_ | _]} = request) do
    {:error,
     adapter_error("Bedrock commit preconditions are not implemented yet",
       reason: :unsupported_preconditions,
       preconditions: Enum.map(request.preconditions, & &1.type)
     )}
  end

  defp compile_commit(repo, ledger, %CommitRequest{} = request) do
    Enum.reduce_while(request.writes, {:ok, %{result: nil, signals: []}}, fn write, {:ok, acc} ->
      case compile_write(repo, ledger, request, write, acc) do
        {:ok, next_acc} -> {:cont, {:ok, next_acc}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, acc} ->
        {:ok,
         Commit.new(
           command_id: request.command_id,
           result: acc.result,
           signals: acc.signals,
           writes: request.writes
         )}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp compile_write(repo, ledger, _request, %{type: :put_intent, key: key, value: value}, acc) do
    intent_id = key || value.id
    put(repo, Keyspace.intent(ledger, intent_id), Value.pack_intent(value))
    {:ok, acc}
  end

  defp compile_write(repo, ledger, _request, %{type: :put_state, key: key, value: value}, acc) do
    put(repo, Keyspace.state(ledger, key), Value.pack_state(value))
    {:ok, acc}
  end

  defp compile_write(
         repo,
         ledger,
         _request,
         %{type: :append_signal, stream: stream, value: signal, metadata: metadata},
         acc
       ) do
    with {:ok, version} <- fetch_metadata_version(metadata) do
      put(repo, Keyspace.stream(ledger, stream, version), Value.pack_signal(signal))
      {:ok, %{acc | signals: acc.signals ++ [signal]}}
    end
  end

  defp compile_write(repo, ledger, request, %{type: :put_idempotency, key: command_id, value: result}, acc) do
    value = %{signature: command_signature(request), result: result}

    put(repo, Keyspace.command(ledger, command_id), Value.pack_command(value))
    {:ok, %{acc | result: result}}
  end

  defp compile_write(repo, ledger, _request, %{type: :put_claim, key: claim_id, value: value}, acc) do
    put(repo, Keyspace.claim(ledger, claim_id), Value.pack_claim(value))
    {:ok, acc}
  end

  defp compile_write(repo, ledger, _request, %{type: :delete_claim, key: claim_id}, acc) do
    clear(repo, Keyspace.claim(ledger, claim_id))
    {:ok, acc}
  end

  defp compile_write(repo, ledger, _request, %{type: :put_shard_lease, key: key, value: value}, acc) do
    with {:ok, queue, shard} <- parse_shard_key(key) do
      put(repo, Keyspace.shard_lease(ledger, queue, shard), Value.pack_shard_lease(value))
      {:ok, acc}
    end
  end

  defp compile_write(repo, ledger, _request, %{type: :delete_shard_lease, key: key}, acc) do
    with {:ok, queue, shard} <- parse_shard_key(key) do
      clear(repo, Keyspace.shard_lease(ledger, queue, shard))
      {:ok, acc}
    end
  end

  defp compile_write(repo, ledger, _request, %{type: :put_outbox, key: key, value: value}, acc) do
    with {:ok, sequence} <- fetch_outbox_sequence(value) do
      entry = Map.put_new(value, :key, key)

      put(repo, Keyspace.outbox(ledger, sequence), Value.pack_outbox(entry))
      {:ok, acc}
    end
  end

  defp compile_write(_repo, _ledger, _request, write, _acc) do
    {:error,
     adapter_error("Bedrock commit write is not implemented yet",
       reason: :unsupported_write,
       write_type: write.type
     )}
  end

  defp put(repo, key, value), do: apply(repo, :put, [key, value])
  defp clear(repo, key), do: apply(repo, :clear, [key])

  defp fetch_metadata_version(metadata) when is_map(metadata) do
    case Map.fetch(metadata, :version) do
      {:ok, version} when is_integer(version) and version >= 0 ->
        {:ok, version}

      _missing_or_invalid ->
        {:error, adapter_error("append_signal writes require metadata.version", reason: :missing_stream_version)}
    end
  end

  defp fetch_metadata_version(_metadata) do
    {:error, adapter_error("append_signal writes require metadata.version", reason: :missing_stream_version)}
  end

  defp fetch_outbox_sequence(value) when is_map(value) do
    case Map.fetch(value, :sequence) do
      {:ok, sequence} when is_integer(sequence) and sequence >= 0 ->
        {:ok, sequence}

      _missing_or_invalid ->
        {:error, adapter_error("put_outbox writes require value.sequence", reason: :missing_outbox_sequence)}
    end
  end

  defp fetch_outbox_sequence(_value) do
    {:error, adapter_error("put_outbox writes require value.sequence", reason: :missing_outbox_sequence)}
  end

  defp parse_shard_key("shard:" <> rest) do
    case String.split(rest, ":", parts: 2) do
      [queue, shard] ->
        parse_shard(queue, shard)

      _other ->
        {:error, adapter_error("invalid shard lease key", reason: :invalid_shard_key, key: "shard:" <> rest)}
    end
  end

  defp parse_shard_key(key),
    do: {:error, adapter_error("invalid shard lease key", reason: :invalid_shard_key, key: key)}

  defp parse_shard(queue, shard) do
    case Integer.parse(shard) do
      {shard, ""} when shard >= 0 ->
        {:ok, queue, shard}

      _invalid ->
        {:error,
         adapter_error("invalid shard lease key", reason: :invalid_shard_key, key: "shard:" <> queue <> ":" <> shard)}
    end
  end

  defp command_signature(%CommitRequest{} = request), do: {request.operation, request.command}

  defp loaded?(module), do: match?({:module, _module}, Code.ensure_loaded(module))

  defp adapter_error(message, details) do
    Error.adapter_runtime(message, Keyword.put(details, :adapter, __MODULE__))
  end
end
