defmodule IntentLedger.Store.Ecto do
  @moduledoc """
  Ecto/Postgres-backed local and single-node store adapter.

  Ecto SQL and Postgrex are optional dependencies so applications that use the
  memory or Bedrock adapters do not pull in an Ecto stack. Projects that
  configure this adapter must include `:ecto_sql` and `:postgrex`, and must pass
  an Ecto repo configured with the Postgres adapter.

  This adapter is intentionally scoped to local durable development and
  single-node deployments. SQL transactions provide atomicity within one Ecto
  repo, but this adapter does not coordinate multiple BEAM nodes and is not the
  clustered production backend for Intent Ledger. Use
  `IntentLedger.Store.Bedrock` for clustered deployments.

  Store V1 coverage is implemented incrementally. Unsupported callbacks return
  normalized `IntentLedger.Error.AdapterRuntimeError` values rather than leaking
  backend-specific failures.
  """

  @behaviour IntentLedger.Store

  use GenServer

  alias IntentLedger.{Error, Store, Time}
  alias IntentLedger.Store.{Commit, CommitRequest, Conflict, Outbox}
  alias IntentLedger.Store.Ecto.{Query, Schema}

  @dependencies [:ecto_sql, :postgrex]
  @postgres_adapter Module.concat([Ecto, Adapters, Postgres])
  @required_modules [
    Ecto,
    Ecto.Changeset,
    Ecto.Multi,
    Ecto.Query,
    Ecto.Schema,
    Ecto.Adapters.SQL,
    Ecto.Adapters.Postgres,
    Postgrex
  ]

  @type option ::
          {:name, GenServer.name()}
          | {:repo, module()}
          | {:prefix, String.t() | nil}
          | {:tables, keyword() | map()}

  defstruct repo: nil,
            prefix: nil,
            tables: %{}

  @type t :: %__MODULE__{
          repo: module(),
          prefix: String.t() | nil,
          tables: map()
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
      GenServer.start_link(
        __MODULE__,
        %{repo: repo, prefix: Keyword.get(opts, :prefix), tables: Keyword.get(opts, :tables, %{})},
        name: name
      )
    end
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
         adapter_error("Ecto SQL and Postgrex dependencies are required to use IntentLedger.Store.Ecto",
           reason: :missing_dependency,
           dependencies: @dependencies,
           missing_modules: missing_modules
         )}
    end
  end

  @doc false
  @impl true
  @spec init(map()) :: {:ok, t()}
  def init(state), do: {:ok, struct!(__MODULE__, state)}

  @doc false
  @impl true
  @spec commit(Store.ref(), atom(), CommitRequest.t(), keyword()) :: Store.commit_result()
  def commit(ref, ledger, %CommitRequest{} = request, opts), do: GenServer.call(ref, {:commit, ledger, request, opts})

  @doc false
  @impl true
  @spec read(Store.ref(), atom(), Store.read_request(), keyword()) :: Store.result()
  def read(ref, ledger, request, opts), do: GenServer.call(ref, {:read, ledger, request, opts})

  @doc false
  @impl true
  @spec lease(Store.ref(), atom(), Store.lease_request(), keyword()) :: Store.result()
  def lease(ref, ledger, request, opts), do: GenServer.call(ref, {:lease, ledger, request, opts})

  @doc false
  @impl true
  @spec listing(Store.ref(), atom(), Store.listing_request(), keyword()) :: Store.result()
  def listing(ref, ledger, request, opts), do: GenServer.call(ref, {:listing, ledger, request, opts})

  @doc false
  @impl true
  @spec outbox(Store.ref(), atom(), Store.outbox_request(), keyword()) :: Store.result()
  def outbox(ref, ledger, request, opts), do: GenServer.call(ref, {:outbox, ledger, request, opts})

  @impl true
  def handle_call({:commit, ledger, %CommitRequest{} = request, opts}, _from, %__MODULE__{} = state) do
    result =
      transact(state.repo, transaction_opts(opts), fn ->
        apply_commit(state, ledger, request)
      end)

    {:reply, result, state}
  end

  def handle_call({operation, _ledger, request, _opts}, _from, %__MODULE__{} = state)
      when operation in [:read, :lease, :listing, :outbox] do
    {:reply, not_implemented(operation, request), state}
  end

  defp fetch_repo(opts) do
    case Keyword.fetch(opts, :repo) do
      {:ok, repo} when is_atom(repo) ->
        validate_repo(repo)

      {:ok, repo} ->
        {:error, adapter_error("Ecto store repo must be a module", reason: :invalid_repo, repo: repo)}

      :error ->
        {:error, adapter_error("Ecto store requires a :repo option", reason: :missing_repo)}
    end
  end

  defp validate_repo(repo) do
    cond do
      not loaded?(repo) ->
        {:error, adapter_error("Ecto store repo module is not available", reason: :invalid_repo, repo: repo)}

      not function_exported?(repo, :__adapter__, 0) ->
        {:error, adapter_error("Ecto store repo must expose an Ecto adapter", reason: :invalid_repo, repo: repo)}

      repo.__adapter__() != @postgres_adapter ->
        {:error,
         adapter_error("Ecto store requires a Postgres repo",
           reason: :unsupported_repo_adapter,
           repo: repo,
           repo_adapter: repo.__adapter__()
         )}

      true ->
        {:ok, repo}
    end
  end

  defp transact(repo, opts, fun) do
    case repo.transaction(fun, opts) do
      {:ok, %Commit{} = commit} ->
        {:ok, commit}

      {:ok, {:error, reason}} ->
        {:error, reason}

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error,
         adapter_error("Ecto transaction returned an invalid result",
           reason: :invalid_transaction_result,
           result: other
         )}
    end
  catch
    kind, reason -> {:error, adapter_error("Ecto transaction failed", reason: {kind, reason})}
  end

  defp apply_commit(%__MODULE__{} = state, ledger, %CommitRequest{} = request) do
    now = Time.utc_now()
    opts = source_opts(state)

    with :ok <- check_preconditions(state, ledger, request, opts) do
      apply_writes(state, ledger, request, now, opts)
    else
      {:replay, entry} ->
        Commit.new(
          command_id: request.command_id,
          result: Map.get(entry, :result),
          replayed: true,
          replay_of: request.command_id
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp check_preconditions(%__MODULE__{} = state, ledger, %CommitRequest{} = request, opts) do
    Enum.reduce_while(request.preconditions, :ok, fn precondition, :ok ->
      case check_precondition(state, ledger, request, precondition, opts) do
        :ok -> {:cont, :ok}
        {:replay, entry} -> {:halt, {:replay, entry}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp check_precondition(state, ledger, %CommitRequest{}, %{type: :command_absent, key: command_id}, opts) do
    case fetch_command(state, ledger, command_id, opts) do
      {:ok, nil} -> :ok
      {:ok, _entry} -> {:error, Conflict.command_conflict(command_id, :absent, :present)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp check_precondition(state, ledger, %CommitRequest{} = request, %{type: :command_replay, key: command_id}, opts) do
    case fetch_command(state, ledger, command_id, opts) do
      {:ok, nil} ->
        {:error, Conflict.new(:command_replay, key: command_id, expected: :present, actual: :absent)}

      {:ok, entry} ->
        if Map.get(entry, :signature) == command_signature(request) do
          {:replay, entry}
        else
          {:error, Conflict.command_conflict(command_id, Map.get(entry, :signature), command_signature(request))}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp check_precondition(
         state,
         ledger,
         %CommitRequest{},
         %{type: :stream_version, stream: stream, expected: expected},
         opts
       ) do
    actual = stream_version(state, ledger, stream, opts)

    if actual == expected do
      :ok
    else
      {:error, Conflict.stream_version(stream, expected, actual)}
    end
  end

  defp check_precondition(_state, _ledger, _request, precondition, _opts) do
    {:error,
     adapter_error("Ecto commit precondition is not implemented yet",
       reason: :unsupported_precondition,
       precondition_type: precondition.type
     )}
  end

  defp apply_writes(%__MODULE__{} = state, ledger, %CommitRequest{} = request, now, opts) do
    request.writes
    |> Enum.reduce_while({nil, [], %{}}, fn write, {result, signals, stream_versions} ->
      case apply_write(state, ledger, write, request, now, opts, stream_versions) do
        {:ok, nil, next_signals, next_stream_versions} ->
          {:cont, {result, signals ++ next_signals, next_stream_versions}}

        {:ok, next_result, next_signals, next_stream_versions} ->
          {:cont, {next_result, signals ++ next_signals, next_stream_versions}}

        {:error, reason} ->
          {:halt, rollback(state.repo, reason)}
      end
    end)
    |> case do
      {:error, reason} ->
        {:error, reason}

      {result, signals, _stream_versions} ->
        Commit.new(
          command_id: request.command_id,
          result: result,
          signals: signals,
          writes: request.writes,
          metadata: request.metadata
        )
    end
  end

  defp apply_write(state, ledger, %{type: :put_intent, key: intent_id, value: intent}, _request, now, _opts, versions) do
    row =
      %{
        ledger: ledger_key(ledger),
        intent_id: intent_id,
        intent: encode_value(intent)
      }
      |> put_timestamps(now)

    insert_all(state, :intents, [row], on_conflict: :nothing, conflict_target: [:ledger, :intent_id])
    {:ok, nil, [], versions}
  end

  defp apply_write(
         state,
         ledger,
         %{type: :put_state, key: intent_id, value: intent_state},
         _request,
         now,
         _opts,
         versions
       ) do
    row =
      intent_state
      |> state_row(ledger, intent_id)
      |> put_timestamps(now)

    insert_all(state, :states, [row],
      on_conflict:
        {:replace,
         [
           :status,
           :queue,
           :shard,
           :priority,
           :visible_at,
           :attempt,
           :claim_id,
           :token_hash,
           :lease_until,
           :state,
           :updated_at
         ]},
      conflict_target: [:ledger, :intent_id]
    )

    {:ok, nil, [], versions}
  end

  defp apply_write(
         state,
         ledger,
         %{type: :append_signal, stream: stream, value: signal, metadata: metadata},
         request,
         now,
         _opts,
         versions
       ) do
    with {:ok, version} <- next_stream_version(stream, metadata, request, versions) do
      signal_row =
        %{
          ledger: ledger_key(ledger),
          stream: stream,
          version: version,
          signal: encode_value(signal)
        }
        |> put_timestamps(now)

      stream_row =
        %{
          ledger: ledger_key(ledger),
          stream: stream,
          version: version
        }
        |> put_timestamps(now)

      insert_all(state, :streams, [stream_row],
        on_conflict: {:replace, [:version, :updated_at]},
        conflict_target: [:ledger, :stream]
      )

      insert_all(state, :signals, [signal_row], on_conflict: :nothing, conflict_target: [:ledger, :stream, :version])

      {:ok, nil, [signal], Map.put(versions, stream, version)}
    end
  end

  defp apply_write(
         state,
         ledger,
         %{type: :put_idempotency, key: command_id, value: result},
         request,
         now,
         _opts,
         versions
       ) do
    row =
      %{
        ledger: ledger_key(ledger),
        command_id: command_id,
        operation: operation_key(request.operation),
        command: encode_value(request.command),
        result: encode_value(result)
      }
      |> put_timestamps(now)

    insert_all(state, :commands, [row], on_conflict: :nothing, conflict_target: [:ledger, :command_id])
    {:ok, result, [], versions}
  end

  defp apply_write(state, ledger, %{type: :put_claim, key: claim_id, value: claim}, _request, now, _opts, versions) do
    row =
      claim
      |> claim_row(ledger, claim_id)
      |> put_timestamps(now)

    insert_all(state, :claims, [row],
      on_conflict: {:replace, [:intent_id, :owner_id, :token_hash, :lease_until, :claim, :updated_at]},
      conflict_target: [:ledger, :claim_id]
    )

    {:ok, nil, [], versions}
  end

  defp apply_write(state, ledger, %{type: :delete_claim, key: claim_id}, _request, _now, opts, versions) do
    state.repo.delete_all(Query.by_fields(:claims, ledger, [claim_id: claim_id], opts), [])
    {:ok, nil, [], versions}
  end

  defp apply_write(state, ledger, %{type: :put_shard_lease, key: key, value: lease}, _request, now, _opts, versions) do
    row =
      lease
      |> shard_lease_row(ledger, key)
      |> put_timestamps(now)

    insert_all(state, :shard_leases, [row],
      on_conflict: {:replace, [:owner_id, :lease_until, :lease, :updated_at]},
      conflict_target: [:ledger, :queue, :shard]
    )

    {:ok, nil, [], versions}
  end

  defp apply_write(state, ledger, %{type: :delete_shard_lease, key: key}, _request, _now, opts, versions) do
    {queue, shard} = parse_shard_key(key)
    state.repo.delete_all(Query.by_fields(:shard_leases, ledger, [queue: queue, shard: shard], opts), [])
    {:ok, nil, [], versions}
  end

  defp apply_write(state, ledger, %{type: :put_outbox, key: key, value: entry}, _request, now, _opts, versions) do
    row =
      entry
      |> outbox_row(ledger, key, next_outbox_sequence(entry, versions))
      |> put_timestamps(now)

    insert_all(state, :outbox, [row], on_conflict: :nothing, conflict_target: [:ledger, :key])
    {:ok, nil, [], Map.update(versions, :outbox_sequence, row.sequence, &max(&1, row.sequence))}
  end

  defp apply_write(state, ledger, %{type: :ack_outbox, key: key, metadata: metadata}, _request, now, opts, versions) do
    fields = [
      acked_at: Map.get(metadata, :acked_at, now),
      consumer: Map.get(metadata, :consumer),
      metadata: encode_value(metadata),
      updated_at: now
    ]

    state.repo.update_all(Query.by_fields(:outbox, ledger, [key: key], opts), set: fields)
    {:ok, nil, [], versions}
  end

  defp apply_write(_state, _ledger, write, _request, _now, _opts, _versions) do
    {:error, adapter_error("Ecto commit write is not implemented yet", reason: :unsupported_write, write: write)}
  end

  defp not_implemented(operation, request) do
    {:error,
     adapter_error("Ecto store operation is not implemented yet",
       reason: :not_implemented,
       operation: operation,
       request: compact_request(request)
     )}
  end

  defp insert_all(%__MODULE__{} = state, table, rows, opts) do
    state.repo.insert_all(Schema.source(table, source_opts(state)), rows, Keyword.put(opts, :prefix, state.prefix))
  end

  defp source_opts(%__MODULE__{} = state), do: [prefix: state.prefix, tables: state.tables]

  defp transaction_opts(opts), do: Keyword.get(opts, :transaction_opts, [])

  defp rollback(repo, reason) do
    if function_exported?(repo, :rollback, 1) do
      repo.rollback(reason)
    else
      {:error, reason}
    end
  end

  defp fetch_command(%__MODULE__{} = state, ledger, command_id, opts) do
    case state.repo.one(Query.by_fields(:commands, ledger, [command_id: command_id], opts)) do
      nil ->
        {:ok, nil}

      row ->
        {:ok, %{signature: command_signature(row), result: field(row, :result)}}
    end
  rescue
    error ->
      {:error, adapter_error("invalid Ecto command replay value", reason: error)}
  end

  defp stream_version(%__MODULE__{} = state, ledger, stream, opts) do
    case state.repo.one(Query.by_fields(:streams, ledger, [stream: stream], opts)) do
      nil -> 0
      row -> field(row, :version, 0)
    end
  end

  defp next_stream_version(stream, metadata, request, versions) do
    case metadata_version(metadata) do
      version when is_integer(version) ->
        {:ok, version}

      nil ->
        cond do
          is_integer(Map.get(versions, stream)) ->
            {:ok, Map.fetch!(versions, stream) + 1}

          expected = stream_precondition(request, stream) ->
            {:ok, expected + 1}

          true ->
            {:error,
             adapter_error("append_signal writes require metadata.version or stream_version precondition",
               reason: :missing_stream_version
             )}
        end
    end
  end

  defp metadata_version(%{} = metadata) do
    case Map.get(metadata, :version) do
      version when is_integer(version) and version >= 0 -> version
      _missing_or_invalid -> nil
    end
  end

  defp metadata_version(_metadata), do: nil

  defp stream_precondition(%CommitRequest{} = request, stream) do
    Enum.find_value(request.preconditions, fn
      %{type: :stream_version, stream: ^stream, expected: expected} when is_integer(expected) -> expected
      _precondition -> nil
    end)
  end

  defp command_signature(%CommitRequest{} = request) do
    {operation_key(request.operation), canonical_value(request.command)}
  end

  defp command_signature(row) do
    {operation_key(field(row, :operation)), canonical_value(field(row, :command))}
  end

  defp operation_key(operation) when is_binary(operation), do: operation
  defp operation_key(operation) when is_atom(operation), do: Atom.to_string(operation)
  defp operation_key(operation), do: to_string(operation)

  defp state_row(intent_state, ledger, intent_id) do
    %{
      ledger: ledger_key(ledger),
      intent_id: intent_id,
      status: field(intent_state, :status) |> to_string(),
      queue: field(intent_state, :queue, "default") |> to_string(),
      shard: field(intent_state, :shard, 0),
      priority: field(intent_state, :priority, 0),
      visible_at: field(intent_state, :visible_at),
      attempt: field(intent_state, :attempt, 0),
      claim_id: field(intent_state, :claim_id),
      token_hash: field(intent_state, :token_hash),
      lease_until: field(intent_state, :lease_until),
      state: encode_value(intent_state)
    }
  end

  defp claim_row(claim, ledger, claim_id) do
    %{
      ledger: ledger_key(ledger),
      claim_id: claim_id,
      intent_id: field(claim, :intent_id),
      owner_id: field(claim, :owner_id),
      token_hash: field(claim, :token_hash),
      lease_until: field(claim, :lease_until),
      claim: encode_value(claim)
    }
  end

  defp shard_lease_row(lease, ledger, key) do
    {queue, shard} = parse_shard_key(key)

    %{
      ledger: ledger_key(ledger),
      queue: field(lease, :queue, queue) |> to_string(),
      shard: field(lease, :shard, shard),
      owner_id: field(lease, :owner_id),
      lease_until: field(lease, :lease_until),
      lease: encode_value(lease)
    }
  end

  defp outbox_row(entry, ledger, key, sequence) do
    signal = field(entry, :signal, %{})

    %{
      ledger: ledger_key(ledger),
      key: key,
      sequence: sequence,
      stream: field(entry, :stream),
      signal_id: field(entry, :signal_id) || field(signal, :id),
      signal_type: field(entry, :signal_type) || field(signal, :type),
      subject: field(entry, :subject) || field(signal, :subject),
      signal: encode_value(signal),
      entry: encode_value(entry),
      acked_at: field(entry, :acked_at),
      consumer: field(entry, :consumer),
      metadata: encode_value(field(entry, :metadata))
    }
  end

  defp next_outbox_sequence(entry, versions) do
    case field(entry, :sequence) do
      sequence when is_integer(sequence) and sequence > 0 -> sequence
      _missing -> Map.get(versions, :outbox_sequence, 0) + 1
    end
  end

  defp put_timestamps(row, now), do: Map.merge(row, %{inserted_at: now, updated_at: now})

  defp encode_value(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp encode_value(%{} = value) when not is_struct(value) do
    Map.new(value, fn {key, nested_value} -> {key, encode_value(nested_value)} end)
  end

  defp encode_value(%_struct{} = value) do
    value
    |> Map.from_struct()
    |> encode_value()
  end

  defp encode_value(values) when is_list(values), do: Enum.map(values, &encode_value/1)
  defp encode_value(value), do: value

  defp canonical_value(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp canonical_value(%{} = value) when not is_struct(value) do
    Map.new(value, fn {key, nested_value} -> {to_string(key), canonical_value(nested_value)} end)
  end

  defp canonical_value(%_struct{} = value) do
    value
    |> Map.from_struct()
    |> canonical_value()
  end

  defp canonical_value(values) when is_list(values), do: Enum.map(values, &canonical_value/1)
  defp canonical_value(value), do: value

  defp field(value, key, default \\ nil)
  defp field(nil, _key, default), do: default
  defp field(%{} = value, key, default), do: Map.get(value, key, Map.get(value, Atom.to_string(key), default))
  defp field(value, key, default) when is_struct(value), do: value |> Map.from_struct() |> field(key, default)
  defp field(_value, _key, default), do: default

  defp ledger_key(ledger), do: ledger |> inspect() |> String.trim_leading("Elixir.")

  defp parse_shard_key("shard:" <> rest) do
    case String.split(rest, ":", parts: 2) do
      [queue, shard] -> {queue, String.to_integer(shard)}
      _invalid -> {rest, 0}
    end
  end

  defp parse_shard_key(_key), do: {"default", 0}

  defp compact_request(%CommitRequest{} = request), do: %{operation: request.operation, command_id: request.command_id}
  defp compact_request(%Outbox{} = request), do: %{type: request.type, key: request.key, consumer: request.consumer}
  defp compact_request(request), do: request

  defp loaded?(module), do: match?({:module, _module}, Code.ensure_loaded(module))

  defp adapter_error(message, details) do
    Error.adapter_runtime(message, Keyword.put(details, :adapter, __MODULE__))
  end
end
