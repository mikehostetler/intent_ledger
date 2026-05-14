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

  alias IntentLedger.{Error, IntentState, Store, Telemetry}
  alias IntentLedger.Store.{Commit, CommitRequest, Conflict, Lineage, Listing, Outbox}
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
  def commit(ref, ledger, %CommitRequest{} = request, opts) do
    Telemetry.instrument_store_commit(opts, ledger, __MODULE__, request, fn ->
      GenServer.call(ref, {:commit, ledger, request, opts})
    end)
  end

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
      transact(
        state.repo,
        fn repo ->
          compile_commit(repo, ledger, request)
        end,
        transaction_opts(state, opts)
      )

    {:reply, normalize_transact_result(result), state}
  end

  @impl true
  def handle_call({:read, ledger, {:lineage_counts, attrs}, opts}, _from, %__MODULE__{} = state)
      when is_map(attrs) or is_list(attrs) do
    result =
      transact(
        state.repo,
        fn repo ->
          read_lineage_counts(repo, ledger, attrs)
        end,
        transaction_opts(state, opts)
      )

    {:reply, normalize_transact_result(result), state}
  end

  def handle_call({:read, ledger, {:stream, stream, read_opts}, opts}, _from, %__MODULE__{} = state) do
    result =
      transact(
        state.repo,
        fn repo ->
          read_stream(repo, ledger, stream, read_opts)
        end,
        transaction_opts(state, opts)
      )

    {:reply, normalize_transact_result(result), state}
  end

  def handle_call({:read, _ledger, request, _opts}, _from, %__MODULE__{} = state) do
    {:reply, {:error, {:unsupported_store_v1_request, :read, request}}, state}
  end

  @impl true
  def handle_call({:listing, ledger, request, opts}, _from, %__MODULE__{} = state) do
    result =
      transact(
        state.repo,
        fn repo ->
          apply_listing_request(repo, ledger, request)
        end,
        transaction_opts(state, opts)
      )

    {:reply, normalize_transact_result(result), state}
  end

  @impl true
  def handle_call({:lease, ledger, request, opts}, _from, %__MODULE__{} = state) do
    result =
      transact(
        state.repo,
        fn repo ->
          apply_lease_request(repo, ledger, request)
        end,
        transaction_opts(state, opts)
      )

    {:reply, normalize_transact_result(result), state}
  end

  @impl true
  def handle_call({:outbox, ledger, request, opts}, _from, %__MODULE__{} = state) do
    result =
      transact(
        state.repo,
        fn repo ->
          apply_outbox_request(repo, ledger, request)
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
  defp normalize_transact_result({:ok, result}), do: {:ok, result}
  defp normalize_transact_result({:error, reason}), do: {:error, reason}
  defp normalize_transact_result(:ok), do: {:ok, Commit.new()}

  defp normalize_transact_result(other),
    do: {:error, adapter_error("Bedrock transaction returned an invalid result", result: other)}

  defp compile_commit(repo, ledger, %CommitRequest{} = request) do
    case check_preconditions(repo, ledger, request) do
      :ok ->
        compile_writes(repo, ledger, request)

      {:replay, entry} ->
        {:ok,
         Commit.new(
           command_id: request.command_id,
           result: Map.get(entry, :result),
           replayed: true,
           replay_of: request.command_id
         )}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp check_preconditions(repo, ledger, %CommitRequest{} = request) do
    Enum.reduce_while(request.preconditions, :ok, fn precondition, :ok ->
      case check_precondition(repo, ledger, request, precondition) do
        :ok -> {:cont, :ok}
        {:replay, entry} -> {:halt, {:replay, entry}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp check_precondition(repo, ledger, %CommitRequest{}, %{type: :command_absent, key: command_id}) do
    case fetch_command(repo, ledger, command_id) do
      {:ok, nil} -> :ok
      {:ok, _entry} -> {:error, Conflict.command_conflict(command_id, :absent, :present)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp check_precondition(repo, ledger, %CommitRequest{} = request, %{type: :command_replay, key: command_id}) do
    case fetch_command(repo, ledger, command_id) do
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

  defp check_precondition(repo, ledger, _request, %{type: :stream_version, stream: stream, expected: expected}) do
    actual = stream_version(repo, ledger, stream)

    if actual == expected do
      :ok
    else
      {:error, Conflict.stream_version(stream, expected, actual)}
    end
  end

  defp check_precondition(repo, ledger, _request, %{type: :intent_status, key: intent_id, expected: expected}) do
    state_key = Keyspace.state(ledger, intent_id)
    add_read_conflict_key(repo, state_key)

    case fetch_state(repo, ledger, intent_id) do
      {:ok, nil} ->
        {:error, Conflict.intent_status(intent_id, expected, :missing)}

      {:ok, state} ->
        actual = state.status

        if actual in expected do
          :ok
        else
          {:error, Conflict.intent_status(intent_id, expected, actual)}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp check_precondition(repo, ledger, _request, %{
         type: :claim_fence,
         key: claim_id,
         expected: expected,
         metadata: metadata
       }) do
    claim_fence_conflict(repo, ledger, claim_id, expected, metadata)
  end

  defp check_precondition(repo, ledger, _request, %{
         type: :shard_lease,
         key: key,
         expected: expected,
         metadata: metadata
       }) do
    shard_lease_check(repo, ledger, key, expected, metadata)
  end

  defp check_precondition(repo, ledger, _request, %{type: :outbox_unacked, key: entry_id}) do
    outbox_unacked_check(repo, ledger, entry_id)
  end

  defp check_precondition(_repo, _ledger, _request, precondition) do
    {:error,
     adapter_error("Bedrock commit precondition is not implemented yet",
       reason: :unsupported_precondition,
       precondition_type: precondition.type
     )}
  end

  defp compile_writes(repo, ledger, %CommitRequest{} = request) do
    initial_acc = %{result: nil, signals: [], stream_versions: stream_precondition_versions(request)}

    Enum.reduce_while(request.writes, {:ok, initial_acc}, fn write, {:ok, acc} ->
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
    with :ok <- clear_previous_queue_index(repo, ledger, key),
         :ok <- put_state_value(repo, ledger, key, value),
         :ok <- put_current_queue_index(repo, ledger, value) do
      {:ok, acc}
    end
  end

  defp compile_write(
         repo,
         ledger,
         _request,
         %{type: :append_signal, stream: stream, value: signal, metadata: metadata},
         acc
       ) do
    with {:ok, version} <- next_stream_version(stream, metadata, acc) do
      put(repo, Keyspace.stream(ledger, stream, version), Value.pack_signal(signal))
      {:ok, %{acc | signals: acc.signals ++ [signal], stream_versions: Map.put(acc.stream_versions, stream, version)}}
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
    with {:ok, _entry} <- put_outbox_entry(repo, ledger, key, value) do
      {:ok, acc}
    end
  end

  defp compile_write(repo, ledger, _request, %{type: :ack_outbox, key: key, metadata: metadata}, acc) do
    with {:ok, _entry} <- ack_outbox_entry(repo, ledger, key, metadata) do
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
  defp get(repo, key), do: apply(repo, :get, [key])
  defp get_range(repo, range), do: apply(repo, :get_range, [range])
  defp clear(repo, key), do: apply(repo, :clear, [key])
  defp add_read_conflict_key(repo, key), do: apply(repo, :add_read_conflict_key, [key])
  defp add_write_conflict_range(repo, range), do: apply(repo, :add_write_conflict_range, [range])

  defp read_lineage_counts(repo, ledger, attrs) do
    with {:ok, intents} <- read_intents(repo, ledger),
         {:ok, states} <- read_states(repo, ledger) do
      {:ok, Lineage.counts(intents, states, attrs)}
    end
  end

  defp read_intents(repo, ledger) do
    repo
    |> get_range(Keyspace.table_range(ledger, :intent))
    |> Enum.sort_by(fn {key, _value} -> key end)
    |> Enum.reduce_while({:ok, []}, fn {_key, encoded}, {:ok, intents} ->
      case Value.unpack_intent(encoded) do
        {:ok, intent} -> {:cont, {:ok, intents ++ [intent]}}
        {:error, reason} -> {:halt, {:error, adapter_error("invalid Bedrock intent value", reason: reason)}}
      end
    end)
  end

  defp read_states(repo, ledger) do
    repo
    |> get_range(Keyspace.table_range(ledger, :state))
    |> decode_state_rows("invalid Bedrock state value")
  end

  defp read_stream(repo, ledger, stream, opts) do
    entries = stream_entries(repo, ledger, stream)

    with {:ok, signals} <- decode_stream_signals(entries) do
      {:ok, %{stream: stream, version: length(signals), signals: window(signals, opts)}}
    end
  end

  defp stream_version(repo, ledger, stream), do: repo |> stream_entries(ledger, stream) |> length()

  defp stream_entries(repo, ledger, stream) do
    repo
    |> get_range(Keyspace.stream_range(ledger, stream))
    |> Enum.sort_by(fn {key, _value} -> key end)
  end

  defp decode_stream_signals(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn {_key, encoded}, {:ok, signals} ->
      case Value.unpack_signal(encoded) do
        {:ok, signal} -> {:cont, {:ok, signals ++ [signal]}}
        {:error, reason} -> {:halt, {:error, adapter_error("invalid Bedrock stream signal value", reason: reason)}}
      end
    end)
  end

  defp window(values, opts) do
    opts = normalize_attrs(opts)
    cursor = opts |> Map.get(:cursor, 0) |> non_negative_or(0)
    limit = opts |> Map.get(:limit, length(values)) |> positive_or(length(values))

    values
    |> Enum.drop(cursor)
    |> Enum.take(limit)
  end

  defp non_negative_or(value, _default) when is_integer(value) and value >= 0, do: value
  defp non_negative_or(_value, default), do: default

  defp positive_or(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_or(_value, default), do: default

  defp clear_previous_queue_index(repo, ledger, intent_id) do
    with {:ok, previous_state} <- fetch_state(repo, ledger, intent_id) do
      clear_queue_index(repo, ledger, previous_state)
    end
  end

  defp put_state_value(repo, ledger, intent_id, %IntentState{} = state) do
    put(repo, Keyspace.state(ledger, intent_id), Value.pack_state(state))
  end

  defp put_state_value(repo, ledger, intent_id, value) when is_map(value) do
    put_state_value(repo, ledger, intent_id, struct!(IntentState, value))
  end

  defp put_current_queue_index(repo, ledger, %IntentState{} = state) do
    if queue_indexed_state?(state) do
      put(repo, queue_index_key(ledger, state), Value.pack_state(state))
    else
      :ok
    end
  end

  defp put_current_queue_index(repo, ledger, value) when is_map(value) do
    put_current_queue_index(repo, ledger, struct!(IntentState, value))
  end

  defp clear_queue_index(_repo, _ledger, nil), do: :ok

  defp clear_queue_index(repo, ledger, %IntentState{} = state) do
    if queue_indexed_state?(state) do
      clear(repo, queue_index_key(ledger, state))
    else
      :ok
    end
  end

  defp queue_indexed_state?(%IntentState{status: status, visible_at: %DateTime{}})
       when status in [:available, :retry_scheduled],
       do: true

  defp queue_indexed_state?(%IntentState{status: status, visible_at: nil})
       when status in [:available, :retry_scheduled] do
    raise adapter_error("queue-indexed states require visible_at", reason: :missing_visible_at)
  end

  defp queue_indexed_state?(%IntentState{}), do: false

  defp queue_index_key(ledger, %IntentState{} = state) do
    Keyspace.queue(
      ledger,
      state.queue,
      state.shard,
      state.visible_at,
      state.priority,
      state.intent_id
    )
  end

  defp fetch_command(repo, ledger, command_id) do
    case get(repo, Keyspace.command(ledger, command_id)) do
      nil ->
        {:ok, nil}

      encoded ->
        case Value.unpack_command(encoded) do
          {:ok, entry} -> {:ok, entry}
          {:error, reason} -> {:error, adapter_error("invalid Bedrock command replay value", reason: reason)}
        end
    end
  end

  defp fetch_state(repo, ledger, intent_id) do
    case get(repo, Keyspace.state(ledger, intent_id)) do
      nil ->
        {:ok, nil}

      encoded ->
        case Value.unpack_state(encoded) do
          {:ok, state} -> {:ok, state}
          {:error, reason} -> {:error, adapter_error("invalid Bedrock state value", reason: reason)}
        end
    end
  end

  defp fetch_claim(repo, ledger, claim_id) do
    case get(repo, Keyspace.claim(ledger, claim_id)) do
      nil ->
        {:ok, nil}

      encoded ->
        case Value.unpack_claim(encoded) do
          {:ok, claim} -> {:ok, claim}
          {:error, reason} -> {:error, adapter_error("invalid Bedrock claim value", reason: reason)}
        end
    end
  end

  defp fetch_shard_lease(repo, ledger, key) do
    with {:ok, queue, shard} <- parse_shard_key(key) do
      case get(repo, Keyspace.shard_lease(ledger, queue, shard)) do
        nil ->
          {:ok, nil}

        encoded ->
          case Value.unpack_shard_lease(encoded) do
            {:ok, lease} -> {:ok, lease}
            {:error, reason} -> {:error, adapter_error("invalid Bedrock shard lease value", reason: reason)}
          end
      end
    end
  end

  defp shard_lease_check(repo, ledger, key, expected, metadata) do
    with {:ok, queue, shard} <- parse_shard_key(key) do
      lease_key = Keyspace.shard_lease(ledger, queue, shard)
      add_read_conflict_key(repo, lease_key)

      case fetch_shard_lease(repo, ledger, key) do
        {:ok, lease} ->
          if shard_lease_matches?(lease, expected, metadata) do
            :ok
          else
            {:error,
             Conflict.new(:shard_lease,
               key: key,
               expected: expected,
               actual: lease || :missing,
               message: "shard lease conflict"
             )}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp shard_lease_matches?(lease, %{available_at: now}, _metadata), do: is_nil(lease) or lease_expired?(lease, now)

  defp shard_lease_matches?(lease, %{expired_at_or_before: now}, _metadata),
    do: not is_nil(lease) and lease_expired?(lease, now)

  defp shard_lease_matches?(lease, %{status: :current, owner_id: owner_id}, metadata) do
    not is_nil(lease) and value_field(lease, :owner_id) == owner_id and lease_current?(lease, Map.get(metadata, :now))
  end

  defp shard_lease_matches?(_lease, _expected, _metadata), do: false

  defp lease_expired?(_lease, nil), do: false

  defp lease_expired?(lease, %DateTime{} = now) do
    case value_field(lease, :lease_until) do
      %DateTime{} = lease_until -> DateTime.compare(lease_until, now) != :gt
      _missing_or_invalid -> false
    end
  end

  defp apply_lease_request(repo, ledger, {:shard, operation, attrs}) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    with {:ok, queue} <- fetch_attr(attrs, :queue),
         {:ok, shard} <- fetch_attr(attrs, :shard),
         {:ok, key} <- {:ok, shard_key(queue, shard)} do
      do_apply_lease_request(repo, ledger, operation, key, attrs)
    else
      _missing_or_invalid -> {:error, {:unsupported_store_v1_request, :lease, {:shard, operation, attrs}}}
    end
  end

  defp apply_lease_request(_repo, _ledger, request), do: {:error, {:unsupported_store_v1_request, :lease, request}}

  defp do_apply_lease_request(repo, ledger, :acquire, key, attrs) do
    expected = %{available_at: Map.get(attrs, :now)}

    with :ok <- shard_lease_check(repo, ledger, key, expected, %{}) do
      lease = shard_lease_value(attrs)
      put_shard_lease(repo, ledger, key, lease)
      {:ok, lease}
    end
  end

  defp do_apply_lease_request(repo, ledger, :renew, key, attrs) do
    expected = %{owner_id: to_string(Map.fetch!(attrs, :owner_id)), status: :current}

    with :ok <- shard_lease_check(repo, ledger, key, expected, %{now: Map.get(attrs, :now)}) do
      lease = shard_lease_value(attrs)
      put_shard_lease(repo, ledger, key, lease)
      {:ok, lease}
    end
  end

  defp do_apply_lease_request(repo, ledger, :release, key, attrs) do
    expected = %{owner_id: to_string(Map.fetch!(attrs, :owner_id)), status: :current}

    with :ok <- shard_lease_check(repo, ledger, key, expected, %{now: Map.get(attrs, :now)}),
         {:ok, lease} <- fetch_shard_lease(repo, ledger, key) do
      clear_shard_lease(repo, ledger, key)
      {:ok, lease}
    end
  end

  defp do_apply_lease_request(repo, ledger, :expire, key, attrs) do
    expected = %{expired_at_or_before: Map.get(attrs, :now)}

    with :ok <- shard_lease_check(repo, ledger, key, expected, %{}),
         {:ok, lease} <- fetch_shard_lease(repo, ledger, key) do
      clear_shard_lease(repo, ledger, key)
      {:ok, lease}
    end
  end

  defp do_apply_lease_request(repo, ledger, :takeover, key, attrs) do
    expected = %{expired_at_or_before: Map.get(attrs, :now)}

    with :ok <- shard_lease_check(repo, ledger, key, expected, %{}) do
      lease = shard_lease_value(attrs)
      put_shard_lease(repo, ledger, key, lease)
      {:ok, lease}
    end
  end

  defp do_apply_lease_request(_repo, _ledger, operation, _key, _attrs),
    do: {:error, {:unsupported_store_v1_request, :lease, {:shard, operation}}}

  defp put_shard_lease(repo, ledger, key, lease) do
    with {:ok, queue, shard} <- parse_shard_key(key) do
      put(repo, Keyspace.shard_lease(ledger, queue, shard), Value.pack_shard_lease(lease))
    end
  end

  defp clear_shard_lease(repo, ledger, key) do
    with {:ok, queue, shard} <- parse_shard_key(key) do
      clear(repo, Keyspace.shard_lease(ledger, queue, shard))
    end
  end

  defp shard_lease_value(attrs) do
    %{
      queue: to_string(Map.fetch!(attrs, :queue)),
      shard: Map.fetch!(attrs, :shard),
      owner_id: to_string(Map.fetch!(attrs, :owner_id)),
      lease_until: Map.fetch!(attrs, :lease_until)
    }
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs

  defp fetch_attr(attrs, key) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> {:ok, value}
      :error -> :error
    end
  end

  defp claim_fence_conflict(repo, ledger, claim_id, expected, metadata) do
    claim_key = Keyspace.claim(ledger, claim_id)
    add_read_conflict_key(repo, claim_key)

    with {:ok, claim} when not is_nil(claim) <- fetch_claim(repo, ledger, claim_id),
         true <- value_field(claim, :token_hash) == Map.get(expected, :token_hash),
         {:ok, state} when not is_nil(state) <- fetch_claim_state(repo, ledger, claim_id, claim),
         true <- claim_state_current?(claim_id, state),
         true <- lease_current?(claim, Map.get(metadata, :now)) do
      :ok
    else
      {:ok, nil} ->
        {:error, Conflict.claim_fence(claim_id, expected, :missing)}

      {:error, reason} ->
        {:error, reason}

      _failed_check ->
        {:ok, claim} = fetch_claim(repo, ledger, claim_id)
        {:error, Conflict.claim_fence(claim_id, expected, claim || :missing)}
    end
  end

  defp fetch_claim_state(repo, ledger, _claim_id, claim) do
    intent_id = value_field(claim, :intent_id)
    state_key = Keyspace.state(ledger, intent_id)
    add_read_conflict_key(repo, state_key)
    fetch_state(repo, ledger, intent_id)
  end

  defp claim_state_current?(claim_id, %IntentState{} = state) do
    state.status == :claimed and state.claim_id in [nil, claim_id]
  end

  defp lease_current?(_claim, nil), do: true

  defp lease_current?(claim, %DateTime{} = now) do
    case value_field(claim, :lease_until) do
      %DateTime{} = lease_until -> DateTime.compare(lease_until, now) == :gt
      _missing_or_invalid -> false
    end
  end

  defp value_field(value, key, default \\ nil)
  defp value_field(value, key, default) when is_map(value), do: Map.get(value, key, default)
  defp value_field(_value, _key, default), do: default

  defp apply_listing_request(repo, ledger, %Listing{} = request), do: do_apply_listing_request(repo, ledger, request)

  defp apply_listing_request(repo, ledger, {type, attrs})
       when type in [:due_intents, :expired_claims] and (is_map(attrs) or is_list(attrs)) do
    apply_listing_request(repo, ledger, Listing.new(type, attrs))
  end

  defp apply_listing_request(_repo, _ledger, request), do: {:error, {:unsupported_store_v1_request, :listing, request}}

  defp do_apply_listing_request(repo, ledger, %Listing{type: :due_intents} = request) do
    repo
    |> get_range(queue_listing_range(ledger, request))
    |> decode_state_rows("invalid Bedrock queue index value")
    |> filter_sort_take_listing(request)
  end

  defp do_apply_listing_request(repo, ledger, %Listing{type: :expired_claims} = request) do
    repo
    |> get_range(Keyspace.table_range(ledger, :state))
    |> decode_state_rows("invalid Bedrock state value")
    |> filter_sort_take_listing(request)
  end

  defp do_apply_listing_request(_repo, _ledger, request),
    do: {:error, {:unsupported_store_v1_request, :listing, request}}

  defp queue_listing_range(ledger, %Listing{queue: queue, shard: nil}), do: Keyspace.queue_range(ledger, queue)
  defp queue_listing_range(ledger, %Listing{queue: queue, shard: shard}), do: Keyspace.queue_range(ledger, queue, shard)

  defp decode_state_rows(entries, error_message) do
    entries
    |> Enum.sort_by(fn {key, _value} -> key end)
    |> Enum.reduce_while({:ok, []}, fn {_key, encoded}, {:ok, states} ->
      case Value.unpack_state(encoded) do
        {:ok, state} -> {:cont, {:ok, states ++ [state]}}
        {:error, reason} -> {:halt, {:error, adapter_error(error_message, reason: reason)}}
      end
    end)
  end

  defp filter_sort_take_listing({:ok, states}, %Listing{} = request) do
    rows =
      states
      |> Enum.filter(&listing_match?(&1, request))
      |> Enum.sort_by(&listing_sort_key(&1, request))
      |> Enum.take(request.limit)

    {:ok, rows}
  end

  defp filter_sort_take_listing({:error, reason}, _request), do: {:error, reason}

  defp listing_match?(state, %Listing{type: :due_intents} = request) do
    state_queue(state) == request.queue and shard_match?(state, request) and
      value_field(state, :status) in [:available, :retry_scheduled] and
      datetime_not_after?(value_field(state, :visible_at), request.at)
  end

  defp listing_match?(state, %Listing{type: :expired_claims} = request) do
    state_queue(state) == request.queue and shard_match?(state, request) and value_field(state, :status) == :claimed and
      datetime_not_after?(value_field(state, :lease_until), request.at)
  end

  defp shard_match?(_state, %Listing{shard: nil}), do: true
  defp shard_match?(state, %Listing{shard: shard}), do: value_field(state, :shard) == shard

  defp state_queue(state), do: state |> value_field(:queue, "default") |> to_string()

  defp listing_sort_key(state, %Listing{type: :due_intents}) do
    {-value_field(state, :priority, 0), datetime_sort_key(value_field(state, :visible_at)),
     value_field(state, :intent_id)}
  end

  defp listing_sort_key(state, %Listing{type: :expired_claims}) do
    {datetime_sort_key(value_field(state, :lease_until)), value_field(state, :intent_id)}
  end

  defp datetime_not_after?(%DateTime{} = value, %DateTime{} = at), do: DateTime.compare(value, at) != :gt
  defp datetime_not_after?(_value, _at), do: false

  defp datetime_sort_key(%DateTime{} = value), do: DateTime.to_unix(value, :microsecond)
  defp datetime_sort_key(_value), do: 0

  defp apply_outbox_request(repo, ledger, %Outbox{} = request), do: do_apply_outbox_request(repo, ledger, request)

  defp apply_outbox_request(repo, ledger, {type, attrs})
       when type in [:insert, :read, :ack, :replay] and (is_map(attrs) or is_list(attrs)) do
    apply_outbox_request(repo, ledger, Outbox.new(type, attrs))
  end

  defp apply_outbox_request(_repo, _ledger, request), do: {:error, {:unsupported_store_v1_request, :outbox, request}}

  defp do_apply_outbox_request(repo, ledger, %Outbox{type: :insert, key: key, value: value}) when not is_nil(key) do
    put_outbox_entry(repo, ledger, key, value)
  end

  defp do_apply_outbox_request(repo, ledger, %Outbox{type: :read, consumer: consumer} = request)
       when not is_nil(consumer) do
    with {:ok, entries} <- outbox_entries(repo, ledger) do
      entries =
        entries
        |> Enum.filter(&(is_nil(value_field(&1, :acked_at)) and outbox_after_cursor?(&1, request.cursor)))
        |> Enum.take(request.limit)

      {:ok, entries}
    end
  end

  defp do_apply_outbox_request(repo, ledger, %Outbox{type: :ack, key: key, consumer: consumer, metadata: metadata})
       when not is_nil(key) and not is_nil(consumer) do
    ack_outbox_entry(repo, ledger, key, Map.put(metadata, :consumer, consumer))
  end

  defp do_apply_outbox_request(repo, ledger, %Outbox{type: :replay} = request) do
    with {:ok, entries} <- outbox_entries(repo, ledger) do
      entries =
        entries
        |> Enum.filter(&outbox_after_cursor?(&1, request.cursor))
        |> Enum.take(request.limit)

      {:ok, entries}
    end
  end

  defp do_apply_outbox_request(_repo, _ledger, request), do: {:error, {:unsupported_store_v1_request, :outbox, request}}

  defp outbox_unacked_check(repo, ledger, entry_id) do
    case fetch_outbox_entry(repo, ledger, entry_id) do
      {:ok, nil} ->
        {:error, Conflict.outbox(entry_id, :unacked, :missing)}

      {:ok, {storage_key, entry}} ->
        add_read_conflict_key(repo, storage_key)

        if is_nil(value_field(entry, :acked_at)) do
          :ok
        else
          {:error, Conflict.outbox(entry_id, :unacked, entry)}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp put_outbox_entry(repo, ledger, key, value) when is_map(value) do
    entry =
      value
      |> Map.put(:key, key)
      |> Map.put_new(:acked_at, nil)

    with {:ok, sequence} <- resolve_outbox_sequence(repo, ledger, entry) do
      entry = Map.put(entry, :sequence, sequence)
      put(repo, Keyspace.outbox(ledger, sequence), Value.pack_outbox(entry))
      {:ok, entry}
    end
  end

  defp put_outbox_entry(_repo, _ledger, key, value) do
    {:error, adapter_error("outbox value must be a map", reason: :invalid_outbox_value, key: key, value: value)}
  end

  defp ack_outbox_entry(repo, ledger, key, metadata) do
    case fetch_outbox_entry(repo, ledger, key) do
      {:ok, nil} ->
        {:error, Conflict.outbox(key, :unacked, :missing)}

      {:ok, {storage_key, entry}} ->
        add_read_conflict_key(repo, storage_key)

        if is_nil(value_field(entry, :acked_at)) do
          acked =
            entry
            |> Map.put(:acked_at, Map.get(metadata, :acked_at))
            |> Map.put(:consumer, Map.get(metadata, :consumer))

          put(repo, storage_key, Value.pack_outbox(acked))
          {:ok, acked}
        else
          {:error, Conflict.outbox(key, :unacked, entry)}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve_outbox_sequence(repo, ledger, entry) do
    case Map.fetch(entry, :sequence) do
      {:ok, sequence} when is_integer(sequence) and sequence >= 0 ->
        {:ok, sequence}

      {:ok, sequence} ->
        {:error,
         adapter_error("outbox sequence must be a non-negative integer",
           reason: :invalid_outbox_sequence,
           sequence: sequence
         )}

      :error ->
        next_outbox_sequence(repo, ledger)
    end
  end

  defp next_outbox_sequence(repo, ledger) do
    range = Keyspace.outbox_range(ledger)
    add_write_conflict_range(repo, range)

    with {:ok, entries} <- outbox_entries(repo, ledger) do
      next_sequence =
        entries
        |> Enum.map(&value_field(&1, :sequence, 0))
        |> Enum.max(fn -> 0 end)
        |> Kernel.+(1)

      {:ok, next_sequence}
    end
  end

  defp fetch_outbox_entry(repo, ledger, entry_id) do
    with {:ok, entries} <- outbox_storage_entries(repo, ledger) do
      {:ok, Enum.find(entries, fn {_storage_key, entry} -> value_field(entry, :key) == entry_id end)}
    end
  end

  defp outbox_entries(repo, ledger) do
    with {:ok, entries} <- outbox_storage_entries(repo, ledger) do
      {:ok, Enum.map(entries, fn {_storage_key, entry} -> entry end)}
    end
  end

  defp outbox_storage_entries(repo, ledger) do
    repo
    |> get_range(Keyspace.outbox_range(ledger))
    |> Enum.sort_by(fn {key, _value} -> key end)
    |> Enum.reduce_while({:ok, []}, fn {storage_key, encoded}, {:ok, entries} ->
      case Value.unpack_outbox(encoded) do
        {:ok, entry} -> {:cont, {:ok, entries ++ [{storage_key, entry}]}}
        {:error, reason} -> {:halt, {:error, adapter_error("invalid Bedrock outbox value", reason: reason)}}
      end
    end)
  end

  defp outbox_after_cursor?(_entry, nil), do: true
  defp outbox_after_cursor?(entry, cursor) when is_integer(cursor), do: value_field(entry, :sequence, 0) > cursor
  defp outbox_after_cursor?(_entry, _cursor), do: true

  defp next_stream_version(stream, metadata, acc) do
    case fetch_metadata_version(metadata) do
      {:ok, version} ->
        {:ok, version}

      {:error, _reason} ->
        case Map.fetch(acc.stream_versions, stream) do
          {:ok, version} ->
            {:ok, version + 1}

          :error ->
            {:error,
             adapter_error("append_signal writes require metadata.version or stream_version precondition",
               reason: :missing_stream_version
             )}
        end
    end
  end

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

  defp stream_precondition_versions(%CommitRequest{} = request) do
    request.preconditions
    |> Enum.reduce(%{}, fn
      %{type: :stream_version, stream: stream, expected: expected}, acc
      when is_binary(stream) and is_integer(expected) ->
        Map.put(acc, stream, expected)

      _precondition, acc ->
        acc
    end)
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

  defp shard_key(queue, shard), do: "shard:" <> to_string(queue) <> ":" <> to_string(shard)

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
