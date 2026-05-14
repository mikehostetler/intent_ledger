defmodule IntentLedger.Telemetry do
  @moduledoc """
  Stable telemetry event catalogue and metadata policy.

  Intent Ledger telemetry events are emitted under `[:intent_ledger]` by
  default. Pass `:telemetry_prefix` when starting a ledger instance to emit the
  same event suffixes under an application-owned prefix.

  Telemetry metadata is intentionally operational. Events may include ledger
  names, low-cardinality operation and status atoms, durable identifiers,
  queue/shard coordinates, consumer names, projection names, and lineage
  identifiers. Events must not include intent payloads, command payloads,
  command results, raw failure payloads, claim tokens, token hashes, headers, or
  secrets. Measurements use native telemetry units for `:duration` and
  `:system_time`; counts are integer counters; lag measurements are
  milliseconds.
  """

  alias IntentLedger.Store.{Commit, CommitRequest, Conflict}

  @default_prefix [:intent_ledger]

  @type event ::
          :command_start
          | :command_stop
          | :command_exception
          | :store_commit_start
          | :store_commit_stop
          | :store_commit_exception
          | :store_conflict
          | :claim_stop
          | :shard_lease_stop
          | :recovery_stop
          | :outbox_read_stop
          | :outbox_ack_stop
          | :dispatcher_stop
          | :replay_stop
          | :projection_stop
          | :inspection_stop

  @type field :: atom()
  @type definition :: %{
          required(:event) => event(),
          required(:name) => [atom()],
          required(:measurements) => [field()],
          required(:required_metadata) => [field()],
          required(:optional_metadata) => [field()]
        }

  @lineage_metadata_fields [
    :actor,
    :causation_id,
    :correlation_id,
    :root_intent_id,
    :parent_intent_id,
    :depth
  ]

  @base_metadata_fields [
    :ledger,
    :operation,
    :status,
    :store,
    :intent_id,
    :claim_id,
    :command_id,
    :idempotency_key,
    :queue,
    :shard,
    :owner_id,
    :consumer,
    :handler,
    :projection,
    :source,
    :stream,
    :cursor,
    :limit,
    :conflict,
    :exception_kind,
    :error_class,
    :classification,
    :retry_at,
    :lease_until,
    :replayed?
  ]

  @safe_metadata_fields @base_metadata_fields ++ @lineage_metadata_fields

  @sensitive_metadata_fields [
    :payload,
    :command_payload,
    :result,
    :error,
    :reason,
    :token,
    :token_hash,
    :secret,
    :password,
    :authorization,
    :headers,
    :private_key,
    :access_key
  ]

  @definitions [
    %{
      event: :command_start,
      name: [:command, :start],
      measurements: [:system_time],
      required_metadata: [:ledger, :operation],
      optional_metadata: [:command_id, :idempotency_key | @lineage_metadata_fields]
    },
    %{
      event: :command_stop,
      name: [:command, :stop],
      measurements: [:duration, :count],
      required_metadata: [:ledger, :operation, :status],
      optional_metadata: [:command_id, :idempotency_key, :replayed? | @lineage_metadata_fields]
    },
    %{
      event: :command_exception,
      name: [:command, :exception],
      measurements: [:duration],
      required_metadata: [:ledger, :operation, :exception_kind],
      optional_metadata: [:command_id, :idempotency_key, :error_class | @lineage_metadata_fields]
    },
    %{
      event: :store_commit_start,
      name: [:store, :commit, :start],
      measurements: [:system_time],
      required_metadata: [:ledger, :store, :operation],
      optional_metadata: [:command_id, :stream]
    },
    %{
      event: :store_commit_stop,
      name: [:store, :commit, :stop],
      measurements: [:duration, :writes, :signals, :outbox_entries],
      required_metadata: [:ledger, :store, :operation, :status],
      optional_metadata: [:command_id, :stream, :replayed?]
    },
    %{
      event: :store_commit_exception,
      name: [:store, :commit, :exception],
      measurements: [:duration],
      required_metadata: [:ledger, :store, :operation, :exception_kind],
      optional_metadata: [:command_id, :stream, :error_class]
    },
    %{
      event: :store_conflict,
      name: [:store, :conflict],
      measurements: [:count],
      required_metadata: [:ledger, :store, :operation, :conflict],
      optional_metadata: [:command_id, :stream, :intent_id, :claim_id, :queue, :shard, :owner_id]
    },
    %{
      event: :claim_stop,
      name: [:claim, :stop],
      measurements: [:duration, :count],
      required_metadata: [:ledger, :queue, :owner_id, :status],
      optional_metadata: [:shard, :limit, :intent_id, :claim_id, :conflict, :error_class]
    },
    %{
      event: :shard_lease_stop,
      name: [:shard_lease, :stop],
      measurements: [:duration],
      required_metadata: [:ledger, :queue, :shard, :operation, :status],
      optional_metadata: [:store, :owner_id, :lease_until, :conflict, :error_class]
    },
    %{
      event: :recovery_stop,
      name: [:recovery, :stop],
      measurements: [:duration, :count],
      required_metadata: [:ledger, :queue, :status],
      optional_metadata: [:shard, :limit, :classification, :error_class]
    },
    %{
      event: :outbox_read_stop,
      name: [:outbox, :read, :stop],
      measurements: [:duration, :count, :lag_ms],
      required_metadata: [:ledger, :consumer, :status],
      optional_metadata: [:cursor, :limit]
    },
    %{
      event: :outbox_ack_stop,
      name: [:outbox, :ack, :stop],
      measurements: [:duration, :count],
      required_metadata: [:ledger, :consumer, :status],
      optional_metadata: [:cursor, :conflict]
    },
    %{
      event: :dispatcher_stop,
      name: [:dispatcher, :stop],
      measurements: [:duration, :count, :failed],
      required_metadata: [:ledger, :consumer, :status],
      optional_metadata: [:handler, :retry_at, :error_class]
    },
    %{
      event: :replay_stop,
      name: [:replay, :stop],
      measurements: [:duration, :count],
      required_metadata: [:ledger, :source, :status],
      optional_metadata: [:stream, :intent_id, :queue, :shard, :cursor, :limit]
    },
    %{
      event: :projection_stop,
      name: [:projection, :stop],
      measurements: [:duration, :count],
      required_metadata: [:ledger, :projection, :source, :status],
      optional_metadata: [:stream, :cursor]
    },
    %{
      event: :inspection_stop,
      name: [:inspection, :stop],
      measurements: [:duration, :count],
      required_metadata: [:ledger, :operation, :status],
      optional_metadata: [:queue, :shard, :intent_id, :claim_id, :consumer, :projection, :limit]
    }
  ]

  @by_event Map.new(@definitions, &{&1.event, &1})
  @by_name Map.new(@definitions, &{&1.name, &1})
  @metadata_policy %{
    required: [:ledger],
    allowed: @safe_metadata_fields,
    lineage: @lineage_metadata_fields,
    sensitive: @sensitive_metadata_fields,
    measurement_units: %{
      duration: :native,
      system_time: :native,
      count: :count,
      writes: :count,
      signals: :count,
      outbox_entries: :count,
      failed: :count,
      lag_ms: :millisecond
    }
  }

  @doc """
  Returns all telemetry event definitions in their stable catalogue order.
  """
  @spec all() :: [definition()]
  def all, do: @definitions

  @doc """
  Returns all telemetry event identifiers in their stable catalogue order.
  """
  @spec events() :: [event()]
  def events, do: Enum.map(@definitions, & &1.event)

  @doc """
  Returns the default event prefix.
  """
  @spec default_prefix() :: [atom()]
  def default_prefix, do: @default_prefix

  @doc """
  Returns the metadata policy for Intent Ledger telemetry events.
  """
  @spec metadata_policy() :: %{
          required: [field()],
          allowed: [field()],
          lineage: [field()],
          sensitive: [field()],
          measurement_units: %{field() => atom()}
        }
  def metadata_policy, do: @metadata_policy

  @doc """
  Returns the allowed metadata field names.
  """
  @spec allowed_metadata_fields() :: [field()]
  def allowed_metadata_fields, do: @safe_metadata_fields

  @doc """
  Returns metadata field names that must not be emitted.
  """
  @spec sensitive_metadata_fields() :: [field()]
  def sensitive_metadata_fields, do: @sensitive_metadata_fields

  @doc """
  Fetches a telemetry event definition.
  """
  @spec fetch(event() | [atom()]) :: {:ok, definition()} | :error
  def fetch(event) when is_atom(event), do: Map.fetch(@by_event, event)
  def fetch(name) when is_list(name), do: Map.fetch(@by_name, name)

  @doc """
  Fetches a telemetry event definition or raises when the event is unknown.
  """
  @spec fetch!(event() | [atom()]) :: definition()
  def fetch!(event_or_name) do
    case fetch(event_or_name) do
      {:ok, definition} -> definition
      :error -> raise ArgumentError, "unknown intent ledger telemetry event: #{inspect(event_or_name)}"
    end
  end

  @doc """
  Returns the fully prefixed telemetry event name.
  """
  @spec event_name(event(), keyword()) :: [atom()]
  def event_name(event, opts \\ []) do
    prefix(opts) ++ fetch!(event).name
  end

  @doc false
  @spec execute(keyword(), event(), map(), map()) :: :ok
  def execute(opts, event, measurements, metadata) when is_atom(event) do
    :telemetry.execute(event_name(event, opts), measurements, metadata)
  end

  @doc false
  @spec execute(keyword(), atom(), list(atom()), map(), map()) :: :ok
  def execute(opts, operation, event, measurements, metadata) do
    :telemetry.execute(prefix(opts) ++ [operation | event], measurements, metadata)
  end

  @doc false
  @spec instrument_store_commit(keyword(), atom(), module(), CommitRequest.t(), (-> term())) :: term()
  def instrument_store_commit(opts, ledger, store_module, %CommitRequest{} = request, fun) when is_function(fun, 0) do
    start = System.monotonic_time()
    metadata = store_commit_metadata(ledger, store_module, request)

    execute(opts, :store_commit_start, %{system_time: System.system_time()}, metadata)

    try do
      result = fun.()

      execute(
        opts,
        :store_commit_stop,
        store_commit_measurements(start, result),
        metadata |> Map.merge(store_commit_result_metadata(result)) |> reject_nil_metadata()
      )

      emit_store_conflict(opts, metadata, result)

      result
    catch
      kind, reason ->
        execute(
          opts,
          :store_commit_exception,
          %{duration: System.monotonic_time() - start},
          metadata
          |> Map.merge(%{exception_kind: kind, error_class: error_class(reason)})
          |> reject_nil_metadata()
        )

        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  @doc false
  @spec instrument_store_lease(keyword(), atom(), module(), term(), (-> term())) :: term()
  def instrument_store_lease(opts, ledger, store_module, request, fun) when is_function(fun, 0) do
    start = System.monotonic_time()
    metadata = shard_lease_metadata(ledger, store_module, request)
    result = fun.()

    execute(
      opts,
      :shard_lease_stop,
      %{duration: System.monotonic_time() - start},
      metadata |> Map.merge(shard_lease_result_metadata(result)) |> reject_nil_metadata()
    )

    emit_store_conflict(opts, metadata, result)

    result
  end

  @doc false
  @spec error_class(term()) :: atom()
  def error_class(%module{}), do: module
  def error_class(reason) when is_atom(reason), do: reason

  def error_class(reason) when is_tuple(reason) and tuple_size(reason) > 0 do
    case elem(reason, 0) do
      class when is_atom(class) -> class
      _other -> :unknown
    end
  end

  def error_class(_reason), do: :unknown

  defp prefix(opts), do: Keyword.get(opts, :telemetry_prefix, @default_prefix)

  defp store_commit_metadata(ledger, store_module, %CommitRequest{} = request) do
    %{
      ledger: ledger,
      store: store_module,
      operation: request.operation,
      command_id: request.command_id,
      stream: first_stream(request)
    }
    |> reject_nil_metadata()
  end

  defp first_stream(%CommitRequest{} = request) do
    request.writes
    |> Enum.find_value(fn
      %{stream: stream} -> stream
      _write -> nil
    end)
  end

  defp store_commit_measurements(start, {:ok, %Commit{} = commit}) do
    %{
      duration: System.monotonic_time() - start,
      writes: length(commit.writes),
      signals: length(commit.signals),
      outbox_entries: count_outbox_entries(commit.writes)
    }
  end

  defp store_commit_measurements(start, _result) do
    %{
      duration: System.monotonic_time() - start,
      writes: 0,
      signals: 0,
      outbox_entries: 0
    }
  end

  defp store_commit_result_metadata({:ok, %Commit{} = commit}) do
    %{
      status: :ok,
      replayed?: commit.replayed
    }
  end

  defp store_commit_result_metadata({:error, %Conflict{} = conflict}) do
    %{
      status: :error,
      conflict: conflict.type
    }
  end

  defp store_commit_result_metadata({:error, reason}) do
    %{
      status: :error,
      error_class: error_class(reason)
    }
  end

  defp store_commit_result_metadata(_result), do: %{status: :unknown}

  defp shard_lease_metadata(ledger, store_module, {:shard, operation, attrs}) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    %{
      ledger: ledger,
      store: store_module,
      operation: operation,
      queue: Map.get(attrs, :queue),
      shard: Map.get(attrs, :shard),
      owner_id: Map.get(attrs, :owner_id),
      lease_until: Map.get(attrs, :lease_until)
    }
    |> reject_nil_metadata()
  end

  defp shard_lease_metadata(ledger, store_module, _request) do
    %{
      ledger: ledger,
      store: store_module,
      operation: :unknown
    }
  end

  defp shard_lease_result_metadata({:ok, _lease}), do: %{status: :ok}

  defp shard_lease_result_metadata({:error, %Conflict{} = conflict}) do
    %{
      status: :error,
      conflict: conflict.type
    }
  end

  defp shard_lease_result_metadata({:error, reason}) do
    %{
      status: :error,
      error_class: error_class(reason)
    }
  end

  defp shard_lease_result_metadata(_result), do: %{status: :unknown}

  defp emit_store_conflict(opts, metadata, {:error, %Conflict{} = conflict}) do
    execute(
      opts,
      :store_conflict,
      %{count: 1},
      metadata
      |> Map.merge(%{conflict: conflict.type})
      |> reject_nil_metadata()
    )
  end

  defp emit_store_conflict(_opts, _metadata, _result), do: :ok

  defp count_outbox_entries(writes) do
    Enum.count(writes, fn
      %{type: :put_outbox} -> true
      _write -> false
    end)
  end

  defp reject_nil_metadata(metadata) do
    Map.reject(metadata, fn {_key, value} -> is_nil(value) end)
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs
end
