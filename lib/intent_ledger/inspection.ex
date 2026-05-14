defmodule IntentLedger.Inspection do
  @moduledoc """
  Read-only operational inspection requests and result builders.

  Inspection requests are safe to route through Store V1 `read/4` callbacks.
  Results expose operational identifiers, counts, lease timing, and lag
  information without returning intent payloads, claim tokens, or token hashes.
  """

  alias IntentLedger.{Telemetry, Time}

  @type kind ::
          :queues
          | :shards
          | :claims
          | :retries
          | :ambiguous
          | :outbox_lag
          | :projection_lag

  @kinds [:queues, :shards, :claims, :retries, :ambiguous, :outbox_lag, :projection_lag]
  @fields [
    :type,
    :queue,
    :shard,
    :at,
    :limit,
    :cursor,
    :consumer,
    :projection,
    :stream,
    :queue_config,
    :metadata
  ]
  @field_names Map.new(@fields, &{Atom.to_string(&1), &1})

  @schema Zoi.struct(__MODULE__, %{
            type: Zoi.enum(@kinds) |> Zoi.default(:queues) |> Zoi.optional(),
            queue: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            shard: Zoi.integer() |> Zoi.gte(0) |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            at: Zoi.datetime() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            limit: Zoi.integer() |> Zoi.positive() |> Zoi.default(100) |> Zoi.optional(),
            cursor: Zoi.integer() |> Zoi.gte(0) |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            consumer: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            projection: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            stream: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            queue_config: Zoi.map() |> Zoi.default(%{}) |> Zoi.optional(),
            metadata: Zoi.map() |> Zoi.default(%{}) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  @doc """
  Returns supported inspection request kinds.
  """
  @spec kinds() :: [kind()]
  def kinds, do: @kinds

  @doc """
  Builds an inspection request.
  """
  @spec new(kind(), keyword() | map()) :: t()
  def new(type, attrs \\ %{}) when type in @kinds do
    attrs =
      attrs
      |> normalize_attrs()
      |> normalize_keys()
      |> Map.put(:type, type)
      |> normalize_queue()
      |> normalize_shard()
      |> normalize_at()
      |> normalize_limit()
      |> normalize_cursor()
      |> normalize_consumer()
      |> normalize_projection()
      |> normalize_stream()
      |> normalize_queue_config()
      |> Map.put_new(:metadata, %{})

    struct!(__MODULE__, attrs)
  end

  @doc """
  Builds a queue depth inspection request.
  """
  @spec queues(keyword() | map()) :: t()
  def queues(attrs \\ %{}), do: new(:queues, attrs)

  @doc """
  Builds a queue shard state inspection request.
  """
  @spec shards(keyword() | map()) :: t()
  def shards(attrs \\ %{}), do: new(:shards, attrs)

  @doc """
  Builds an active claim inspection request.
  """
  @spec claims(keyword() | map()) :: t()
  def claims(attrs \\ %{}), do: new(:claims, attrs)

  @doc """
  Builds a retry backlog inspection request.
  """
  @spec retries(keyword() | map()) :: t()
  def retries(attrs \\ %{}), do: new(:retries, attrs)

  @doc """
  Builds an ambiguous intent inspection request.
  """
  @spec ambiguous(keyword() | map()) :: t()
  def ambiguous(attrs \\ %{}), do: new(:ambiguous, attrs)

  @doc """
  Builds an outbox lag inspection request.
  """
  @spec outbox_lag(keyword() | map()) :: t()
  def outbox_lag(attrs \\ %{}), do: new(:outbox_lag, attrs)

  @doc """
  Builds a projection lag inspection request.
  """
  @spec projection_lag(String.t() | atom() | module(), keyword() | map()) :: t()
  def projection_lag(projection, attrs \\ %{}) do
    attrs = attrs |> normalize_attrs() |> Map.put(:projection, projection)
    new(:projection_lag, attrs)
  end

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Inspection.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  @doc false
  @spec evaluate(t(), map()) :: {:ok, [map()] | map()}
  def evaluate(%__MODULE__{type: :queues} = request, data), do: {:ok, inspect_queues(request, data)}
  def evaluate(%__MODULE__{type: :shards} = request, data), do: {:ok, inspect_shards(request, data)}
  def evaluate(%__MODULE__{type: :claims} = request, data), do: {:ok, inspect_claims(request, data)}
  def evaluate(%__MODULE__{type: :retries} = request, data), do: {:ok, inspect_retries(request, data)}
  def evaluate(%__MODULE__{type: :ambiguous} = request, data), do: {:ok, inspect_ambiguous(request, data)}
  def evaluate(%__MODULE__{type: :outbox_lag} = request, data), do: {:ok, inspect_outbox_lag(request, data)}
  def evaluate(%__MODULE__{type: :projection_lag} = request, data), do: {:ok, inspect_projection_lag(request, data)}

  defp inspect_queues(%__MODULE__{} = request, data) do
    states = scoped_states(request, states(data))

    request
    |> queue_names(data)
    |> Enum.map(fn queue ->
      queue_states = Enum.filter(states, &(state_queue(&1) == queue))

      %{
        queue: queue,
        shards: shard_count(request, data, queue),
        depth: Enum.count(queue_states, &due?(&1, request.at)),
        available: Enum.count(queue_states, &(state_status(&1) == :available)),
        retry_scheduled: Enum.count(queue_states, &(state_status(&1) == :retry_scheduled)),
        claimed: Enum.count(queue_states, &(state_status(&1) == :claimed)),
        expired_claims: Enum.count(queue_states, &expired_claim?(&1, request.at)),
        ambiguous: Enum.count(queue_states, &(state_status(&1) == :ambiguous)),
        total_open: Enum.count(queue_states, &open?/1)
      }
    end)
  end

  defp inspect_shards(%__MODULE__{} = request, data) do
    states = scoped_states(request, states(data))
    leases = scoped_leases(request, shard_leases(data))

    request
    |> queue_shards(data)
    |> Enum.map(fn {queue, shard} ->
      shard_states = Enum.filter(states, &(state_queue(&1) == queue and state_shard(&1) == shard))
      lease = Enum.find(leases, &(lease_queue(&1) == queue and lease_shard(&1) == shard))

      %{
        queue: queue,
        shard: shard,
        status: shard_status(lease, request.at),
        owner_id: field(lease, :owner_id),
        lease_until: field(lease, :lease_until),
        depth: Enum.count(shard_states, &due?(&1, request.at)),
        claimed: Enum.count(shard_states, &(state_status(&1) == :claimed)),
        expired_claims: Enum.count(shard_states, &expired_claim?(&1, request.at)),
        retry_scheduled: Enum.count(shard_states, &(state_status(&1) == :retry_scheduled)),
        ambiguous: Enum.count(shard_states, &(state_status(&1) == :ambiguous))
      }
      |> reject_nil()
    end)
  end

  defp inspect_claims(%__MODULE__{} = request, data) do
    intents = intents_by_id(data)
    claims = claims_by_id(data)
    claims_by_intent = claims_by_intent_id(data)

    request
    |> scoped_states(states(data))
    |> Enum.filter(&(state_status(&1) == :claimed))
    |> Enum.sort_by(&datetime_sort_key(field(&1, :lease_until)))
    |> Enum.take(request.limit)
    |> Enum.map(fn state ->
      claim_id = field(state, :claim_id)
      intent_id = field(state, :intent_id)
      claim = Map.get(claims, claim_id) || Map.get(claims_by_intent, intent_id, %{})

      state
      |> intent_row(intents)
      |> Map.merge(%{
        claim_id: claim_id,
        owner_id: field(claim, :owner_id),
        lease_until: field(state, :lease_until),
        expired?: expired_claim?(state, request.at)
      })
      |> reject_nil()
    end)
  end

  defp inspect_retries(%__MODULE__{} = request, data) do
    intents = intents_by_id(data)

    request
    |> scoped_states(states(data))
    |> Enum.filter(&(state_status(&1) == :retry_scheduled))
    |> Enum.sort_by(&datetime_sort_key(field(&1, :visible_at)))
    |> Enum.take(request.limit)
    |> Enum.map(fn state ->
      state
      |> intent_row(intents)
      |> Map.merge(%{
        retry_at: field(state, :visible_at),
        due?: not_after?(field(state, :visible_at), request.at)
      })
      |> reject_nil()
    end)
  end

  defp inspect_ambiguous(%__MODULE__{} = request, data) do
    intents = intents_by_id(data)

    request
    |> scoped_states(states(data))
    |> Enum.filter(&(state_status(&1) == :ambiguous))
    |> Enum.sort_by(&{state_queue(&1), state_shard(&1), field(&1, :intent_id)})
    |> Enum.take(request.limit)
    |> Enum.map(fn state ->
      state
      |> intent_row(intents)
      |> Map.merge(%{
        updated_at: field(state, :updated_at),
        error_class: state |> field(:error) |> Telemetry.error_class()
      })
      |> reject_nil()
    end)
  end

  defp inspect_outbox_lag(%__MODULE__{} = request, data) do
    entries = data |> outbox_entries() |> Enum.sort_by(&field(&1, :sequence, 0))
    max_sequence = entries |> Enum.map(&field(&1, :sequence, 0)) |> Enum.max(fn -> 0 end)

    last_acked_sequence =
      entries |> acked_entries(request.consumer) |> Enum.map(&field(&1, :sequence, 0)) |> Enum.max(fn -> 0 end)

    cursor = request.cursor || last_acked_sequence
    unacked = Enum.filter(entries, &(is_nil(field(&1, :acked_at)) and field(&1, :sequence, 0) > cursor))
    oldest = List.first(unacked)

    %{
      consumer: request.consumer,
      cursor: cursor,
      max_sequence: max_sequence,
      lag: max(max_sequence - cursor, 0),
      unacked: length(unacked),
      total: length(entries),
      oldest_unacked_sequence: field(oldest, :sequence),
      oldest_unacked_age_ms: oldest_unacked_age_ms(oldest, request.at)
    }
    |> reject_nil()
  end

  defp inspect_projection_lag(%__MODULE__{} = request, data) do
    stream_version = data |> Map.get(:stream_version, 0) |> non_negative_or(0)
    cursor = request.cursor || 0

    %{
      projection: request.projection,
      stream: request.stream,
      cursor: cursor,
      stream_version: stream_version,
      lag: max(stream_version - cursor, 0)
    }
    |> reject_nil()
  end

  defp queue_names(%__MODULE__{} = request, data) do
    configured = Map.keys(request.queue_config)
    observed = Enum.map(states(data), &state_queue/1) ++ Enum.map(shard_leases(data), &lease_queue/1)

    (configured ++ observed)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Enum.filter(&(is_nil(request.queue) or &1 == request.queue))
    |> Enum.sort()
  end

  defp queue_shards(%__MODULE__{} = request, data) do
    configured =
      request
      |> queue_names(data)
      |> Enum.flat_map(fn queue ->
        request
        |> shard_numbers(data, queue)
        |> Enum.map(&{queue, &1})
      end)

    observed =
      (Enum.map(states(data), &{state_queue(&1), state_shard(&1)}) ++
         Enum.map(shard_leases(data), &{lease_queue(&1), lease_shard(&1)}))
      |> Enum.reject(fn {queue, shard} -> is_nil(queue) or is_nil(shard) end)

    (configured ++ observed)
    |> Enum.filter(fn {queue, shard} ->
      (is_nil(request.queue) or queue == request.queue) and (is_nil(request.shard) or shard == request.shard)
    end)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp shard_numbers(%__MODULE__{} = request, data, queue) do
    configured_count = shard_count(request, data, queue)
    configured = Enum.to_list(0..(configured_count - 1))

    observed =
      (data |> states() |> Enum.filter(&(state_queue(&1) == queue)) |> Enum.map(&state_shard/1)) ++
        (data |> shard_leases() |> Enum.filter(&(lease_queue(&1) == queue)) |> Enum.map(&lease_shard/1))

    (configured ++ observed)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp shard_count(%__MODULE__{} = request, data, queue) do
    configured =
      request.queue_config
      |> Map.get(queue, %{})
      |> field(:shards)

    observed =
      data
      |> states()
      |> Enum.filter(&(state_queue(&1) == queue))
      |> Enum.map(&state_shard/1)
      |> Enum.reject(&is_nil/1)
      |> Enum.max(fn -> -1 end)
      |> Kernel.+(1)

    max(configured || 1, observed)
  end

  defp scoped_states(%__MODULE__{} = request, states) do
    Enum.filter(states, fn state ->
      (is_nil(request.queue) or state_queue(state) == request.queue) and
        (is_nil(request.shard) or state_shard(state) == request.shard)
    end)
  end

  defp scoped_leases(%__MODULE__{} = request, leases) do
    Enum.filter(leases, fn lease ->
      (is_nil(request.queue) or lease_queue(lease) == request.queue) and
        (is_nil(request.shard) or lease_shard(lease) == request.shard)
    end)
  end

  defp states(data), do: Map.get(data, :states, [])
  defp shard_leases(data), do: Map.get(data, :shard_leases, [])
  defp outbox_entries(data), do: Map.get(data, :outbox, [])

  defp intents_by_id(data) do
    data
    |> Map.get(:intents, [])
    |> Map.new(fn intent -> {field(intent, :id), intent} end)
    |> Map.delete(nil)
  end

  defp claims_by_id(data) do
    data
    |> Map.get(:claims, [])
    |> Map.new(fn claim -> {field(claim, :claim_id) || field(claim, :id), claim} end)
    |> Map.delete(nil)
  end

  defp claims_by_intent_id(data) do
    data
    |> Map.get(:claims, [])
    |> Map.new(fn claim -> {field(claim, :intent_id), claim} end)
    |> Map.delete(nil)
  end

  defp intent_row(state, intents) do
    intent_id = field(state, :intent_id)
    intent = Map.get(intents, intent_id, %{})

    %{
      intent_id: intent_id,
      key: field(intent, :key),
      kind: field(intent, :kind),
      queue: state_queue(state),
      shard: state_shard(state),
      attempt: field(state, :attempt, 0)
    }
  end

  defp due?(state, at) do
    state_status(state) in [:available, :retry_scheduled] and not_after?(field(state, :visible_at), at)
  end

  defp expired_claim?(state, at), do: state_status(state) == :claimed and not_after?(field(state, :lease_until), at)
  defp open?(state), do: state_status(state) not in [:completed, :failed, :cancelled]

  defp shard_status(nil, _at), do: :unowned
  defp shard_status(lease, at), do: if(not_after?(field(lease, :lease_until), at), do: :expired, else: :owned)

  defp acked_entries(entries, nil), do: Enum.reject(entries, &is_nil(field(&1, :acked_at)))

  defp acked_entries(entries, consumer) do
    Enum.filter(entries, &(not is_nil(field(&1, :acked_at)) and field(&1, :consumer) == consumer))
  end

  defp oldest_unacked_age_ms(nil, _now), do: nil

  defp oldest_unacked_age_ms(entry, %DateTime{} = now) do
    case signal_time(entry) do
      %DateTime{} = time -> max(DateTime.diff(now, time, :millisecond), 0)
      _missing -> nil
    end
  end

  defp signal_time(entry) do
    entry
    |> field(:signal, %{})
    |> field(:time)
    |> normalize_datetime()
  end

  defp state_queue(state), do: state |> field(:queue, "default") |> to_string()
  defp state_shard(state), do: field(state, :shard, 0)
  defp lease_queue(lease), do: lease |> field(:queue, "default") |> to_string()
  defp lease_shard(lease), do: field(lease, :shard, 0)

  defp state_status(state) do
    case field(state, :status) do
      status when is_atom(status) -> status
      status when is_binary(status) -> String.to_existing_atom(status)
      status -> status
    end
  rescue
    ArgumentError -> field(state, :status)
  end

  defp not_after?(%DateTime{} = value, %DateTime{} = at), do: DateTime.compare(value, at) != :gt
  defp not_after?(_value, _at), do: false

  defp datetime_sort_key(%DateTime{} = value), do: DateTime.to_unix(value, :microsecond)
  defp datetime_sort_key(_value), do: 0

  defp normalize_datetime(%DateTime{} = value), do: value

  defp normalize_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      {:error, _reason} -> nil
    end
  end

  defp normalize_datetime(_value), do: nil

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs

  defp normalize_keys(attrs) do
    Map.new(attrs, fn
      {key, value} when is_binary(key) -> {Map.get(@field_names, key, key), value}
      {key, value} -> {key, value}
    end)
  end

  defp normalize_queue(%{queue: nil} = attrs), do: attrs
  defp normalize_queue(%{queue: queue} = attrs), do: %{attrs | queue: to_string(queue)}
  defp normalize_queue(attrs), do: Map.put(attrs, :queue, nil)

  defp normalize_shard(%{shard: shard} = attrs) when is_integer(shard) and shard >= 0, do: attrs
  defp normalize_shard(attrs), do: Map.put(attrs, :shard, nil)

  defp normalize_at(%{at: %DateTime{}} = attrs), do: attrs
  defp normalize_at(%{at: at} = attrs) when is_binary(at), do: %{attrs | at: normalize_datetime(at) || Time.utc_now()}
  defp normalize_at(attrs), do: Map.put(attrs, :at, Time.utc_now())

  defp normalize_limit(%{limit: limit} = attrs) when is_integer(limit) and limit > 0, do: attrs
  defp normalize_limit(attrs), do: Map.put(attrs, :limit, 100)

  defp normalize_cursor(%{cursor: cursor} = attrs) when is_integer(cursor) and cursor >= 0, do: attrs
  defp normalize_cursor(attrs), do: Map.put(attrs, :cursor, nil)

  defp normalize_consumer(%{consumer: nil} = attrs), do: attrs
  defp normalize_consumer(%{consumer: consumer} = attrs), do: %{attrs | consumer: to_string(consumer)}
  defp normalize_consumer(attrs), do: Map.put(attrs, :consumer, nil)

  defp normalize_projection(%{projection: nil} = attrs), do: attrs

  defp normalize_projection(%{projection: projection} = attrs) when is_atom(projection) do
    %{attrs | projection: projection |> Atom.to_string() |> String.trim_leading("Elixir.")}
  end

  defp normalize_projection(%{projection: projection} = attrs), do: %{attrs | projection: to_string(projection)}
  defp normalize_projection(attrs), do: Map.put(attrs, :projection, nil)

  defp normalize_stream(%{stream: nil} = attrs), do: attrs
  defp normalize_stream(%{stream: stream} = attrs), do: %{attrs | stream: to_string(stream)}
  defp normalize_stream(attrs), do: Map.put(attrs, :stream, nil)

  defp normalize_queue_config(%{queue_config: config} = attrs) when is_map(config) do
    %{attrs | queue_config: Map.new(config, fn {queue, opts} -> {to_string(queue), normalize_queue_opts(opts)} end)}
  end

  defp normalize_queue_config(attrs), do: Map.put(attrs, :queue_config, %{})

  defp normalize_queue_opts(opts) when is_list(opts), do: opts |> Map.new() |> normalize_queue_opts()

  defp normalize_queue_opts(opts) when is_map(opts) do
    shards = opts |> field(:shards, 1) |> positive_or(1)
    %{shards: shards}
  end

  defp normalize_queue_opts(_opts), do: %{shards: 1}

  defp field(value, key, default \\ nil)
  defp field(nil, _key, default), do: default
  defp field(value, key, default) when is_struct(value), do: value |> Map.from_struct() |> field(key, default)
  defp field(%{} = value, key, default), do: Map.get(value, key, Map.get(value, Atom.to_string(key), default))
  defp field(_value, _key, default), do: default

  defp positive_or(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_or(_value, default), do: default

  defp non_negative_or(value, _default) when is_integer(value) and value >= 0, do: value
  defp non_negative_or(_value, default), do: default

  defp reject_nil(map), do: Map.reject(map, fn {_key, value} -> is_nil(value) end)
end
