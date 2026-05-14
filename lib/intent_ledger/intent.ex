defmodule IntentLedger.Intent do
  @moduledoc """
  A durable unit of deferred work.

  Intents are immutable command envelopes. Runtime progress lives in
  `IntentLedger.IntentState`; lifecycle history is emitted as `Jido.Signal`
  records.
  """

  alias IntentLedger.ID
  alias IntentLedger.Time

  @type ambiguity_policy :: :retry | :manual | :reconcile

  @schema Zoi.struct(__MODULE__, %{
            id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            key: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            kind: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            queue: Zoi.string() |> Zoi.default("default") |> Zoi.optional(),
            shard: Zoi.integer() |> Zoi.gte(0) |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            payload: Zoi.any() |> Zoi.default(%{}) |> Zoi.optional(),
            context: Zoi.map() |> Zoi.default(%{}) |> Zoi.optional(),
            visible_at: Zoi.datetime() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            max_attempts: Zoi.integer() |> Zoi.positive() |> Zoi.default(3) |> Zoi.optional(),
            priority: Zoi.integer() |> Zoi.default(0) |> Zoi.optional(),
            idempotency_key: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            correlation_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            causation_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            root_intent_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            parent_intent_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            depth: Zoi.integer() |> Zoi.gte(0) |> Zoi.default(0) |> Zoi.optional(),
            actor: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            ambiguity_policy: Zoi.enum([:retry, :manual, :reconcile]) |> Zoi.default(:retry) |> Zoi.optional(),
            metadata: Zoi.map() |> Zoi.default(%{}) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  @fields MapSet.new([
            :id,
            :key,
            :kind,
            :queue,
            :shard,
            :payload,
            :context,
            :visible_at,
            :max_attempts,
            :priority,
            :idempotency_key,
            :correlation_id,
            :causation_id,
            :root_intent_id,
            :parent_intent_id,
            :depth,
            :actor,
            :ambiguity_policy,
            :metadata
          ])
  @ambiguity_policies [:retry, :manual, :reconcile]

  @doc """
  Builds and validates an intent from a map, keyword list, or existing struct.
  """
  @spec new(t() | map() | keyword(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs, opts \\ [])

  def new(%__MODULE__{} = intent, opts) do
    intent
    |> Map.from_struct()
    |> new(opts)
  end

  def new(attrs, opts) when is_list(attrs) do
    attrs
    |> Map.new()
    |> new(opts)
  end

  def new(attrs, opts) when is_map(attrs) do
    now = Keyword.get(opts, :now, Time.utc_now())
    attrs = normalize_keys(attrs)

    with {:ok, visible_at} <- Time.normalize(Map.get(attrs, :visible_at), now),
         {:ok, queue} <- normalize_string(Map.get(attrs, :queue, "default"), :queue),
         {:ok, kind} <- normalize_string(Map.get(attrs, :kind), :kind),
         {:ok, key} <- normalize_key(attrs),
         {:ok, max_attempts} <-
           normalize_positive_integer(Map.get(attrs, :max_attempts, 3), :max_attempts),
         {:ok, priority} <- normalize_integer(Map.get(attrs, :priority, 0), :priority),
         {:ok, shard} <- normalize_shard(Map.get(attrs, :shard)),
         {:ok, ambiguity_policy} <-
           normalize_ambiguity_policy(Map.get(attrs, :ambiguity_policy, :retry)),
         {:ok, context} <- normalize_map(Map.get(attrs, :context, %{}), :context),
         {:ok, metadata} <- normalize_map(Map.get(attrs, :metadata, %{}), :metadata),
         {:ok, correlation_id} <-
           normalize_optional_string(lineage_attr(attrs, metadata, :correlation_id), :correlation_id),
         {:ok, causation_id} <-
           normalize_optional_string(lineage_attr(attrs, metadata, :causation_id), :causation_id),
         {:ok, root_intent_id} <-
           normalize_optional_string(lineage_attr(attrs, metadata, :root_intent_id), :root_intent_id),
         {:ok, parent_intent_id} <-
           normalize_optional_string(lineage_attr(attrs, metadata, :parent_intent_id), :parent_intent_id),
         {:ok, depth} <- normalize_non_negative_integer(lineage_attr(attrs, metadata, :depth, 0), :depth),
         {:ok, actor} <- normalize_optional_string(lineage_attr(attrs, metadata, :actor), :actor) do
      id = attrs |> Map.get(:id, ID.generate("int")) |> to_string()
      idempotency_key = Map.get(attrs, :idempotency_key)
      correlation_id = correlation_id || id
      root_intent_id = root_intent_id || id

      metadata =
        put_lineage_metadata(metadata, %{
          correlation_id: correlation_id,
          causation_id: causation_id,
          root_intent_id: root_intent_id,
          parent_intent_id: parent_intent_id,
          depth: depth,
          actor: actor
        })

      %__MODULE__{
        id: id,
        key: key,
        kind: kind,
        queue: queue,
        shard: shard,
        payload: Map.get(attrs, :payload, %{}),
        context: context,
        visible_at: visible_at,
        max_attempts: max_attempts,
        priority: priority,
        idempotency_key: if(is_nil(idempotency_key), do: nil, else: to_string(idempotency_key)),
        correlation_id: correlation_id,
        causation_id: causation_id,
        root_intent_id: root_intent_id,
        parent_intent_id: parent_intent_id,
        depth: depth,
        actor: actor,
        ambiguity_policy: ambiguity_policy,
        metadata: metadata
      }
      |> validate()
    end
  end

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Intent.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  @doc false
  @spec with_shard(t(), non_neg_integer()) :: t()
  def with_shard(%__MODULE__{} = intent, shard) when is_integer(shard) and shard >= 0 do
    %{intent | shard: shard}
  end

  defp normalize_keys(attrs) do
    Enum.reduce(attrs, %{}, fn {key, value}, acc ->
      Map.put(acc, normalize_key_name(key), value)
    end)
  end

  defp normalize_key_name(key) when is_atom(key), do: key

  defp normalize_key_name(key) when is_binary(key) do
    Enum.find(@fields, key, &(Atom.to_string(&1) == key))
  end

  defp normalize_key_name(key), do: key

  defp normalize_string(nil, field), do: {:error, {:required, field}}

  defp normalize_string(value, field) when is_binary(value) do
    if String.trim(value) == "" do
      {:error, {:required, field}}
    else
      {:ok, value}
    end
  end

  defp normalize_string(value, field) when is_atom(value),
    do: normalize_string(to_string(value), field)

  defp normalize_string(value, field), do: {:error, {:invalid_string, field, value}}

  defp normalize_key(attrs) do
    attrs
    |> Map.get(:key, Map.get(attrs, :idempotency_key))
    |> normalize_string(:key)
  end

  defp normalize_positive_integer(value, _field) when is_integer(value) and value > 0,
    do: {:ok, value}

  defp normalize_positive_integer(value, field),
    do: {:error, {:invalid_positive_integer, field, value}}

  defp normalize_integer(value, _field) when is_integer(value), do: {:ok, value}
  defp normalize_integer(value, field), do: {:error, {:invalid_integer, field, value}}

  defp normalize_non_negative_integer(value, field) do
    with {:ok, integer} <- normalize_lineage_integer(value, field) do
      if integer >= 0 do
        {:ok, integer}
      else
        {:error, {:invalid_non_negative_integer, field, value}}
      end
    end
  end

  defp normalize_lineage_integer(value, _field) when is_integer(value), do: {:ok, value}

  defp normalize_lineage_integer(value, field) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> {:ok, integer}
      _invalid -> {:error, {:invalid_integer, field, value}}
    end
  end

  defp normalize_lineage_integer(value, field), do: {:error, {:invalid_integer, field, value}}

  defp normalize_shard(nil), do: {:ok, nil}
  defp normalize_shard(value) when is_integer(value) and value >= 0, do: {:ok, value}
  defp normalize_shard(value), do: {:error, {:invalid_shard, value}}

  defp normalize_ambiguity_policy(value) when value in @ambiguity_policies, do: {:ok, value}

  defp normalize_ambiguity_policy(value) when is_binary(value) do
    value
    |> String.to_existing_atom()
    |> normalize_ambiguity_policy()
  rescue
    ArgumentError -> {:error, {:invalid_ambiguity_policy, value}}
  end

  defp normalize_ambiguity_policy(value), do: {:error, {:invalid_ambiguity_policy, value}}

  defp normalize_map(value, _field) when is_map(value), do: {:ok, value}
  defp normalize_map(value, field), do: {:error, {:invalid_map, field, value}}

  defp normalize_optional_string(nil, _field), do: {:ok, nil}

  defp normalize_optional_string(value, field) do
    case value |> to_string() |> String.trim() do
      "" -> {:error, {:invalid_string, field, value}}
      normalized -> {:ok, normalized}
    end
  end

  defp lineage_attr(attrs, metadata, field, default \\ nil) do
    Map.get(attrs, field, metadata_field(metadata, field, default))
  end

  defp metadata_field(metadata, field, default) do
    Map.get(metadata, field, Map.get(metadata, Atom.to_string(field), default))
  end

  defp put_lineage_metadata(metadata, lineage) do
    Enum.reduce(lineage, metadata, fn
      {_field, nil}, acc -> acc
      {field, value}, acc -> Map.put(acc, field, value)
    end)
  end

  defp validate(%__MODULE__{} = intent) do
    case Zoi.parse(@schema, intent) do
      {:ok, intent} -> {:ok, intent}
      {:error, errors} -> {:error, {:invalid_intent, errors}}
    end
  end
end
