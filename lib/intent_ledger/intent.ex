defmodule IntentLedger.Intent do
  @moduledoc """
  Durable application-level object for one unit of deferred work.

  Payloads are ordinary Elixir terms. IntentLedger stores the full Intent with
  Erlang external term encoding in Bedrock, while `bedrock_job_queue` receives
  only a minimal pointer to the Intent ID.
  """

  alias Jido.Signal.ID

  @type status ::
          :enqueued
          | :started
          | :completed
          | :failed
          | :retry_scheduled
          | :discarded
          | :canceled
          | :ambiguous

  @statuses [
    :enqueued,
    :started,
    :completed,
    :failed,
    :retry_scheduled,
    :discarded,
    :canceled,
    :ambiguous
  ]

  @schema Zoi.struct(__MODULE__, %{
            id: Zoi.string() |> Zoi.default(nil) |> Zoi.optional(),
            key: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            topic: Zoi.string(),
            queue: Zoi.string() |> Zoi.default("default") |> Zoi.optional(),
            payload: Zoi.any() |> Zoi.default(%{}) |> Zoi.optional(),
            context: Zoi.map() |> Zoi.default(%{}) |> Zoi.optional(),
            status: Zoi.enum(@statuses) |> Zoi.default(:enqueued) |> Zoi.optional(),
            priority: Zoi.integer() |> Zoi.gte(0) |> Zoi.default(100) |> Zoi.optional(),
            attempt: Zoi.integer() |> Zoi.gte(0) |> Zoi.default(0) |> Zoi.optional(),
            max_attempts: Zoi.integer() |> Zoi.positive() |> Zoi.default(3) |> Zoi.optional(),
            scheduled_at: Zoi.datetime() |> Zoi.default(nil) |> Zoi.optional(),
            created_at: Zoi.datetime() |> Zoi.default(nil) |> Zoi.optional(),
            updated_at: Zoi.datetime() |> Zoi.default(nil) |> Zoi.optional(),
            completed_at: Zoi.datetime() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            result: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            error: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            cancel_reason: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            root_intent_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            parent_intent_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            depth: Zoi.integer() |> Zoi.gte(0) |> Zoi.default(0) |> Zoi.optional(),
            correlation_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            causation_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            actor: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            metadata: Zoi.map() |> Zoi.default(%{}) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  @fields MapSet.new([
            :id,
            :key,
            :topic,
            :queue,
            :payload,
            :context,
            :status,
            :priority,
            :attempt,
            :max_attempts,
            :scheduled_at,
            :created_at,
            :updated_at,
            :completed_at,
            :result,
            :error,
            :cancel_reason,
            :root_intent_id,
            :parent_intent_id,
            :depth,
            :correlation_id,
            :causation_id,
            :actor,
            :metadata
          ])
  @terminal_statuses [:completed, :failed, :discarded, :canceled]
  @runnable_statuses [:enqueued, :started, :retry_scheduled]

  @doc """
  Builds and validates an Intent from attrs.
  """
  @spec new(map() | keyword() | t(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs, opts \\ [])

  def new(%__MODULE__{} = intent, opts), do: intent |> Map.from_struct() |> new(opts)
  def new(attrs, opts) when is_list(attrs), do: attrs |> Map.new() |> new(opts)

  def new(attrs, opts) when is_map(attrs) do
    now = Keyword.get_lazy(opts, :now, &DateTime.utc_now/0)
    attrs = normalize_keys(attrs)

    with {:ok, topic} <- normalize_string(Map.get(attrs, :topic), :topic),
         {:ok, queue} <- normalize_string(Map.get(attrs, :queue, "default"), :queue),
         {:ok, key} <- normalize_optional_string(Map.get(attrs, :key), :key),
         {:ok, context} <- normalize_map(Map.get(attrs, :context, %{}), :context),
         {:ok, metadata} <- normalize_map(Map.get(attrs, :metadata, %{}), :metadata),
         {:ok, scheduled_at} <- normalize_scheduled_at(Map.get(attrs, :scheduled_at), now),
         {:ok, priority} <- normalize_non_negative_integer(Map.get(attrs, :priority, 100), :priority),
         {:ok, max_attempts} <- normalize_positive_integer(Map.get(attrs, :max_attempts, 3), :max_attempts),
         {:ok, depth} <- normalize_non_negative_integer(lineage_attr(attrs, metadata, :depth, 0), :depth),
         {:ok, correlation_id} <-
           normalize_optional_string(lineage_attr(attrs, metadata, :correlation_id), :correlation_id),
         {:ok, causation_id} <-
           normalize_optional_string(lineage_attr(attrs, metadata, :causation_id), :causation_id),
         {:ok, root_intent_id} <-
           normalize_optional_string(lineage_attr(attrs, metadata, :root_intent_id), :root_intent_id),
         {:ok, parent_intent_id} <-
           normalize_optional_string(lineage_attr(attrs, metadata, :parent_intent_id), :parent_intent_id),
         {:ok, actor} <- normalize_optional_string(lineage_attr(attrs, metadata, :actor), :actor) do
      id = attrs |> Map.get(:id, ID.generate!()) |> to_string()
      correlation_id = correlation_id || id
      root_intent_id = root_intent_id || id

      metadata =
        metadata
        |> put_metadata(:correlation_id, correlation_id)
        |> put_metadata(:causation_id, causation_id)
        |> put_metadata(:root_intent_id, root_intent_id)
        |> put_metadata(:parent_intent_id, parent_intent_id)
        |> put_metadata(:depth, depth)
        |> put_metadata(:actor, actor)

      %__MODULE__{
        id: id,
        key: key,
        topic: topic,
        queue: queue,
        payload: Map.get(attrs, :payload, %{}),
        context: context,
        status: Map.get(attrs, :status, :enqueued),
        priority: priority,
        attempt: Map.get(attrs, :attempt, 0),
        max_attempts: max_attempts,
        scheduled_at: scheduled_at,
        created_at: Map.get(attrs, :created_at, now),
        updated_at: Map.get(attrs, :updated_at, now),
        completed_at: Map.get(attrs, :completed_at),
        result: Map.get(attrs, :result),
        error: Map.get(attrs, :error),
        cancel_reason: Map.get(attrs, :cancel_reason),
        root_intent_id: root_intent_id,
        parent_intent_id: parent_intent_id,
        depth: depth,
        correlation_id: correlation_id,
        causation_id: causation_id,
        actor: actor,
        metadata: metadata
      }
      |> validate()
    end
  end

  @doc """
  Returns true when the Intent is in a terminal status.
  """
  @spec terminal?(t()) :: boolean()
  def terminal?(%__MODULE__{status: status}), do: status in @terminal_statuses

  @doc """
  Returns true when the Intent can be handed to a handler.
  """
  @spec runnable?(t()) :: boolean()
  def runnable?(%__MODULE__{status: status}), do: status in @runnable_statuses

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Intent.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  defp normalize_keys(attrs) do
    Map.new(attrs, fn {key, value} -> {normalize_key(key), value} end)
  end

  defp normalize_key(key) when is_atom(key), do: key

  defp normalize_key(key) when is_binary(key) do
    Enum.find(@fields, key, &(Atom.to_string(&1) == key)) || key
  end

  defp normalize_key(key), do: key

  defp normalize_string(nil, field), do: {:error, {:required, field}}

  defp normalize_string(value, field) do
    value
    |> to_string()
    |> String.trim()
    |> case do
      "" -> {:error, {:required, field}}
      normalized -> {:ok, normalized}
    end
  end

  defp normalize_optional_string(nil, _field), do: {:ok, nil}

  defp normalize_optional_string(value, field) do
    value
    |> to_string()
    |> String.trim()
    |> case do
      "" -> {:error, {:invalid_string, field, value}}
      normalized -> {:ok, normalized}
    end
  end

  defp normalize_map(value, _field) when is_map(value), do: {:ok, value}
  defp normalize_map(value, field), do: {:error, {:invalid_map, field, value}}

  defp normalize_scheduled_at(nil, %DateTime{} = default), do: {:ok, default}
  defp normalize_scheduled_at(%DateTime{} = datetime, _default), do: {:ok, datetime}

  defp normalize_scheduled_at(value, _default) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, reason} -> {:error, {:invalid_datetime, value, reason}}
    end
  end

  defp normalize_scheduled_at(value, _default), do: {:error, {:invalid_datetime, value}}

  defp normalize_positive_integer(value, _field) when is_integer(value) and value > 0, do: {:ok, value}
  defp normalize_positive_integer(value, field), do: {:error, {:invalid_positive_integer, field, value}}

  defp normalize_non_negative_integer(value, _field) when is_integer(value) and value >= 0, do: {:ok, value}
  defp normalize_non_negative_integer(value, field), do: {:error, {:invalid_non_negative_integer, field, value}}

  defp lineage_attr(attrs, metadata, field, default \\ nil) do
    Map.get(attrs, field, Map.get(metadata, field, Map.get(metadata, Atom.to_string(field), default)))
  end

  defp put_metadata(metadata, _key, nil), do: metadata
  defp put_metadata(metadata, key, value), do: Map.put(metadata, key, value)

  defp validate(%__MODULE__{} = intent) do
    case Zoi.parse(@schema, intent) do
      {:ok, intent} -> {:ok, intent}
      {:error, errors} -> {:error, {:invalid_intent, errors}}
    end
  end
end
