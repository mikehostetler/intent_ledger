defmodule IntentLedger.Config do
  @moduledoc false

  @type queue_id :: String.t()
  @type queue_config :: %{required(:id) => queue_id(), optional(term()) => term()}
  @type queue_configs :: %{queue_id() => queue_config()}
  @type topic :: String.t()
  @type intent_config :: %{
          required(:topic) => topic(),
          required(:handler) => module(),
          optional(:queue) => queue_id(),
          optional(term()) => term()
        }
  @type intent_configs :: %{topic() => intent_config()}

  @doc false
  @spec normalize_queues!(term()) :: queue_configs()
  def normalize_queues!(queues), do: normalize_queues!(queues, %{})

  @doc false
  @spec normalize_queues!(term(), intent_configs()) :: queue_configs()
  def normalize_queues!(queues, intents) do
    queue_configs = normalize_explicit_queues(queues) ++ queues_from_intents(intents)

    queue_configs =
      if queue_configs == [] do
        [%{id: "default"}]
      else
        queue_configs
      end

    build_queue_map(queue_configs)
  end

  @doc false
  @spec normalize_intents!(term()) :: intent_configs()
  def normalize_intents!(nil), do: raise(ArgumentError, "IntentLedger requires :intents")
  def normalize_intents!([]), do: raise(ArgumentError, "IntentLedger requires at least one intent")

  def normalize_intents!(intents) when is_map(intents) do
    intents
    |> Enum.map(fn {topic, attrs} -> normalize_intent_definition!({topic, attrs}) end)
    |> build_intent_map()
  end

  def normalize_intents!(intents) do
    raise ArgumentError, "invalid IntentLedger intents config: #{inspect(intents)}"
  end

  @doc false
  @spec handlers_from_intents!(term()) :: %{topic() => module()}
  def handlers_from_intents!(intents), do: intents |> normalize_intents!() |> handlers_from_intents()

  @doc false
  @spec handlers_from_intents(intent_configs()) :: %{topic() => module()}
  def handlers_from_intents(intents) do
    Map.new(intents, fn {topic, %{handler: handler}} -> {topic, handler} end)
  end

  @doc false
  @spec normalize_topic(term()) :: {:ok, topic()} | {:error, term()}
  def normalize_topic(topic) when is_binary(topic) or is_atom(topic) do
    topic
    |> to_string()
    |> String.trim()
    |> case do
      "" -> {:error, {:invalid_topic, topic}}
      normalized -> {:ok, normalized}
    end
  end

  def normalize_topic(topic), do: {:error, {:invalid_topic, topic}}

  @doc false
  @spec normalize_default_queue!(term(), queue_configs()) :: queue_id()
  def normalize_default_queue!(nil, queues) do
    cond do
      Map.has_key?(queues, "default") -> "default"
      queues == %{} -> raise ArgumentError, "IntentLedger queues config must define at least one queue"
      true -> queues |> Map.keys() |> Enum.sort() |> List.first()
    end
  end

  def normalize_default_queue!(queue, queues) do
    queue = normalize_queue_id!(queue)

    if Map.has_key?(queues, queue) do
      queue
    else
      raise ArgumentError, "IntentLedger default_queue #{inspect(queue)} is not present in queues config"
    end
  end

  @doc false
  @spec normalize_queue_id(term()) :: {:ok, queue_id()} | {:error, term()}
  def normalize_queue_id(queue) when is_binary(queue) or is_atom(queue) do
    queue
    |> to_string()
    |> String.trim()
    |> case do
      "" -> {:error, {:invalid_queue, queue}}
      normalized -> {:ok, normalized}
    end
  end

  def normalize_queue_id(queue), do: {:error, {:invalid_queue, queue}}

  @doc false
  @spec queue_ids(queue_configs()) :: [queue_id()]
  def queue_ids(queues), do: queues |> Map.keys() |> Enum.sort()

  defp normalize_explicit_queues(nil), do: []
  defp normalize_explicit_queues([]), do: []

  defp normalize_explicit_queues(queues) when is_map(queues) do
    queues
    |> Enum.map(fn {id, attrs} -> normalize_queue_definition!({id, attrs}) end)
    |> ensure_unique_explicit_queues!()
  end

  defp normalize_explicit_queues(queues) when is_list(queues) do
    queues
    |> Enum.map(&normalize_queue_definition!/1)
    |> ensure_unique_explicit_queues!()
  end

  defp normalize_explicit_queues(queues) do
    raise ArgumentError, "invalid IntentLedger queues config: #{inspect(queues)}"
  end

  defp normalize_queue_definition!({id, attrs}) when is_list(attrs) or is_map(attrs) do
    normalize_queue_map!(id, attrs)
  end

  defp normalize_queue_definition!(id) when is_binary(id) or is_atom(id), do: %{id: normalize_queue_id!(id)}

  defp normalize_queue_definition!(definition) do
    raise ArgumentError, "invalid IntentLedger queue definition: #{inspect(definition)}"
  end

  defp normalize_queue_map!(id, attrs) do
    attrs
    |> Map.new()
    |> Map.delete(:id)
    |> Map.delete("id")
    |> Map.put(:id, normalize_queue_id!(id))
  end

  defp normalize_queue_id!(queue) do
    case normalize_queue_id(queue) do
      {:ok, normalized} -> normalized
      {:error, reason} -> raise ArgumentError, "invalid IntentLedger queue id: #{inspect(reason)}"
    end
  end

  defp normalize_intent_definition!({topic, attrs}) when is_list(attrs) or is_map(attrs) do
    normalize_intent_map!(topic, attrs)
  end

  defp normalize_intent_definition!(definition) do
    raise ArgumentError, "invalid IntentLedger intent definition: #{inspect(definition)}"
  end

  defp normalize_intent_map!(topic, attrs) do
    attrs = Map.new(attrs)
    topic = normalize_topic!(Map.get(attrs, :topic, Map.get(attrs, "topic", topic)))
    handler = Map.get(attrs, :handler, Map.get(attrs, "handler"))
    queue = Map.get(attrs, :queue, Map.get(attrs, "queue"))

    unless is_atom(handler) and not is_nil(handler) do
      raise ArgumentError, "IntentLedger intent #{inspect(topic)} requires a handler module"
    end

    validate_handler_topic!(topic, handler)

    attrs =
      attrs
      |> Map.delete(:topic)
      |> Map.delete("topic")
      |> Map.delete(:handler)
      |> Map.delete("handler")
      |> Map.delete(:queue)
      |> Map.delete("queue")
      |> Map.put(:topic, topic)
      |> Map.put(:handler, handler)

    case queue do
      nil -> attrs
      queue -> Map.put(attrs, :queue, normalize_queue_id!(queue))
    end
  end

  defp normalize_topic!(topic) do
    case normalize_topic(topic) do
      {:ok, normalized} -> normalized
      {:error, reason} -> raise ArgumentError, "invalid IntentLedger topic: #{inspect(reason)}"
    end
  end

  defp queues_from_intents(intents) do
    queues =
      intents
      |> Map.values()
      |> Enum.flat_map(fn
        %{queue: queue} -> [%{id: queue}]
        _intent -> []
      end)

    if Enum.any?(intents, fn {_topic, intent} -> not Map.has_key?(intent, :queue) end) do
      [%{id: "default"} | queues]
    else
      queues
    end
  end

  defp ensure_unique_explicit_queues!(configs) do
    ids = Enum.map(configs, & &1.id)
    duplicate_ids = ids -- Enum.uniq(ids)

    case Enum.uniq(duplicate_ids) do
      [] ->
        configs

      duplicates ->
        raise ArgumentError, "IntentLedger queues config contains duplicate queue ids: #{inspect(duplicates)}"
    end
  end

  defp validate_handler_topic!(topic, handler) do
    with {:module, ^handler} <- Code.ensure_loaded(handler),
         true <- function_exported?(handler, :__intent_handler__, 0) do
      validate_loaded_handler_topic!(topic, handler, handler.__intent_handler__().topic)
    else
      _not_loaded_or_no_intent_handler -> :ok
    end
  end

  defp validate_loaded_handler_topic!(_topic, _handler, nil), do: :ok

  defp validate_loaded_handler_topic!(topic, handler, declared_topic) do
    case normalize_topic(declared_topic) do
      {:ok, ^topic} ->
        :ok

      {:ok, normalized} ->
        raise ArgumentError,
              "IntentLedger intent #{inspect(topic)} uses handler #{inspect(handler)} declared for topic #{inspect(normalized)}"

      {:error, reason} ->
        raise ArgumentError,
              "IntentLedger handler #{inspect(handler)} declares invalid topic: #{inspect(reason)}"
    end
  end

  defp build_intent_map(configs) do
    topics = Enum.map(configs, & &1.topic)
    unique_count = topics |> MapSet.new() |> MapSet.size()

    if length(topics) != unique_count do
      raise ArgumentError, "IntentLedger intents config contains duplicate topics"
    end

    Map.new(configs, fn %{topic: topic} = config -> {topic, config} end)
  end

  defp build_queue_map(configs) do
    Enum.reduce(configs, %{}, fn %{id: id} = config, acc -> Map.put_new(acc, id, config) end)
  end
end
