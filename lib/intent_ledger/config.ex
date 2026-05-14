defmodule IntentLedger.Config do
  @moduledoc false

  @type queue_id :: String.t()
  @type queue_config :: %{required(:id) => queue_id(), optional(term()) => term()}
  @type queue_configs :: %{queue_id() => queue_config()}

  @doc false
  @spec normalize_queues!(term()) :: queue_configs()
  def normalize_queues!(nil), do: normalize_queues!(["default"])
  def normalize_queues!([]), do: normalize_queues!(["default"])

  def normalize_queues!(queues) when is_map(queues) do
    queues
    |> Enum.map(fn {id, attrs} -> normalize_queue_definition!({id, attrs}) end)
    |> build_queue_map()
  end

  def normalize_queues!(queues) when is_list(queues) do
    queues
    |> Enum.map(&normalize_queue_definition!/1)
    |> build_queue_map()
  end

  def normalize_queues!(queues) do
    raise ArgumentError, "invalid IntentLedger queues config: #{inspect(queues)}"
  end

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

  defp normalize_queue_definition!(%{id: id} = attrs), do: normalize_queue_map!(id, attrs)
  defp normalize_queue_definition!(%{"id" => id} = attrs), do: normalize_queue_map!(id, attrs)

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

  defp build_queue_map(configs) do
    ids = Enum.map(configs, & &1.id)
    unique_count = ids |> MapSet.new() |> MapSet.size()

    if length(ids) != unique_count do
      raise ArgumentError, "IntentLedger queues config contains duplicate queue IDs"
    end

    Map.new(configs, fn %{id: id} = config -> {id, config} end)
  end
end
