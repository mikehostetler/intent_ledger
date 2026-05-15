defmodule IntentLedger.Runtime.Enqueue do
  @moduledoc false

  alias IntentLedger.{BedrockStore, Command, Config, Intent, Telemetry}
  alias IntentLedger.Runtime.Queue

  @transaction_opts [:retry_limit, :timeout_in_ms, :transaction_system_layout]

  @spec many(module(), Enumerable.t(), keyword()) :: {:ok, [Intent.t()]} | {:error, term()}
  @doc false
  def many(ledger, entries, opts \\ []) do
    start = System.monotonic_time()
    opts = ensure_command_metadata(opts)
    {transaction_opts, intent_opts} = Keyword.split(opts, @transaction_opts)

    result =
      with {:ok, normalized} <- normalize_entries(ledger, entries, intent_opts) do
        BedrockStore.transact(
          ledger,
          fn repo, root ->
            now = DateTime.utc_now()

            normalized
            |> Enum.reduce_while({:ok, []}, fn attrs, {:ok, acc} ->
              case Intent.new(attrs, now: now) do
                {:ok, intent} ->
                  case BedrockStore.create_intent(ledger, repo, root, intent, now: now) do
                    {:ok, created, :created} ->
                      Queue.enqueue_intent(repo, ledger, created, now)
                      {:cont, {:ok, [created | acc]}}

                    {:ok, existing, :existing} ->
                      {:cont, {:ok, [existing | acc]}}
                  end

                {:error, reason} ->
                  {:halt, {:error, reason}}
              end
            end)
            |> case do
              {:ok, intents} -> {:ok, Enum.reverse(intents)}
              error -> error
            end
          end,
          transaction_opts
        )
      end

    Telemetry.emit(:enqueue, result, start, ledger, [count: result_count(result)] ++ telemetry_metadata(opts))
    result
  end

  defp normalize_entries(ledger, entries, opts) do
    entries
    |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
      case normalize_entry(ledger, entry, opts) do
        {:ok, attrs} -> {:cont, {:ok, [attrs | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, attrs} -> {:ok, Enum.reverse(attrs)}
      error -> error
    end
  end

  defp normalize_entry(ledger, {topic, payload}, opts), do: normalize_entry(ledger, {topic, payload, []}, opts)

  defp normalize_entry(ledger, {topic, payload, entry_opts}, opts) when is_list(entry_opts) do
    with {:ok, topic} <- Config.normalize_topic(topic),
         {:ok, intent_config} <- intent_for(ledger, topic),
         attrs =
           opts
           |> merge_entry_opts(entry_opts)
           |> Map.new()
           |> Map.merge(%{topic: topic, payload: payload}),
         {:ok, attrs} <- put_configured_queue(ledger, intent_config, attrs) do
      {:ok, attrs}
    end
  end

  defp normalize_entry(ledger, %{topic: topic, payload: payload} = entry, opts) do
    normalize_map_entry(ledger, entry, topic, payload, opts)
  end

  defp normalize_entry(ledger, %{"topic" => topic, "payload" => payload} = entry, opts) do
    normalize_map_entry(ledger, entry, topic, payload, opts)
  end

  defp normalize_entry(_ledger, entry, _opts), do: {:error, {:invalid_entry, entry}}

  defp normalize_map_entry(ledger, entry, topic, payload, opts) do
    entry_opts = Keyword.merge(Map.get(entry, :opts, []) || [], Map.get(entry, "opts", []) || [])

    attrs =
      opts
      |> merge_entry_opts(entry_opts)
      |> Map.new()
      |> Map.merge(Map.drop(entry, [:topic, "topic", :payload, "payload", :opts, "opts"]))
      |> Map.merge(%{topic: topic, payload: payload})

    with {:ok, topic} <- Config.normalize_topic(topic),
         {:ok, intent_config} <- intent_for(ledger, topic),
         {:ok, attrs} <- put_configured_queue(ledger, intent_config, attrs) do
      {:ok, %{attrs | topic: topic}}
    end
  end

  defp put_configured_queue(ledger, intent_config, attrs) do
    config = ledger.__intent_ledger__()

    attrs
    |> Map.get(:queue, Map.get(attrs, "queue", Map.get(intent_config, :queue, config.default_queue)))
    |> Config.normalize_queue_id()
    |> case do
      {:ok, queue} ->
        with :ok <- ensure_configured_queue(config, queue) do
          {:ok, attrs |> Map.delete("queue") |> Map.put(:queue, queue)}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_configured_queue(%{queues: queues}, queue) do
    if Map.has_key?(queues, queue), do: :ok, else: {:error, {:unknown_queue, queue}}
  end

  defp ensure_command_metadata(opts) do
    if Keyword.has_key?(opts, :command_metadata) do
      opts
    else
      Command.direct_opts(opts)
    end
  end

  defp merge_entry_opts(opts, entry_opts) do
    merged = Keyword.merge(opts, entry_opts)
    metadata = Map.merge(metadata_map(Keyword.get(entry_opts, :metadata)), metadata_map(Keyword.get(opts, :metadata)))

    Keyword.put(merged, :metadata, metadata)
  end

  defp metadata_map(metadata) when is_map(metadata), do: metadata

  defp metadata_map(metadata) when is_list(metadata) do
    Map.new(metadata)
  rescue
    ArgumentError -> %{}
  end

  defp metadata_map(_metadata), do: %{}

  defp intent_for(ledger, topic) do
    case Map.fetch(ledger.__intent_ledger__().intents, topic) do
      {:ok, intent_config} -> {:ok, intent_config}
      :error -> {:error, {:unknown_topic, topic}}
    end
  end

  defp telemetry_metadata(opts) do
    case Keyword.get(opts, :command_metadata) do
      metadata when is_map(metadata) -> Map.to_list(metadata)
      _other -> []
    end
  end

  defp result_count({:ok, values}) when is_list(values), do: length(values)
  defp result_count(_result), do: 0
end
