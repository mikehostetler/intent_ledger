defmodule IntentLedger.Runtime.Inspection do
  @moduledoc false

  alias IntentLedger.{BedrockStore, Config, Telemetry}

  @type replay_source :: :ledger | :outbox | {:intent, String.t()}
  @type projection_ref :: module() | String.t()
  @type outbox_consumer_ref :: module() | String.t()

  @spec fetch(module(), String.t()) :: {:ok, IntentLedger.Intent.t()} | {:error, :not_found}
  @doc false
  def fetch(ledger, intent_id), do: BedrockStore.fetch(ledger, intent_id)

  @spec history(module(), String.t(), keyword()) :: {:ok, [Jido.Signal.t()]} | {:error, term()}
  @doc false
  def history(ledger, intent_id, opts \\ []), do: BedrockStore.history(ledger, intent_id, opts)

  @spec replay(module(), replay_source(), keyword()) :: {:ok, [Jido.Signal.t()]} | {:error, term()}
  @doc false
  def replay(ledger, source, opts \\ []) do
    start = System.monotonic_time()
    result = BedrockStore.replay(ledger, source, opts)
    Telemetry.emit(:replay, result, start, ledger, source: replay_source_metadata(source), count: result_count(result))
    result
  end

  @spec replay_entries(module(), replay_source(), keyword()) :: {:ok, [IntentLedger.ReplayEntry.t()]} | {:error, term()}
  @doc false
  def replay_entries(ledger, source, opts \\ []) do
    start = System.monotonic_time()
    result = BedrockStore.replay_entries(ledger, source, opts)
    Telemetry.emit(:replay, result, start, ledger, source: replay_source_metadata(source), count: result_count(result))
    result
  end

  @spec read_outbox(module(), outbox_consumer_ref(), keyword()) :: {:ok, map()} | {:error, term()}
  @doc false
  def read_outbox(ledger, consumer, opts \\ []) do
    start = System.monotonic_time()
    result = BedrockStore.read_outbox(ledger, consumer, opts)

    Telemetry.emit(:outbox, result, start, ledger,
      operation: :read,
      consumer: consumer,
      count: outbox_entry_count(result)
    )

    result
  end

  @spec outbox_cursor(module(), outbox_consumer_ref(), keyword()) ::
          {:ok, non_neg_integer() | nil} | {:error, term()}
  @doc false
  def outbox_cursor(ledger, consumer, opts \\ []) do
    start = System.monotonic_time()
    result = BedrockStore.outbox_cursor(ledger, consumer, opts)
    Telemetry.emit(:outbox, result, start, ledger, operation: :cursor, consumer: consumer)
    result
  end

  @spec ack_outbox(module(), outbox_consumer_ref(), non_neg_integer(), keyword()) :: {:ok, map()} | {:error, term()}
  @doc false
  def ack_outbox(ledger, consumer, cursor, opts \\ []) do
    start = System.monotonic_time()
    result = BedrockStore.ack_outbox(ledger, consumer, cursor, opts)
    Telemetry.emit(:outbox, result, start, ledger, operation: :ack, consumer: consumer, cursor: cursor)
    result
  end

  @spec projection_cursor(module(), projection_ref(), keyword()) ::
          {:ok, non_neg_integer() | nil} | {:error, term()}
  @doc false
  def projection_cursor(ledger, projection, opts \\ []) do
    start = System.monotonic_time()
    result = BedrockStore.projection_cursor(ledger, projection, opts)
    Telemetry.emit(:projection, result, start, ledger, operation: :cursor, projection: projection)
    result
  end

  @spec put_projection_cursor(module(), projection_ref(), non_neg_integer(), keyword()) :: :ok | {:error, term()}
  @doc false
  def put_projection_cursor(ledger, projection, cursor, opts \\ []) do
    start = System.monotonic_time()
    result = BedrockStore.put_projection_cursor(ledger, projection, cursor, opts)

    Telemetry.emit(:projection, result, start, ledger,
      operation: :put_cursor,
      projection: projection,
      cursor: cursor
    )

    result
  end

  @spec view(module(), atom(), keyword()) :: {:ok, term()} | {:error, term()}
  @doc false
  def view(ledger, :queues, opts), do: stats(ledger, opts)
  def view(ledger, :intents, opts), do: inspect_intents(ledger, opts)
  def view(ledger, :retries, opts), do: inspect_intents(ledger, Keyword.put(opts, :status, :retry_scheduled))
  def view(ledger, :ambiguous, opts), do: inspect_intents(ledger, Keyword.put(opts, :status, :ambiguous))
  def view(ledger, :outbox, opts), do: BedrockStore.outbox(ledger, opts)
  def view(ledger, :projections, opts), do: BedrockStore.projections(ledger, opts)
  def view(_ledger, view, _opts), do: {:error, {:unsupported_view, view}}

  @spec stats(module(), keyword()) :: {:ok, map()} | {:error, term()}
  @doc false
  def stats(ledger, opts \\ []) do
    config = ledger.__intent_ledger__()

    case Keyword.fetch(opts, :queue) do
      {:ok, queue} ->
        with {:ok, queue} <- Config.normalize_queue_id(queue),
             :ok <- ensure_configured_queue(config, queue),
             {:ok, stats} <- queue_stats(ledger, queue, opts) do
          {:ok, %{queue => stats}}
        end

      :error ->
        config.queues
        |> Config.queue_ids()
        |> Enum.reduce_while({:ok, %{}}, fn queue, {:ok, acc} ->
          case queue_stats(ledger, queue, opts) do
            {:ok, stats} -> {:cont, {:ok, Map.put(acc, queue, stats)}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
    end
  end

  @spec health(module(), keyword()) :: {:ok, map()}
  @doc false
  def health(ledger, _opts \\ []) do
    start = System.monotonic_time()
    config = ledger.__intent_ledger__()

    queue_stats = stats(ledger)
    heads = BedrockStore.heads(ledger)

    result =
      {:ok,
       %{
         status: health_status(queue_stats, heads),
         repo: config.repo,
         job_queue: config.job_queue,
         queues: Config.queue_ids(config.queues),
         default_queue: config.default_queue,
         topics: config.intents |> Map.keys() |> Enum.sort(),
         queue_stats: unwrap_health_value(queue_stats),
         cursors: unwrap_health_value(heads),
         errors: health_errors(queue_stats, heads)
       }}

    Telemetry.emit(:health, result, start, ledger)
    result
  end

  defp inspect_intents(ledger, opts) do
    with {:ok, opts} <- normalize_intent_inspection_opts(ledger.__intent_ledger__(), opts) do
      BedrockStore.intents(ledger, opts)
    end
  end

  defp normalize_intent_inspection_opts(config, opts) do
    with {:ok, opts} <- normalize_inspection_queue(config, opts),
         {:ok, opts} <- normalize_inspection_topic(opts) do
      {:ok, opts}
    end
  end

  defp normalize_inspection_queue(config, opts) do
    case Keyword.fetch(opts, :queue) do
      {:ok, queue} ->
        with {:ok, queue} <- Config.normalize_queue_id(queue),
             :ok <- ensure_configured_queue(config, queue) do
          {:ok, Keyword.put(opts, :queue, queue)}
        end

      :error ->
        {:ok, opts}
    end
  end

  defp normalize_inspection_topic(opts) do
    case Keyword.fetch(opts, :topic) do
      {:ok, topic} ->
        with {:ok, topic} <- Config.normalize_topic(topic) do
          {:ok, Keyword.put(opts, :topic, topic)}
        end

      :error ->
        {:ok, opts}
    end
  end

  defp ensure_configured_queue(%{queues: queues}, queue) do
    if Map.has_key?(queues, queue), do: :ok, else: {:error, {:unknown_queue, queue}}
  end

  defp queue_stats(ledger, queue, opts) do
    case ledger.__intent_ledger__().job_queue.stats(queue, opts) do
      %{pending_count: _pending, processing_count: _processing} = stats -> {:ok, stats}
      {:error, reason} -> {:error, reason}
      other -> {:ok, other}
    end
  end

  defp health_status({:ok, _queue_stats}, {:ok, _heads}), do: :ok
  defp health_status(_queue_stats, _heads), do: :degraded

  defp unwrap_health_value({:ok, value}), do: value
  defp unwrap_health_value({:error, reason}), do: %{error: reason}

  defp health_errors(queue_stats, heads) do
    []
    |> maybe_health_error(:queue_stats, queue_stats)
    |> maybe_health_error(:cursors, heads)
    |> Enum.reverse()
  end

  defp maybe_health_error(errors, _field, {:ok, _value}), do: errors
  defp maybe_health_error(errors, field, {:error, reason}), do: [%{field: field, reason: reason} | errors]

  defp replay_source_metadata({:intent, _intent_id}), do: :intent
  defp replay_source_metadata(source), do: source

  defp outbox_entry_count({:ok, %{entries: entries}}), do: length(entries)
  defp outbox_entry_count(_result), do: 0

  defp result_count({:ok, values}) when is_list(values), do: length(values)
  defp result_count(_result), do: 0
end
