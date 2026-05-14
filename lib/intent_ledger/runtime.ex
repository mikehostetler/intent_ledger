defmodule IntentLedger.Runtime do
  @moduledoc false

  alias Bedrock.JobQueue.{Internal, Item, Lease, Store}
  alias IntentLedger.{BedrockStore, Config, Context, Intent, Telemetry, Time}

  @type replay_source :: :ledger | :outbox | {:intent, String.t()}
  @type projection_ref :: module() | String.t()

  @doc false
  @spec enqueue(module(), String.t() | atom(), term(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  def enqueue(ledger, topic, payload, opts \\ []) do
    with {:ok, [intent]} <- enqueue_many(ledger, [{topic, payload, opts}], []) do
      {:ok, intent}
    end
  end

  @doc false
  @spec enqueue_many(module(), Enumerable.t(), keyword()) :: {:ok, [Intent.t()]} | {:error, term()}
  def enqueue_many(ledger, entries, opts \\ []) do
    start = System.monotonic_time()

    result =
      with {:ok, normalized} <- normalize_entries(ledger, entries, opts) do
        BedrockStore.transact(ledger, fn repo, root ->
          now = Time.utc_now()
          queue_root = queue_root(ledger)

          normalized
          |> Enum.reduce_while({:ok, []}, fn attrs, {:ok, acc} ->
            case Intent.new(attrs, now: now) do
              {:ok, intent} ->
                case BedrockStore.create_intent(ledger, repo, root, intent, now: now) do
                  {:ok, created, :created} ->
                    enqueue_queue_item(repo, queue_root, created, ledger, now)
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
        end)
      end

    Telemetry.emit(:enqueue, result, start, ledger, count: result_count(result))
    result
  end

  @doc false
  @spec fetch(module(), String.t()) :: {:ok, Intent.t()} | {:error, :not_found}
  def fetch(ledger, intent_id), do: BedrockStore.fetch(ledger, intent_id)

  @doc false
  @spec history(module(), String.t(), keyword()) :: {:ok, [Jido.Signal.t()]} | {:error, term()}
  def history(ledger, intent_id, opts \\ []), do: BedrockStore.history(ledger, intent_id, opts)

  @doc false
  @spec replay(module(), replay_source(), keyword()) :: {:ok, [Jido.Signal.t()]} | {:error, term()}
  def replay(ledger, source, opts \\ []), do: BedrockStore.replay(ledger, source, opts)

  @doc false
  @spec projection_cursor(module(), projection_ref(), keyword()) ::
          {:ok, non_neg_integer() | nil} | {:error, term()}
  def projection_cursor(ledger, projection, opts \\ []), do: BedrockStore.projection_cursor(ledger, projection, opts)

  @doc false
  @spec put_projection_cursor(module(), projection_ref(), non_neg_integer(), keyword()) :: :ok | {:error, term()}
  def put_projection_cursor(ledger, projection, cursor, opts \\ []) do
    BedrockStore.put_projection_cursor(ledger, projection, cursor, opts)
  end

  @doc false
  @spec cancel(module(), String.t(), term(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  def cancel(ledger, intent_id, reason, _opts \\ []) do
    start = System.monotonic_time()

    result =
      BedrockStore.update_intent(ledger, intent_id, :canceled, %{reason: reason}, fn intent, now ->
        %{intent | status: :canceled, cancel_reason: reason, updated_at: now, completed_at: now}
      end)

    Telemetry.emit(:command, result, start, ledger, command: :cancel, intent_id: intent_id)
    result
  end

  @doc false
  @spec requeue(module(), String.t(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  def requeue(ledger, intent_id, opts \\ []) do
    start = System.monotonic_time()

    result =
      BedrockStore.transact(ledger, fn repo, root ->
        with {:ok, intent} <- BedrockStore.fetch(repo, root, intent_id),
             :ok <- ensure_configured_queue(ledger.__intent_ledger__(), intent.queue),
             :ok <- ensure_requeueable(intent) do
          now = Time.utc_now()

          next = %{
            intent
            | status: :retry_scheduled,
              error: nil,
              result: nil,
              cancel_reason: nil,
              scheduled_at: Keyword.get(opts, :scheduled_at, now),
              updated_at: now,
              completed_at: nil
          }

          BedrockStore.put_intent(repo, root, next)

          Store.enqueue(repo, queue_root(ledger), queue_item(next, ledger, now),
            now: DateTime.to_unix(now, :millisecond)
          )

          BedrockStore.record_lifecycle(repo, root, ledger, next, :retry_scheduled, %{
            reason: Keyword.get(opts, :reason, :manual_requeue)
          })

          {:ok, next}
        end
      end)

    Telemetry.emit(:command, result, start, ledger, command: :requeue, intent_id: intent_id)
    result
  end

  @doc false
  @spec mark_ambiguous(module(), String.t(), term(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  def mark_ambiguous(ledger, intent_id, reason, _opts \\ []) do
    start = System.monotonic_time()

    result =
      BedrockStore.update_intent(ledger, intent_id, :ambiguous, %{reason: reason}, fn intent, now ->
        %{intent | status: :ambiguous, error: reason, updated_at: now}
      end)

    Telemetry.emit(:command, result, start, ledger, command: :mark_ambiguous, intent_id: intent_id)
    result
  end

  @doc false
  @spec inspect(module(), atom(), keyword()) :: {:ok, term()} | {:error, term()}
  def inspect(ledger, :queues, opts), do: stats(ledger, opts)
  def inspect(ledger, :outbox, opts), do: BedrockStore.outbox(ledger, opts)
  def inspect(_ledger, view, _opts), do: {:error, {:unsupported_inspection_view, view}}

  @doc false
  @spec stats(module(), keyword()) :: {:ok, map()} | {:error, term()}
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

  @doc false
  @spec health(module(), keyword()) :: {:ok, map()}
  def health(ledger, _opts \\ []) do
    config = ledger.__intent_ledger__()

    {:ok,
     %{
       status: :ok,
       repo: config.repo,
       job_queue: config.job_queue,
       queues: Config.queue_ids(config.queues),
       default_queue: config.default_queue,
       topics: config.intents |> Map.keys() |> Enum.sort()
     }}
  end

  @doc false
  @spec perform(module(), term(), map()) :: IntentLedger.Handler.result()
  def perform(handler, queue_payload, job_meta) do
    start = System.monotonic_time()
    job_meta = normalize_job_meta(job_meta)

    {ledger, intent_id, result} =
      case decode_queue_payload(queue_payload) do
        {:ok, %{ledger: ledger, intent_id: intent_id}} ->
          result =
            case BedrockStore.fetch(ledger, intent_id) do
              {:ok, intent} ->
                if Intent.runnable?(intent) do
                  execute_handler(ledger, handler, intent, job_meta)
                else
                  :ok
                end

              {:error, :not_found} ->
                {:discard, :intent_not_found}
            end

          {ledger, intent_id, result}

        {:error, reason} ->
          {nil, nil, {:discard, reason}}
      end

    Telemetry.emit(:handler, result, start, ledger, handler_metadata(handler, intent_id, job_meta))
    result
  end

  @doc false
  @spec apply_queue_action(module(), module(), Lease.t(), term(), term(), term()) :: :ok | {:error, term()}
  def apply_queue_action(ledger, repo, %Lease{} = lease, action, handler_result, queue_result) do
    root = BedrockStore.root_keyspace(ledger)

    with :ok <- ensure_queue_result(queue_result) do
      case BedrockStore.fetch(repo, root, lease.item_id) do
        {:ok, %Intent{} = intent} ->
          if Intent.runnable?(intent) do
            apply_lifecycle_after_queue_action(repo, root, ledger, intent, action, handler_result, queue_result)
          else
            :ok
          end

        {:error, :not_found} ->
          :ok
      end
    end
  end

  defp execute_handler(ledger, handler, %Intent{} = intent, job_meta) do
    with {:ok, started} <- mark_started(ledger, intent, job_meta),
         {:ok, payload} <- validate_payload(handler, started.payload) do
      context = Context.new(ledger, started, job_meta)

      handler
      |> safe_handle(payload, context)
      |> normalize_handler_result(handler)
    else
      {:error, {:invalid_payload, _errors} = reason} ->
        {:discard, reason}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp normalize_handler_result(:ok, _handler), do: :ok

  defp normalize_handler_result({:ok, result}, handler) do
    case validate_result(handler, result) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:discard, reason}
    end
  end

  defp normalize_handler_result({:discard, reason}, _handler), do: {:discard, reason}

  defp normalize_handler_result({:snooze, delay_ms}, _handler)
       when is_integer(delay_ms) and delay_ms >= 0 do
    {:snooze, delay_ms}
  end

  defp normalize_handler_result({:error, reason}, _handler), do: {:error, reason}

  defp normalize_handler_result(other, _handler) do
    reason = {:invalid_handler_return, other}
    {:discard, reason}
  end

  defp apply_lifecycle_after_queue_action(repo, root, ledger, intent, :complete, :ok, :ok) do
    hook_result(mark_completed(repo, root, ledger, intent, nil))
  end

  defp apply_lifecycle_after_queue_action(repo, root, ledger, intent, :complete, {:ok, result}, :ok) do
    hook_result(mark_completed(repo, root, ledger, intent, result))
  end

  defp apply_lifecycle_after_queue_action(repo, root, ledger, intent, :complete, {:discard, reason}, :ok) do
    hook_result(mark_discarded(repo, root, ledger, intent, reason))
  end

  defp apply_lifecycle_after_queue_action(
         repo,
         root,
         ledger,
         intent,
         {:snooze, delay_ms},
         {:snooze, delay_ms},
         {:ok, :requeued}
       ) do
    hook_result(mark_retry_scheduled(repo, root, ledger, intent, {:snooze, delay_ms}))
  end

  defp apply_lifecycle_after_queue_action(repo, root, ledger, intent, :requeue, {:error, reason}, {:ok, :requeued}) do
    hook_result(mark_retry_scheduled(repo, root, ledger, intent, reason))
  end

  defp apply_lifecycle_after_queue_action(repo, root, ledger, intent, :requeue, {:error, reason}, {:ok, :dead_lettered}) do
    hook_result(mark_failed(repo, root, ledger, intent, reason))
  end

  defp apply_lifecycle_after_queue_action(_repo, _root, _ledger, _intent, _action, _handler_result, _queue_result) do
    :ok
  end

  defp hook_result({:ok, _intent}), do: :ok
  defp hook_result({:error, reason}), do: {:error, {:intent_lifecycle_update_failed, reason}}

  defp ensure_queue_result(:ok), do: :ok
  defp ensure_queue_result({:ok, _status}), do: :ok
  defp ensure_queue_result({:error, reason}), do: {:error, reason}

  defp mark_started(ledger, intent, job_meta) do
    BedrockStore.update_intent(
      ledger,
      intent.id,
      :started,
      %{attempt: Map.fetch!(job_meta, :attempt), queue: intent.queue, topic: intent.topic},
      fn intent, now ->
        %{intent | status: :started, attempt: Map.fetch!(job_meta, :attempt), updated_at: now, error: nil}
      end
    )
  end

  defp mark_completed(repo, root, ledger, intent, result) do
    BedrockStore.update_intent(
      repo,
      root,
      ledger,
      intent.id,
      :completed,
      %{attempt: intent.attempt, result: result},
      fn intent, now ->
        %{intent | status: :completed, result: result, updated_at: now, completed_at: now}
      end
    )
  end

  defp mark_failed(repo, root, ledger, intent, reason) do
    BedrockStore.update_intent(
      repo,
      root,
      ledger,
      intent.id,
      :failed,
      %{attempt: intent.attempt, error: reason},
      fn intent, now ->
        %{
          intent
          | status: :failed,
            error: reason,
            updated_at: now,
            completed_at: now
        }
      end
    )
  end

  defp mark_retry_scheduled(repo, root, ledger, intent, reason) do
    BedrockStore.update_intent(
      repo,
      root,
      ledger,
      intent.id,
      :retry_scheduled,
      %{attempt: intent.attempt, error: reason},
      fn intent, now ->
        %{intent | status: :retry_scheduled, error: reason, updated_at: now}
      end
    )
  end

  defp mark_discarded(repo, root, ledger, intent, reason) do
    BedrockStore.update_intent(repo, root, ledger, intent.id, :discarded, %{reason: reason}, fn intent, now ->
      %{intent | status: :discarded, error: reason, updated_at: now, completed_at: now}
    end)
  end

  defp safe_handle(handler, payload, context) do
    handler.handle(payload, context)
  rescue
    exception -> {:error, {:exception, exception}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp validate_payload(handler, payload) do
    case handler.__intent_handler__().payload_schema do
      nil -> {:ok, payload}
      schema -> parse_schema(schema, payload, :invalid_payload)
    end
  end

  defp validate_result(handler, result) do
    case handler.__intent_handler__().result_schema do
      nil -> {:ok, result}
      schema -> parse_schema(schema, result, :invalid_result)
    end
  end

  defp parse_schema(schema, value, error_tag) do
    case Zoi.parse(schema, value) do
      {:ok, parsed} -> {:ok, parsed}
      {:error, errors} -> {:error, {error_tag, errors}}
    end
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
           |> Keyword.merge(entry_opts)
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
      |> Keyword.merge(entry_opts)
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

  defp intent_for(ledger, topic) do
    case Map.fetch(ledger.__intent_ledger__().intents, topic) do
      {:ok, intent_config} -> {:ok, intent_config}
      :error -> {:error, {:unknown_topic, topic}}
    end
  end

  defp enqueue_queue_item(repo, queue_root, intent, ledger, now) do
    Store.enqueue(repo, queue_root, queue_item(intent, ledger, now), now: DateTime.to_unix(now, :millisecond))
  end

  defp queue_item(%Intent{} = intent, ledger, now) do
    Item.new(intent.queue, intent.topic, queue_payload(ledger, intent.id),
      id: intent.id,
      priority: intent.priority,
      max_retries: intent.max_attempts,
      vesting_time: DateTime.to_unix(intent.scheduled_at || now, :millisecond),
      now: DateTime.to_unix(now, :millisecond)
    )
  end

  defp queue_payload(ledger, intent_id), do: :erlang.term_to_binary(%{ledger: ledger, intent_id: intent_id})

  defp decode_queue_payload(%{raw: binary}) when is_binary(binary), do: decode_raw_queue_payload(binary)
  defp decode_queue_payload(%{"raw" => binary}) when is_binary(binary), do: decode_raw_queue_payload(binary)
  defp decode_queue_payload(%{ledger: ledger, intent_id: intent_id}), do: {:ok, %{ledger: ledger, intent_id: intent_id}}

  defp decode_queue_payload(%{"ledger" => ledger, "intent_id" => intent_id}) when is_binary(ledger) do
    {:ok, %{ledger: String.to_existing_atom(ledger), intent_id: intent_id}}
  rescue
    ArgumentError -> {:error, {:unknown_ledger, ledger}}
  end

  defp decode_queue_payload(_payload), do: {:error, :invalid_queue_payload}

  defp normalize_job_meta(meta) do
    %{
      topic: Map.get(meta, :topic),
      queue_id: Map.get(meta, :queue_id),
      item_id: Map.get(meta, :item_id),
      attempt: Map.get(meta, :attempt, 1)
    }
  end

  defp handler_metadata(handler, intent_id, job_meta) do
    [
      handler: handler,
      intent_id: intent_id,
      topic: Map.get(job_meta, :topic),
      queue: Map.get(job_meta, :queue_id),
      item_id: Map.get(job_meta, :item_id),
      attempt: Map.get(job_meta, :attempt)
    ]
  end

  defp queue_root(ledger), do: Internal.root_keyspace(ledger.__intent_ledger__().job_queue)

  defp queue_stats(ledger, queue, opts) do
    case ledger.__intent_ledger__().job_queue.stats(queue, opts) do
      %{pending_count: _pending, processing_count: _processing} = stats -> {:ok, stats}
      {:error, reason} -> {:error, reason}
      other -> {:ok, other}
    end
  end

  defp decode_raw_queue_payload(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    ArgumentError -> {:error, :invalid_queue_payload}
  end

  defp ensure_requeueable(%Intent{status: status}) when status in [:failed, :discarded], do: :ok
  defp ensure_requeueable(%Intent{status: status}), do: {:error, {:not_requeueable, status}}

  defp result_count({:ok, values}) when is_list(values), do: length(values)
  defp result_count(_result), do: 0
end
