defmodule IntentLedger.Runtime do
  @moduledoc false

  alias Bedrock.JobQueue.{Internal, Item, Store}
  alias IntentLedger.{BedrockStore, Context, Intent, Telemetry, Time}

  @type replay_source :: :ledger | {:intent, String.t()}

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
  @spec cancel(module(), String.t(), term(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  def cancel(ledger, intent_id, reason, _opts \\ []) do
    BedrockStore.update_intent(ledger, intent_id, :canceled, %{reason: reason}, fn intent, now ->
      %{intent | status: :canceled, cancel_reason: reason, updated_at: now, completed_at: now}
    end)
  end

  @doc false
  @spec requeue(module(), String.t(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  def requeue(ledger, intent_id, opts \\ []) do
    BedrockStore.transact(ledger, fn repo, root ->
      with {:ok, intent} <- BedrockStore.fetch(repo, root, intent_id),
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
        Store.enqueue(repo, queue_root(ledger), queue_item(next, ledger, now), now: DateTime.to_unix(now, :millisecond))

        BedrockStore.record_lifecycle(repo, root, ledger, next, :retry_scheduled, %{
          reason: Keyword.get(opts, :reason, :manual_requeue)
        })

        {:ok, next}
      end
    end)
  end

  @doc false
  @spec mark_ambiguous(module(), String.t(), term(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  def mark_ambiguous(ledger, intent_id, reason, _opts \\ []) do
    BedrockStore.update_intent(ledger, intent_id, :ambiguous, %{reason: reason}, fn intent, now ->
      %{intent | status: :ambiguous, error: reason, updated_at: now}
    end)
  end

  @doc false
  @spec inspect(module(), atom(), keyword()) :: {:ok, term()} | {:error, term()}
  def inspect(ledger, :queues, opts), do: stats(ledger, opts)
  def inspect(ledger, :outbox, opts), do: BedrockStore.outbox(ledger, opts)
  def inspect(_ledger, view, _opts), do: {:error, {:unsupported_inspection_view, view}}

  @doc false
  @spec stats(module(), keyword()) :: {:ok, map()} | {:error, term()}
  def stats(ledger, opts \\ []) do
    queue = opts |> Keyword.get(:queue, "default") |> to_string()

    case ledger.__intent_ledger__().job_queue.stats(queue, opts) do
      %{pending_count: _pending, processing_count: _processing} = stats -> {:ok, %{queue => stats}}
      {:error, reason} -> {:error, reason}
      other -> {:ok, %{queue => other}}
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
       topics: config.handlers |> Map.keys() |> Enum.sort()
     }}
  end

  @doc false
  @spec perform(module(), term(), map()) :: IntentLedger.Handler.result()
  def perform(handler, queue_payload, job_meta) do
    with {:ok, %{ledger: ledger, intent_id: intent_id}} <- decode_queue_payload(queue_payload),
         {:ok, intent} <- BedrockStore.fetch(ledger, intent_id) do
      if Intent.runnable?(intent) do
        execute_handler(ledger, handler, intent, normalize_job_meta(job_meta))
      else
        :ok
      end
    else
      {:error, :not_found} -> {:discard, :intent_not_found}
      {:error, reason} -> {:discard, reason}
    end
  end

  defp execute_handler(ledger, handler, %Intent{} = intent, job_meta) do
    with {:ok, started} <- mark_started(ledger, intent, job_meta),
         {:ok, payload} <- validate_payload(handler, started.payload) do
      context = Context.new(ledger, started, job_meta)

      handler
      |> safe_handle(payload, context)
      |> finalize_handler_result(ledger, handler, started, job_meta)
    else
      {:error, {:invalid_payload, _errors} = reason} ->
        lifecycle_result(mark_discarded(ledger, intent, reason), {:discard, reason})

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp finalize_handler_result(:ok, ledger, _handler, intent, _job_meta) do
    lifecycle_result(mark_completed(ledger, intent, nil), :ok)
  end

  defp finalize_handler_result({:ok, result}, ledger, handler, intent, _job_meta) do
    case validate_result(handler, result) do
      {:ok, result} ->
        lifecycle_result(mark_completed(ledger, intent, result), {:ok, result})

      {:error, reason} ->
        lifecycle_result(mark_discarded(ledger, intent, reason), {:discard, reason})
    end
  end

  defp finalize_handler_result({:discard, reason}, ledger, _handler, intent, _job_meta) do
    lifecycle_result(mark_discarded(ledger, intent, reason), {:discard, reason})
  end

  defp finalize_handler_result({:snooze, delay_ms}, ledger, _handler, intent, job_meta)
       when is_integer(delay_ms) and delay_ms >= 0 do
    lifecycle_result(mark_retry_scheduled(ledger, intent, job_meta, {:snooze, delay_ms}), {:snooze, delay_ms})
  end

  defp finalize_handler_result({:error, reason}, ledger, _handler, intent, job_meta) do
    lifecycle =
      if Map.fetch!(job_meta, :attempt) >= intent.max_attempts do
        mark_failed(ledger, intent, job_meta, reason)
      else
        mark_retry_scheduled(ledger, intent, job_meta, reason)
      end

    lifecycle_result(lifecycle, {:error, reason})
  end

  defp finalize_handler_result(other, ledger, _handler, intent, _job_meta) do
    reason = {:invalid_handler_return, other}
    lifecycle_result(mark_discarded(ledger, intent, reason), {:discard, reason})
  end

  defp lifecycle_result({:ok, _intent}, queue_result), do: queue_result
  defp lifecycle_result({:error, reason}, _queue_result), do: {:error, {:intent_lifecycle_update_failed, reason}}

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

  defp mark_completed(ledger, intent, result) do
    BedrockStore.update_intent(ledger, intent.id, :completed, %{attempt: intent.attempt, result: result}, fn intent,
                                                                                                             now ->
      %{intent | status: :completed, result: result, updated_at: now, completed_at: now}
    end)
  end

  defp mark_failed(ledger, intent, job_meta, reason) do
    BedrockStore.update_intent(
      ledger,
      intent.id,
      :failed,
      %{attempt: Map.fetch!(job_meta, :attempt), error: reason},
      fn intent, now ->
        %{
          intent
          | status: :failed,
            error: reason,
            attempt: Map.fetch!(job_meta, :attempt),
            updated_at: now,
            completed_at: now
        }
      end
    )
  end

  defp mark_retry_scheduled(ledger, intent, job_meta, reason) do
    BedrockStore.update_intent(
      ledger,
      intent.id,
      :retry_scheduled,
      %{attempt: Map.fetch!(job_meta, :attempt), error: reason},
      fn intent, now ->
        %{intent | status: :retry_scheduled, error: reason, attempt: Map.fetch!(job_meta, :attempt), updated_at: now}
      end
    )
  end

  defp mark_discarded(ledger, intent, reason) do
    BedrockStore.update_intent(ledger, intent.id, :discarded, %{reason: reason}, fn intent, now ->
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
    topic = normalize_topic(topic)

    with {:ok, _handler} <- handler_for(ledger, topic) do
      attrs =
        opts
        |> Keyword.merge(entry_opts)
        |> Map.new()
        |> Map.merge(%{topic: topic, payload: payload})

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

    topic = normalize_topic(topic)

    with {:ok, _handler} <- handler_for(ledger, topic) do
      {:ok, %{attrs | topic: topic}}
    end
  end

  defp handler_for(ledger, topic) do
    case Map.fetch(ledger.__intent_ledger__().handlers, topic) do
      {:ok, handler} -> {:ok, handler}
      :error -> {:error, {:unknown_topic, topic}}
    end
  end

  defp normalize_topic(topic) when is_atom(topic), do: Atom.to_string(topic)
  defp normalize_topic(topic), do: to_string(topic)

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

  defp queue_root(ledger), do: Internal.root_keyspace(ledger.__intent_ledger__().job_queue)

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
