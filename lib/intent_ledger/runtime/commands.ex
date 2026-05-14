defmodule IntentLedger.Runtime.Commands do
  @moduledoc false

  alias Bedrock.Keyspace
  alias Bedrock.JobQueue.{Internal, Item, Store}
  alias IntentLedger.{BedrockStore, Command, Config, Intent, Telemetry, Time}

  @queue_neutralization_scan_limit 10_000

  @spec submit(module(), Jido.Signal.t(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  @doc false
  def submit(ledger, signal, opts \\ [])

  def submit(ledger, %Jido.Signal{} = signal, opts) do
    with {:ok, command} <- Command.from_signal(signal, opts) do
      execute(ledger, command)
    end
  end

  def submit(_ledger, signal, _opts), do: {:error, {:invalid_command_signal, signal}}

  @spec enqueue(module(), String.t() | atom(), term(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  @doc false
  def enqueue(ledger, topic, payload, opts \\ []) do
    with {:ok, command} <- Command.enqueue(topic, payload, opts) do
      execute(ledger, command)
    end
  end

  @spec enqueue_many(module(), Enumerable.t(), keyword()) :: {:ok, [Intent.t()]} | {:error, term()}
  @doc false
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

    Telemetry.emit(:enqueue, result, start, ledger, [count: result_count(result)] ++ telemetry_metadata(opts))
    result
  end

  @spec cancel(module(), String.t(), term(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  @doc false
  def cancel(ledger, intent_id, reason, opts \\ []) do
    with {:ok, command} <- Command.cancel(intent_id, reason, opts) do
      execute(ledger, command)
    end
  end

  @spec requeue(module(), String.t(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  @doc false
  def requeue(ledger, intent_id, opts \\ []) do
    with {:ok, command} <- Command.requeue(intent_id, opts) do
      execute(ledger, command)
    end
  end

  @spec mark_ambiguous(module(), String.t(), term(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  @doc false
  def mark_ambiguous(ledger, intent_id, reason, opts \\ []) do
    with {:ok, command} <- Command.mark_ambiguous(intent_id, reason, opts) do
      execute(ledger, command)
    end
  end

  @spec execute(module(), Command.t()) :: {:ok, Intent.t()} | {:error, term()}
  @doc false
  def execute(ledger, %Command{type: :enqueue, topic: topic, payload: payload, opts: opts}) do
    with {:ok, [intent]} <- enqueue_many(ledger, [{topic, payload, opts}], command_outer_opts(opts)) do
      {:ok, intent}
    end
  end

  def execute(ledger, %Command{type: :cancel, intent_id: intent_id, reason: reason, opts: opts}) do
    start = System.monotonic_time()

    result =
      BedrockStore.transact(ledger, fn repo, root ->
        with {:ok, intent} <- BedrockStore.fetch(repo, root, intent_id),
             :ok <- ensure_cancelable(intent) do
          if intent.status == :canceled do
            {:ok, intent}
          else
            now = Time.utc_now()

            next = %{
              intent
              | status: :canceled,
                cancel_reason: reason,
                updated_at: now,
                completed_at: now
            }

            BedrockStore.put_intent(repo, root, next)

            neutralization =
              repo
              |> neutralize_pending_queue_item(queue_root(ledger), intent, opts)
              |> neutralization_status()

            BedrockStore.record_lifecycle(
              repo,
              root,
              ledger,
              next,
              :canceled,
              command_data(%{reason: reason, queue_neutralization: neutralization}, opts)
            )

            {:ok, next}
          end
        end
      end)

    Telemetry.emit(
      :command,
      result,
      start,
      ledger,
      [command: :cancel, intent_id: intent_id] ++ telemetry_metadata(opts)
    )

    result
  end

  def execute(ledger, %Command{type: :requeue, intent_id: intent_id, opts: opts}) do
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

          BedrockStore.record_lifecycle(
            repo,
            root,
            ledger,
            next,
            :retry_scheduled,
            command_data(%{reason: Keyword.get(opts, :reason, :manual_requeue)}, opts)
          )

          {:ok, next}
        end
      end)

    Telemetry.emit(
      :command,
      result,
      start,
      ledger,
      [command: :requeue, intent_id: intent_id] ++ telemetry_metadata(opts)
    )

    result
  end

  def execute(ledger, %Command{type: :mark_ambiguous, intent_id: intent_id, reason: reason, opts: opts}) do
    start = System.monotonic_time()

    result =
      BedrockStore.transact(ledger, fn repo, root ->
        with {:ok, intent} <- BedrockStore.fetch(repo, root, intent_id),
             :ok <- ensure_ambiguousable(intent) do
          if intent.status == :ambiguous do
            {:ok, intent}
          else
            now = Time.utc_now()
            next = %{intent | status: :ambiguous, error: reason, updated_at: now}

            BedrockStore.put_intent(repo, root, next)

            neutralization =
              repo
              |> neutralize_pending_queue_item(queue_root(ledger), intent, opts)
              |> neutralization_status()

            BedrockStore.record_lifecycle(
              repo,
              root,
              ledger,
              next,
              :ambiguous,
              command_data(%{reason: reason, queue_neutralization: neutralization}, opts)
            )

            {:ok, next}
          end
        end
      end)

    Telemetry.emit(
      :command,
      result,
      start,
      ledger,
      [command: :mark_ambiguous, intent_id: intent_id] ++ telemetry_metadata(opts)
    )

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

  defp queue_root(ledger), do: Internal.root_keyspace(ledger.__intent_ledger__().job_queue)

  defp neutralize_pending_queue_item(repo, queue_root, %Intent{} = intent, opts) do
    keyspaces = Store.queue_keyspaces(queue_root, intent.queue)
    limit = Keyword.get(opts, :queue_neutralization_scan_limit, @queue_neutralization_scan_limit)

    keyspaces.items
    |> repo.get_range(limit: limit)
    |> Enum.find_value(:missing, fn {item_key, value} ->
      case :erlang.binary_to_term(value) do
        %Item{id: id, lease_id: nil} when id == intent.id ->
          repo.clear(keyspaces.items, item_key)
          decrement_pending_stat(repo, keyspaces)
          :removed

        %Item{id: id} when id == intent.id ->
          :leased

        _other ->
          false
      end
    end)
  end

  defp decrement_pending_stat(repo, keyspaces) do
    keyspaces.stats
    |> Keyspace.pack("pending")
    |> repo.add(<<-1::64-signed-little>>)
  end

  defp neutralization_status(:removed), do: :removed
  defp neutralization_status(:leased), do: :leased
  defp neutralization_status(:missing), do: :missing

  defp ensure_requeueable(%Intent{status: status}) when status in [:failed, :discarded], do: :ok
  defp ensure_requeueable(%Intent{status: status}), do: {:error, {:not_requeueable, status}}

  defp ensure_cancelable(%Intent{status: status}) when status in [:completed, :failed, :discarded],
    do: {:error, {:not_cancelable, status}}

  defp ensure_cancelable(%Intent{}), do: :ok

  defp ensure_ambiguousable(%Intent{status: status}) when status in [:completed, :failed, :discarded, :canceled],
    do: {:error, {:not_ambiguousable, status}}

  defp ensure_ambiguousable(%Intent{}), do: :ok

  defp command_data(data, opts) do
    case Keyword.get(opts, :command_metadata) do
      metadata when is_map(metadata) -> Map.merge(data, metadata)
      _other -> data
    end
  end

  defp telemetry_metadata(opts) do
    case Keyword.get(opts, :command_metadata) do
      metadata when is_map(metadata) -> Map.to_list(metadata)
      _other -> []
    end
  end

  defp command_outer_opts(opts) do
    case Keyword.fetch(opts, :command_metadata) do
      {:ok, command_metadata} -> [command_metadata: command_metadata]
      :error -> []
    end
  end

  defp result_count({:ok, values}) when is_list(values), do: length(values)
  defp result_count(_result), do: 0
end
