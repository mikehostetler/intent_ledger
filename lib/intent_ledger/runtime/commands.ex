defmodule IntentLedger.Runtime.Commands do
  @moduledoc false

  alias IntentLedger.{BedrockStore, Command, DurableTerm, Intent, Telemetry}
  alias IntentLedger.Runtime.Queue

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
  defdelegate enqueue_many(ledger, entries, opts \\ []), to: IntentLedger.Runtime.Enqueue, as: :many

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
    with {:ok, [intent]} <-
           IntentLedger.Runtime.Enqueue.many(ledger, [{topic, payload, opts}], command_outer_opts(opts)) do
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
            now = DateTime.utc_now()
            reason = DurableTerm.summarize(reason)

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
              |> Queue.neutralize_pending_item(ledger, intent, opts)

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
          now = DateTime.utc_now()
          reason = opts |> Keyword.get(:reason, :manual_requeue) |> DurableTerm.summarize()

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

          Queue.enqueue_intent(repo, ledger, next, now)

          BedrockStore.record_lifecycle(
            repo,
            root,
            ledger,
            next,
            :retry_scheduled,
            command_data(%{reason: reason}, opts)
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
            now = DateTime.utc_now()
            reason = DurableTerm.summarize(reason)
            next = %{intent | status: :ambiguous, error: reason, updated_at: now}

            BedrockStore.put_intent(repo, root, next)

            neutralization =
              repo
              |> Queue.neutralize_pending_item(ledger, intent, opts)

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

  defp ensure_configured_queue(%{queues: queues}, queue) do
    if Map.has_key?(queues, queue), do: :ok, else: {:error, {:unknown_queue, queue}}
  end

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
    opts
    |> Keyword.take([:retry_limit, :timeout_in_ms, :transaction_system_layout])
    |> maybe_put_command_metadata(opts)
  end

  defp maybe_put_command_metadata(outer_opts, opts) do
    case Keyword.fetch(opts, :command_metadata) do
      {:ok, command_metadata} -> Keyword.put(outer_opts, :command_metadata, command_metadata)
      :error -> outer_opts
    end
  end
end
