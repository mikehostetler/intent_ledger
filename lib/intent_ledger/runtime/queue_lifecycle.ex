defmodule IntentLedger.Runtime.QueueLifecycle do
  @moduledoc false

  alias Bedrock.JobQueue.Lease
  alias IntentLedger.{BedrockStore, Intent}

  @spec apply_queue_action(module(), module(), Lease.t(), term(), term(), term()) :: :ok | {:error, term()}
  @doc false
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
end
