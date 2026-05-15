defmodule IntentLedger.Runtime do
  @moduledoc false

  alias Bedrock.JobQueue.Lease
  alias IntentLedger.Intent
  alias IntentLedger.Runtime.{Commands, Execution, Inspection, QueueLifecycle}

  @type replay_source :: Inspection.replay_source()
  @type projection_ref :: Inspection.projection_ref()
  @type outbox_consumer_ref :: Inspection.outbox_consumer_ref()

  @doc false
  @spec submit(module(), Jido.Signal.t(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  defdelegate submit(ledger, signal, opts \\ []), to: Commands

  @doc false
  @spec enqueue(module(), String.t() | atom(), term(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  defdelegate enqueue(ledger, topic, payload, opts \\ []), to: Commands

  @doc false
  @spec enqueue_many(module(), Enumerable.t(), keyword()) :: {:ok, [Intent.t()]} | {:error, term()}
  defdelegate enqueue_many(ledger, entries, opts \\ []), to: Commands

  @doc false
  @spec fetch(module(), String.t()) :: {:ok, Intent.t()} | {:error, :not_found}
  defdelegate fetch(ledger, intent_id), to: Inspection

  @doc false
  @spec history(module(), String.t(), keyword()) :: {:ok, [Jido.Signal.t()]} | {:error, term()}
  defdelegate history(ledger, intent_id, opts \\ []), to: Inspection

  @doc false
  @spec replay(module(), replay_source(), keyword()) :: {:ok, [Jido.Signal.t()]} | {:error, term()}
  defdelegate replay(ledger, source, opts \\ []), to: Inspection

  @doc false
  @spec replay_entries(module(), replay_source(), keyword()) ::
          {:ok, [IntentLedger.ReplayEntry.t()]} | {:error, term()}
  defdelegate replay_entries(ledger, source, opts \\ []), to: Inspection

  @doc false
  @spec read_outbox(module(), outbox_consumer_ref(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate read_outbox(ledger, consumer, opts \\ []), to: Inspection

  @doc false
  @spec outbox_cursor(module(), outbox_consumer_ref(), keyword()) ::
          {:ok, non_neg_integer() | nil} | {:error, term()}
  defdelegate outbox_cursor(ledger, consumer, opts \\ []), to: Inspection

  @doc false
  @spec ack_outbox(module(), outbox_consumer_ref(), non_neg_integer(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate ack_outbox(ledger, consumer, cursor, opts \\ []), to: Inspection

  @doc false
  @spec projection_cursor(module(), projection_ref(), keyword()) ::
          {:ok, non_neg_integer() | nil} | {:error, term()}
  defdelegate projection_cursor(ledger, projection, opts \\ []), to: Inspection

  @doc false
  @spec put_projection_cursor(module(), projection_ref(), non_neg_integer(), keyword()) :: :ok | {:error, term()}
  defdelegate put_projection_cursor(ledger, projection, cursor, opts \\ []), to: Inspection

  @doc false
  @spec cancel(module(), String.t(), term(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  defdelegate cancel(ledger, intent_id, reason, opts \\ []), to: Commands

  @doc false
  @spec requeue(module(), String.t(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  defdelegate requeue(ledger, intent_id, opts \\ []), to: Commands

  @doc false
  @spec mark_ambiguous(module(), String.t(), term(), keyword()) :: {:ok, Intent.t()} | {:error, term()}
  defdelegate mark_ambiguous(ledger, intent_id, reason, opts \\ []), to: Commands

  @doc false
  @spec inspect(module(), atom(), keyword()) :: {:ok, term()} | {:error, term()}
  defdelegate inspect(ledger, view, opts), to: Inspection

  @doc false
  @spec stats(module(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate stats(ledger, opts \\ []), to: Inspection

  @doc false
  @spec health(module(), keyword()) :: {:ok, map()}
  defdelegate health(ledger, opts \\ []), to: Inspection

  @doc false
  @spec perform(module(), term(), map()) :: IntentLedger.Handler.result()
  defdelegate perform(handler, queue_payload, job_meta), to: Execution

  @doc false
  @spec apply_queue_action(module(), module(), Lease.t(), term(), term(), term()) :: :ok | {:error, term()}
  defdelegate apply_queue_action(ledger, repo, lease, action, handler_result, queue_result), to: QueueLifecycle
end
