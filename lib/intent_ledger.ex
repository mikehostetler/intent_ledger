defmodule Jido.IntentLedger do
  @moduledoc """
  Public API for named intent ledger instances.

  A ledger records deferred work as immutable `Jido.IntentLedger.Intent` structs,
  tracks mutable lifecycle state in `Jido.IntentLedger.IntentState`, and emits every
  transition as a `Jido.Signal`.

  ## Supervision

      children = [
        {Jido.IntentLedger,
         name: MyApp.IntentLedger,
         queues: [default: [shards: 4]],
         store: Jido.IntentLedger.Store.Memory}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  ## Lifecycle

      {:ok, record} =
        Jido.IntentLedger.submit(MyApp.IntentLedger, %{
          key: "invoice:123",
          kind: "invoice.send",
          payload: %{invoice_id: 123}
        })

      {:ok, claimed} = Jido.IntentLedger.claim(MyApp.IntentLedger, "default", "worker-1")
      {:ok, _record} = Jido.IntentLedger.complete(MyApp.IntentLedger, claimed.claim.id, claimed.claim.token, :ok)
  """

  @type ledger :: GenServer.server()

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  defdelegate child_spec(opts), to: Jido.IntentLedger.Instance

  @spec start_link(keyword()) :: Supervisor.on_start()
  defdelegate start_link(opts), to: Jido.IntentLedger.Instance

  @doc """
  Submits one intent to a ledger.
  """
  @spec submit(ledger(), Jido.IntentLedger.Intent.t() | map() | keyword(), keyword()) ::
          {:ok, Jido.IntentLedger.Record.t()} | {:error, term()}
  def submit(ledger, intent, opts \\ []) do
    call(ledger, {:submit, intent, opts}, opts)
  end

  @doc """
  Submits a batch of intents atomically for the in-memory adapter.
  """
  @spec submit_many(ledger(), [Jido.IntentLedger.Intent.t() | map() | keyword()], keyword()) ::
          {:ok, [Jido.IntentLedger.Record.t()]} | {:error, term()}
  def submit_many(ledger, intents, opts \\ []) do
    call(ledger, {:submit_many, intents, opts}, opts)
  end

  @doc """
  Reads the materialized record for an intent.
  """
  @spec get(ledger(), String.t()) :: {:ok, Jido.IntentLedger.Record.t()} | {:error, :not_found}
  def get(ledger, intent_id), do: GenServer.call(ledger, {:get, intent_id})

  @doc """
  Returns the lifecycle signal history for an intent.
  """
  @spec history(ledger(), String.t()) :: {:ok, [Jido.Signal.t()]} | {:error, :not_found}
  def history(ledger, intent_id), do: GenServer.call(ledger, {:history, intent_id})

  @doc """
  Claims the next available intent from a queue.
  """
  @spec claim(ledger(), String.t() | atom(), String.t(), keyword()) ::
          {:ok, Jido.IntentLedger.Claimed.t() | [Jido.IntentLedger.Claimed.t()]} | :empty | {:error, term()}
  def claim(ledger, queue, owner_id, opts \\ []) do
    call(ledger, {:claim, queue, owner_id, opts}, opts)
  end

  @doc """
  Extends a claim lease.
  """
  @spec heartbeat(ledger(), String.t(), String.t(), keyword()) ::
          {:ok, Jido.IntentLedger.Claim.t()} | {:error, term()}
  def heartbeat(ledger, claim_id, token, opts \\ []) do
    call(ledger, {:heartbeat, claim_id, token, opts}, opts)
  end

  @doc """
  Completes a claimed intent.
  """
  @spec complete(ledger(), String.t(), String.t(), term(), keyword()) ::
          {:ok, Jido.IntentLedger.Record.t()} | {:error, term()}
  def complete(ledger, claim_id, token, result, opts \\ []) do
    call(ledger, {:complete, claim_id, token, result, opts}, opts)
  end

  @doc """
  Fails a claimed intent, retrying or finalizing according to its policy.
  """
  @spec fail(ledger(), String.t(), String.t(), term(), keyword()) ::
          {:ok, Jido.IntentLedger.Record.t()} | {:error, term()}
  def fail(ledger, claim_id, token, error, opts \\ []) do
    call(ledger, {:fail, claim_id, token, error, opts}, opts)
  end

  @doc """
  Releases a claim back to the queue.
  """
  @spec release(ledger(), String.t(), String.t(), keyword()) ::
          {:ok, Jido.IntentLedger.Record.t()} | {:error, term()}
  def release(ledger, claim_id, token, opts \\ []) do
    call(ledger, {:release, claim_id, token, opts}, opts)
  end

  @doc """
  Cancels a non-final intent.
  """
  @spec cancel(ledger(), String.t(), term(), keyword()) ::
          {:ok, Jido.IntentLedger.Record.t()} | {:error, term()}
  def cancel(ledger, intent_id, reason, opts \\ []) do
    call(ledger, {:cancel, intent_id, reason, opts}, opts)
  end

  @doc """
  Requeues a non-final intent for a future attempt.
  """
  @spec requeue(ledger(), String.t(), keyword()) ::
          {:ok, Jido.IntentLedger.Record.t()} | {:error, term()}
  def requeue(ledger, intent_id, opts \\ []) do
    call(ledger, {:requeue, intent_id, opts}, opts)
  end

  @doc """
  Moves a non-final intent into manual ambiguity handling.
  """
  @spec mark_ambiguous(ledger(), String.t(), term(), keyword()) ::
          {:ok, Jido.IntentLedger.Record.t()} | {:error, term()}
  def mark_ambiguous(ledger, intent_id, reason, opts \\ []) do
    call(ledger, {:mark_ambiguous, intent_id, reason, opts}, opts)
  end

  @doc """
  Recovers expired claims for a queue.
  """
  @spec recover(ledger(), String.t() | atom(), keyword()) ::
          {:ok, [Jido.IntentLedger.Record.t()]} | {:error, term()}
  def recover(ledger, queue, opts \\ []) do
    call(ledger, {:recover, queue, opts}, opts)
  end

  defp call(ledger, message, opts) do
    GenServer.call(ledger, message, Keyword.get(opts, :timeout, 5000))
  end
end
