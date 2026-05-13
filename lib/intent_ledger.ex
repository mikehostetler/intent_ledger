defmodule IntentLedger do
  @moduledoc """
  Public API for named intent ledger instances.

  A ledger records deferred work as immutable `IntentLedger.Intent` structs,
  tracks mutable lifecycle state in `IntentLedger.IntentState`, and emits every
  transition as a `Jido.Signal`.

  ## Supervision

      children = [
        {IntentLedger,
         name: MyApp.IntentLedger,
         queues: [default: [shards: 4]],
         store: IntentLedger.Store.Memory}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  ## Lifecycle

      {:ok, record} =
        IntentLedger.submit(MyApp.IntentLedger, %{
          key: "invoice:123",
          kind: "invoice.send",
          payload: %{invoice_id: 123}
        })

      {:ok, claimed} = IntentLedger.claim(MyApp.IntentLedger, "default", "worker-1")
      {:ok, _record} = IntentLedger.complete(MyApp.IntentLedger, claimed.claim.id, claimed.claim.token, :ok)
  """

  alias IntentLedger.Command

  @type ledger :: GenServer.server()

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  defdelegate child_spec(opts), to: IntentLedger.Instance

  @spec start_link(keyword()) :: Supervisor.on_start()
  defdelegate start_link(opts), to: IntentLedger.Instance

  @doc """
  Submits one intent to a ledger.
  """
  @spec submit(ledger(), IntentLedger.Intent.t() | map() | keyword(), keyword()) ::
          {:ok, IntentLedger.Record.t()} | {:error, term()}
  def submit(ledger, intent, opts \\ []) do
    command_call(ledger, Command.submit(ledger, intent, opts), {:submit, intent, opts}, opts)
  end

  @doc """
  Submits a batch of intents atomically for the in-memory adapter.
  """
  @spec submit_many(ledger(), [IntentLedger.Intent.t() | map() | keyword()], keyword()) ::
          {:ok, [IntentLedger.Record.t()]} | {:error, term()}
  def submit_many(ledger, intents, opts \\ []) do
    command_call(ledger, Command.submit_many(ledger, intents, opts), {:submit_many, intents, opts}, opts)
  end

  @doc """
  Reads the materialized record for an intent.
  """
  @spec get(ledger(), String.t()) :: {:ok, IntentLedger.Record.t()} | {:error, :not_found}
  def get(ledger, intent_id), do: GenServer.call(ledger, {:get, intent_id})

  @doc """
  Returns the lifecycle signal history for an intent.
  """
  @spec history(ledger(), String.t()) :: {:ok, [Jido.Signal.t()]} | {:error, :not_found}
  def history(ledger, intent_id), do: GenServer.call(ledger, {:history, intent_id})

  @doc """
  Executes a command signal against a ledger.
  """
  @spec command(ledger(), Jido.Signal.t(), keyword()) ::
          {:ok, term()} | :empty | {:error, term()}
  def command(ledger, %Jido.Signal{} = signal, opts \\ []) do
    case Command.normalize(signal) do
      {:ok, command} -> call(ledger, {:command, command, message_for(command)}, opts)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Claims the next available intent from a queue.
  """
  @spec claim(ledger(), String.t() | atom(), String.t(), keyword()) ::
          {:ok, IntentLedger.Claimed.t() | [IntentLedger.Claimed.t()]} | :empty | {:error, term()}
  def claim(ledger, queue, owner_id, opts \\ []) do
    command_call(ledger, Command.claim(ledger, queue, owner_id, opts), {:claim, queue, owner_id, opts}, opts)
  end

  @doc """
  Extends a claim lease.
  """
  @spec heartbeat(ledger(), String.t(), String.t(), keyword()) ::
          {:ok, IntentLedger.Claim.t()} | {:error, term()}
  def heartbeat(ledger, claim_id, token, opts \\ []) do
    command_call(
      ledger,
      Command.heartbeat(ledger, claim_id, token, opts),
      {:heartbeat, claim_id, token, opts},
      opts
    )
  end

  @doc """
  Completes a claimed intent.
  """
  @spec complete(ledger(), String.t(), String.t(), term(), keyword()) ::
          {:ok, IntentLedger.Record.t()} | {:error, term()}
  def complete(ledger, claim_id, token, result, opts \\ []) do
    command_call(
      ledger,
      Command.complete(ledger, claim_id, token, result, opts),
      {:complete, claim_id, token, result, opts},
      opts
    )
  end

  @doc """
  Fails a claimed intent, retrying or finalizing according to its policy.
  """
  @spec fail(ledger(), String.t(), String.t(), term(), keyword()) ::
          {:ok, IntentLedger.Record.t()} | {:error, term()}
  def fail(ledger, claim_id, token, error, opts \\ []) do
    command_call(
      ledger,
      Command.fail(ledger, claim_id, token, error, opts),
      {:fail, claim_id, token, error, opts},
      opts
    )
  end

  @doc """
  Releases a claim back to the queue.
  """
  @spec release(ledger(), String.t(), String.t(), keyword()) ::
          {:ok, IntentLedger.Record.t()} | {:error, term()}
  def release(ledger, claim_id, token, opts \\ []) do
    command_call(
      ledger,
      Command.release(ledger, claim_id, token, opts),
      {:release, claim_id, token, opts},
      opts
    )
  end

  @doc """
  Cancels a non-final intent.
  """
  @spec cancel(ledger(), String.t(), term(), keyword()) ::
          {:ok, IntentLedger.Record.t()} | {:error, term()}
  def cancel(ledger, intent_id, reason, opts \\ []) do
    command_call(
      ledger,
      Command.cancel(ledger, intent_id, reason, opts),
      {:cancel, intent_id, reason, opts},
      opts
    )
  end

  @doc """
  Requeues a non-final intent for a future attempt.
  """
  @spec requeue(ledger(), String.t(), keyword()) ::
          {:ok, IntentLedger.Record.t()} | {:error, term()}
  def requeue(ledger, intent_id, opts \\ []) do
    command_call(ledger, Command.requeue(ledger, intent_id, opts), {:requeue, intent_id, opts}, opts)
  end

  @doc """
  Moves a non-final intent into manual ambiguity handling.
  """
  @spec mark_ambiguous(ledger(), String.t(), term(), keyword()) ::
          {:ok, IntentLedger.Record.t()} | {:error, term()}
  def mark_ambiguous(ledger, intent_id, reason, opts \\ []) do
    command_call(
      ledger,
      Command.mark_ambiguous(ledger, intent_id, reason, opts),
      {:mark_ambiguous, intent_id, reason, opts},
      opts
    )
  end

  @doc """
  Recovers expired claims for a queue.
  """
  @spec recover(ledger(), String.t() | atom(), keyword()) ::
          {:ok, [IntentLedger.Record.t()]} | {:error, term()}
  def recover(ledger, queue, opts \\ []) do
    command_call(ledger, Command.recover(ledger, queue, opts), {:recover, queue, opts}, opts)
  end

  defp command_call(ledger, signal, message, opts) do
    case Command.normalize(signal) do
      {:ok, command} -> call(ledger, {:command, command, message}, opts)
      {:error, reason} -> {:error, reason}
    end
  end

  defp message_for(%{operation: :submit, data: data}) do
    {:submit, Map.fetch!(data, :intent), command_opts(data, [:now])}
  end

  defp message_for(%{operation: :submit_many, data: data}) do
    {:submit_many, Map.fetch!(data, :intents), command_opts(data, [:now])}
  end

  defp message_for(%{operation: :claim, data: data}) do
    {:claim, Map.fetch!(data, :queue), Map.fetch!(data, :owner_id), command_opts(data, [:limit, :lease_ms, :now])}
  end

  defp message_for(%{operation: :heartbeat, data: data}) do
    {:heartbeat, Map.fetch!(data, :claim_id), Map.fetch!(data, :token), command_opts(data, [:lease_ms, :now])}
  end

  defp message_for(%{operation: :complete, data: data}) do
    {:complete, Map.fetch!(data, :claim_id), Map.fetch!(data, :token), Map.fetch!(data, :result),
     command_opts(data, [:now])}
  end

  defp message_for(%{operation: :fail, data: data}) do
    {:fail, Map.fetch!(data, :claim_id), Map.fetch!(data, :token), Map.fetch!(data, :error),
     command_opts(data, [:retry_at, :retry_ms, :now])}
  end

  defp message_for(%{operation: :release, data: data}) do
    {:release, Map.fetch!(data, :claim_id), Map.fetch!(data, :token), command_opts(data, [:now])}
  end

  defp message_for(%{operation: :cancel, data: data}) do
    {:cancel, Map.fetch!(data, :intent_id), Map.fetch!(data, :reason), command_opts(data, [:now])}
  end

  defp message_for(%{operation: :requeue, data: data}) do
    {:requeue, Map.fetch!(data, :intent_id), command_opts(data, [:retry_at, :now])}
  end

  defp message_for(%{operation: :mark_ambiguous, data: data}) do
    {:mark_ambiguous, Map.fetch!(data, :intent_id), Map.fetch!(data, :reason), command_opts(data, [:now])}
  end

  defp message_for(%{operation: :recover, data: data}) do
    {:recover, Map.fetch!(data, :queue), command_opts(data, [:limit, :now])}
  end

  defp command_opts(data, fields) do
    data
    |> Map.take(fields)
    |> Enum.map(fn
      {field, value} when field in [:now, :retry_at] -> {field, normalize_command_time(value)}
      field_and_value -> field_and_value
    end)
  end

  defp normalize_command_time(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _invalid -> value
    end
  end

  defp normalize_command_time(value), do: value

  defp call(ledger, message, opts) do
    GenServer.call(ledger, message, Keyword.get(opts, :timeout, 5000))
  end
end
