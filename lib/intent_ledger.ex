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

  alias IntentLedger.{Command, Projection, Telemetry}

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
  Replays a window of lifecycle signals for one intent.
  """
  @spec replay_intent(ledger(), String.t(), keyword()) :: {:ok, [Jido.Signal.t() | map()]} | {:error, term()}
  def replay_intent(ledger, intent_id, opts \\ []) do
    GenServer.call(ledger, {:replay_intent, intent_id, opts})
  end

  @doc """
  Replays a window of lifecycle signals for one queue shard.
  """
  @spec replay_queue(ledger(), String.t() | atom(), non_neg_integer(), keyword()) ::
          {:ok, [Jido.Signal.t() | map()]} | {:error, term()}
  def replay_queue(ledger, queue, shard, opts \\ []) do
    GenServer.call(ledger, {:replay_queue, queue, shard, opts})
  end

  @doc """
  Replays a window of lifecycle signals for the whole ledger stream.
  """
  @spec replay_ledger(ledger(), keyword()) :: {:ok, [Jido.Signal.t() | map()]} | {:error, term()}
  def replay_ledger(ledger, opts \\ []) do
    GenServer.call(ledger, {:replay_ledger, opts})
  end

  @doc """
  Replays durable outbox entries without mutating acknowledgement state.
  """
  @spec replay_outbox(ledger(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def replay_outbox(ledger, opts \\ []) do
    GenServer.call(ledger, {:replay_outbox, opts})
  end

  @doc """
  Rebuilds a projection module from replayed lifecycle signals.

  Options:

    * `:source` - one of `:ledger`, `{:intent, intent_id}`, or
      `{:queue, queue, shard}`. Defaults to `:ledger`.
    * `:replay` - replay window options passed to the selected replay API.
    * `:projection` - options passed to `IntentLedger.Projection`.
  """
  @spec rebuild_projection(ledger(), module(), keyword()) :: {:ok, term()} | {:error, term()}
  def rebuild_projection(ledger, projection, opts \\ []) when is_atom(projection) do
    start = System.monotonic_time()
    source = Keyword.get(opts, :source, :ledger)
    replay_opts = Keyword.get(opts, :replay, [])
    projection_opts = Keyword.get(opts, :projection, [])
    telemetry = Keyword.take(opts, [:telemetry_prefix])

    case replay_projection_source(ledger, source, replay_opts) do
      {:ok, signals} ->
        result = Projection.rebuild(projection, signals, projection_opts)
        emit_projection_stop(telemetry, ledger, projection, source, start, length(signals), result)
        result

      {:error, reason} = error ->
        emit_projection_stop(telemetry, ledger, projection, source, start, 0, error)
        {:error, reason}
    end
  end

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

  defp replay_projection_source(ledger, :ledger, opts), do: replay_ledger(ledger, opts)
  defp replay_projection_source(ledger, {:intent, intent_id}, opts), do: replay_intent(ledger, intent_id, opts)

  defp replay_projection_source(ledger, {:queue, queue, shard}, opts) do
    replay_queue(ledger, queue, shard, opts)
  end

  defp replay_projection_source(_ledger, source, _opts), do: {:error, {:invalid_projection_source, source}}

  defp emit_projection_stop(telemetry, ledger, projection, source, start, count, result) do
    metadata =
      source
      |> projection_source_metadata()
      |> Map.merge(%{
        ledger: ledger,
        projection: projection,
        status: result_status(result)
      })
      |> maybe_put_result_error(result)
      |> reject_nil_metadata()

    Telemetry.execute(
      telemetry,
      :projection_stop,
      %{duration: System.monotonic_time() - start, count: count},
      metadata
    )
  end

  defp projection_source_metadata(:ledger), do: %{source: :ledger}
  defp projection_source_metadata({:intent, intent_id}), do: %{source: :intent, intent_id: to_string(intent_id)}

  defp projection_source_metadata({:queue, queue, shard}) do
    %{source: :queue, queue: to_string(queue), shard: shard}
  end

  defp projection_source_metadata(source), do: %{source: source}

  defp result_status({:ok, _result}), do: :ok
  defp result_status({:error, _reason}), do: :error

  defp maybe_put_result_error(metadata, {:error, reason}),
    do: Map.put(metadata, :error_class, Telemetry.error_class(reason))

  defp maybe_put_result_error(metadata, _result), do: metadata

  defp reject_nil_metadata(metadata) do
    Map.reject(metadata, fn {_key, value} -> is_nil(value) end)
  end

  defp command_call(ledger, signal, message, opts) do
    case Command.normalize(signal) do
      {:ok, command} -> call(ledger, {:command, command, propagate_command(command, message)}, opts)
      {:error, reason} -> {:error, reason}
    end
  end

  defp message_for(%{operation: :submit, data: data}) do
    {:submit, put_intent_lineage(Map.fetch!(data, :intent), data), command_opts(data, [:now])}
  end

  defp message_for(%{operation: :submit_many, data: data}) do
    intents = data |> Map.fetch!(:intents) |> put_intents_lineage(data)
    {:submit_many, intents, command_opts(data, [:now])}
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

  defp propagate_command(%{operation: :submit, data: data}, {:submit, attrs, opts}) do
    {:submit, put_intent_lineage(attrs, data), merge_command_opts(opts, data, [:now])}
  end

  defp propagate_command(%{operation: :submit_many, data: data}, {:submit_many, attrs_list, opts}) do
    {:submit_many, put_intents_lineage(attrs_list, data), merge_command_opts(opts, data, [:now])}
  end

  defp propagate_command(%{operation: :claim, data: data}, {:claim, queue, owner_id, opts}) do
    {:claim, queue, owner_id, merge_command_opts(opts, data, [:limit, :lease_ms, :now])}
  end

  defp propagate_command(%{operation: :heartbeat, data: data}, {:heartbeat, claim_id, token, opts}) do
    {:heartbeat, claim_id, token, merge_command_opts(opts, data, [:lease_ms, :now])}
  end

  defp propagate_command(%{operation: :complete, data: data}, {:complete, claim_id, token, result, opts}) do
    {:complete, claim_id, token, result, merge_command_opts(opts, data, [:now])}
  end

  defp propagate_command(%{operation: :fail, data: data}, {:fail, claim_id, token, error, opts}) do
    {:fail, claim_id, token, error, merge_command_opts(opts, data, [:retry_at, :retry_ms, :now])}
  end

  defp propagate_command(%{operation: :release, data: data}, {:release, claim_id, token, opts}) do
    {:release, claim_id, token, merge_command_opts(opts, data, [:now])}
  end

  defp propagate_command(%{operation: :cancel, data: data}, {:cancel, intent_id, reason, opts}) do
    {:cancel, intent_id, reason, merge_command_opts(opts, data, [:now])}
  end

  defp propagate_command(%{operation: :requeue, data: data}, {:requeue, intent_id, opts}) do
    {:requeue, intent_id, merge_command_opts(opts, data, [:retry_at, :now])}
  end

  defp propagate_command(%{operation: :mark_ambiguous, data: data}, {:mark_ambiguous, intent_id, reason, opts}) do
    {:mark_ambiguous, intent_id, reason, merge_command_opts(opts, data, [:now])}
  end

  defp propagate_command(%{operation: :recover, data: data}, {:recover, queue, opts}) do
    {:recover, queue, merge_command_opts(opts, data, [:limit, :now])}
  end

  defp propagate_command(_command, message), do: message

  defp command_opts(data, fields) do
    data
    |> Map.take(fields ++ Command.common_metadata_fields())
    |> Enum.map(fn
      {field, value} when field in [:now, :retry_at] -> {field, normalize_command_time(value)}
      field_and_value -> field_and_value
    end)
  end

  defp merge_command_opts(opts, data, fields), do: Keyword.merge(opts, command_opts(data, fields))

  defp put_intent_lineage(intent, data) when is_list(intent) do
    intent
    |> Map.new()
    |> put_intent_lineage(data)
  end

  defp put_intent_lineage(%IntentLedger.Intent{} = intent, data) do
    intent
    |> Map.from_struct()
    |> put_intent_lineage(data)
  end

  defp put_intent_lineage(%{} = intent, data) do
    data
    |> lineage_attrs()
    |> Enum.reduce(intent, fn {field, value}, acc ->
      if lineage_present?(acc, field) do
        acc
      else
        Map.put(acc, field, value)
      end
    end)
  end

  defp put_intent_lineage(intent, _data), do: intent

  defp put_intents_lineage(intents, data) when is_list(intents), do: Enum.map(intents, &put_intent_lineage(&1, data))
  defp put_intents_lineage(intents, _data), do: intents

  defp lineage_attrs(data) do
    data
    |> Map.take(Command.lineage_fields())
    |> Map.reject(fn {_field, value} -> is_nil(value) end)
  end

  defp lineage_present?(attrs, field) do
    Map.has_key?(attrs, field) or Map.has_key?(attrs, Atom.to_string(field))
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
