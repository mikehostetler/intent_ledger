defmodule IntentLedger.Server do
  @moduledoc false

  use GenServer

  alias IntentLedger.{Intent, Lifecycle, Telemetry, Time}

  require Logger

  @default_lease_ms 30_000
  @default_queue_opts [shards: 1]

  @type t :: %__MODULE__{
          name: atom(),
          store_module: module(),
          store_ref: GenServer.server(),
          lifecycle: module() | nil,
          queues: map(),
          lease_ms: pos_integer(),
          telemetry: keyword(),
          command_results: %{optional(String.t()) => term()}
        }

  defstruct [
    :name,
    :store_module,
    :store_ref,
    :lifecycle,
    :queues,
    :lease_ms,
    :telemetry,
    command_results: %{}
  ]

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5000
    }
  end

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc false
  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    {store_module, store_ref} = Keyword.fetch!(opts, :store)

    {:ok,
     %__MODULE__{
       name: name,
       store_module: store_module,
       store_ref: store_ref,
       lifecycle: Keyword.get(opts, :lifecycle),
       queues: normalize_queues(Keyword.get(opts, :queues, default: @default_queue_opts)),
       lease_ms: Keyword.get(opts, :lease_ms, @default_lease_ms),
       telemetry: Keyword.take(opts, [:telemetry_prefix])
     }}
  end

  @impl true
  def handle_call({:command, %{command_id: command_id}, message}, _from, state) do
    case Map.fetch(state.command_results, command_id) do
      {:ok, reply} ->
        {:reply, reply, state}

      :error ->
        {reply, next_state} = execute_public_command(message, state)
        {:reply, reply, put_command_result(next_state, command_id, reply)}
    end
  end

  def handle_call({:submit, attrs, opts}, _from, state) do
    now = now(opts)

    with {:ok, intent} <- build_intent(attrs, state, now),
         {:ok, intent} <- Lifecycle.before_submit(state.lifecycle, intent, context(state, opts)),
         {:ok, record, signals} <-
           state.store_module.submit(
             state.store_ref,
             state.name,
             intent,
             Keyword.put(opts, :now, now)
           ) do
      notify(state, signals, :submit, %{count: 1}, %{intent_id: record.intent.id})
      {:reply, {:ok, record}, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:submit_many, attrs_list, opts}, _from, state) do
    now = now(opts)

    with {:ok, intents} <- build_intents(attrs_list, state, now, opts),
         {:ok, records, signals} <-
           state.store_module.submit_many(
             state.store_ref,
             state.name,
             intents,
             Keyword.put(opts, :now, now)
           ) do
      notify(state, signals, :submit_many, %{count: length(records)}, %{})
      {:reply, {:ok, records}, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get, intent_id}, _from, state) do
    {:reply, state.store_module.get(state.store_ref, to_string(intent_id)), state}
  end

  def handle_call({:history, intent_id}, _from, state) do
    {:reply, state.store_module.history(state.store_ref, to_string(intent_id)), state}
  end

  def handle_call({:claim, queue, owner_id, opts}, _from, state) do
    limit = Keyword.get(opts, :limit, 1)
    opts = opts |> Keyword.put_new(:now, now(opts)) |> Keyword.put_new(:lease_ms, state.lease_ms)

    case state.store_module.claim(
           state.store_ref,
           state.name,
           to_string(queue),
           to_string(owner_id),
           opts
         ) do
      {:ok, [], signals} ->
        notify(state, signals, :claim, %{count: 0}, %{queue: to_string(queue)})
        {:reply, :empty, state}

      {:ok, [claimed], signals} when limit == 1 ->
        notify(state, signals, :claim, %{count: 1}, %{queue: to_string(queue)})
        {:reply, {:ok, claimed}, state}

      {:ok, claimed, signals} ->
        notify(state, signals, :claim, %{count: length(claimed)}, %{queue: to_string(queue)})
        {:reply, {:ok, claimed}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:heartbeat, claim_id, token, opts}, _from, state) do
    opts = opts |> Keyword.put_new(:now, now(opts)) |> Keyword.put_new(:lease_ms, state.lease_ms)

    reply_commit(
      state,
      :heartbeat,
      state.store_module.heartbeat(
        state.store_ref,
        state.name,
        to_string(claim_id),
        to_string(token),
        opts
      ),
      %{claim_id: to_string(claim_id)}
    )
  end

  def handle_call({:complete, claim_id, token, result, opts}, _from, state) do
    opts = Keyword.put_new(opts, :now, now(opts))

    reply_commit(
      state,
      :complete,
      state.store_module.complete(
        state.store_ref,
        state.name,
        to_string(claim_id),
        to_string(token),
        result,
        opts
      ),
      %{claim_id: to_string(claim_id)}
    )
  end

  def handle_call({:fail, claim_id, token, error, opts}, _from, state) do
    opts = Keyword.put_new(opts, :now, now(opts))

    reply_commit(
      state,
      :fail,
      state.store_module.fail(
        state.store_ref,
        state.name,
        to_string(claim_id),
        to_string(token),
        error,
        opts
      ),
      %{claim_id: to_string(claim_id)}
    )
  end

  def handle_call({:release, claim_id, token, opts}, _from, state) do
    opts = Keyword.put_new(opts, :now, now(opts))

    reply_commit(
      state,
      :release,
      state.store_module.release(
        state.store_ref,
        state.name,
        to_string(claim_id),
        to_string(token),
        opts
      ),
      %{claim_id: to_string(claim_id)}
    )
  end

  def handle_call({:cancel, intent_id, reason, opts}, _from, state) do
    opts = Keyword.put_new(opts, :now, now(opts))

    reply_commit(
      state,
      :cancel,
      state.store_module.cancel(state.store_ref, state.name, to_string(intent_id), reason, opts),
      %{intent_id: to_string(intent_id)}
    )
  end

  def handle_call({:requeue, intent_id, opts}, _from, state) do
    opts = Keyword.put_new(opts, :now, now(opts))

    reply_commit(
      state,
      :requeue,
      state.store_module.requeue(state.store_ref, state.name, to_string(intent_id), opts),
      %{intent_id: to_string(intent_id)}
    )
  end

  def handle_call({:mark_ambiguous, intent_id, reason, opts}, _from, state) do
    opts = Keyword.put_new(opts, :now, now(opts))

    reply_commit(
      state,
      :mark_ambiguous,
      state.store_module.mark_ambiguous(
        state.store_ref,
        state.name,
        to_string(intent_id),
        reason,
        opts
      ),
      %{intent_id: to_string(intent_id)}
    )
  end

  def handle_call({:recover, queue, opts}, _from, state) do
    opts = Keyword.put_new(opts, :now, now(opts))

    reply_commit(
      state,
      :recover,
      state.store_module.recover(state.store_ref, state.name, to_string(queue), opts),
      %{queue: to_string(queue)}
    )
  end

  defp execute_public_command({:submit, attrs, opts}, state) do
    now = now(opts)

    with {:ok, intent} <- build_intent(attrs, state, now),
         {:ok, intent} <- Lifecycle.before_submit(state.lifecycle, intent, context(state, opts)),
         {:ok, record, signals} <-
           state.store_module.submit(
             state.store_ref,
             state.name,
             intent,
             Keyword.put(opts, :now, now)
           ) do
      notify(state, signals, :submit, %{count: 1}, %{intent_id: record.intent.id})
      {{:ok, record}, state}
    else
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  defp execute_public_command({:submit_many, attrs_list, opts}, state) do
    now = now(opts)

    with {:ok, intents} <- build_intents(attrs_list, state, now, opts),
         {:ok, records, signals} <-
           state.store_module.submit_many(
             state.store_ref,
             state.name,
             intents,
             Keyword.put(opts, :now, now)
           ) do
      notify(state, signals, :submit_many, %{count: length(records)}, %{})
      {{:ok, records}, state}
    else
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  defp execute_public_command({:claim, queue, owner_id, opts}, state) do
    limit = Keyword.get(opts, :limit, 1)
    opts = opts |> Keyword.put_new(:now, now(opts)) |> Keyword.put_new(:lease_ms, state.lease_ms)

    case state.store_module.claim(
           state.store_ref,
           state.name,
           to_string(queue),
           to_string(owner_id),
           opts
         ) do
      {:ok, [], signals} ->
        notify(state, signals, :claim, %{count: 0}, %{queue: to_string(queue)})
        {:empty, state}

      {:ok, [claimed], signals} when limit == 1 ->
        notify(state, signals, :claim, %{count: 1}, %{queue: to_string(queue)})
        {{:ok, claimed}, state}

      {:ok, claimed, signals} ->
        notify(state, signals, :claim, %{count: length(claimed)}, %{queue: to_string(queue)})
        {{:ok, claimed}, state}

      {:error, reason} ->
        {{:error, reason}, state}
    end
  end

  defp execute_public_command({:heartbeat, claim_id, token, opts}, state) do
    opts = opts |> Keyword.put_new(:now, now(opts)) |> Keyword.put_new(:lease_ms, state.lease_ms)

    unwrap_reply(
      reply_commit(
        state,
        :heartbeat,
        state.store_module.heartbeat(
          state.store_ref,
          state.name,
          to_string(claim_id),
          to_string(token),
          opts
        ),
        %{claim_id: to_string(claim_id)}
      )
    )
  end

  defp execute_public_command({:complete, claim_id, token, result, opts}, state) do
    opts = Keyword.put_new(opts, :now, now(opts))

    unwrap_reply(
      reply_commit(
        state,
        :complete,
        state.store_module.complete(
          state.store_ref,
          state.name,
          to_string(claim_id),
          to_string(token),
          result,
          opts
        ),
        %{claim_id: to_string(claim_id)}
      )
    )
  end

  defp execute_public_command({:fail, claim_id, token, error, opts}, state) do
    opts = Keyword.put_new(opts, :now, now(opts))

    unwrap_reply(
      reply_commit(
        state,
        :fail,
        state.store_module.fail(
          state.store_ref,
          state.name,
          to_string(claim_id),
          to_string(token),
          error,
          opts
        ),
        %{claim_id: to_string(claim_id)}
      )
    )
  end

  defp execute_public_command({:release, claim_id, token, opts}, state) do
    opts = Keyword.put_new(opts, :now, now(opts))

    unwrap_reply(
      reply_commit(
        state,
        :release,
        state.store_module.release(
          state.store_ref,
          state.name,
          to_string(claim_id),
          to_string(token),
          opts
        ),
        %{claim_id: to_string(claim_id)}
      )
    )
  end

  defp execute_public_command({:cancel, intent_id, reason, opts}, state) do
    opts = Keyword.put_new(opts, :now, now(opts))

    unwrap_reply(
      reply_commit(
        state,
        :cancel,
        state.store_module.cancel(state.store_ref, state.name, to_string(intent_id), reason, opts),
        %{intent_id: to_string(intent_id)}
      )
    )
  end

  defp execute_public_command({:requeue, intent_id, opts}, state) do
    opts = Keyword.put_new(opts, :now, now(opts))

    unwrap_reply(
      reply_commit(
        state,
        :requeue,
        state.store_module.requeue(state.store_ref, state.name, to_string(intent_id), opts),
        %{intent_id: to_string(intent_id)}
      )
    )
  end

  defp execute_public_command({:mark_ambiguous, intent_id, reason, opts}, state) do
    opts = Keyword.put_new(opts, :now, now(opts))

    unwrap_reply(
      reply_commit(
        state,
        :mark_ambiguous,
        state.store_module.mark_ambiguous(
          state.store_ref,
          state.name,
          to_string(intent_id),
          reason,
          opts
        ),
        %{intent_id: to_string(intent_id)}
      )
    )
  end

  defp execute_public_command({:recover, queue, opts}, state) do
    opts = Keyword.put_new(opts, :now, now(opts))

    unwrap_reply(
      reply_commit(
        state,
        :recover,
        state.store_module.recover(state.store_ref, state.name, to_string(queue), opts),
        %{queue: to_string(queue)}
      )
    )
  end

  defp execute_public_command(message, state), do: {{:error, {:unknown_command, message}}, state}

  defp build_intents(attrs_list, state, now, opts) when is_list(attrs_list) do
    Enum.reduce_while(attrs_list, {:ok, []}, fn attrs, {:ok, acc} ->
      with {:ok, intent} <- build_intent(attrs, state, now),
           {:ok, intent} <- Lifecycle.before_submit(state.lifecycle, intent, context(state, opts)) do
        {:cont, {:ok, [intent | acc]}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, intents} -> {:ok, Enum.reverse(intents)}
      error -> error
    end
  end

  defp build_intents(value, _state, _now, _opts), do: {:error, {:invalid_intents, value}}

  defp build_intent(attrs, state, now) do
    with {:ok, intent} <- Intent.new(attrs, now: now) do
      {:ok, Intent.with_shard(intent, shard_for(state, intent))}
    end
  end

  defp shard_for(_state, %Intent{shard: shard}) when is_integer(shard), do: shard

  defp shard_for(state, %Intent{queue: queue, key: key}) do
    shards =
      state.queues
      |> Map.get(queue, %{})
      |> Map.get(:shards, 1)

    shard_count = max(shards, 1)
    :erlang.phash2(key, shard_count)
  end

  defp reply_commit(state, operation, {:ok, result, signals}, metadata) do
    notify(state, signals, operation, %{count: signal_count(signals)}, metadata)
    {:reply, {:ok, result}, state}
  end

  defp reply_commit(state, _operation, {:error, reason}, _metadata) do
    {:reply, {:error, reason}, state}
  end

  defp unwrap_reply({:reply, reply, state}), do: {reply, state}

  defp put_command_result(state, command_id, reply) do
    %{state | command_results: Map.put(state.command_results, command_id, reply)}
  end

  defp notify(state, signals, operation, measurements, metadata) do
    lifecycle_result =
      Lifecycle.after_transition(
        state.lifecycle,
        signals,
        Map.merge(context(state, []), metadata)
      )

    case lifecycle_result do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("intent ledger lifecycle hook failed: #{inspect(reason)}")
    end

    Telemetry.execute(
      state.telemetry,
      operation,
      [:stop],
      measurements,
      Map.put(metadata, :ledger, state.name)
    )
  end

  defp signal_count(signals) when is_list(signals), do: length(signals)

  defp context(state, opts) do
    %{
      ledger: state.name,
      opts: opts
    }
  end

  defp now(opts), do: Keyword.get(opts, :now, Time.utc_now())

  defp normalize_queues(queues) when is_map(queues) do
    queues
    |> Enum.map(fn {queue, opts} -> normalize_queue(queue, opts) end)
    |> Map.new()
  end

  defp normalize_queues(queues) when is_list(queues) do
    queues
    |> Enum.map(fn
      {queue, opts} -> normalize_queue(queue, opts)
      queue -> normalize_queue(queue, [])
    end)
    |> Map.new()
  end

  defp normalize_queue(queue, opts) when is_map(opts),
    do: normalize_queue(queue, Map.to_list(opts))

  defp normalize_queue(queue, opts) when is_list(opts) do
    queue = to_string(queue)
    opts = Keyword.merge(@default_queue_opts, opts)
    shards = opts |> Keyword.get(:shards, 1) |> max(1)

    {queue, %{shards: shards}}
  end
end
