defmodule IntentLedger.Server do
  @moduledoc false

  use GenServer

  alias IntentLedger.{Intent, Lifecycle, Notifier, Record, Telemetry, Time}
  alias IntentLedger.Store.Outbox

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
          max_depth: non_neg_integer() | nil,
          max_children_per_intent: non_neg_integer() | nil,
          max_open_descendants: non_neg_integer() | nil,
          wakeups?: boolean(),
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
    :max_depth,
    :max_children_per_intent,
    :max_open_descendants,
    :wakeups?,
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
       max_depth: normalize_max_depth(Keyword.get(opts, :max_depth)),
       max_children_per_intent:
         normalize_guardrail_limit(
           :max_children_per_intent,
           Keyword.get(opts, :max_children_per_intent, Keyword.get(opts, :max_children))
         ),
       max_open_descendants: normalize_guardrail_limit(:max_open_descendants, Keyword.get(opts, :max_open_descendants)),
       wakeups?: Keyword.get(opts, :wakeups?, false),
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
         :ok <- enforce_submit_guardrails(state, [intent]),
         {:ok, record, signals} <-
           state.store_module.submit(
             state.store_ref,
             state.name,
             intent,
             Keyword.put(opts, :now, now)
           ) do
      notify(state, signals, :submit, %{count: 1}, %{intent_id: record.intent.id})
      wake_claimable(state, record)
      {:reply, {:ok, record}, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:submit_many, attrs_list, opts}, _from, state) do
    now = now(opts)

    with {:ok, intents} <- build_intents(attrs_list, state, now, opts),
         :ok <- enforce_submit_guardrails(state, intents),
         {:ok, records, signals} <-
           state.store_module.submit_many(
             state.store_ref,
             state.name,
             intents,
             Keyword.put(opts, :now, now)
           ) do
      notify(state, signals, :submit_many, %{count: length(records)}, %{})
      wake_claimable(state, records)
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

  def handle_call({:replay_intent, intent_id, opts}, _from, state) do
    {:reply, replay_stream(state, intent_stream(intent_id), opts), state}
  end

  def handle_call({:replay_queue, queue, shard, opts}, _from, state) do
    {:reply, replay_stream(state, queue_stream(queue, shard), opts), state}
  end

  def handle_call({:replay_ledger, opts}, _from, state) do
    {:reply, replay_stream(state, ledger_stream(state.name), opts), state}
  end

  def handle_call({:replay_outbox, opts}, _from, state) do
    {:reply, state.store_module.outbox(state.store_ref, state.name, Outbox.replay(opts), []), state}
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
    opts =
      opts
      |> Keyword.put_new(:now, now(opts))
      |> put_failure_classifier(state)

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
    opts =
      opts
      |> Keyword.put_new(:now, now(opts))
      |> put_expired_claim_classifier(state)

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
         :ok <- enforce_submit_guardrails(state, [intent]),
         {:ok, record, signals} <-
           state.store_module.submit(
             state.store_ref,
             state.name,
             intent,
             Keyword.put(opts, :now, now)
           ) do
      notify(state, signals, :submit, %{count: 1}, %{intent_id: record.intent.id})
      wake_claimable(state, record)
      {{:ok, record}, state}
    else
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  defp execute_public_command({:submit_many, attrs_list, opts}, state) do
    now = now(opts)

    with {:ok, intents} <- build_intents(attrs_list, state, now, opts),
         :ok <- enforce_submit_guardrails(state, intents),
         {:ok, records, signals} <-
           state.store_module.submit_many(
             state.store_ref,
             state.name,
             intents,
             Keyword.put(opts, :now, now)
           ) do
      notify(state, signals, :submit_many, %{count: length(records)}, %{})
      wake_claimable(state, records)
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
    opts =
      opts
      |> Keyword.put_new(:now, now(opts))
      |> put_failure_classifier(state)

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
    opts =
      opts
      |> Keyword.put_new(:now, now(opts))
      |> put_expired_claim_classifier(state)

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

  defp enforce_submit_guardrails(state, intents) do
    with :ok <- enforce_max_depth(state, intents),
         :ok <- enforce_max_children_per_intent(state, intents) do
      enforce_max_open_descendants(state, intents)
    end
  end

  defp enforce_max_depth(%{max_depth: max_depth}, intents) when is_integer(max_depth) do
    case Enum.find(intents, &(&1.depth > max_depth)) do
      nil ->
        :ok

      %Intent{} = intent ->
        max_depth_violation(intent, max_depth)
    end
  end

  defp enforce_max_depth(_state, _intents), do: :ok

  defp max_depth_violation(%Intent{depth: depth} = intent, max_depth) do
    {:error,
     {:guardrail_violation, :max_depth,
      %{
        depth: depth,
        max_depth: max_depth,
        intent_id: intent.id,
        root_intent_id: intent.root_intent_id,
        parent_intent_id: intent.parent_intent_id
      }}}
  end

  defp enforce_max_children_per_intent(%{max_children_per_intent: max_children} = state, intents)
       when is_integer(max_children) do
    intents
    |> Enum.reject(&is_nil(&1.parent_intent_id))
    |> Enum.group_by(& &1.parent_intent_id)
    |> Enum.reduce_while(:ok, fn {parent_intent_id, proposed_children}, :ok ->
      with {:ok, counts} <- lineage_counts(state, parent_intent_id: parent_intent_id) do
        existing_children = Map.get(counts, :children, 0)
        proposed_count = length(proposed_children)

        if existing_children + proposed_count > max_children do
          {:halt,
           {:error,
            {:guardrail_violation, :max_children_per_intent,
             %{
               parent_intent_id: parent_intent_id,
               children: existing_children,
               proposed_children: proposed_count,
               max_children_per_intent: max_children
             }}}}
        else
          {:cont, :ok}
        end
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp enforce_max_children_per_intent(_state, _intents), do: :ok

  defp enforce_max_open_descendants(%{max_open_descendants: max_open_descendants} = state, intents)
       when is_integer(max_open_descendants) do
    intents
    |> Enum.filter(&descendant_intent?/1)
    |> Enum.group_by(& &1.root_intent_id)
    |> Enum.reduce_while(:ok, fn {root_intent_id, proposed_descendants}, :ok ->
      with {:ok, counts} <- lineage_counts(state, root_intent_id: root_intent_id) do
        existing_open_descendants = Map.get(counts, :open_descendants, 0)
        proposed_count = length(proposed_descendants)

        if existing_open_descendants + proposed_count > max_open_descendants do
          {:halt,
           {:error,
            {:guardrail_violation, :max_open_descendants,
             %{
               root_intent_id: root_intent_id,
               open_descendants: existing_open_descendants,
               proposed_open_descendants: proposed_count,
               max_open_descendants: max_open_descendants
             }}}}
        else
          {:cont, :ok}
        end
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp enforce_max_open_descendants(_state, _intents), do: :ok

  defp lineage_counts(state, attrs) do
    state.store_module.read(state.store_ref, state.name, {:lineage_counts, attrs}, [])
  end

  defp put_failure_classifier(opts, state) do
    context = context(state, opts)

    Keyword.put(opts, :classify_failure, fn record, error ->
      Lifecycle.classify_failure(state.lifecycle, record, error, context)
    end)
  end

  defp put_expired_claim_classifier(opts, state) do
    context = context(state, opts)

    Keyword.put(opts, :classify_expired_claim, fn record ->
      Lifecycle.classify_expired_claim(state.lifecycle, record, context)
    end)
  end

  defp descendant_intent?(%Intent{} = intent) do
    not is_nil(intent.parent_intent_id) or intent.root_intent_id != intent.id
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
    wake_claimable(state, result)
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

  defp wake_claimable(state, records) when is_list(records) do
    Enum.each(records, &wake_claimable(state, &1))
  end

  defp wake_claimable(state, %Record{} = record) do
    if state.wakeups? and record.state.status in [:available, :retry_scheduled] do
      Notifier.wake(state.name, record.state.queue, record.state.shard)
    end
  end

  defp wake_claimable(_state, _result), do: :ok

  defp replay_stream(state, stream, opts) do
    case state.store_module.read(state.store_ref, state.name, {:stream, stream, opts}, []) do
      {:ok, %{signals: signals}} -> {:ok, signals}
      {:error, reason} -> {:error, reason}
    end
  end

  defp context(state, opts) do
    %{
      ledger: state.name,
      opts: opts
    }
  end

  defp now(opts), do: Keyword.get(opts, :now, Time.utc_now())

  defp normalize_max_depth(max_depth), do: normalize_guardrail_limit(:max_depth, max_depth)

  defp normalize_guardrail_limit(_field, nil), do: nil
  defp normalize_guardrail_limit(_field, :infinity), do: nil
  defp normalize_guardrail_limit(_field, limit) when is_integer(limit) and limit >= 0, do: limit

  defp normalize_guardrail_limit(field, limit) do
    raise ArgumentError, "expected #{inspect(field)} to be a non-negative integer or :infinity, got: #{inspect(limit)}"
  end

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

  defp ledger_stream(ledger), do: "ledger:" <> inspect(ledger)
  defp queue_stream(queue, shard), do: "queue:" <> to_string(queue) <> ":" <> to_string(shard)
  defp intent_stream(intent_id), do: "intent:" <> to_string(intent_id)
end
