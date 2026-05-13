defmodule IntentLedger.QueueShardServer do
  @moduledoc """
  Runtime process for one queue shard in a named ledger instance.

  The initial clustered runtime scaffold registers one process per configured
  queue shard. Lease ownership and polling behavior are layered onto this
  process by the later runtime tasks.
  """

  use GenServer

  alias IntentLedger.{Claim, ID, Names, Time}
  alias IntentLedger.Store.{CommitRequest, Listing, Precondition, Write}

  @type option ::
          {:name, atom()}
          | {:store, {module(), GenServer.server()}}
          | {:queue, String.t() | atom()}
          | {:shard, non_neg_integer()}
          | {:lease_ms, pos_integer()}
          | {:lease_renew_ms, pos_integer() | nil}
          | {:lease_retry_ms, pos_integer() | nil}
          | {:poll_interval_ms, pos_integer() | nil}
          | {:claim_batch_size, pos_integer() | nil}
          | {:owner_id, String.t()}

  @type t :: %__MODULE__{
          name: atom(),
          store_module: module(),
          store_ref: GenServer.server(),
          queue: String.t(),
          shard: non_neg_integer(),
          owner_id: String.t(),
          lease_ms: pos_integer(),
          lease_renew_ms: pos_integer(),
          lease_retry_ms: pos_integer(),
          poll_interval_ms: pos_integer(),
          claim_batch_size: pos_integer(),
          lease_until: DateTime.t() | nil,
          lease_timer_ref: reference() | nil,
          poll_timer_ref: reference() | nil,
          claimed_count: non_neg_integer(),
          last_claimed: [map()]
        }

  defstruct [
    :name,
    :store_module,
    :store_ref,
    :queue,
    :shard,
    :owner_id,
    :lease_ms,
    :lease_renew_ms,
    :lease_retry_ms,
    :poll_interval_ms,
    :claim_batch_size,
    :lease_until,
    :lease_timer_ref,
    :poll_timer_ref,
    claimed_count: 0,
    last_claimed: []
  ]

  @default_poll_interval_ms 1_000
  @default_claim_batch_size 10

  @doc false
  @spec child_spec([option()]) :: Supervisor.child_spec()
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)
    queue = opts |> Keyword.fetch!(:queue) |> to_string()
    shard = Keyword.fetch!(opts, :shard)

    %{
      id: {__MODULE__, name, queue, shard},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5000
    }
  end

  @doc false
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    queue = opts |> Keyword.fetch!(:queue) |> to_string()
    shard = Keyword.fetch!(opts, :shard)

    GenServer.start_link(__MODULE__, opts, name: Names.via(name, Names.queue_shard(queue, shard)))
  end

  @doc false
  @spec state(GenServer.server()) :: t()
  def state(server), do: GenServer.call(server, :state)

  @doc false
  @spec wake(GenServer.server()) :: :ok
  def wake(server), do: GenServer.cast(server, :wake)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    name = Keyword.fetch!(opts, :name)
    queue = opts |> Keyword.fetch!(:queue) |> to_string()
    shard = Keyword.fetch!(opts, :shard)
    lease_ms = Keyword.fetch!(opts, :lease_ms)
    {store_module, store_ref} = Keyword.fetch!(opts, :store)

    state = %__MODULE__{
      name: name,
      store_module: store_module,
      store_ref: store_ref,
      queue: queue,
      shard: shard,
      owner_id: Keyword.get_lazy(opts, :owner_id, fn -> default_owner_id(name, queue, shard) end),
      lease_ms: lease_ms,
      lease_renew_ms: Keyword.get(opts, :lease_renew_ms) || renew_interval(lease_ms),
      lease_retry_ms: Keyword.get(opts, :lease_retry_ms) || renew_interval(lease_ms),
      poll_interval_ms: Keyword.get(opts, :poll_interval_ms) || @default_poll_interval_ms,
      claim_batch_size: Keyword.get(opts, :claim_batch_size) || @default_claim_batch_size,
      lease_until: nil,
      lease_timer_ref: nil,
      poll_timer_ref: nil
    }

    {:ok, state, {:continue, :acquire_lease}}
  end

  @impl true
  def handle_continue(:acquire_lease, %__MODULE__{} = state) do
    {:noreply, acquire_lease(state)}
  end

  @impl true
  def handle_call(:state, _from, %__MODULE__{} = state) do
    {:reply, state, state}
  end

  @impl true
  def handle_cast(:wake, %__MODULE__{} = state) do
    {:noreply, poll_due_intents(state)}
  end

  @impl true
  def handle_info({:lease_timer, ref, :acquire}, %__MODULE__{lease_timer_ref: ref} = state) do
    {:noreply, acquire_lease(%{state | lease_timer_ref: nil})}
  end

  def handle_info({:lease_timer, ref, :renew}, %__MODULE__{lease_timer_ref: ref} = state) do
    {:noreply, renew_lease(%{state | lease_timer_ref: nil})}
  end

  def handle_info({:lease_timer, _stale_ref, _operation}, %__MODULE__{} = state) do
    {:noreply, state}
  end

  def handle_info({:poll_timer, ref}, %__MODULE__{poll_timer_ref: ref} = state) do
    {:noreply, poll_due_intents(%{state | poll_timer_ref: nil})}
  end

  def handle_info({:poll_timer, _stale_ref}, %__MODULE__{} = state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %__MODULE__{} = state) do
    release_lease(state)
  end

  defp acquire_lease(%__MODULE__{} = state) do
    case lease(state, :acquire) do
      {:ok, lease} ->
        state
        |> put_lease(lease)
        |> schedule_lease(:renew, state.lease_renew_ms)
        |> schedule_poll()

      {:error, _reason} ->
        state
        |> clear_lease()
        |> schedule_lease(:acquire, state.lease_retry_ms)
    end
  end

  defp renew_lease(%__MODULE__{lease_until: nil} = state), do: acquire_lease(state)

  defp renew_lease(%__MODULE__{} = state) do
    case lease(state, :renew) do
      {:ok, lease} ->
        state
        |> put_lease(lease)
        |> schedule_lease(:renew, state.lease_renew_ms)
        |> schedule_poll()

      {:error, _reason} ->
        state
        |> clear_lease()
        |> schedule_lease(:acquire, state.lease_retry_ms)
    end
  end

  defp poll_due_intents(%__MODULE__{} = state) do
    if lease_current?(state) do
      now = Time.utc_now()

      claimed =
        state
        |> due_intents(now)
        |> Enum.flat_map(&claim_due_intent(state, &1, now))

      state
      |> Map.update!(:claimed_count, &(&1 + length(claimed)))
      |> Map.put(:last_claimed, claimed)
      |> schedule_poll()
    else
      state
    end
  end

  defp due_intents(%__MODULE__{} = state, now) do
    case state.store_module.listing(
           state.store_ref,
           state.name,
           Listing.due_intents(state.queue, state.shard, now, limit: state.claim_batch_size),
           []
         ) do
      {:ok, intents} -> intents
      {:error, _reason} -> []
    end
  end

  defp claim_due_intent(%__MODULE__{} = state, intent_state, now) do
    stream = "intent:#{intent_state.intent_id}"

    with {:ok, %{version: stream_version}} <-
           state.store_module.read(state.store_ref, state.name, {:stream, stream, []}, []),
         {:ok, commit} <- commit_claim(state, intent_state, stream, stream_version, now) do
      [commit.result]
    else
      _conflict_or_error -> []
    end
  end

  defp commit_claim(%__MODULE__{} = state, intent_state, stream, stream_version, now) do
    attempt = intent_state.attempt + 1
    claim_lease_until = DateTime.add(now, state.lease_ms, :millisecond)
    claim = Claim.new(intent_state.intent_id, state.owner_id, attempt, claim_lease_until)
    command_id = "cmd:runtime:claim:#{state.owner_id}:#{intent_state.intent_id}:#{System.unique_integer([:positive])}"
    command = %{intent_id: intent_state.intent_id, queue: state.queue, shard: state.shard, owner_id: state.owner_id}
    signal = claim_signal(intent_state.intent_id, claim, now)

    result = %{
      intent_id: intent_state.intent_id,
      status: :claimed,
      claim_id: claim.id,
      token: claim.token,
      owner_id: state.owner_id,
      lease_until: claim.lease_until,
      command_id: command_id
    }

    claimed_state = %{
      intent_state
      | status: :claimed,
        attempt: attempt,
        claim_id: claim.id,
        claim_token_hash: Claim.token_hash(claim.token),
        lease_until: claim.lease_until,
        updated_at: now
    }

    state.store_module.commit(
      state.store_ref,
      state.name,
      CommitRequest.new(
        command_id: command_id,
        operation: :claim,
        command: command,
        preconditions: [
          Precondition.command_absent(command_id),
          Precondition.stream_version(stream, stream_version),
          Precondition.intent_status(intent_state.intent_id, [:available, :retry_scheduled])
        ],
        writes: [
          Write.new(:put_state, key: intent_state.intent_id, value: claimed_state),
          Write.put_claim(claim.id, %{
            intent_id: intent_state.intent_id,
            owner_id: state.owner_id,
            token_hash: Claim.token_hash(claim.token),
            lease_until: claim.lease_until
          }),
          Write.append_signal(stream, signal),
          Write.put_idempotency(command_id, result)
        ]
      ),
      []
    )
  end

  defp release_lease(%__MODULE__{lease_until: nil}), do: :ok

  defp release_lease(%__MODULE__{} = state) do
    _ = lease(state, :release)
    :ok
  catch
    :exit, _reason -> :ok
  end

  defp lease(%__MODULE__{} = state, operation) do
    now = Time.utc_now()

    state.store_module.lease(
      state.store_ref,
      state.name,
      {:shard, operation,
       %{
         queue: state.queue,
         shard: state.shard,
         owner_id: state.owner_id,
         lease_until: DateTime.add(now, state.lease_ms, :millisecond),
         now: now
       }},
      []
    )
  end

  defp put_lease(%__MODULE__{} = state, lease) do
    %{state | lease_until: Map.fetch!(lease, :lease_until)}
  end

  defp clear_lease(%__MODULE__{} = state), do: %{state | lease_until: nil, poll_timer_ref: nil}

  defp schedule_lease(%__MODULE__{} = state, operation, delay_ms) do
    ref = make_ref()
    Process.send_after(self(), {:lease_timer, ref, operation}, delay_ms)
    %{state | lease_timer_ref: ref}
  end

  defp schedule_poll(%__MODULE__{poll_timer_ref: nil} = state) do
    ref = make_ref()
    Process.send_after(self(), {:poll_timer, ref}, state.poll_interval_ms)
    %{state | poll_timer_ref: ref}
  end

  defp schedule_poll(%__MODULE__{} = state), do: state

  defp lease_current?(%__MODULE__{lease_until: nil}), do: false
  defp lease_current?(%__MODULE__{lease_until: lease_until}), do: DateTime.compare(lease_until, Time.utc_now()) == :gt

  defp renew_interval(lease_ms), do: max(1, div(lease_ms, 3))

  defp claim_signal(intent_id, %Claim{} = claim, now) do
    %{
      id: ID.generate("sig"),
      type: "intent_ledger.intent.claimed",
      subject: intent_id,
      time: now,
      metadata: %{claim_id: claim.id, owner_id: claim.owner_id}
    }
  end

  defp default_owner_id(name, queue, shard) do
    [inspect(name), Atom.to_string(node()), queue, Integer.to_string(shard), inspect(self())]
    |> Enum.join(":")
  end
end
