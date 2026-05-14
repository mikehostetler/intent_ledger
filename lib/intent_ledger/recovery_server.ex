defmodule IntentLedger.RecoveryServer do
  @moduledoc """
  Periodically recovers expired claims and expired shard lease rows.
  """

  use GenServer

  alias IntentLedger.{Names, Time}

  @default_interval_ms 1_000
  @default_limit 100
  @default_queue_opts [shards: 1]

  @type option ::
          {:name, atom()}
          | {:store, {module(), GenServer.server()}}
          | {:queues, keyword() | map()}
          | {:recovery_interval_ms, pos_integer()}
          | {:recovery_limit, pos_integer()}
          | {:telemetry_prefix, [atom()]}

  @type t :: %__MODULE__{
          name: atom(),
          store_module: module(),
          store_ref: GenServer.server(),
          queues: map(),
          interval_ms: pos_integer(),
          limit: pos_integer(),
          telemetry: keyword(),
          timer_ref: reference() | nil,
          recovered_count: non_neg_integer(),
          expired_leases_count: non_neg_integer(),
          last_recovered: [term()]
        }

  defstruct [
    :name,
    :store_module,
    :store_ref,
    :queues,
    :interval_ms,
    :limit,
    :telemetry,
    :timer_ref,
    recovered_count: 0,
    expired_leases_count: 0,
    last_recovered: []
  ]

  @doc false
  @spec child_spec([option()]) :: Supervisor.child_spec()
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
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: Names.recovery_server(name))
  end

  @doc false
  @spec state(GenServer.server()) :: t()
  def state(server), do: GenServer.call(server, :state)

  @impl true
  def init(opts) do
    {store_module, store_ref} = Keyword.fetch!(opts, :store)

    state = %__MODULE__{
      name: Keyword.fetch!(opts, :name),
      store_module: store_module,
      store_ref: store_ref,
      queues: opts |> Keyword.get(:queues, default: @default_queue_opts) |> normalize_queues(),
      interval_ms: Keyword.get(opts, :recovery_interval_ms, @default_interval_ms),
      limit: Keyword.get(opts, :recovery_limit, @default_limit),
      telemetry: Keyword.take(opts, [:telemetry_prefix]),
      timer_ref: nil
    }

    {:ok, schedule_recovery(state)}
  end

  @impl true
  def handle_call(:state, _from, %__MODULE__{} = state) do
    {:reply, state, state}
  end

  @impl true
  def handle_info({:recover, ref}, %__MODULE__{timer_ref: ref} = state) do
    {:noreply, state |> Map.put(:timer_ref, nil) |> recover_once() |> schedule_recovery()}
  end

  def handle_info({:recover, _stale_ref}, %__MODULE__{} = state) do
    {:noreply, state}
  end

  defp recover_once(%__MODULE__{} = state) do
    now = Time.utc_now()

    recovered =
      state.queues
      |> Map.keys()
      |> Enum.flat_map(&recover_claims(state, &1, now))

    expired_leases =
      state.queues
      |> Enum.flat_map(fn {queue, %{shards: shards}} ->
        Enum.map(0..(shards - 1), &expire_shard_lease(state, queue, &1, now))
      end)
      |> Enum.count(&(&1 == :expired))

    %{
      state
      | recovered_count: state.recovered_count + length(recovered),
        expired_leases_count: state.expired_leases_count + expired_leases,
        last_recovered: recovered
    }
  end

  defp recover_claims(%__MODULE__{} = state, queue, now) do
    if function_exported?(state.store_module, :recover, 4) do
      case IntentLedger.recover(state.name, queue, now: now, limit: state.limit) do
        {:ok, records} -> records
        {:error, _reason} -> []
      end
    else
      []
    end
  catch
    :exit, _reason -> []
  end

  defp expire_shard_lease(%__MODULE__{} = state, queue, shard, now) do
    if function_exported?(state.store_module, :lease, 4) do
      case state.store_module.lease(
             state.store_ref,
             state.name,
             {:shard, :expire, %{queue: queue, shard: shard, now: now}},
             state.telemetry
           ) do
        {:ok, _lease} -> :expired
        {:error, _reason} -> :unchanged
      end
    else
      :unchanged
    end
  catch
    :exit, _reason -> :unchanged
  end

  defp schedule_recovery(%__MODULE__{} = state) do
    ref = make_ref()
    Process.send_after(self(), {:recover, ref}, state.interval_ms)
    %{state | timer_ref: ref}
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
end
