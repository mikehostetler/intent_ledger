defmodule IntentLedger.QueueShardServer do
  @moduledoc """
  Runtime process for one queue shard in a named ledger instance.

  The initial clustered runtime scaffold registers one process per configured
  queue shard. Lease ownership and polling behavior are layered onto this
  process by the later runtime tasks.
  """

  use GenServer

  alias IntentLedger.{Names, Time}

  @type option ::
          {:name, atom()}
          | {:store, {module(), GenServer.server()}}
          | {:queue, String.t() | atom()}
          | {:shard, non_neg_integer()}
          | {:lease_ms, pos_integer()}
          | {:lease_renew_ms, pos_integer() | nil}
          | {:lease_retry_ms, pos_integer() | nil}
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
          lease_until: DateTime.t() | nil,
          timer_ref: reference() | nil
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
    :lease_until,
    :timer_ref
  ]

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
      lease_until: nil,
      timer_ref: nil
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
  def handle_info({:lease_timer, ref, :acquire}, %__MODULE__{timer_ref: ref} = state) do
    {:noreply, acquire_lease(%{state | timer_ref: nil})}
  end

  def handle_info({:lease_timer, ref, :renew}, %__MODULE__{timer_ref: ref} = state) do
    {:noreply, renew_lease(%{state | timer_ref: nil})}
  end

  def handle_info({:lease_timer, _stale_ref, _operation}, %__MODULE__{} = state) do
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
        |> schedule(:renew, state.lease_renew_ms)

      {:error, _reason} ->
        state
        |> clear_lease()
        |> schedule(:acquire, state.lease_retry_ms)
    end
  end

  defp renew_lease(%__MODULE__{lease_until: nil} = state), do: acquire_lease(state)

  defp renew_lease(%__MODULE__{} = state) do
    case lease(state, :renew) do
      {:ok, lease} ->
        state
        |> put_lease(lease)
        |> schedule(:renew, state.lease_renew_ms)

      {:error, _reason} ->
        state
        |> clear_lease()
        |> schedule(:acquire, state.lease_retry_ms)
    end
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

  defp clear_lease(%__MODULE__{} = state), do: %{state | lease_until: nil}

  defp schedule(%__MODULE__{} = state, operation, delay_ms) do
    ref = make_ref()
    Process.send_after(self(), {:lease_timer, ref, operation}, delay_ms)
    %{state | timer_ref: ref}
  end

  defp renew_interval(lease_ms), do: max(1, div(lease_ms, 3))

  defp default_owner_id(name, queue, shard) do
    [inspect(name), Atom.to_string(node()), queue, Integer.to_string(shard), inspect(self())]
    |> Enum.join(":")
  end
end
