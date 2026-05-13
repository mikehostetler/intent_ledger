defmodule IntentLedger.SignalDispatcher do
  @moduledoc """
  Per-ledger durable outbox dispatcher scaffold.

  The dispatcher owns the supervised polling loop for lifecycle signal outbox
  entries. Handler registration and ack/retry semantics are layered on top by
  later runtime tasks.
  """

  use GenServer

  alias IntentLedger.Names
  alias IntentLedger.Store.Outbox

  @default_interval_ms 1_000
  @default_batch_size 100
  @default_consumer "intent_ledger.signal_dispatcher"

  @type option ::
          {:name, atom()}
          | {:store, {module(), GenServer.server()}}
          | {:dispatcher_interval_ms, pos_integer()}
          | {:dispatcher_batch_size, pos_integer()}
          | {:dispatcher_consumer, String.t() | atom()}

  @type t :: %__MODULE__{
          name: atom(),
          store_module: module(),
          store_ref: GenServer.server(),
          interval_ms: pos_integer(),
          batch_size: pos_integer(),
          consumer: String.t(),
          timer_ref: reference() | nil,
          poll_count: non_neg_integer(),
          read_count: non_neg_integer(),
          last_entries: [map()],
          last_error: term()
        }

  defstruct [
    :name,
    :store_module,
    :store_ref,
    :interval_ms,
    :batch_size,
    :consumer,
    :timer_ref,
    poll_count: 0,
    read_count: 0,
    last_entries: [],
    last_error: nil
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
    GenServer.start_link(__MODULE__, opts, name: Names.signal_dispatcher(name))
  end

  @doc false
  @spec state(GenServer.server()) :: t()
  def state(server), do: GenServer.call(server, :state)

  @doc false
  @spec poll_once(GenServer.server()) :: {:ok, [map()]} | {:error, term()}
  def poll_once(server), do: GenServer.call(server, :poll_once)

  @impl true
  def init(opts) do
    {store_module, store_ref} = Keyword.fetch!(opts, :store)

    state = %__MODULE__{
      name: Keyword.fetch!(opts, :name),
      store_module: store_module,
      store_ref: store_ref,
      interval_ms: Keyword.get(opts, :dispatcher_interval_ms, @default_interval_ms),
      batch_size: Keyword.get(opts, :dispatcher_batch_size, @default_batch_size),
      consumer: opts |> Keyword.get(:dispatcher_consumer, @default_consumer) |> to_string(),
      timer_ref: nil
    }

    {:ok, schedule_poll(state)}
  end

  @impl true
  def handle_call(:state, _from, %__MODULE__{} = state) do
    {:reply, state, state}
  end

  def handle_call(:poll_once, _from, %__MODULE__{} = state) do
    {reply, next_state} = poll_outbox(state)
    {:reply, reply, next_state}
  end

  @impl true
  def handle_info({:dispatch_poll, ref}, %__MODULE__{timer_ref: ref} = state) do
    {_reply, next_state} = poll_outbox(%{state | timer_ref: nil})
    {:noreply, schedule_poll(next_state)}
  end

  def handle_info({:dispatch_poll, _stale_ref}, %__MODULE__{} = state) do
    {:noreply, state}
  end

  defp poll_outbox(%__MODULE__{} = state) do
    request = Outbox.read(state.consumer, limit: state.batch_size)

    case state.store_module.outbox(state.store_ref, state.name, request, []) do
      {:ok, entries} ->
        next_state = %{
          state
          | poll_count: state.poll_count + 1,
            read_count: state.read_count + length(entries),
            last_entries: entries,
            last_error: nil
        }

        {{:ok, entries}, next_state}

      {:error, reason} ->
        next_state = %{state | poll_count: state.poll_count + 1, last_error: reason}
        {{:error, reason}, next_state}
    end
  end

  defp schedule_poll(%__MODULE__{} = state) do
    ref = make_ref()
    Process.send_after(self(), {:dispatch_poll, ref}, state.interval_ms)
    %{state | timer_ref: ref}
  end
end
