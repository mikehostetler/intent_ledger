defmodule IntentLedger.Notifier do
  @moduledoc """
  Local best-effort wakeups for runtime queue shard workers.
  """

  use GenServer

  alias IntentLedger.{Names, QueueShardServer}

  @type option :: {:name, atom()}

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
    GenServer.start_link(__MODULE__, opts, name: Names.notifier(name))
  end

  @doc false
  @spec wake(atom(), String.t() | atom(), non_neg_integer() | nil) :: :ok
  def wake(name, queue, shard) when is_atom(name) and not is_nil(shard) do
    case Process.whereis(Names.notifier(name)) do
      nil -> :ok
      notifier -> GenServer.cast(notifier, {:wake, to_string(queue), shard})
    end
  end

  def wake(_name, _queue, _shard), do: :ok

  @impl true
  def init(opts) do
    {:ok, %{name: Keyword.fetch!(opts, :name)}}
  end

  @impl true
  def handle_cast({:wake, queue, shard}, %{name: name} = state) do
    case Registry.lookup(Names.registry(name), Names.queue_shard(queue, shard)) do
      [{pid, _value} | _rest] -> QueueShardServer.wake(pid)
      [] -> :ok
    end

    {:noreply, state}
  end
end
