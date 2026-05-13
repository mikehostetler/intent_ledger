defmodule IntentLedger.QueueShardServer do
  @moduledoc """
  Runtime process for one queue shard in a named ledger instance.

  The initial clustered runtime scaffold registers one process per configured
  queue shard. Lease ownership and polling behavior are layered onto this
  process by the later runtime tasks.
  """

  use GenServer

  alias IntentLedger.Names

  @type option ::
          {:name, atom()}
          | {:queue, String.t() | atom()}
          | {:shard, non_neg_integer()}
          | {:lease_ms, pos_integer()}

  @type t :: %__MODULE__{
          name: atom(),
          queue: String.t(),
          shard: non_neg_integer(),
          lease_ms: pos_integer()
        }

  defstruct [:name, :queue, :shard, :lease_ms]

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

  @impl true
  def init(opts) do
    {:ok,
     %__MODULE__{
       name: Keyword.fetch!(opts, :name),
       queue: opts |> Keyword.fetch!(:queue) |> to_string(),
       shard: Keyword.fetch!(opts, :shard),
       lease_ms: Keyword.fetch!(opts, :lease_ms)
     }}
  end
end
