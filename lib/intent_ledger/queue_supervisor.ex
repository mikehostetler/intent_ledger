defmodule IntentLedger.QueueSupervisor do
  @moduledoc """
  Supervises queue shard runtime workers for one named ledger instance.
  """

  use Supervisor

  alias IntentLedger.{Names, QueueShardServer}

  @default_queue_opts [shards: 1]
  @default_lease_ms 30_000

  @type option ::
          {:name, atom()}
          | {:store, {module(), GenServer.server()}}
          | {:queues, keyword() | map()}
          | {:lease_ms, pos_integer()}
          | {:lease_renew_ms, pos_integer()}
          | {:lease_retry_ms, pos_integer()}
          | {:poll_interval_ms, pos_integer()}
          | {:claim_batch_size, pos_integer()}
          | {:telemetry_prefix, [atom()]}

  @doc false
  @spec child_spec([option()]) :: Supervisor.child_spec()
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :permanent,
      shutdown: 5000
    }
  end

  @doc false
  @spec start_link([option()]) :: Supervisor.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: Names.queue_supervisor(name))
  end

  @doc false
  @spec shard_child_specs([option()]) :: [Supervisor.child_spec()]
  def shard_child_specs(opts) do
    name = Keyword.fetch!(opts, :name)
    store = Keyword.fetch!(opts, :store)
    queues = Keyword.get(opts, :queues, default: @default_queue_opts)
    lease_ms = Keyword.get(opts, :lease_ms, @default_lease_ms)
    lease_renew_ms = Keyword.get(opts, :lease_renew_ms)
    lease_retry_ms = Keyword.get(opts, :lease_retry_ms)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms)
    claim_batch_size = Keyword.get(opts, :claim_batch_size)
    telemetry = Keyword.take(opts, [:telemetry_prefix])

    for {queue, %{shards: shards}} <- normalize_queues(queues),
        shard <- 0..(shards - 1) do
      [
        name: name,
        store: store,
        queue: queue,
        shard: shard,
        lease_ms: lease_ms,
        lease_renew_ms: lease_renew_ms,
        lease_retry_ms: lease_retry_ms,
        poll_interval_ms: poll_interval_ms,
        claim_batch_size: claim_batch_size
      ]
      |> Keyword.merge(telemetry)
      |> QueueShardServer.child_spec()
    end
  end

  @impl true
  def init(opts) do
    Supervisor.init(shard_child_specs(opts), strategy: :one_for_one)
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
