defmodule IntentLedger.InstanceSupervisor do
  @moduledoc """
  Supervises a named intent ledger instance.

  A ledger instance has a public process name, a private store process, and a
  registry reserved for follow-on queue workers/subscribers.
  """

  use Supervisor

  alias IntentLedger.{Names, Store}

  @type option ::
          {:name, atom()}
          | {:store, module() | {module(), keyword()}}
          | {:queues, keyword() | map()}
          | {:lease_ms, pos_integer()}
          | {:lease_renew_ms, pos_integer()}
          | {:lease_retry_ms, pos_integer()}
          | {:poll_interval_ms, pos_integer()}
          | {:claim_batch_size, pos_integer()}
          | {:recovery_interval_ms, pos_integer()}
          | {:recovery_limit, pos_integer()}
          | {:dispatcher_interval_ms, pos_integer()}
          | {:dispatcher_batch_size, pos_integer()}
          | {:dispatcher_consumer, String.t() | atom()}
          | {:signal_handlers, [IntentLedger.SignalHandler.spec()]}
          | {:wakeups?, boolean()}
          | {:lifecycle, module()}
          | {:telemetry_prefix, [atom()]}
          | {:shutdown, timeout()}

  @spec child_spec([option()]) :: Supervisor.child_spec()
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)
    shutdown = Keyword.get(opts, :shutdown, 5000)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :permanent,
      shutdown: shutdown
    }
  end

  @doc """
  Starts the supervisor for a named ledger instance.
  """
  @spec start_link([option()]) :: Supervisor.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: Names.supervisor(name))
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    {store_module, store_opts} = Store.normalize_spec(Keyword.get(opts, :store))
    store_name = Names.store(name)

    server_opts =
      opts
      |> Keyword.delete(:store)
      |> Keyword.put(:store, {store_module, store_name})

    queue_opts =
      opts
      |> Keyword.take([
        :name,
        :queues,
        :lease_ms,
        :lease_renew_ms,
        :lease_retry_ms,
        :poll_interval_ms,
        :claim_batch_size
      ])
      |> Keyword.put(:store, {store_module, store_name})

    recovery_opts =
      opts
      |> Keyword.take([:name, :queues, :recovery_interval_ms, :recovery_limit])
      |> Keyword.put(:store, {store_module, store_name})

    dispatcher_opts =
      opts
      |> Keyword.take([:name, :dispatcher_interval_ms, :dispatcher_batch_size, :dispatcher_consumer, :signal_handlers])
      |> Keyword.put(:store, {store_module, store_name})

    children = [
      {Registry, keys: :unique, name: Names.registry(name)},
      {IntentLedger.Notifier, name: name},
      store_module.child_spec(Keyword.put(store_opts, :name, store_name)),
      {IntentLedger.Server, server_opts},
      {IntentLedger.QueueSupervisor, queue_opts},
      {IntentLedger.RecoveryServer, recovery_opts},
      {IntentLedger.SignalDispatcher, dispatcher_opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
