defmodule IntentLedger.Names do
  @moduledoc false

  @doc false
  @spec supervisor(atom()) :: atom()
  def supervisor(name), do: Module.concat(name, Supervisor)

  @doc false
  @spec registry(atom()) :: atom()
  def registry(name), do: Module.concat(name, Registry)

  @doc false
  @spec notifier(atom()) :: atom()
  def notifier(name), do: Module.concat(name, Notifier)

  @doc false
  @spec queue_supervisor(atom()) :: atom()
  def queue_supervisor(name), do: Module.concat(name, QueueSupervisor)

  @doc false
  @spec recovery_server(atom()) :: atom()
  def recovery_server(name), do: Module.concat(name, RecoveryServer)

  @doc false
  @spec store(atom()) :: atom()
  def store(name), do: Module.concat(name, Store)

  @doc false
  @spec queue_shard(String.t() | atom(), non_neg_integer()) :: {:queue_shard, String.t(), non_neg_integer()}
  def queue_shard(queue, shard), do: {:queue_shard, to_string(queue), shard}

  @doc false
  @spec via(atom(), term()) :: {:via, Registry, {atom(), term()}}
  def via(name, key), do: {:via, Registry, {registry(name), key}}
end
