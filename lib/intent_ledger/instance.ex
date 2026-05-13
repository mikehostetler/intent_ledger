defmodule IntentLedger.Instance do
  @moduledoc """
  Convenience API for supervising named ledgers.
  """

  alias IntentLedger.Names

  @doc """
  Returns a child specification for a named ledger instance.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts), do: IntentLedger.InstanceSupervisor.child_spec(opts)

  @doc """
  Starts a named ledger instance.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts), do: IntentLedger.InstanceSupervisor.start_link(opts)

  @spec running?(atom()) :: boolean()
  def running?(name) when is_atom(name) do
    case Process.whereis(Names.supervisor(name)) do
      nil -> false
      pid -> Process.alive?(pid)
    end
  end

  @spec stop(atom(), timeout()) :: :ok
  def stop(name, timeout \\ 5000) when is_atom(name) do
    case Process.whereis(Names.supervisor(name)) do
      nil ->
        :ok

      pid ->
        Supervisor.stop(pid, :normal, timeout)
    end
  catch
    :exit, _reason -> :ok
  end
end
