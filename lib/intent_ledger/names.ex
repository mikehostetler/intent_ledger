defmodule Jido.IntentLedger.Names do
  @moduledoc false

  @doc false
  @spec supervisor(atom()) :: atom()
  def supervisor(name), do: Module.concat(name, Supervisor)

  @doc false
  @spec registry(atom()) :: atom()
  def registry(name), do: Module.concat(name, Registry)

  @doc false
  @spec store(atom()) :: atom()
  def store(name), do: Module.concat(name, Store)
end
