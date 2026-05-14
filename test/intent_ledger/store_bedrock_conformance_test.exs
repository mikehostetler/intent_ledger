defmodule IntentLedger.StoreBedrockConformanceTest.Repo do
  @moduledoc false

  use Agent

  def start_link(_opts), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  def transact(fun, _opts) do
    Agent.get_and_update(__MODULE__, fn values ->
      try do
        Process.put(:values, values)

        result = fun.(__MODULE__)

        next_values =
          case result do
            {:error, _reason} -> values
            _success -> Process.get(:values, values)
          end

        {result, next_values}
      after
        Process.delete(:values)
      end
    end)
  end

  def put(key, value) do
    Process.put(:values, Map.put(Process.get(:values, %{}), key, value))
    :ok
  end

  def clear(key) do
    Process.put(:values, Map.delete(Process.get(:values, %{}), key))
    :ok
  end

  def get(key), do: Map.get(Process.get(:values, %{}), key)

  def get_range({start_key, end_key}) do
    Process.get(:values, %{})
    |> Enum.filter(fn {key, _value} -> key >= start_key and key < end_key end)
    |> Enum.sort_by(fn {key, _value} -> key end)
  end

  def add_read_conflict_key(_key), do: :ok
  def add_write_conflict_range(_range), do: :ok
end

defmodule IntentLedger.StoreBedrockConformanceTest do
  use IntentLedger.StoreCase,
    async: false,
    store_module: IntentLedger.Store.Bedrock,
    store_opts: [name: __MODULE__.Store, repo: IntentLedger.StoreBedrockConformanceTest.Repo]

  use IntentLedger.StoreCase.AtomicCommitTests
  use IntentLedger.StoreCase.SemanticTests
  use IntentLedger.StoreCase.InspectionTests

  setup_all do
    start_supervised!(IntentLedger.StoreBedrockConformanceTest.Repo)
    :ok
  end
end
