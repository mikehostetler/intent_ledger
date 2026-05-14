defmodule IntentLedger.FakeRepo do
  @moduledoc false

  use Agent

  alias Bedrock.Keyspace

  def start_link(_opts \\ []) do
    Agent.start(fn -> %{} end, name: __MODULE__)
  end

  def reset! do
    ensure_started()
    Agent.update(__MODULE__, fn _state -> %{} end)
  end

  def transact(fun, _opts \\ []) when is_function(fun, 0), do: fun.()

  def get(%Keyspace{} = keyspace, key), do: keyspace |> Keyspace.pack(key) |> get()

  def get(key) when is_binary(key) do
    ensure_started()
    Agent.get(__MODULE__, &Map.get(&1, key))
  end

  def put(%Keyspace{} = keyspace, key, value), do: keyspace |> Keyspace.pack(key) |> put(value)

  def put(key, value) when is_binary(key) and is_binary(value) do
    ensure_started()
    Agent.update(__MODULE__, &Map.put(&1, key, value))
  end

  def clear(%Keyspace{} = keyspace, key), do: keyspace |> Keyspace.pack(key) |> clear()

  def clear(key) when is_binary(key) do
    ensure_started()
    Agent.update(__MODULE__, &Map.delete(&1, key))
  end

  def get_range(%Keyspace{} = keyspace, opts) do
    prefix = Keyspace.prefix(keyspace)
    limit = Keyword.get(opts, :limit, 100)

    ensure_started()

    Agent.get(__MODULE__, fn state ->
      state
      |> Enum.filter(fn {key, _value} -> String.starts_with?(key, prefix) end)
      |> Enum.sort_by(fn {key, _value} -> key end)
      |> Enum.take(limit)
      |> Enum.map(fn {key, value} -> {Keyspace.unpack(keyspace, key), value} end)
    end)
  end

  def get_range({start_key, end_key}, opts) when is_binary(start_key) and is_binary(end_key) do
    limit = Keyword.get(opts, :limit, 100)

    ensure_started()

    Agent.get(__MODULE__, fn state ->
      state
      |> Enum.filter(fn {key, _value} -> key >= start_key and key < end_key end)
      |> Enum.sort_by(fn {key, _value} -> key end)
      |> Enum.take(limit)
    end)
  end

  def max(key, value) when is_binary(key) and is_binary(value) do
    ensure_started()

    Agent.update(__MODULE__, fn state ->
      Map.update(state, key, value, &max_binary(&1, value))
    end)
  end

  def add(key, <<delta::64-signed-little>>) when is_binary(key) do
    ensure_started()

    Agent.update(__MODULE__, fn state ->
      Map.update(state, key, <<delta::64-signed-little>>, fn
        <<current::64-signed-little>> -> <<current + delta::64-signed-little>>
        _other -> <<delta::64-signed-little>>
      end)
    end)
  end

  defp max_binary(left, right) when left >= right, do: left
  defp max_binary(_left, right), do: right

  defp ensure_started do
    case Process.whereis(__MODULE__) do
      nil ->
        case start_link([]) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
        end

      _pid ->
        :ok
    end
  end
end
