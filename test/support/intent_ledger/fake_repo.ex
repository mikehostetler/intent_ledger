defmodule IntentLedger.FakeRepo do
  @moduledoc false

  use Agent

  alias Bedrock.Keyspace

  @tx_state {__MODULE__, :tx_state}

  def start_link(_opts \\ []) do
    Agent.start(fn -> %{} end, name: __MODULE__)
  end

  def reset! do
    ensure_started()
    Agent.update(__MODULE__, fn _state -> %{} end)
  end

  def snapshot! do
    ensure_started()
    Agent.get(__MODULE__, & &1)
  end

  def restart!(state \\ %{}) when is_map(state) do
    ensure_started()
    Agent.stop(__MODULE__, :normal)
    {:ok, _pid} = start_link([])
    Agent.update(__MODULE__, fn _state -> state end)
    :ok
  end

  def transact(fun, _opts \\ []) when is_function(fun, 0) do
    ensure_started()

    case Process.get(@tx_state) do
      nil ->
        Agent.get_and_update(__MODULE__, fn state ->
          Process.put(@tx_state, state)

          try do
            result = fun.()
            next_state = Process.get(@tx_state)
            {{:ok, result}, next_state}
          rescue
            exception ->
              {{:raised, exception, __STACKTRACE__}, state}
          catch
            {__MODULE__, :rollback, reason} ->
              {{:rollback, reason}, state}

            kind, reason ->
              {{:caught, kind, reason}, state}
          after
            Process.delete(@tx_state)
          end
        end)
        |> case do
          {:ok, result} -> result
          {:rollback, reason} -> {:error, reason}
          {:raised, exception, stacktrace} -> reraise exception, stacktrace
          {:caught, kind, reason} -> :erlang.raise(kind, reason, [])
        end

      _state ->
        fun.()
    end
  end

  def rollback(reason), do: throw({__MODULE__, :rollback, reason})

  def get(%Keyspace{} = keyspace, key), do: keyspace |> Keyspace.pack(key) |> get()

  def get(key) when is_binary(key) do
    ensure_started()
    with_state(&Map.get(&1, key))
  end

  def put(%Keyspace{} = keyspace, key, value), do: keyspace |> Keyspace.pack(key) |> put(value)

  def put(key, value) when is_binary(key) and is_binary(value) do
    ensure_started()
    update_state(&Map.put(&1, key, value))
  end

  def clear(%Keyspace{} = keyspace, key), do: keyspace |> Keyspace.pack(key) |> clear()

  def clear(key) when is_binary(key) do
    ensure_started()
    update_state(&Map.delete(&1, key))
  end

  def get_range(%Keyspace{} = keyspace, opts) do
    prefix = Keyspace.prefix(keyspace)
    limit = Keyword.get(opts, :limit, 100)

    ensure_started()

    with_state(fn state ->
      state
      |> Enum.filter(fn {key, _value} -> String.starts_with?(key, prefix) end)
      |> Enum.sort_by(fn {key, _value} -> key end)
      |> Enum.flat_map(fn {key, value} ->
        case unpack(keyspace, key) do
          {:ok, unpacked} -> [{unpacked, value}]
          :error -> []
        end
      end)
      |> Enum.take(limit)
    end)
  end

  def get_range({start_key, end_key}, opts) when is_binary(start_key) and is_binary(end_key) do
    limit = Keyword.get(opts, :limit, 100)

    ensure_started()

    with_state(fn state ->
      state
      |> Enum.filter(fn {key, _value} -> key >= start_key and key < end_key end)
      |> Enum.sort_by(fn {key, _value} -> key end)
      |> Enum.take(limit)
    end)
  end

  def max(key, value) when is_binary(key) and is_binary(value) do
    ensure_started()

    update_state(fn state -> Map.update(state, key, value, &max_binary(&1, value)) end)
  end

  def add(key, <<delta::64-signed-little>>) when is_binary(key) do
    ensure_started()

    update_state(fn state ->
      Map.update(state, key, <<delta::64-signed-little>>, fn
        <<current::64-signed-little>> -> <<current + delta::64-signed-little>>
        _other -> <<delta::64-signed-little>>
      end)
    end)
  end

  defp with_state(fun) do
    case Process.get(@tx_state) do
      nil -> Agent.get(__MODULE__, fun)
      state -> fun.(state)
    end
  end

  defp update_state(fun) do
    case Process.get(@tx_state) do
      nil -> Agent.update(__MODULE__, fun)
      state -> Process.put(@tx_state, fun.(state))
    end
  end

  defp unpack(keyspace, key) do
    {:ok, Keyspace.unpack(keyspace, key)}
  rescue
    ArgumentError -> :error
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
