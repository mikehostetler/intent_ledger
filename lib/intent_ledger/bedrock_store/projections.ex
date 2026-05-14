defmodule IntentLedger.BedrockStore.Projections do
  @moduledoc false

  alias IntentLedger.BedrockStore
  alias IntentLedger.BedrockStore.{Keyspaces, Options, Streams}
  alias IntentLedger.Time

  @type projection_ref :: module() | String.t()

  @spec projections(module(), keyword()) :: {:ok, [map()]} | {:error, term()}
  @doc false
  def projections(ledger, opts \\ []) do
    with {:ok, limit} <- Options.positive_integer(opts, :limit, 100) do
      BedrockStore.transact(ledger, fn repo, root ->
        head = Streams.head(repo, root, "ledger")

        entries =
          root
          |> Keyspaces.projection_offset()
          |> repo.get_range(limit: limit)
          |> Stream.map(fn {_key, value} -> Keyspaces.decode(value) end)
          |> Stream.map(&Map.merge(&1, %{head_cursor: head, lag: max(head - &1.cursor, 0)}))
          |> Enum.to_list()

        {:ok, entries}
      end)
    end
  end

  @spec cursor(module(), projection_ref(), keyword()) :: {:ok, non_neg_integer() | nil} | {:error, term()}
  @doc false
  def cursor(ledger, projection, _opts \\ []) do
    BedrockStore.transact(ledger, fn repo, root ->
      with {:ok, key} <- projection_key(projection) do
        case repo.get(Keyspaces.projection_offset(root), key) do
          nil -> {:ok, nil}
          value -> {:ok, Keyspaces.decode(value).cursor}
        end
      end
    end)
  end

  @spec put_cursor(module(), projection_ref(), non_neg_integer(), keyword()) :: :ok | {:error, term()}
  @doc false
  def put_cursor(ledger, projection, cursor, opts \\ []) do
    with {:ok, key} <- projection_key(projection),
         :ok <- validate_cursor(cursor) do
      BedrockStore.transact(ledger, fn repo, root ->
        current = read_cursor(repo, root, key) || 0
        head = Streams.head(repo, root, "ledger")
        force? = Keyword.get(opts, :force, false)
        allow_ahead? = Keyword.get(opts, :allow_ahead, false)

        cond do
          cursor < current and not force? ->
            {:error, {:stale_projection_cursor, key, cursor, current}}

          cursor > head and not allow_ahead? ->
            {:error, {:projection_cursor_past_head, key, cursor, head}}

          true ->
            repo.put(
              Keyspaces.projection_offset(root),
              key,
              Keyspaces.encode(%{projection: key, cursor: cursor, updated_at: Time.utc_now()})
            )

            :ok
        end
      end)
    end
  end

  @spec read_cursor(module(), Bedrock.Keyspace.t(), String.t()) :: non_neg_integer() | nil
  @doc false
  def read_cursor(repo, root, key) do
    case repo.get(Keyspaces.projection_offset(root), key) do
      nil -> nil
      value -> Keyspaces.decode(value).cursor
    end
  end

  defp projection_key(projection) when projection in [nil, true, false], do: {:error, {:invalid_projection, projection}}
  defp projection_key(projection) when is_atom(projection), do: {:ok, "module:#{Keyspaces.module_key(projection)}"}

  defp projection_key(projection) when is_binary(projection) and byte_size(projection) > 0,
    do: {:ok, "name:#{projection}"}

  defp projection_key(projection), do: {:error, {:invalid_projection, projection}}

  defp validate_cursor(cursor) when is_integer(cursor) and cursor >= 0, do: :ok
  defp validate_cursor(cursor), do: {:error, {:invalid_projection_cursor, cursor}}
end
