defmodule IntentLedger.BedrockStore.Outbox do
  @moduledoc false

  alias IntentLedger.BedrockStore
  alias IntentLedger.BedrockStore.{Keyspaces, Options}
  alias IntentLedger.Time

  @type consumer_ref :: module() | String.t()

  @spec append(module(), Bedrock.Keyspace.t(), Jido.Signal.t()) :: pos_integer()
  @doc false
  def append(repo, root, signal) do
    cursor = next_counter(repo, Keyspaces.outbox_version(root), "global")

    repo.put(
      Keyspaces.outbox(root),
      cursor,
      Keyspaces.encode(%{cursor: cursor, signal: signal, recorded_at: Time.utc_now()})
    )

    cursor
  end

  @spec outbox(module(), keyword()) :: {:ok, [map()]} | {:error, term()}
  @doc false
  def outbox(ledger, opts \\ []) do
    with {:ok, cursor} <- Options.non_negative_integer(opts, :cursor, 0),
         {:ok, limit} <- Options.positive_integer(opts, :limit, 100) do
      BedrockStore.transact(ledger, fn repo, root ->
        entries =
          root
          |> Keyspaces.outbox()
          |> Keyspaces.range_from_cursor(cursor)
          |> repo.get_range(limit: limit)
          |> Stream.map(fn {_key, value} -> Keyspaces.decode(value) end)
          |> Enum.to_list()

        {:ok, entries}
      end)
    end
  end

  @spec read(module(), consumer_ref(), keyword()) :: {:ok, map()} | {:error, term()}
  @doc false
  def read(ledger, consumer, opts \\ []) do
    with {:ok, key} <- consumer_key(consumer),
         {:ok, limit} <- Options.positive_integer(opts, :limit, 100) do
      BedrockStore.transact(ledger, fn repo, root ->
        acked_cursor = read_cursor(repo, root, key) || 0
        entries = read_entries(repo, root, acked_cursor, limit)
        head_cursor = head(repo, root)
        next_cursor = entries |> List.last() |> then(&if &1, do: &1.cursor, else: acked_cursor)

        {:ok,
         %{
           consumer: key,
           acked_cursor: acked_cursor,
           next_cursor: next_cursor,
           head_cursor: head_cursor,
           lag: max(head_cursor - next_cursor, 0),
           entries: entries
         }}
      end)
    end
  end

  @spec cursor(module(), consumer_ref(), keyword()) :: {:ok, non_neg_integer() | nil} | {:error, term()}
  @doc false
  def cursor(ledger, consumer, _opts \\ []) do
    BedrockStore.transact(ledger, fn repo, root ->
      with {:ok, key} <- consumer_key(consumer) do
        {:ok, read_cursor(repo, root, key)}
      end
    end)
  end

  @spec ack(module(), consumer_ref(), non_neg_integer(), keyword()) :: {:ok, map()} | {:error, term()}
  @doc false
  def ack(ledger, consumer, cursor, opts \\ []) do
    with {:ok, key} <- consumer_key(consumer),
         :ok <- validate_cursor(cursor) do
      BedrockStore.transact(ledger, fn repo, root ->
        current = read_cursor(repo, root, key) || 0
        head = head(repo, root)
        force? = Keyword.get(opts, :force, false)
        allow_ahead? = Keyword.get(opts, :allow_ahead, false)

        cond do
          cursor < current and not force? ->
            {:error, {:stale_outbox_ack, key, cursor, current}}

          cursor > head and not allow_ahead? ->
            {:error, {:outbox_ack_past_head, key, cursor, head}}

          true ->
            record = %{consumer: key, cursor: cursor, updated_at: Time.utc_now()}
            repo.put(Keyspaces.outbox_consumer(root), key, Keyspaces.encode(record))
            {:ok, record}
        end
      end)
    end
  end

  @spec head(module(), Bedrock.Keyspace.t()) :: non_neg_integer()
  @doc false
  def head(repo, root) do
    case repo.get(Keyspaces.outbox_version(root), "global") do
      nil -> 0
      value -> Keyspaces.decode(value)
    end
  end

  @spec read_cursor(module(), Bedrock.Keyspace.t(), String.t()) :: non_neg_integer() | nil
  @doc false
  def read_cursor(repo, root, key) do
    case repo.get(Keyspaces.outbox_consumer(root), key) do
      nil -> nil
      value -> Keyspaces.decode(value).cursor
    end
  end

  defp read_entries(repo, root, cursor, limit) do
    root
    |> Keyspaces.outbox()
    |> Keyspaces.range_from_cursor(cursor)
    |> repo.get_range(limit: limit)
    |> Stream.map(fn {_key, value} -> Keyspaces.decode(value) end)
    |> Enum.to_list()
  end

  defp consumer_key(consumer) when consumer in [nil, true, false],
    do: {:error, {:invalid_outbox_consumer, consumer}}

  defp consumer_key(consumer) when is_atom(consumer), do: {:ok, "module:#{Keyspaces.module_key(consumer)}"}

  defp consumer_key(consumer) when is_binary(consumer) and byte_size(consumer) > 0,
    do: {:ok, "name:#{consumer}"}

  defp consumer_key(consumer), do: {:error, {:invalid_outbox_consumer, consumer}}

  defp validate_cursor(cursor) when is_integer(cursor) and cursor >= 0, do: :ok
  defp validate_cursor(cursor), do: {:error, {:invalid_projection_cursor, cursor}}

  defp next_counter(repo, keyspace, key) do
    next =
      case repo.get(keyspace, key) do
        nil -> 1
        value -> Keyspaces.decode(value) + 1
      end

    repo.put(keyspace, key, Keyspaces.encode(next))
    next
  end
end
