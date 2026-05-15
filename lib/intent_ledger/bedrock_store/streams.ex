defmodule IntentLedger.BedrockStore.Streams do
  @moduledoc false

  alias IntentLedger.BedrockStore
  alias IntentLedger.BedrockStore.{Codec, Keyspaces, Outbox}
  alias IntentLedger.{Intent, ReplayEntry, Signal}

  @type stream_source :: :ledger | :outbox | {:intent, String.t()}

  @spec record_lifecycle(module(), Bedrock.Keyspace.t(), module(), Intent.t(), atom(), map()) :: Jido.Signal.t()
  @doc false
  def record_lifecycle(repo, root, ledger, %Intent{} = intent, event, data) do
    append_lifecycle(repo, root, ledger, intent, event, data)
  end

  @spec append_lifecycle(module(), Bedrock.Keyspace.t(), module(), Intent.t(), atom(), map()) :: Jido.Signal.t()
  @doc false
  def append_lifecycle(repo, root, ledger, %Intent{} = intent, event, data) do
    signal = Signal.lifecycle(event, ledger, intent, data)

    append(repo, root, "ledger", signal)
    append(repo, root, "intent:#{intent.id}", signal)
    Outbox.append(repo, root, signal)

    signal
  end

  @spec history(module(), String.t(), keyword()) :: {:ok, [Jido.Signal.t()]} | {:error, term()}
  @doc false
  def history(ledger, intent_id, opts \\ []) do
    replay(ledger, {:intent, intent_id}, opts)
  end

  @spec replay(module(), stream_source(), keyword()) :: {:ok, [Jido.Signal.t()]} | {:error, term()}
  @doc false
  def replay(ledger, source, opts \\ [])

  def replay(ledger, :outbox, opts) do
    with {:ok, entries} <- Outbox.outbox(ledger, opts) do
      {:ok, Enum.map(entries, & &1.signal)}
    end
  end

  def replay(ledger, source, opts) do
    with {:ok, entries} <- replay_entries(ledger, source, opts) do
      {:ok, Enum.map(entries, & &1.signal)}
    end
  end

  @spec replay_entries(module(), stream_source(), keyword()) :: {:ok, [ReplayEntry.t()]} | {:error, term()}
  @doc false
  def replay_entries(ledger, source, opts \\ [])

  def replay_entries(ledger, :outbox, opts) do
    with {:ok, entries} <- Outbox.outbox(ledger, opts) do
      entries
      |> Enum.map(&entry_from_outbox/1)
      |> collect_entries()
    end
  end

  def replay_entries(ledger, source, opts) do
    with {:ok, stream} <- stream_name(source),
         {:ok, cursor} <- non_negative_integer(opts, :cursor, 0),
         {:ok, limit} <- positive_integer(opts, :limit, 100) do
      BedrockStore.transact(ledger, fn repo, root ->
        keyspace = Keyspaces.stream(root, stream)

        entries =
          keyspace
          |> Keyspaces.range_from_cursor(cursor)
          |> repo.get_range(limit: limit)
          |> Stream.map(fn {_key, value} -> Codec.decode(value) end)
          |> Stream.map(&entry_from_stream/1)
          |> Enum.to_list()

        collect_entries(entries)
      end)
    end
  end

  @spec head(module(), Bedrock.Keyspace.t(), String.t()) :: non_neg_integer()
  @doc false
  def head(repo, root, stream) do
    case repo.get(Keyspaces.stream_version(root), stream) do
      nil -> 0
      value -> Codec.decode(value)
    end
  end

  @spec append(module(), Bedrock.Keyspace.t(), String.t(), Jido.Signal.t()) :: pos_integer()
  @doc false
  def append(repo, root, stream, signal) do
    versions = Keyspaces.stream_version(root)
    streams = Keyspaces.stream(root, stream)
    cursor = next_counter(repo, versions, stream)

    repo.put(
      streams,
      cursor,
      Codec.encode(%{stream: stream, cursor: cursor, signal: signal, recorded_at: DateTime.utc_now()})
    )

    cursor
  end

  defp next_counter(repo, keyspace, key) do
    next =
      case repo.get(keyspace, key) do
        nil -> 1
        value -> Codec.decode(value) + 1
      end

    repo.put(keyspace, key, Codec.encode(next))
    next
  end

  defp stream_name(:ledger), do: {:ok, "ledger"}
  defp stream_name({:intent, intent_id}) when is_binary(intent_id), do: {:ok, "intent:#{intent_id}"}
  defp stream_name(source), do: {:error, {:unsupported_replay_source, source}}

  defp non_negative_integer(opts, key, default) do
    value = Keyword.get(opts, key, default)

    if is_integer(value) and value >= 0 do
      {:ok, value}
    else
      {:error, {:invalid_option, key, value}}
    end
  end

  defp positive_integer(opts, key, default) do
    value = Keyword.get(opts, key, default)

    if is_integer(value) and value > 0 do
      {:ok, value}
    else
      {:error, {:invalid_option, key, value}}
    end
  end

  defp entry_from_stream(entry) do
    ReplayEntry.new(%{
      stream: entry.stream,
      cursor: entry.cursor,
      signal: entry.signal,
      recorded_at: Map.get(entry, :recorded_at)
    })
  end

  defp entry_from_outbox(entry) do
    ReplayEntry.new(%{
      stream: "outbox",
      cursor: entry.cursor,
      signal: entry.signal,
      recorded_at: Map.get(entry, :recorded_at)
    })
  end

  defp collect_entries(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn
      {:ok, entry}, {:ok, acc} -> {:cont, {:ok, [entry | acc]}}
      {:error, reason}, {:ok, _acc} -> {:halt, {:error, reason}}
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      error -> error
    end
  end
end
