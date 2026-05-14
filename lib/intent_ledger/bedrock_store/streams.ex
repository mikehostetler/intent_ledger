defmodule IntentLedger.BedrockStore.Streams do
  @moduledoc false

  alias IntentLedger.BedrockStore
  alias IntentLedger.BedrockStore.{Keyspaces, Options, Outbox}
  alias IntentLedger.{Intent, Signal}

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
    with {:ok, stream} <- stream_name(source),
         {:ok, cursor} <- Options.non_negative_integer(opts, :cursor, 0),
         {:ok, limit} <- Options.positive_integer(opts, :limit, 100) do
      BedrockStore.transact(ledger, fn repo, root ->
        keyspace = Keyspaces.stream(root, stream)

        entries =
          keyspace
          |> Keyspaces.range_from_cursor(cursor)
          |> repo.get_range(limit: limit)
          |> Stream.map(fn {_key, value} -> Keyspaces.decode(value) end)
          |> Enum.to_list()

        {:ok, Enum.map(entries, & &1.signal)}
      end)
    end
  end

  @spec head(module(), Bedrock.Keyspace.t(), String.t()) :: non_neg_integer()
  @doc false
  def head(repo, root, stream) do
    case repo.get(Keyspaces.stream_version(root), stream) do
      nil -> 0
      value -> Keyspaces.decode(value)
    end
  end

  @spec append(module(), Bedrock.Keyspace.t(), String.t(), Jido.Signal.t()) :: pos_integer()
  @doc false
  def append(repo, root, stream, signal) do
    versions = Keyspaces.stream_version(root)
    streams = Keyspaces.stream(root, stream)
    cursor = next_counter(repo, versions, stream)
    repo.put(streams, cursor, Keyspaces.encode(%{stream: stream, cursor: cursor, signal: signal}))
    cursor
  end

  defp next_counter(repo, keyspace, key) do
    next =
      case repo.get(keyspace, key) do
        nil -> 1
        value -> Keyspaces.decode(value) + 1
      end

    repo.put(keyspace, key, Keyspaces.encode(next))
    next
  end

  defp stream_name(:ledger), do: {:ok, "ledger"}
  defp stream_name({:intent, intent_id}) when is_binary(intent_id), do: {:ok, "intent:#{intent_id}"}
  defp stream_name(source), do: {:error, {:unsupported_replay_source, source}}
end
