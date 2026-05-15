defmodule IntentLedger.BedrockStore.Intents do
  @moduledoc false

  alias IntentLedger.BedrockStore
  alias IntentLedger.BedrockStore.{Codec, Keyspaces, Streams}
  alias IntentLedger.Intent

  @intent_statuses [
    :enqueued,
    :started,
    :completed,
    :failed,
    :retry_scheduled,
    :discarded,
    :canceled,
    :ambiguous
  ]
  @intent_status_by_name Map.new(@intent_statuses, fn status -> {Atom.to_string(status), status} end)

  @spec create(module(), module(), Bedrock.Keyspace.t(), Intent.t(), keyword()) ::
          {:ok, Intent.t(), :created | :existing}
  @doc false
  def create(ledger, repo, root, %Intent{} = intent, opts \\ []) do
    keys = Keyspaces.key_index(root)
    intents = Keyspaces.intent(root)

    case existing_by_key(repo, keys, intents, intent.key) do
      {:ok, %Intent{} = existing} ->
        {:ok, existing, :existing}

      :error ->
        now = Keyword.get_lazy(opts, :now, &DateTime.utc_now/0)
        intent = %{intent | created_at: intent.created_at || now, updated_at: now}

        repo.put(intents, intent.id, Codec.encode(intent))
        put_status_index(repo, root, intent)

        if intent.key do
          repo.put(keys, intent.key, intent.id)
        end

        Streams.append_lifecycle(
          repo,
          root,
          ledger,
          intent,
          :enqueued,
          lifecycle_data(intent, %{
            key: intent.key,
            topic: intent.topic,
            queue: intent.queue,
            scheduled_at: intent.scheduled_at,
            max_attempts: intent.max_attempts,
            priority: intent.priority
          })
        )

        {:ok, intent, :created}
    end
  end

  @spec fetch(module(), String.t()) :: {:ok, Intent.t()} | {:error, :not_found}
  @doc false
  def fetch(ledger, intent_id) do
    BedrockStore.transact(ledger, fn repo, root -> fetch(repo, root, intent_id) end)
  end

  @spec fetch(module(), Bedrock.Keyspace.t(), String.t()) :: {:ok, Intent.t()} | {:error, :not_found}
  @doc false
  def fetch(repo, root, intent_id) when is_binary(intent_id) do
    case repo.get(Keyspaces.intent(root), intent_id) do
      nil -> {:error, :not_found}
      value -> {:ok, Codec.decode(value)}
    end
  end

  @spec put(module(), Bedrock.Keyspace.t(), Intent.t()) :: :ok
  @doc false
  def put(repo, root, %Intent{} = intent) do
    previous =
      case fetch(repo, root, intent.id) do
        {:ok, %Intent{} = existing} -> existing
        {:error, :not_found} -> nil
      end

    repo.put(Keyspaces.intent(root), intent.id, Codec.encode(intent))
    update_status_index(repo, root, previous, intent)
  end

  @spec update(
          module(),
          Bedrock.Keyspace.t(),
          module(),
          String.t(),
          atom(),
          map(),
          (Intent.t(), DateTime.t() -> Intent.t())
        ) ::
          {:ok, Intent.t()} | {:error, term()}
  @doc false
  def update(repo, root, ledger, intent_id, event, data, update_fun)
      when is_atom(event) and is_map(data) and is_function(update_fun, 2) do
    with {:ok, intent} <- fetch(repo, root, intent_id) do
      now = DateTime.utc_now()
      next = update_fun.(intent, now)
      repo.put(Keyspaces.intent(root), next.id, Codec.encode(next))
      update_status_index(repo, root, intent, next)
      Streams.append_lifecycle(repo, root, ledger, next, event, data)
      {:ok, next}
    end
  end

  @spec all(module(), keyword()) :: {:ok, [Intent.t()]} | {:error, term()}
  @doc false
  def all(ledger, opts \\ []) do
    with {:ok, limit} <- positive_integer(opts, :limit, 100),
         {:ok, statuses} <- status_filter(Keyword.get(opts, :status)) do
      queue = Keyword.get(opts, :queue)
      topic = Keyword.get(opts, :topic)

      BedrockStore.transact(ledger, fn repo, root ->
        intents =
          statuses
          |> sources(repo, root, limit)
          |> Stream.map(&Codec.decode/1)
          |> Stream.filter(&matches_filter?(&1, :queue, queue))
          |> Stream.filter(&matches_filter?(&1, :topic, topic))
          |> Enum.take(limit)

        {:ok, intents}
      end)
    end
  end

  defp lifecycle_data(%Intent{} = intent, data) do
    command_keys = [
      :command_id,
      :command_ingress,
      :command_source,
      :command_submitted_at,
      :command_signal_id,
      :command_signal_type,
      :command_signal_source
    ]

    command_data =
      intent.metadata
      |> Map.take(command_keys)
      |> Map.new()

    Map.merge(data, command_data)
  end

  defp update_status_index(repo, root, nil, %Intent{} = next), do: put_status_index(repo, root, next)

  defp update_status_index(repo, root, %Intent{} = previous, %Intent{} = next) do
    if previous.status != next.status do
      clear_status_index(repo, root, previous)
    end

    put_status_index(repo, root, next)
  end

  defp put_status_index(repo, root, %Intent{} = intent) do
    root
    |> Keyspaces.status_index(intent.status)
    |> repo.put(intent.id, Codec.encode(intent))
  end

  defp clear_status_index(repo, root, %Intent{} = intent) do
    root
    |> Keyspaces.status_index(intent.status)
    |> repo.clear(intent.id)
  end

  defp sources(nil, repo, root, limit) do
    root
    |> Keyspaces.intent()
    |> repo.get_range(limit: limit)
    |> Stream.map(fn {_key, value} -> value end)
  end

  defp sources(statuses, repo, root, limit) do
    statuses
    |> Stream.flat_map(fn status ->
      root
      |> Keyspaces.status_index(status)
      |> repo.get_range(limit: limit)
      |> Stream.map(fn {_key, value} -> value end)
    end)
  end

  defp matches_filter?(_intent, _field, nil), do: true
  defp matches_filter?(%Intent{} = intent, field, value), do: Map.fetch!(intent, field) == value

  defp status_filter(nil), do: {:ok, nil}

  defp status_filter(statuses) when is_list(statuses) do
    statuses
    |> Enum.reduce_while({:ok, []}, fn status, {:ok, acc} ->
      case normalize_status(status) do
        {:ok, status} -> {:cont, {:ok, [status | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, statuses} -> {:ok, statuses |> Enum.reverse() |> Enum.uniq()}
      error -> error
    end
  end

  defp status_filter(status) do
    with {:ok, status} <- normalize_status(status), do: {:ok, [status]}
  end

  defp normalize_status(status) when status in @intent_statuses, do: {:ok, status}

  defp normalize_status(status) when is_binary(status) do
    case Map.fetch(@intent_status_by_name, status) do
      {:ok, status} -> {:ok, status}
      :error -> {:error, {:invalid_status, status}}
    end
  end

  defp normalize_status(status), do: {:error, {:invalid_status, status}}

  defp positive_integer(opts, key, default) do
    value = Keyword.get(opts, key, default)

    if is_integer(value) and value > 0 do
      {:ok, value}
    else
      {:error, {:invalid_option, key, value}}
    end
  end

  defp existing_by_key(_repo, _keys, _intents, nil), do: :error

  defp existing_by_key(repo, keys, intents, key) do
    case repo.get(keys, key) do
      nil ->
        :error

      intent_id ->
        case repo.get(intents, intent_id) do
          nil -> :error
          value -> {:ok, Codec.decode(value)}
        end
    end
  end
end
