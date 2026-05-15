defmodule IntentLedger.Runtime.Queue do
  @moduledoc false

  alias Bedrock.{KeyRange, Keyspace}
  alias Bedrock.JobQueue.{Internal, Item, Store}
  alias IntentLedger.Intent

  @queue_neutralization_scan_limit 10_000

  @spec root(module()) :: Bedrock.Keyspace.t()
  @doc false
  def root(ledger), do: Internal.root_keyspace(ledger.__intent_ledger__().job_queue)

  @spec enqueue_intent(module(), module(), Intent.t(), DateTime.t()) :: term()
  @doc false
  def enqueue_intent(repo, ledger, %Intent{} = intent, %DateTime{} = now) do
    Store.enqueue(repo, root(ledger), item(intent, ledger, now), now: DateTime.to_unix(now, :millisecond))
  end

  @spec item(Intent.t(), module(), DateTime.t()) :: Item.t()
  @doc false
  def item(%Intent{} = intent, ledger, %DateTime{} = now) do
    Item.new(intent.queue, intent.topic, payload(ledger, intent.id),
      id: intent.id,
      priority: intent.priority,
      max_retries: intent.max_attempts,
      vesting_time: DateTime.to_unix(intent.scheduled_at || now, :millisecond),
      now: DateTime.to_unix(now, :millisecond)
    )
  end

  @spec payload(module(), String.t()) :: binary()
  @doc false
  def payload(ledger, intent_id), do: :erlang.term_to_binary(%{ledger: ledger, intent_id: intent_id})

  @spec neutralize_pending_item(module(), module(), Intent.t(), keyword()) :: :removed | :leased | :missing | :unknown
  @doc false
  def neutralize_pending_item(repo, ledger, %Intent{} = intent, opts) do
    keyspaces = Store.queue_keyspaces(root(ledger), intent.queue)
    limit = Keyword.get(opts, :queue_neutralization_scan_limit, @queue_neutralization_scan_limit)

    case neutralize_known_pending_item(repo, keyspaces, intent) do
      :missing -> scan_pending_queue_item(repo, keyspaces, intent, limit)
      status -> status
    end
  end

  defp neutralize_known_pending_item(repo, keyspaces, %Intent{} = intent) do
    item_key = {intent.priority, DateTime.to_unix(intent.scheduled_at, :millisecond), intent.id}

    case repo.get(keyspaces.items, item_key) do
      nil ->
        :missing

      value ->
        case :erlang.binary_to_term(value) do
          %Item{id: id, lease_id: nil} when id == intent.id ->
            repo.clear(keyspaces.items, item_key)
            decrement_pending_stat(repo, keyspaces)
            :removed

          %Item{id: id} when id == intent.id ->
            :leased

          _other ->
            :missing
        end
    end
  end

  defp scan_pending_queue_item(repo, keyspaces, %Intent{} = intent, limit) do
    entries =
      repo
      |> item_rows(keyspaces, limit: limit + 1)
      |> Enum.to_list()

    entries
    |> Enum.take(limit)
    |> Enum.find_value(:missing, fn {item_key, value} ->
      case :erlang.binary_to_term(value) do
        %Item{id: id, lease_id: nil} when id == intent.id ->
          repo.clear(keyspaces.items, item_key)
          decrement_pending_stat(repo, keyspaces)
          :removed

        %Item{id: id} when id == intent.id ->
          :leased

        _other ->
          false
      end
    end)
    |> case do
      :missing when length(entries) > limit -> :unknown
      status -> status
    end
  end

  defp decrement_pending_stat(repo, keyspaces) do
    keyspaces.stats
    |> Keyspace.pack("pending")
    |> repo.add(<<-1::64-signed-little>>)
  end

  defp item_rows(repo, keyspaces, opts) do
    if function_exported?(repo, :__cluster__, 0) do
      keyspaces.items
      |> Keyspace.prefix()
      |> KeyRange.from_prefix()
      |> repo.get_range(opts)
    else
      repo.get_range(keyspaces.items, opts)
    end
  end
end
