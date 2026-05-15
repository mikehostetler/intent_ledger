defmodule IntentLedger.Repair.QueueConsistency do
  @moduledoc false

  alias Bedrock.JobQueue.{Internal, Store}
  alias IntentLedger.BedrockStore
  alias IntentLedger.{Config, Intent}

  @spec snapshot(module(), pos_integer()) :: {:ok, map()} | {:error, term()}
  @doc false
  def snapshot(ledger, limit) do
    config = ledger.__intent_ledger__()
    queue_root = Internal.root_keyspace(config.job_queue)
    queues = Config.queue_ids(config.queues)

    BedrockStore.transact(ledger, fn repo, _root ->
      snapshots =
        Map.new(queues, fn queue ->
          {queue, queue_snapshot(repo, queue_root, queue, limit)}
        end)

      {:ok, snapshots}
    end)
  end

  @spec check([Intent.t()], map()) :: map()
  @doc false
  def check(intents, queue_state) do
    intents_by_id = Map.new(intents, &{&1.id, &1})
    item_refs = queue_items(queue_state)
    item_ids = Enum.map(item_refs, & &1.item.id)
    item_id_set = MapSet.new(item_ids)

    runnable_ids =
      intents
      |> Enum.filter(&queue_item_expected?/1)
      |> MapSet.new(& &1.id)

    missing_runnable =
      runnable_ids
      |> MapSet.difference(item_id_set)
      |> Enum.sort()

    duplicate_items =
      item_ids
      |> Enum.frequencies()
      |> Enum.filter(fn {_id, count} -> count > 1 end)
      |> Enum.map(fn {id, count} -> %{id: id, count: count} end)

    unexpected_items =
      item_refs
      |> Enum.flat_map(&unexpected_queue_item(&1, intents_by_id))

    lease_ids = queue_state |> queue_leases() |> MapSet.new(& &1.lease.item_id)

    lease_without_item =
      lease_ids
      |> MapSet.difference(item_id_set)
      |> Enum.sort()

    stat_mismatches =
      queue_state
      |> Enum.flat_map(fn {queue, snapshot} ->
        expected_pending = Enum.count(snapshot.items, fn {_key, item} -> item.lease_id == nil end)
        expected_processing = length(snapshot.leases)
        stats = snapshot.stats

        if stats.pending_count == expected_pending and stats.processing_count == expected_processing do
          []
        else
          [
            %{
              queue: queue,
              expected: %{pending_count: expected_pending, processing_count: expected_processing},
              actual: stats
            }
          ]
        end
      end)

    truncated =
      queue_state
      |> Enum.flat_map(fn {queue, snapshot} ->
        []
        |> maybe_truncated(queue, :items, snapshot.items_truncated?)
        |> maybe_truncated(queue, :leases, snapshot.leases_truncated?)
      end)

    ok? =
      missing_runnable == [] and duplicate_items == [] and unexpected_items == [] and lease_without_item == [] and
        stat_mismatches == [] and truncated == []

    %{
      name: :queue_consistency,
      status: if(ok?, do: :ok, else: :drift),
      details: %{
        missing_runnable: missing_runnable,
        duplicate_items: duplicate_items,
        unexpected_items: unexpected_items,
        lease_without_item: lease_without_item,
        stat_mismatches: stat_mismatches,
        truncated: truncated
      }
    }
  end

  defp queue_snapshot(repo, queue_root, queue, limit) do
    keyspaces = Store.queue_keyspaces(queue_root, queue)

    item_entries =
      keyspaces.items
      |> repo.get_range(limit: limit + 1)
      |> Enum.map(fn {key, value} -> {key, :erlang.binary_to_term(value)} end)

    lease_entries =
      keyspaces.leases
      |> repo.get_range(limit: limit + 1)
      |> Enum.map(fn {key, value} -> {key, :erlang.binary_to_term(value)} end)

    %{
      items: Enum.take(item_entries, limit),
      items_truncated?: length(item_entries) > limit,
      leases: Enum.take(lease_entries, limit),
      leases_truncated?: length(lease_entries) > limit,
      stats: Store.stats(repo, queue_root, queue)
    }
  end

  defp queue_items(queue_state) do
    Enum.flat_map(queue_state, fn {queue, snapshot} ->
      Enum.map(snapshot.items, fn {key, item} -> %{queue: queue, key: key, item: item} end)
    end)
  end

  defp queue_leases(queue_state) do
    Enum.flat_map(queue_state, fn {queue, snapshot} ->
      Enum.map(snapshot.leases, fn {key, lease} -> %{queue: queue, key: key, lease: lease} end)
    end)
  end

  defp queue_item_expected?(%Intent{status: status}) when status in [:enqueued, :started, :retry_scheduled], do: true
  defp queue_item_expected?(%Intent{}), do: false

  defp unexpected_queue_item(%{item: item} = ref, intents_by_id) do
    case Map.fetch(intents_by_id, item.id) do
      {:ok, %Intent{status: status}} when status in [:completed, :failed, :discarded] ->
        [queue_item_drift(ref, status)]

      {:ok, %Intent{status: status}} when status in [:canceled, :ambiguous] and item.lease_id == nil ->
        [queue_item_drift(ref, status)]

      {:ok, %Intent{}} ->
        []

      :error ->
        [queue_item_drift(ref, :missing_intent)]
    end
  end

  defp queue_item_drift(%{queue: queue, item: item}, status) do
    %{id: item.id, queue: queue, intent_status: status, leased?: item.lease_id != nil}
  end

  defp maybe_truncated(acc, _queue, _kind, false), do: acc
  defp maybe_truncated(acc, queue, kind, true), do: [%{queue: queue, kind: kind} | acc]
end
