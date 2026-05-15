defmodule IntentLedger.Repair do
  @moduledoc false

  alias Bedrock.JobQueue.{Internal, Store}
  alias IntentLedger.BedrockStore
  alias IntentLedger.BedrockStore.Keyspaces
  alias IntentLedger.{Config, Intent}

  @statuses [
    :enqueued,
    :started,
    :completed,
    :failed,
    :retry_scheduled,
    :discarded,
    :canceled,
    :ambiguous
  ]

  @type check :: %{name: atom(), status: :ok | :drift, details: map()}
  @type report :: %{valid?: boolean(), checks: [check()]}

  @spec verify(module(), keyword()) :: {:ok, report()} | {:error, term()}
  @doc false
  def verify(ledger, opts \\ []) when is_atom(ledger) do
    with {:ok, heads} <- BedrockStore.heads(ledger),
         limit = verify_limit(heads, opts),
         {:ok, ledger_entries} <- BedrockStore.replay_entries(ledger, :ledger, limit: limit),
         {:ok, outbox_entries} <- BedrockStore.replay_entries(ledger, :outbox, limit: limit),
         {:ok, intents} <- BedrockStore.intents(ledger, limit: limit),
         {:ok, key_index} <- key_index(ledger, limit),
         {:ok, queue_state} <- queue_state(ledger, limit) do
      expected = expected_intents(ledger_entries)

      checks = [
        ledger_head_check(ledger_entries, heads.ledger),
        outbox_mirror_check(ledger_entries, outbox_entries, heads.outbox),
        intent_state_check(expected, intents),
        status_index_check(ledger, expected, limit),
        key_index_check(intents, key_index),
        queue_consistency_check(intents, queue_state)
      ]

      {:ok, %{valid?: Enum.all?(checks, &(&1.status == :ok)), checks: checks}}
    end
  end

  defp expected_intents(entries) do
    Enum.reduce(entries, %{}, fn entry, acc ->
      signal = entry.signal

      Map.update(acc, signal.subject, expected_from_signal(signal), fn expected ->
        merge_expected(expected, expected_from_signal(signal))
      end)
    end)
  end

  defp expected_from_signal(%Jido.Signal{} = signal) do
    data = signal.data || %{}

    %{
      id: signal.subject,
      status: status_from_type(signal.type),
      key: get_field(data, :key),
      topic: get_field(signal.extensions, :topic),
      queue: get_field(signal.extensions, :queue)
    }
  end

  defp status_from_type("intent.enqueued"), do: :enqueued
  defp status_from_type("intent.started"), do: :started
  defp status_from_type("intent.completed"), do: :completed
  defp status_from_type("intent.failed"), do: :failed
  defp status_from_type("intent.retry_scheduled"), do: :retry_scheduled
  defp status_from_type("intent.discarded"), do: :discarded
  defp status_from_type("intent.canceled"), do: :canceled
  defp status_from_type("intent.ambiguous"), do: :ambiguous
  defp status_from_type(_type), do: :unknown

  defp ledger_head_check(entries, head) do
    expected_head = length(entries)

    check(:ledger_head, expected_head == head, %{
      expected: expected_head,
      actual: head
    })
  end

  defp outbox_mirror_check(ledger_entries, outbox_entries, head) do
    ledger_facts = Enum.map(ledger_entries, &signal_fingerprint(&1.signal))
    outbox_facts = Enum.map(outbox_entries, &signal_fingerprint(&1.signal))

    check(:outbox_mirror, ledger_facts == outbox_facts and length(outbox_entries) == head, %{
      ledger_count: length(ledger_entries),
      outbox_count: length(outbox_entries),
      outbox_head: head
    })
  end

  defp intent_state_check(expected, intents) do
    actual =
      Map.new(intents, fn %Intent{} = intent ->
        {intent.id, %{status: intent.status, key: intent.key, topic: intent.topic, queue: intent.queue}}
      end)

    missing = expected |> Map.keys() |> Enum.reject(&Map.has_key?(actual, &1))

    mismatched =
      expected
      |> Enum.flat_map(fn {id, expected_intent} ->
        case Map.fetch(actual, id) do
          {:ok, actual_intent} ->
            expected_view = Map.take(expected_intent, [:status, :key, :topic, :queue])

            if expected_view == actual_intent do
              []
            else
              [%{id: id, expected: expected_view, actual: actual_intent}]
            end

          :error ->
            []
        end
      end)

    check(:intent_state, missing == [] and mismatched == [], %{missing: missing, mismatched: mismatched})
  end

  defp status_index_check(ledger, expected, limit) do
    actual_by_status =
      Enum.reduce_while(@statuses, {:ok, %{}}, fn status, {:ok, acc} ->
        case BedrockStore.intents(ledger, status: status, limit: limit) do
          {:ok, intents} -> {:cont, {:ok, Map.put(acc, status, MapSet.new(intents, & &1.id))}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)

    case actual_by_status do
      {:ok, actual_by_status} ->
        expected_by_status =
          expected
          |> Enum.group_by(fn {_id, expected_intent} -> expected_intent.status end, fn {id, _expected_intent} -> id end)
          |> Map.new(fn {status, ids} -> {status, MapSet.new(ids)} end)

        mismatched =
          @statuses
          |> Enum.flat_map(fn status ->
            expected_ids = Map.get(expected_by_status, status, MapSet.new())
            actual_ids = Map.get(actual_by_status, status, MapSet.new())

            if expected_ids == actual_ids do
              []
            else
              [%{status: status, expected: Enum.sort(expected_ids), actual: Enum.sort(actual_ids)}]
            end
          end)

        check(:status_indexes, mismatched == [], %{mismatched: mismatched})

      {:error, reason} ->
        check(:status_indexes, false, %{error: reason})
    end
  end

  defp key_index_check(intents, key_index) do
    expected =
      intents
      |> Enum.filter(& &1.key)
      |> Map.new(&{&1.key, &1.id})

    check(:key_index, expected == key_index, %{expected: expected, actual: key_index})
  end

  defp key_index(ledger, limit) do
    BedrockStore.transact(ledger, fn repo, root ->
      entries =
        root
        |> Keyspaces.key_index()
        |> repo.get_range(limit: limit)
        |> Map.new(fn {key, intent_id} -> {key, intent_id} end)

      {:ok, entries}
    end)
  end

  defp queue_state(ledger, limit) do
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

  defp verify_limit(heads, opts) do
    Keyword.get_lazy(opts, :limit, fn ->
      max(heads.ledger, heads.outbox)
      |> max(1)
    end)
  end

  defp merge_expected(previous, next) do
    %{
      id: next.id || previous.id,
      status: next.status,
      key: next.key || previous.key,
      topic: next.topic || previous.topic,
      queue: next.queue || previous.queue
    }
  end

  defp get_field(map, field) when is_map(map) do
    Map.get(map, field, Map.get(map, Atom.to_string(field)))
  end

  defp get_field(_value, _field), do: nil

  defp queue_consistency_check(intents, queue_state) do
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

    check(
      :queue_consistency,
      missing_runnable == [] and duplicate_items == [] and unexpected_items == [] and lease_without_item == [] and
        stat_mismatches == [] and truncated == [],
      %{
        missing_runnable: missing_runnable,
        duplicate_items: duplicate_items,
        unexpected_items: unexpected_items,
        lease_without_item: lease_without_item,
        stat_mismatches: stat_mismatches,
        truncated: truncated
      }
    )
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

  defp signal_fingerprint(signal), do: {signal.id, signal.type, signal.subject}

  defp check(name, true, details), do: %{name: name, status: :ok, details: details}
  defp check(name, false, details), do: %{name: name, status: :drift, details: details}
end
