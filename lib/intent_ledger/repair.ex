defmodule IntentLedger.Repair do
  @moduledoc false

  alias IntentLedger.BedrockStore
  alias IntentLedger.BedrockStore.Keyspaces
  alias IntentLedger.Intent
  alias IntentLedger.Repair.QueueConsistency

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
         {:ok, queue_state} <- QueueConsistency.snapshot(ledger, limit) do
      expected = expected_intents(ledger_entries)

      checks = [
        ledger_head_check(ledger_entries, heads.ledger),
        outbox_mirror_check(ledger_entries, outbox_entries, heads.outbox),
        intent_state_check(expected, intents),
        status_index_check(ledger, expected, limit),
        key_index_check(intents, key_index),
        QueueConsistency.check(intents, queue_state)
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

  defp signal_fingerprint(signal), do: {signal.id, signal.type, signal.subject}

  defp check(name, true, details), do: %{name: name, status: :ok, details: details}
  defp check(name, false, details), do: %{name: name, status: :drift, details: details}
end
