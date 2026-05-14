defmodule IntentLedger.StoreCaseSemanticHarnessStore do
  @behaviour IntentLedger.Store

  alias IntentLedger.Store.{Commit, Conflict, Listing, Outbox}

  def child_spec(_opts) do
    %{
      id: {__MODULE__, make_ref()},
      start:
        {Agent, :start_link,
         [
           fn ->
             %{
               streams: %{},
               idempotency: %{},
               intents: %{},
               lineage_intents: %{},
               lineage_states: %{},
               claims: %{},
               shard_leases: %{},
               outbox: %{},
               next_outbox_sequence: 1
             }
           end
         ]}
    }
  end

  def commit(ref, _ledger, request, _opts) do
    Agent.get_and_update(ref, fn state ->
      case check_preconditions(state, request) do
        {:replay, entry} ->
          commit =
            Commit.new(
              command_id: request.command_id,
              result: entry.result,
              replayed: true,
              replay_of: request.command_id
            )

          {{:ok, commit}, state}

        :ok ->
          {next_state, commit_result} = apply_writes(state, request)

          commit =
            Commit.new(
              command_id: request.command_id,
              result: commit_result,
              writes: request.writes,
              signals: signals(request.writes)
            )

          {{:ok, commit}, next_state}

        {:error, conflict} ->
          {{:error, conflict}, state}
      end
    end)
  end

  def read(ref, _ledger, {:lineage_counts, attrs}, _opts) do
    Agent.get(ref, fn state ->
      counts =
        IntentLedger.Store.Lineage.counts(
          Map.values(state.lineage_intents),
          Map.values(state.lineage_states),
          attrs
        )

      {:ok, counts}
    end)
  end

  def read(_ref, _ledger, _request, _opts), do: {:ok, nil}
  def lease(_ref, _ledger, _request, _opts), do: {:ok, nil}

  def listing(ref, _ledger, %Listing{} = request, _opts), do: do_listing(ref, request)
  def listing(ref, _ledger, {type, attrs}, opts), do: listing(ref, nil, Listing.new(type, attrs), opts)

  def outbox(ref, _ledger, %Outbox{} = request, _opts), do: do_outbox(ref, request)
  def outbox(ref, _ledger, {type, attrs}, opts), do: outbox(ref, nil, Outbox.new(type, attrs), opts)

  defp check_preconditions(state, request) do
    Enum.reduce_while(request.preconditions, :ok, fn
      %{type: :command_replay, key: command_id}, :ok ->
        case Map.fetch(state.idempotency, command_id) do
          {:ok, entry} ->
            if entry.signature == signature(request) do
              {:halt, {:replay, entry}}
            else
              {:halt, {:error, Conflict.command_conflict(command_id, entry.signature, signature(request))}}
            end

          :error ->
            {:halt, {:error, Conflict.command_replay(command_id, nil)}}
        end

      %{type: :command_absent, key: command_id}, :ok ->
        if Map.has_key?(state.idempotency, command_id) do
          {:halt, {:error, Conflict.command_conflict(command_id, :absent, :present)}}
        else
          {:cont, :ok}
        end

      %{type: :stream_version, stream: stream, expected: expected}, :ok ->
        actual = state.streams |> Map.get(stream, []) |> length()

        if actual == expected do
          {:cont, :ok}
        else
          {:halt, {:error, Conflict.stream_version(stream, expected, actual)}}
        end

      %{type: :intent_status, key: intent_id, expected: expected}, :ok ->
        actual = state.intents |> Map.get(intent_id, %{}) |> Map.get(:status, :missing)

        if actual in expected do
          {:cont, :ok}
        else
          {:halt, {:error, Conflict.intent_status(intent_id, expected, actual)}}
        end

      %{type: :claim_fence, key: claim_id, expected: expected, metadata: metadata}, :ok ->
        case claim_fence_conflict(state, claim_id, expected, metadata) do
          nil -> {:cont, :ok}
          conflict -> {:halt, {:error, conflict}}
        end

      %{type: :shard_lease, key: key, expected: expected, metadata: metadata}, :ok ->
        case shard_lease_conflict(state, key, expected, metadata) do
          nil -> {:cont, :ok}
          conflict -> {:halt, {:error, conflict}}
        end

      %{type: :outbox_unacked, key: entry_id}, :ok ->
        case Map.fetch(state.outbox, entry_id) do
          {:ok, %{acked_at: nil}} -> {:cont, :ok}
          {:ok, entry} -> {:halt, {:error, Conflict.outbox(entry_id, :unacked, entry)}}
          :error -> {:halt, {:error, Conflict.outbox(entry_id, :unacked, :missing)}}
        end

      _precondition, :ok ->
        {:cont, :ok}
    end)
  end

  defp claim_fence_conflict(state, claim_id, expected, metadata) do
    now = Map.get(metadata, :now)

    with {:ok, claim} <- Map.fetch(state.claims, claim_id),
         true <- Map.get(claim, :token_hash) == expected.token_hash,
         true <- current_lease?(claim, now) do
      nil
    else
      _ -> Conflict.claim_fence(claim_id, expected, Map.get(state.claims, claim_id, :missing))
    end
  end

  defp shard_lease_conflict(state, key, expected, metadata) do
    lease = Map.get(state.shard_leases, key)
    now = Map.get(metadata, :now)

    cond do
      Map.has_key?(expected, :available_at) and (is_nil(lease) or expired_at?(lease, expected.available_at)) ->
        nil

      Map.has_key?(expected, :expired_at_or_before) and not is_nil(lease) and
          expired_at?(lease, expected.expired_at_or_before) ->
        nil

      Map.get(expected, :status) == :current and not is_nil(lease) and
        Map.get(lease, :owner_id) == expected.owner_id and current_lease?(lease, now) ->
        nil

      true ->
        Conflict.new(:shard_lease,
          key: key,
          expected: expected,
          actual: lease || :missing,
          message: "shard lease conflict"
        )
    end
  end

  defp apply_writes(state, request) do
    Enum.reduce(request.writes, {state, nil}, fn write, {acc, result} ->
      {next_acc, next_result} = apply_write(acc, write, request)
      {next_acc, next_result || result}
    end)
  end

  defp apply_write(state, %{type: :append_signal, stream: stream, value: signal}, _request) do
    {update_in(state, [:streams, stream], fn signals -> List.wrap(signals) ++ [signal] end), nil}
  end

  defp apply_write(state, %{type: :put_idempotency, key: command_id, value: value}, request) do
    entry = %{signature: signature(request), result: value}
    {put_in(state, [:idempotency, command_id], entry), value}
  end

  defp apply_write(state, %{type: :put_intent, key: key, value: value}, _request) do
    {put_in(state, [:lineage_intents, key], value), nil}
  end

  defp apply_write(state, %{type: :put_state, key: key, value: value}, _request) do
    next_state =
      state
      |> put_in([:intents, key], value)
      |> put_in([:lineage_states, key], value)

    {next_state, nil}
  end

  defp apply_write(state, %{type: :put_claim, key: key, value: value}, _request) do
    {put_in(state, [:claims, key], value), nil}
  end

  defp apply_write(state, %{type: :delete_claim, key: key}, _request) do
    {update_in(state.claims, &Map.delete(&1, key)) |> then(&%{state | claims: &1}), nil}
  end

  defp apply_write(state, %{type: :put_shard_lease, key: key, value: value}, _request) do
    {put_in(state, [:shard_leases, key], value), nil}
  end

  defp apply_write(state, %{type: :delete_shard_lease, key: key}, _request) do
    {update_in(state.shard_leases, &Map.delete(&1, key)) |> then(&%{state | shard_leases: &1}), nil}
  end

  defp apply_write(state, %{type: :put_outbox, key: key, value: value}, _request) do
    sequence = Map.get(value, :sequence, state.next_outbox_sequence)

    entry =
      value
      |> Map.put(:key, key)
      |> Map.put(:sequence, sequence)
      |> Map.put_new(:acked_at, nil)

    next_state =
      state
      |> put_in([:outbox, key], entry)
      |> Map.put(:next_outbox_sequence, max(state.next_outbox_sequence, sequence + 1))

    {next_state, nil}
  end

  defp apply_write(state, %{type: :ack_outbox, key: key, metadata: metadata}, _request) do
    acked_at = Map.get(metadata, :acked_at)
    consumer = Map.get(metadata, :consumer)
    {update_in(state, [:outbox, key], &Map.merge(&1, %{acked_at: acked_at, consumer: consumer})), nil}
  end

  defp apply_write(state, _write, _request), do: {state, nil}

  defp do_listing(ref, request) do
    Agent.get(ref, fn state ->
      rows =
        state.intents
        |> Map.values()
        |> Enum.filter(&listing_match?(&1, request))
        |> Enum.sort_by(&listing_sort_key(&1, request))
        |> Enum.take(request.limit)

      {:ok, rows}
    end)
  end

  defp listing_match?(state, %{type: :due_intents} = request) do
    state.queue == request.queue and shard_match?(state, request) and state.status in [:available, :retry_scheduled] and
      DateTime.compare(state.visible_at, request.at) != :gt
  end

  defp listing_match?(state, %{type: :expired_claims} = request) do
    state.queue == request.queue and shard_match?(state, request) and state.status == :claimed and
      DateTime.compare(state.lease_until, request.at) != :gt
  end

  defp shard_match?(_state, %{shard: nil}), do: true
  defp shard_match?(state, request), do: state.shard == request.shard

  defp listing_sort_key(state, %{type: :due_intents}) do
    {-Map.get(state, :priority, 0), DateTime.to_unix(state.visible_at, :microsecond), state.intent_id}
  end

  defp listing_sort_key(state, %{type: :expired_claims}) do
    {DateTime.to_unix(state.lease_until, :microsecond), state.intent_id}
  end

  defp do_outbox(ref, %{type: :read} = request) do
    Agent.get(ref, fn state ->
      {:ok,
       state.outbox
       |> Map.values()
       |> Enum.filter(&(is_nil(&1.acked_at) and after_cursor?(&1, request.cursor)))
       |> Enum.sort_by(& &1.sequence)
       |> Enum.take(request.limit)}
    end)
  end

  defp do_outbox(ref, %{type: :replay} = request) do
    Agent.get(ref, fn state ->
      {:ok,
       state.outbox
       |> Map.values()
       |> Enum.filter(&after_cursor?(&1, request.cursor))
       |> Enum.sort_by(& &1.sequence)
       |> Enum.take(request.limit)}
    end)
  end

  defp do_outbox(ref, %{type: :ack, key: key, consumer: consumer, metadata: metadata}) do
    Agent.get_and_update(ref, fn state ->
      case Map.fetch(state.outbox, key) do
        {:ok, %{acked_at: nil} = entry} ->
          acked_at = Map.get(metadata, :acked_at)
          acked = Map.merge(entry, %{acked_at: acked_at, consumer: consumer})
          {{:ok, acked}, put_in(state, [:outbox, key], acked)}

        {:ok, entry} ->
          {{:error, Conflict.outbox(key, :unacked, entry)}, state}

        :error ->
          {{:error, Conflict.outbox(key, :unacked, :missing)}, state}
      end
    end)
  end

  defp do_outbox(ref, %{type: :insert, key: key, value: value}) do
    Agent.get_and_update(ref, fn state ->
      {next_state, _result} = apply_write(state, %{type: :put_outbox, key: key, value: value}, %{})
      {{:ok, Map.fetch!(next_state.outbox, key)}, next_state}
    end)
  end

  defp current_lease?(_lease, nil), do: true
  defp current_lease?(lease, now), do: DateTime.compare(lease.lease_until, now) == :gt
  defp expired_at?(lease, now), do: DateTime.compare(lease.lease_until, now) != :gt

  defp after_cursor?(_entry, nil), do: true
  defp after_cursor?(entry, cursor) when is_integer(cursor), do: entry.sequence > cursor
  defp after_cursor?(_entry, _cursor), do: true

  defp signals(writes) do
    writes
    |> Enum.filter(&(&1.type == :append_signal))
    |> Enum.map(& &1.value)
  end

  defp signature(request), do: {request.operation, request.command}
end

defmodule IntentLedger.StoreCaseSemanticTest do
  use IntentLedger.StoreCase,
    async: true,
    store_module: IntentLedger.StoreCaseSemanticHarnessStore

  use IntentLedger.StoreCase.SemanticTests
end
