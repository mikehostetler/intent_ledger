defmodule IntentLedger.StoreCaseAtomicCommitHarnessStore do
  @behaviour IntentLedger.Store

  alias IntentLedger.Store.{Commit, Conflict}

  def child_spec(_opts) do
    %{
      id: {__MODULE__, make_ref()},
      start: {Agent, :start_link, [fn -> %{streams: %{}, idempotency: %{}} end]}
    }
  end

  def commit(ref, _ledger, request, _opts) do
    Agent.get_and_update(ref, fn state ->
      case check_preconditions(state, request.preconditions) do
        :ok ->
          next_state = Enum.reduce(request.writes, state, &apply_write/2)

          commit =
            Commit.new(
              command_id: request.command_id,
              writes: request.writes,
              signals: signals(request.writes)
            )

          {{:ok, commit}, next_state}

        {:error, conflict} ->
          {{:error, conflict}, state}
      end
    end)
  end

  def read(_ref, _ledger, _request, _opts), do: {:ok, nil}
  def lease(_ref, _ledger, _request, _opts), do: {:ok, nil}
  def listing(_ref, _ledger, _request, _opts), do: {:ok, []}
  def outbox(_ref, _ledger, _request, _opts), do: {:ok, []}

  defp check_preconditions(state, preconditions) do
    Enum.reduce_while(preconditions, :ok, fn
      %{type: :stream_version, stream: stream, expected: expected}, :ok ->
        actual = stream_version(state, stream)

        if actual == expected do
          {:cont, :ok}
        else
          {:halt, {:error, Conflict.stream_version(stream, expected, actual)}}
        end

      %{type: :command_absent, key: command_id}, :ok ->
        if Map.has_key?(state.idempotency, command_id) do
          {:halt, {:error, Conflict.command_conflict(command_id, :absent, :present)}}
        else
          {:cont, :ok}
        end

      _precondition, :ok ->
        {:cont, :ok}
    end)
  end

  defp apply_write(%{type: :append_signal, stream: stream, value: signal}, state) do
    update_in(state, [:streams, stream], fn signals -> List.wrap(signals) ++ [signal] end)
  end

  defp apply_write(%{type: :put_idempotency, key: command_id, value: value}, state) do
    put_in(state, [:idempotency, command_id], value)
  end

  defp apply_write(_write, state), do: state

  defp signals(writes) do
    writes
    |> Enum.filter(&(&1.type == :append_signal))
    |> Enum.map(& &1.value)
  end

  defp stream_version(state, stream), do: state.streams |> Map.get(stream, []) |> length()
end

defmodule IntentLedger.StoreCaseAtomicCommitTest do
  use IntentLedger.StoreCase,
    async: true,
    store_module: IntentLedger.StoreCaseAtomicCommitHarnessStore

  use IntentLedger.StoreCase.AtomicCommitTests
end
