defmodule IntentLedger.Telemetry do
  @moduledoc """
  Small telemetry boundary for IntentLedger runtime operations.
  """

  @default_prefix [:intent_ledger]

  @type event :: :enqueue | :handler

  @doc """
  Returns the default event prefix.
  """
  @spec default_prefix() :: [atom()]
  def default_prefix, do: @default_prefix

  @doc false
  @spec emit(event(), term(), integer(), module(), keyword()) :: :ok
  def emit(event, result, start, ledger, metadata \\ []) do
    duration = System.monotonic_time() - start
    status = status(result)

    :telemetry.execute(
      @default_prefix ++ [event, :stop],
      %{duration: duration, count: Keyword.get(metadata, :count, 1)},
      metadata
      |> Keyword.put(:ledger, ledger)
      |> Keyword.put(:status, status)
      |> Map.new()
    )
  end

  defp status({:ok, _}), do: :ok
  defp status(:ok), do: :ok
  defp status({:error, _}), do: :error
  defp status(_), do: :unknown
end
