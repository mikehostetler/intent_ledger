defmodule IntentLedger.Telemetry do
  @moduledoc """
  Small telemetry boundary for IntentLedger runtime operations.
  """

  @default_prefix [:intent_ledger]

  @type event :: :command | :enqueue | :handler | :health | :outbox | :projection | :replay

  @doc """
  Returns the default event prefix.
  """
  @spec default_prefix() :: [atom()]
  def default_prefix, do: @default_prefix

  @doc false
  @spec emit(event(), term(), integer(), module() | nil, keyword()) :: :ok
  def emit(event, result, start, ledger, metadata \\ []) do
    duration = System.monotonic_time() - start
    status = status(result)

    :telemetry.execute(
      @default_prefix ++ [event, :stop],
      %{duration: duration, count: Keyword.get(metadata, :count, 1)},
      metadata
      |> Keyword.put(:ledger, ledger)
      |> Keyword.put(:status, status)
      |> put_error_metadata(result)
      |> Map.new()
    )
  end

  defp status({:ok, _}), do: :ok
  defp status(:ok), do: :ok
  defp status({:discard, _}), do: :discard
  defp status({:snooze, _}), do: :snooze
  defp status({:error, _}), do: :error
  defp status(_), do: :unknown

  defp put_error_metadata(metadata, {:error, reason}), do: Keyword.put(metadata, :error_kind, reason_kind(reason))
  defp put_error_metadata(metadata, {:discard, reason}), do: Keyword.put(metadata, :error_kind, reason_kind(reason))
  defp put_error_metadata(metadata, _result), do: metadata

  defp reason_kind(reason) when is_atom(reason), do: reason
  defp reason_kind({kind, _details}) when is_atom(kind), do: kind
  defp reason_kind({kind, _details, _extra}) when is_atom(kind), do: kind
  defp reason_kind(%{__exception__: true, __struct__: module}), do: module
  defp reason_kind(_reason), do: :unknown
end
