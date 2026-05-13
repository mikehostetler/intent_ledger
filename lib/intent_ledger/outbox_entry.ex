defmodule IntentLedger.OutboxEntry do
  @moduledoc false

  @doc false
  @spec key(Jido.Signal.t() | map()) :: String.t()
  def key(signal), do: "sig:" <> signal_id(signal)

  @doc false
  @spec new(String.t(), Jido.Signal.t() | map()) :: map()
  def new(stream, signal) when is_binary(stream) do
    %{
      stream: stream,
      signal: signal,
      signal_id: signal_id(signal),
      signal_type: field(signal, :type),
      subject: field(signal, :subject),
      inserted_at: field(signal, :time)
    }
  end

  defp signal_id(signal) do
    case field(signal, :id) do
      id when is_binary(id) -> id
      id -> to_string(id)
    end
  end

  defp field(%Jido.Signal{} = signal, key), do: Map.get(signal, key)

  defp field(%{} = signal, key) do
    Map.get(signal, key) || Map.get(signal, Atom.to_string(key))
  end
end
