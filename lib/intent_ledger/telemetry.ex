defmodule Jido.IntentLedger.Telemetry do
  @moduledoc false

  @default_prefix [:intent_ledger]

  @doc false
  @spec execute(keyword(), atom(), list(atom()), map(), map()) :: :ok
  def execute(opts, operation, event, measurements, metadata) do
    prefix = Keyword.get(opts, :telemetry_prefix, @default_prefix)
    :telemetry.execute(prefix ++ [operation | event], measurements, metadata)
  end
end
