defmodule IntentLedger.Signal do
  @moduledoc false

  @source_prefix "/intent_ledger"
  @types %{
    intent_submitted: "intent_ledger.intent.submitted",
    intent_available: "intent_ledger.intent.available",
    intent_claimed: "intent_ledger.intent.claimed",
    intent_completed: "intent_ledger.intent.completed",
    intent_failed: "intent_ledger.intent.failed",
    intent_retry_scheduled: "intent_ledger.intent.retry_scheduled",
    intent_cancelled: "intent_ledger.intent.cancelled",
    intent_marked_ambiguous: "intent_ledger.intent.marked_ambiguous",
    intent_released: "intent_ledger.intent.released",
    claim_heartbeat: "intent_ledger.claim.heartbeat",
    claim_lease_expired: "intent_ledger.claim.lease_expired"
  }

  @doc false
  @spec lifecycle(atom(), atom(), String.t(), map()) :: Jido.Signal.t()
  def lifecycle(event, ledger, subject, data) when is_atom(event) and is_map(data) do
    Jido.Signal.new!(type_for(event), data,
      source: source_for(ledger),
      subject: subject,
      datacontenttype: "application/json"
    )
  end

  @doc false
  @spec type_for(atom()) :: String.t()
  def type_for(event) when is_atom(event) do
    Map.fetch!(@types, event)
  end

  defp source_for(ledger) do
    @source_prefix <> "/" <> (ledger |> inspect() |> String.trim_leading("Elixir."))
  end
end
