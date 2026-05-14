defmodule IntentLedger.Signal do
  @moduledoc false

  alias IntentLedger.Intent

  @type event ::
          :enqueued
          | :started
          | :completed
          | :failed
          | :retry_scheduled
          | :discarded
          | :canceled
          | :ambiguous

  @definitions %{
    enqueued: "intent.enqueued",
    started: "intent.started",
    completed: "intent.completed",
    failed: "intent.failed",
    retry_scheduled: "intent.retry_scheduled",
    discarded: "intent.discarded",
    canceled: "intent.canceled",
    ambiguous: "intent.ambiguous"
  }

  @doc false
  @spec events() :: [event()]
  def events, do: Map.keys(@definitions)

  @doc false
  @spec type_for(event()) :: String.t()
  def type_for(event), do: Map.fetch!(@definitions, event)

  @doc false
  @spec lifecycle(event(), module(), Intent.t(), map()) :: Jido.Signal.t()
  def lifecycle(event, ledger, %Intent{} = intent, data) when is_atom(event) and is_map(data) do
    Jido.Signal.new!(type_for(event), normalize(data),
      source: source_for(ledger),
      subject: intent.id,
      datacontenttype: "application/x-erlang-term",
      dataschema: "https://hexdocs.pm/intent_ledger/lifecycle/#{event}/v1",
      extensions: %{
        intent_id: intent.id,
        topic: intent.topic,
        queue: intent.queue,
        status: intent.status,
        correlation_id: intent.correlation_id,
        causation_id: intent.causation_id,
        root_intent_id: intent.root_intent_id,
        parent_intent_id: intent.parent_intent_id,
        depth: intent.depth,
        actor: intent.actor
      }
    )
  end

  defp normalize(data) do
    Map.new(data, fn {key, value} -> {key, normalize_value(value)} end)
  end

  defp normalize_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp normalize_value(%{} = value), do: Map.new(value, fn {key, nested} -> {key, normalize_value(nested)} end)
  defp normalize_value(values) when is_list(values), do: Enum.map(values, &normalize_value/1)
  defp normalize_value(value), do: value

  defp source_for(ledger) do
    "/intent_ledger/" <> (ledger |> Module.split() |> Enum.join("."))
  end
end
