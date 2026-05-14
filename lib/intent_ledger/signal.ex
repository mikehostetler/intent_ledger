defmodule IntentLedger.Signal do
  @moduledoc false

  @type event ::
          :intent_submitted
          | :intent_available
          | :intent_claimed
          | :intent_completed
          | :intent_failed
          | :intent_retry_scheduled
          | :intent_cancelled
          | :intent_marked_ambiguous
          | :intent_released
          | :claim_heartbeat
          | :claim_lease_expired

  @type field :: atom()

  @type definition :: %{
          required(:event) => event(),
          required(:type) => String.t(),
          required(:version) => pos_integer(),
          required(:required) => [field()],
          required(:optional) => [field()]
        }

  @source_prefix "/intent_ledger"
  @lineage_fields [
    :correlation_id,
    :causation_id,
    :root_intent_id,
    :parent_intent_id,
    :depth,
    :actor
  ]

  @definitions [
    %{
      event: :intent_submitted,
      type: "intent_ledger.intent.submitted",
      version: 1,
      required: [
        :intent_id,
        :key,
        :kind,
        :queue,
        :shard,
        :visible_at,
        :max_attempts,
        :ambiguity_policy
      ],
      optional: [:idempotency_key | @lineage_fields]
    },
    %{
      event: :intent_available,
      type: "intent_ledger.intent.available",
      version: 1,
      required: [:visible_at],
      optional: @lineage_fields
    },
    %{
      event: :intent_claimed,
      type: "intent_ledger.intent.claimed",
      version: 1,
      required: [:claim_id, :owner_id, :attempt, :lease_until],
      optional: @lineage_fields
    },
    %{
      event: :intent_completed,
      type: "intent_ledger.intent.completed",
      version: 1,
      required: [:claim_id, :result],
      optional: @lineage_fields
    },
    %{
      event: :intent_failed,
      type: "intent_ledger.intent.failed",
      version: 1,
      required: [:claim_id, :error, :attempt],
      optional: @lineage_fields
    },
    %{
      event: :intent_retry_scheduled,
      type: "intent_ledger.intent.retry_scheduled",
      version: 1,
      required: [:retry_at],
      optional: [:attempt | @lineage_fields]
    },
    %{
      event: :intent_cancelled,
      type: "intent_ledger.intent.cancelled",
      version: 1,
      required: [:reason],
      optional: @lineage_fields
    },
    %{
      event: :intent_marked_ambiguous,
      type: "intent_ledger.intent.marked_ambiguous",
      version: 1,
      required: [:reason],
      optional: [:error | @lineage_fields]
    },
    %{
      event: :intent_released,
      type: "intent_ledger.intent.released",
      version: 1,
      required: [:claim_id],
      optional: @lineage_fields
    },
    %{
      event: :claim_heartbeat,
      type: "intent_ledger.claim.heartbeat",
      version: 1,
      required: [:claim_id, :lease_until],
      optional: @lineage_fields
    },
    %{
      event: :claim_lease_expired,
      type: "intent_ledger.claim.lease_expired",
      version: 1,
      required: [:claim_id, :lease_until],
      optional: @lineage_fields
    }
  ]

  @by_event Map.new(@definitions, &{&1.event, &1})
  @by_type Map.new(@definitions, &{&1.type, &1})

  @doc false
  @spec all() :: [definition()]
  def all, do: @definitions

  @doc false
  @spec events() :: [event()]
  def events, do: Enum.map(@definitions, & &1.event)

  @doc false
  @spec lineage_fields() :: [field()]
  def lineage_fields, do: @lineage_fields

  @doc false
  @spec fetch(event() | String.t()) :: {:ok, definition()} | :error
  def fetch(event) when is_atom(event), do: Map.fetch(@by_event, event)
  def fetch(type) when is_binary(type), do: Map.fetch(@by_type, type)

  @doc false
  @spec fetch!(event() | String.t()) :: definition()
  def fetch!(event_or_type) do
    case fetch(event_or_type) do
      {:ok, definition} -> definition
      :error -> raise ArgumentError, "unknown intent ledger lifecycle signal: #{inspect(event_or_type)}"
    end
  end

  @doc false
  @spec lifecycle(event(), GenServer.server(), String.t(), map()) :: Jido.Signal.t()
  def lifecycle(event, ledger, subject, data) when is_atom(event) and is_map(data) do
    definition = fetch!(event)

    payload =
      data
      |> normalize_data()
      |> Map.put(:schema_version, definition.version)

    require_fields!(definition, payload)

    Jido.Signal.new!(definition.type, payload,
      source: source_for(ledger),
      subject: subject,
      datacontenttype: "application/json",
      dataschema: dataschema_for(definition)
    )
  end

  @doc false
  @spec type_for(event()) :: String.t()
  def type_for(event) when is_atom(event), do: fetch!(event).type

  defp require_fields!(definition, data) do
    case Enum.find(definition.required, &(field(data, &1) == nil)) do
      nil -> :ok
      field -> raise ArgumentError, "missing lifecycle signal field #{inspect(field)} for #{definition.event}"
    end
  end

  defp normalize_data(data) do
    Map.new(data, fn {key, value} -> {key, normalize_value(value)} end)
  end

  defp normalize_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp normalize_value(values) when is_list(values), do: Enum.map(values, &normalize_value/1)

  defp normalize_value(%{} = value) when not is_struct(value) do
    Map.new(value, fn {key, nested_value} -> {key, normalize_value(nested_value)} end)
  end

  defp normalize_value(value), do: value

  defp field(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp dataschema_for(definition) do
    "https://hexdocs.pm/intent_ledger/lifecycle/#{definition.event}/v#{definition.version}.json"
  end

  defp source_for(ledger) do
    @source_prefix <> "/" <> (ledger |> inspect() |> String.trim_leading("Elixir."))
  end
end
