defmodule IntentLedger.Command do
  @moduledoc """
  Stable catalogue for mutating intent ledger commands.

  Commands are the external request facts that drive lifecycle commits. Each
  command has a stable `Jido.Signal` type, a schema version, operation-specific
  data fields, and shared metadata fields used for replay and lineage.
  """

  @type operation ::
          :submit
          | :submit_many
          | :claim
          | :heartbeat
          | :complete
          | :fail
          | :release
          | :cancel
          | :requeue
          | :mark_ambiguous
          | :recover

  @type field :: atom()
  @type lineage_field ::
          :actor
          | :causation_id
          | :correlation_id
          | :root_intent_id
          | :parent_intent_id
          | :depth

  @type definition :: %{
          required(:operation) => operation(),
          required(:type) => String.t(),
          required(:version) => pos_integer(),
          required(:required) => [field()],
          required(:optional) => [field()]
        }

  @type normalized :: %{
          required(:operation) => operation(),
          required(:type) => String.t(),
          required(:schema_version) => pos_integer(),
          required(:command_id) => String.t(),
          required(:idempotency_key) => String.t() | nil,
          required(:actor) => String.t() | nil,
          required(:causation_id) => String.t() | nil,
          required(:correlation_id) => String.t() | nil,
          required(:root_intent_id) => String.t() | nil,
          required(:parent_intent_id) => String.t() | nil,
          required(:depth) => non_neg_integer(),
          required(:data) => map()
        }

  @lineage_fields [
    :actor,
    :causation_id,
    :correlation_id,
    :root_intent_id,
    :parent_intent_id,
    :depth
  ]
  @common_metadata_fields [:command_id, :idempotency_key | @lineage_fields]

  @definitions [
    %{
      operation: :submit,
      type: "intent_ledger.command.submit",
      version: 1,
      required: [:intent],
      optional: @common_metadata_fields ++ [:now]
    },
    %{
      operation: :submit_many,
      type: "intent_ledger.command.submit_many",
      version: 1,
      required: [:intents],
      optional: @common_metadata_fields ++ [:now]
    },
    %{
      operation: :claim,
      type: "intent_ledger.command.claim",
      version: 1,
      required: [:queue, :owner_id],
      optional: @common_metadata_fields ++ [:limit, :lease_ms, :now]
    },
    %{
      operation: :heartbeat,
      type: "intent_ledger.command.heartbeat",
      version: 1,
      required: [:claim_id, :token],
      optional: @common_metadata_fields ++ [:lease_ms, :now]
    },
    %{
      operation: :complete,
      type: "intent_ledger.command.complete",
      version: 1,
      required: [:claim_id, :token, :result],
      optional: @common_metadata_fields ++ [:now]
    },
    %{
      operation: :fail,
      type: "intent_ledger.command.fail",
      version: 1,
      required: [:claim_id, :token, :error],
      optional: @common_metadata_fields ++ [:retry_at, :retry_ms, :now]
    },
    %{
      operation: :release,
      type: "intent_ledger.command.release",
      version: 1,
      required: [:claim_id, :token],
      optional: @common_metadata_fields ++ [:now]
    },
    %{
      operation: :cancel,
      type: "intent_ledger.command.cancel",
      version: 1,
      required: [:intent_id, :reason],
      optional: @common_metadata_fields ++ [:now]
    },
    %{
      operation: :requeue,
      type: "intent_ledger.command.requeue",
      version: 1,
      required: [:intent_id],
      optional: @common_metadata_fields ++ [:retry_at, :now]
    },
    %{
      operation: :mark_ambiguous,
      type: "intent_ledger.command.mark_ambiguous",
      version: 1,
      required: [:intent_id, :reason],
      optional: @common_metadata_fields ++ [:now]
    },
    %{
      operation: :recover,
      type: "intent_ledger.command.recover",
      version: 1,
      required: [:queue],
      optional: @common_metadata_fields ++ [:limit, :now]
    }
  ]

  @by_operation Map.new(@definitions, &{&1.operation, &1})
  @by_type Map.new(@definitions, &{&1.type, &1})
  @source_prefix "/intent_ledger"

  @doc """
  Returns the command catalogue in stable operation order.
  """
  @spec all() :: [definition()]
  def all, do: @definitions

  @doc """
  Returns every mutating operation supported by the public API.
  """
  @spec operations() :: [operation()]
  def operations, do: Enum.map(@definitions, & &1.operation)

  @doc """
  Returns shared command metadata fields.
  """
  @spec common_metadata_fields() :: [field()]
  def common_metadata_fields, do: @common_metadata_fields

  @doc """
  Returns command fields that carry lineage and actor context.
  """
  @spec lineage_fields() :: [lineage_field()]
  def lineage_fields, do: @lineage_fields

  @doc """
  Looks up a command definition by operation or signal type.
  """
  @spec fetch(operation() | String.t()) :: {:ok, definition()} | :error
  def fetch(operation) when is_atom(operation), do: Map.fetch(@by_operation, operation)
  def fetch(type) when is_binary(type), do: Map.fetch(@by_type, type)

  @doc """
  Looks up a command definition by operation or signal type, raising when absent.
  """
  @spec fetch!(operation() | String.t()) :: definition()
  def fetch!(operation_or_type) do
    case fetch(operation_or_type) do
      {:ok, definition} -> definition
      :error -> raise ArgumentError, "unknown intent ledger command: #{inspect(operation_or_type)}"
    end
  end

  @doc """
  Returns the stable command signal type for an operation.
  """
  @spec type_for(operation()) :: String.t()
  def type_for(operation), do: fetch!(operation).type

  @doc """
  Returns the command operation for a stable command signal type.
  """
  @spec operation_for_type(String.t()) :: {:ok, operation()} | :error
  def operation_for_type(type) when is_binary(type) do
    case fetch(type) do
      {:ok, definition} -> {:ok, definition.operation}
      :error -> :error
    end
  end

  @doc """
  Parses a `Jido.Signal` command envelope into normalized command data.
  """
  @spec parse(Jido.Signal.t()) :: {:ok, normalized()} | {:error, term()}
  def parse(%Jido.Signal{} = signal), do: normalize(signal)

  @doc """
  Normalizes a command signal.

  The normalized command keeps operation data in atom-keyed form for catalogue
  fields and extracts command id, idempotency, actor, causation, correlation,
  lineage, and depth metadata.
  """
  @spec normalize(Jido.Signal.t()) :: {:ok, normalized()} | {:error, term()}
  def normalize(%Jido.Signal{} = signal) do
    with {:ok, definition} <- definition_for_signal(signal),
         {:ok, data} <- signal_data(signal),
         data <- canonicalize_data(data, definition),
         {:ok, schema_version} <- normalize_schema_version(data, definition),
         :ok <- require_fields(definition, data),
         {:ok, metadata} <- normalize_metadata(data, signal) do
      data =
        data
        |> Map.merge(metadata)
        |> Map.put(:schema_version, schema_version)

      {:ok,
       metadata
       |> Map.merge(%{
         operation: definition.operation,
         type: definition.type,
         schema_version: schema_version,
         data: data
       })}
    end
  end

  def normalize(value), do: {:error, {:invalid_command_signal, value}}

  @doc """
  Builds a `Jido.Signal` command envelope for a catalogue operation.
  """
  @spec new(GenServer.server(), operation(), map() | keyword(), keyword()) :: Jido.Signal.t()
  def new(ledger, operation, data \\ %{}, opts \\ []) when is_list(opts) do
    definition = fetch!(operation)

    payload =
      data
      |> normalize_payload()
      |> Map.put(:schema_version, definition.version)

    signal_attrs =
      opts
      |> Keyword.get(:signal_attrs, [])
      |> Keyword.put_new(:source, source_for(ledger))
      |> Keyword.put_new(:subject, subject_for(ledger, payload))
      |> Keyword.put_new(:datacontenttype, "application/json")
      |> Keyword.put_new(:dataschema, dataschema_for(definition))
      |> maybe_put_signal_id(payload)

    Jido.Signal.new!(definition.type, payload, signal_attrs)
  end

  @doc """
  Builds a submit command signal.
  """
  @spec submit(GenServer.server(), IntentLedger.Intent.t() | map() | keyword(), keyword()) :: Jido.Signal.t()
  def submit(ledger, intent, opts \\ []) do
    new(ledger, :submit, command_data(%{intent: intent}, opts), opts)
  end

  @doc """
  Builds a submit-many command signal.
  """
  @spec submit_many(GenServer.server(), [IntentLedger.Intent.t() | map() | keyword()], keyword()) ::
          Jido.Signal.t()
  def submit_many(ledger, intents, opts \\ []) do
    new(ledger, :submit_many, command_data(%{intents: intents}, opts), opts)
  end

  @doc """
  Builds a claim command signal.
  """
  @spec claim(GenServer.server(), String.t() | atom(), String.t(), keyword()) :: Jido.Signal.t()
  def claim(ledger, queue, owner_id, opts \\ []) do
    new(ledger, :claim, command_data(%{queue: queue, owner_id: owner_id}, opts), opts)
  end

  @doc """
  Builds a heartbeat command signal.
  """
  @spec heartbeat(GenServer.server(), String.t(), String.t(), keyword()) :: Jido.Signal.t()
  def heartbeat(ledger, claim_id, token, opts \\ []) do
    new(ledger, :heartbeat, command_data(%{claim_id: claim_id, token: token}, opts), opts)
  end

  @doc """
  Builds a complete command signal.
  """
  @spec complete(GenServer.server(), String.t(), String.t(), term(), keyword()) :: Jido.Signal.t()
  def complete(ledger, claim_id, token, result, opts \\ []) do
    new(ledger, :complete, command_data(%{claim_id: claim_id, token: token, result: result}, opts), opts)
  end

  @doc """
  Builds a fail command signal.
  """
  @spec fail(GenServer.server(), String.t(), String.t(), term(), keyword()) :: Jido.Signal.t()
  def fail(ledger, claim_id, token, error, opts \\ []) do
    new(ledger, :fail, command_data(%{claim_id: claim_id, token: token, error: error}, opts), opts)
  end

  @doc """
  Builds a release command signal.
  """
  @spec release(GenServer.server(), String.t(), String.t(), keyword()) :: Jido.Signal.t()
  def release(ledger, claim_id, token, opts \\ []) do
    new(ledger, :release, command_data(%{claim_id: claim_id, token: token}, opts), opts)
  end

  @doc """
  Builds a cancel command signal.
  """
  @spec cancel(GenServer.server(), String.t(), term(), keyword()) :: Jido.Signal.t()
  def cancel(ledger, intent_id, reason, opts \\ []) do
    new(ledger, :cancel, command_data(%{intent_id: intent_id, reason: reason}, opts), opts)
  end

  @doc """
  Builds a requeue command signal.
  """
  @spec requeue(GenServer.server(), String.t(), keyword()) :: Jido.Signal.t()
  def requeue(ledger, intent_id, opts \\ []) do
    new(ledger, :requeue, command_data(%{intent_id: intent_id}, opts), opts)
  end

  @doc """
  Builds a mark-ambiguous command signal.
  """
  @spec mark_ambiguous(GenServer.server(), String.t(), term(), keyword()) :: Jido.Signal.t()
  def mark_ambiguous(ledger, intent_id, reason, opts \\ []) do
    new(ledger, :mark_ambiguous, command_data(%{intent_id: intent_id, reason: reason}, opts), opts)
  end

  @doc """
  Builds a recover command signal.
  """
  @spec recover(GenServer.server(), String.t() | atom(), keyword()) :: Jido.Signal.t()
  def recover(ledger, queue, opts \\ []) do
    new(ledger, :recover, command_data(%{queue: queue}, opts), opts)
  end

  defp command_data(required, opts) do
    opts
    |> Keyword.drop([:signal_attrs, :timeout])
    |> Map.new()
    |> Map.merge(required)
  end

  defp normalize_payload(data) when is_list(data) do
    data
    |> Map.new()
    |> normalize_payload()
  end

  defp normalize_payload(data) when is_map(data) do
    Map.new(data, fn {key, value} -> {key, normalize_value(value)} end)
  end

  defp normalize_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp normalize_value(%IntentLedger.Intent{} = value), do: value |> Map.from_struct() |> normalize_value()

  defp normalize_value(values) when is_list(values), do: Enum.map(values, &normalize_value/1)

  defp normalize_value(%{} = value) when not is_struct(value) do
    Map.new(value, fn {key, nested_value} -> {key, normalize_value(nested_value)} end)
  end

  defp normalize_value(value), do: value

  defp definition_for_signal(%Jido.Signal{type: type}) do
    case fetch(type) do
      {:ok, definition} -> {:ok, definition}
      :error -> {:error, {:unknown_command_type, type}}
    end
  end

  defp signal_data(%Jido.Signal{data: data}) when is_map(data) or is_list(data) do
    {:ok, normalize_payload(data)}
  end

  defp signal_data(%Jido.Signal{data: data}), do: {:error, {:invalid_command_data, data}}

  defp canonicalize_data(data, definition) do
    known_fields = MapSet.new([:schema_version | definition.required ++ definition.optional])

    Map.new(data, fn {key, value} ->
      {canonical_key(key, known_fields), value}
    end)
  end

  defp canonical_key(key, known_fields) when is_binary(key) do
    Enum.find(known_fields, key, &(Atom.to_string(&1) == key))
  end

  defp canonical_key(key, _known_fields), do: key

  defp normalize_schema_version(data, definition) do
    with {:ok, raw_version} <- fetch_required_field(data, :schema_version),
         {:ok, version} <- normalize_positive_integer(raw_version, :schema_version) do
      if version == definition.version do
        {:ok, version}
      else
        {:error, {:unsupported_command_version, definition.operation, version}}
      end
    end
  end

  defp require_fields(definition, data) do
    case Enum.find(definition.required, &(field(data, &1) == nil)) do
      nil -> :ok
      field -> {:error, {:required, field}}
    end
  end

  defp fetch_required_field(data, key) do
    case field(data, key) do
      nil -> {:error, {:required, key}}
      value -> {:ok, value}
    end
  end

  defp normalize_metadata(data, %Jido.Signal{} = signal) do
    with {:ok, command_id} <- normalize_command_id(field(data, :command_id), signal.id),
         {:ok, idempotency_key} <- normalize_optional_string(field(data, :idempotency_key), :idempotency_key),
         {:ok, actor} <- normalize_optional_string(field(data, :actor), :actor),
         {:ok, causation_id} <- normalize_optional_string(field(data, :causation_id), :causation_id),
         {:ok, correlation_id} <- normalize_optional_string(field(data, :correlation_id), :correlation_id),
         {:ok, root_intent_id} <- normalize_optional_string(field(data, :root_intent_id), :root_intent_id),
         {:ok, parent_intent_id} <- normalize_optional_string(field(data, :parent_intent_id), :parent_intent_id),
         {:ok, depth} <- normalize_non_negative_integer(field(data, :depth), :depth) do
      {:ok,
       %{
         command_id: command_id,
         idempotency_key: idempotency_key,
         actor: actor,
         causation_id: causation_id,
         correlation_id: correlation_id,
         root_intent_id: root_intent_id,
         parent_intent_id: parent_intent_id,
         depth: depth
       }}
    end
  end

  defp normalize_command_id(nil, signal_id), do: normalize_required_string(signal_id, :command_id)

  defp normalize_command_id(command_id, signal_id) do
    with {:ok, command_id} <- normalize_required_string(command_id, :command_id),
         {:ok, signal_id} <- normalize_required_string(signal_id, :command_id) do
      if command_id == signal_id do
        {:ok, command_id}
      else
        {:error, {:command_id_mismatch, signal_id, command_id}}
      end
    end
  end

  defp normalize_optional_string(nil, _field), do: {:ok, nil}

  defp normalize_optional_string(value, field) do
    case value |> to_string() |> String.trim() do
      "" -> {:error, {:invalid_string, field, value}}
      normalized -> {:ok, normalized}
    end
  end

  defp normalize_required_string(value, field) do
    case normalize_optional_string(value, field) do
      {:ok, nil} -> {:error, {:required, field}}
      result -> result
    end
  end

  defp normalize_non_negative_integer(nil, _field), do: {:ok, 0}

  defp normalize_non_negative_integer(value, field) do
    with {:ok, integer} <- normalize_integer(value, field) do
      if integer >= 0 do
        {:ok, integer}
      else
        {:error, {:invalid_non_negative_integer, field, value}}
      end
    end
  end

  defp normalize_positive_integer(value, field) do
    with {:ok, integer} <- normalize_integer(value, field) do
      if integer > 0 do
        {:ok, integer}
      else
        {:error, {:invalid_positive_integer, field, value}}
      end
    end
  end

  defp normalize_integer(value, _field) when is_integer(value), do: {:ok, value}

  defp normalize_integer(value, field) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> {:ok, integer}
      _invalid -> {:error, {:invalid_integer, field, value}}
    end
  end

  defp normalize_integer(value, field), do: {:error, {:invalid_integer, field, value}}

  defp maybe_put_signal_id(attrs, payload) do
    case field(payload, :command_id) do
      nil -> attrs
      command_id -> Keyword.put_new(attrs, :id, to_string(command_id))
    end
  end

  defp dataschema_for(definition) do
    "https://hexdocs.pm/intent_ledger/commands/#{definition.operation}/v#{definition.version}.json"
  end

  defp subject_for(ledger, payload) do
    cond do
      field(payload, :intent_id) ->
        "intent:" <> to_string(field(payload, :intent_id))

      field(payload, :claim_id) ->
        "claim:" <> to_string(field(payload, :claim_id))

      intent_id = intent_id(payload) ->
        "intent:" <> to_string(intent_id)

      field(payload, :queue) ->
        "queue:" <> to_string(field(payload, :queue))

      true ->
        "ledger:" <> ledger_name(ledger)
    end
  end

  defp intent_id(payload) do
    case field(payload, :intent) do
      nil -> nil
      intent -> field(intent, :id)
    end
  end

  defp field(map, key) when is_map(map) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp field(_value, _key), do: nil

  defp source_for(ledger), do: @source_prefix <> "/" <> ledger_name(ledger)

  defp ledger_name(ledger) do
    ledger
    |> inspect()
    |> String.trim_leading("Elixir.")
  end
end
