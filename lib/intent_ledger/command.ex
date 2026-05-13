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

  @type definition :: %{
          required(:operation) => operation(),
          required(:type) => String.t(),
          required(:version) => pos_integer(),
          required(:required) => [field()],
          required(:optional) => [field()]
        }

  @common_metadata_fields [
    :command_id,
    :idempotency_key,
    :actor,
    :causation_id,
    :correlation_id,
    :root_intent_id,
    :parent_intent_id,
    :depth
  ]

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
end
