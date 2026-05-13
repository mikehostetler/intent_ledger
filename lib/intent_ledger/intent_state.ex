defmodule IntentLedger.IntentState do
  @moduledoc """
  Mutable lifecycle state for an `IntentLedger.Intent`.
  """

  alias IntentLedger.Intent

  @type status ::
          :available
          | :claimed
          | :completed
          | :failed
          | :retry_scheduled
          | :cancelled
          | :ambiguous

  @schema Zoi.struct(__MODULE__, %{
            intent_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            queue: Zoi.string() |> Zoi.default("default") |> Zoi.optional(),
            shard: Zoi.integer() |> Zoi.gte(0) |> Zoi.default(0) |> Zoi.optional(),
            status:
              Zoi.enum([:available, :claimed, :completed, :failed, :retry_scheduled, :cancelled, :ambiguous])
              |> Zoi.default(:available)
              |> Zoi.optional(),
            visible_at: Zoi.datetime() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            priority: Zoi.integer() |> Zoi.default(0) |> Zoi.optional(),
            attempt: Zoi.integer() |> Zoi.gte(0) |> Zoi.default(0) |> Zoi.optional(),
            max_attempts: Zoi.integer() |> Zoi.positive() |> Zoi.default(3) |> Zoi.optional(),
            claim_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            claim_token_hash: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            lease_until: Zoi.datetime() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            idempotency_key: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            updated_at: Zoi.datetime() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            completed_at: Zoi.datetime() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            result: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            error: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            cancel_reason: Zoi.any() |> Zoi.default(nil) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  @final_statuses [:completed, :failed, :cancelled]

  @doc """
  Builds the initial lifecycle state for an intent.
  """
  @spec new(Intent.t(), DateTime.t()) :: t()
  def new(%Intent{} = intent, %DateTime{} = now) do
    %__MODULE__{
      intent_id: intent.id,
      queue: intent.queue,
      shard: intent.shard || 0,
      status: :available,
      visible_at: intent.visible_at,
      priority: intent.priority,
      max_attempts: intent.max_attempts,
      idempotency_key: intent.idempotency_key,
      updated_at: now
    }
  end

  @doc """
  Returns true when the state is terminal.
  """
  @spec final?(t()) :: boolean()
  def final?(%__MODULE__{status: status}), do: status in @final_statuses

  @doc """
  Returns the Zoi schema for `t:IntentLedger.IntentState.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema
end
