defmodule IntentLedger.Context do
  @moduledoc """
  Execution context passed to an Intent handler.
  """

  alias IntentLedger.Intent

  @schema Zoi.struct(__MODULE__, %{
            ledger: Zoi.atom(),
            intent: Zoi.struct(Intent),
            topic: Zoi.string(),
            queue: Zoi.string(),
            attempt: Zoi.integer() |> Zoi.positive(),
            job_meta: Zoi.map() |> Zoi.default(%{}) |> Zoi.optional(),
            metadata: Zoi.map() |> Zoi.default(%{}) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  @doc false
  @spec new(module(), Intent.t(), map()) :: t()
  def new(ledger, %Intent{} = intent, job_meta) when is_atom(ledger) and is_map(job_meta) do
    %__MODULE__{
      ledger: ledger,
      intent: intent,
      topic: intent.topic,
      queue: intent.queue,
      attempt: Map.get(job_meta, :attempt, intent.attempt + 1),
      job_meta: job_meta,
      metadata: intent.metadata
    }
  end

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Context.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema
end
