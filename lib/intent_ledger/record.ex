defmodule IntentLedger.Record do
  @moduledoc """
  Current materialized view of an intent and its lifecycle state.
  """

  alias IntentLedger.{Intent, IntentState}

  @schema Zoi.struct(__MODULE__, %{
            intent: Zoi.struct(Intent) |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            state: Zoi.struct(IntentState) |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Record.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema
end
