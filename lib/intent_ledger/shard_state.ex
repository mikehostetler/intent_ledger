defmodule IntentLedger.ShardState do
  @moduledoc """
  Cursor and lease metadata for a queue shard.
  """

  @schema Zoi.struct(__MODULE__, %{
            queue: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            shard: Zoi.integer() |> Zoi.gte(0) |> Zoi.default(0) |> Zoi.optional(),
            cursor: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            lease_owner: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            lease_until: Zoi.datetime() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            updated_at: Zoi.datetime() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  @derive Jason.Encoder
  defstruct Zoi.Struct.struct_fields(@schema)

  @doc """
  Returns the Zoi schema for `t:IntentLedger.ShardState.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema
end
