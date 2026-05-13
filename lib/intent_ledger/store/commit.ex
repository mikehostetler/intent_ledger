defmodule IntentLedger.Store.Commit do
  @moduledoc """
  Result of applying a Store V1 commit request.
  """

  alias IntentLedger.Store.Write

  @schema Zoi.struct(__MODULE__, %{
            command_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            result: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            replayed: Zoi.boolean() |> Zoi.default(false) |> Zoi.optional(),
            replay_of: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            signals: Zoi.array(Zoi.any()) |> Zoi.default([]) |> Zoi.optional(),
            writes: Zoi.array(Zoi.struct(Write)) |> Zoi.default([]) |> Zoi.optional(),
            metadata: Zoi.map() |> Zoi.default(%{}) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  @doc """
  Builds a commit result.
  """
  @spec new(keyword() | map()) :: t()
  def new(attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> then(&struct!(__MODULE__, &1))
  end

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Store.Commit.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs
end
