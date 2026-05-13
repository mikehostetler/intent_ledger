defmodule IntentLedger.Store.CommitRequest do
  @moduledoc """
  Semantic request for an atomic Store V1 commit.
  """

  alias IntentLedger.Store.{Precondition, Write}

  @schema Zoi.struct(__MODULE__, %{
            ledger: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            command_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            operation: Zoi.atom() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            command: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            preconditions: Zoi.array(Zoi.struct(Precondition)) |> Zoi.default([]) |> Zoi.optional(),
            writes: Zoi.array(Zoi.struct(Write)) |> Zoi.default([]) |> Zoi.optional(),
            metadata: Zoi.map() |> Zoi.default(%{}) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  @doc """
  Builds a commit request.
  """
  @spec new(keyword() | map()) :: t()
  def new(attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> then(&struct!(__MODULE__, &1))
  end

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Store.CommitRequest.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs
end
