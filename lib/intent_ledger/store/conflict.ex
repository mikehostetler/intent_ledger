defmodule IntentLedger.Store.Conflict do
  @moduledoc """
  Explicit store conflict returned when commit preconditions fail.
  """

  @type kind ::
          :stream_version
          | :command_replay
          | :command_conflict
          | :claim_fence
          | :shard_lease
          | :intent_status
          | :outbox

  @kinds [
    :stream_version,
    :command_replay,
    :command_conflict,
    :claim_fence,
    :shard_lease,
    :intent_status,
    :outbox
  ]

  @schema Zoi.struct(__MODULE__, %{
            type: Zoi.enum(@kinds) |> Zoi.default(:stream_version) |> Zoi.optional(),
            key: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            expected: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            actual: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            message: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            metadata: Zoi.map() |> Zoi.default(%{}) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  @doc """
  Returns supported conflict kinds.
  """
  @spec kinds() :: [kind()]
  def kinds, do: @kinds

  @doc """
  Builds a conflict struct.
  """
  @spec new(kind(), keyword() | map()) :: t()
  def new(type, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.put(:type, type)
    |> then(&struct!(__MODULE__, &1))
  end

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Store.Conflict.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs
end
