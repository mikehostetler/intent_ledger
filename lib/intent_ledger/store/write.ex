defmodule IntentLedger.Store.Write do
  @moduledoc """
  Store writes that make up an atomic semantic commit.
  """

  @type kind ::
          :put_intent
          | :put_state
          | :append_signal
          | :put_idempotency
          | :put_claim
          | :delete_claim
          | :put_shard_lease
          | :delete_shard_lease
          | :put_outbox
          | :ack_outbox

  @kinds [
    :put_intent,
    :put_state,
    :append_signal,
    :put_idempotency,
    :put_claim,
    :delete_claim,
    :put_shard_lease,
    :delete_shard_lease,
    :put_outbox,
    :ack_outbox
  ]

  @schema Zoi.struct(__MODULE__, %{
            type: Zoi.enum(@kinds) |> Zoi.default(:append_signal) |> Zoi.optional(),
            key: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            stream: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            value: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            metadata: Zoi.map() |> Zoi.default(%{}) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  @doc """
  Returns supported write kinds.
  """
  @spec kinds() :: [kind()]
  def kinds, do: @kinds

  @doc """
  Builds a write struct.
  """
  @spec new(kind(), keyword() | map()) :: t()
  def new(type, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.put(:type, type)
    |> then(&struct!(__MODULE__, &1))
  end

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Store.Write.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs
end
