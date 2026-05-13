defmodule IntentLedger.Store.Listing do
  @moduledoc """
  Semantic listing requests for Store V1 queue indexes.
  """

  @type kind :: :due_intents | :expired_claims

  @kinds [:due_intents, :expired_claims]
  @due_order [:priority_desc, :visible_at_asc, :intent_id_asc]
  @expired_order [:lease_until_asc, :intent_id_asc]

  @schema Zoi.struct(__MODULE__, %{
            type: Zoi.enum(@kinds) |> Zoi.default(:due_intents) |> Zoi.optional(),
            queue: Zoi.string() |> Zoi.default("default") |> Zoi.optional(),
            shard: Zoi.integer() |> Zoi.gte(0) |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            at: Zoi.datetime() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            limit: Zoi.integer() |> Zoi.positive() |> Zoi.default(100) |> Zoi.optional(),
            cursor: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            order: Zoi.array(Zoi.any()) |> Zoi.default([]) |> Zoi.optional(),
            metadata: Zoi.map() |> Zoi.default(%{}) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  @doc """
  Returns supported listing request kinds.
  """
  @spec kinds() :: [kind()]
  def kinds, do: @kinds

  @doc """
  Builds a listing request struct.
  """
  @spec new(kind(), keyword() | map()) :: t()
  def new(type, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> normalize_queue()
    |> Map.put(:type, type)
    |> then(&struct!(__MODULE__, &1))
  end

  @doc """
  Lists intents due for claim in a queue shard at or before `at`.

  Due intent listings include `:available` and `:retry_scheduled` states whose
  `visible_at` is not greater than `at`.
  """
  @spec due_intents(String.t() | atom(), non_neg_integer() | nil, DateTime.t(), keyword() | map()) :: t()
  def due_intents(queue, shard, %DateTime{} = at, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.merge(%{queue: to_string(queue), shard: shard, at: at, order: @due_order})
    |> then(&new(:due_intents, &1))
  end

  @doc """
  Lists claimed intents whose claim lease expired at or before `at`.
  """
  @spec expired_claims(String.t() | atom(), non_neg_integer() | nil, DateTime.t(), keyword() | map()) :: t()
  def expired_claims(queue, shard, %DateTime{} = at, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.merge(%{queue: to_string(queue), shard: shard, at: at, order: @expired_order})
    |> then(&new(:expired_claims, &1))
  end

  @doc """
  Converts a listing struct into the tuple request accepted by `c:IntentLedger.Store.listing/4`.
  """
  @spec to_request(t()) :: {kind(), map()}
  def to_request(%__MODULE__{} = listing) do
    attrs =
      listing
      |> Map.from_struct()
      |> Map.delete(:type)

    {listing.type, attrs}
  end

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Store.Listing.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  defp normalize_queue(%{queue: queue} = attrs), do: %{attrs | queue: to_string(queue)}
  defp normalize_queue(attrs), do: attrs

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs
end
