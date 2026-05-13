defmodule IntentLedger.Store.Outbox do
  @moduledoc """
  Semantic durable outbox requests for Store V1 adapters.
  """

  @type kind :: :insert | :read | :ack | :replay

  @kinds [:insert, :read, :ack, :replay]

  @schema Zoi.struct(__MODULE__, %{
            type: Zoi.enum(@kinds) |> Zoi.default(:read) |> Zoi.optional(),
            key: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            stream: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            sequence: Zoi.integer() |> Zoi.gte(0) |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            cursor: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            limit: Zoi.integer() |> Zoi.positive() |> Zoi.default(100) |> Zoi.optional(),
            consumer: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            value: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            metadata: Zoi.map() |> Zoi.default(%{}) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  @doc """
  Returns supported outbox request kinds.
  """
  @spec kinds() :: [kind()]
  def kinds, do: @kinds

  @doc """
  Builds an outbox request struct.
  """
  @spec new(kind(), keyword() | map()) :: t()
  def new(type, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> normalize_consumer()
    |> Map.put(:type, type)
    |> then(&struct!(__MODULE__, &1))
  end

  @doc """
  Requests insertion of a durable outbox entry for a stream and signal.
  """
  @spec insert(String.t(), Jido.Signal.t() | map(), keyword() | map()) :: t()
  def insert(stream, signal, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.merge(%{stream: stream, value: signal})
    |> then(&new(:insert, &1))
  end

  @doc """
  Requests unread outbox entries for a consumer, ordered by sequence.
  """
  @spec read(String.t(), keyword() | map()) :: t()
  def read(consumer, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.put(:consumer, to_string(consumer))
    |> then(&new(:read, &1))
  end

  @doc """
  Requests acknowledgement of a delivered outbox entry.
  """
  @spec ack(String.t(), String.t(), keyword() | map()) :: t()
  def ack(entry_id, consumer, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.merge(%{key: entry_id, consumer: to_string(consumer)})
    |> then(&new(:ack, &1))
  end

  @doc """
  Requests replay of durable outbox entries from a cursor without mutating ack state.
  """
  @spec replay(keyword() | map()) :: t()
  def replay(attrs \\ %{}) do
    new(:replay, attrs)
  end

  @doc """
  Converts an outbox struct into the tuple request accepted by `c:IntentLedger.Store.outbox/4`.
  """
  @spec to_request(t()) :: {kind(), map()}
  def to_request(%__MODULE__{} = outbox) do
    attrs =
      outbox
      |> Map.from_struct()
      |> Map.delete(:type)

    {outbox.type, attrs}
  end

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Store.Outbox.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  defp normalize_consumer(%{consumer: nil} = attrs), do: attrs
  defp normalize_consumer(%{consumer: consumer} = attrs), do: %{attrs | consumer: to_string(consumer)}
  defp normalize_consumer(attrs), do: attrs

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs
end
