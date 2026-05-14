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
  @derive Jason.Encoder
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
  Appends a lifecycle signal to a versioned stream.
  """
  @spec append_signal(String.t(), Jido.Signal.t() | map(), keyword() | map()) :: t()
  def append_signal(stream, signal, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.merge(%{stream: stream, value: signal})
    |> then(&new(:append_signal, &1))
  end

  @doc """
  Writes a deterministic command result for idempotent replay.
  """
  @spec put_idempotency(String.t(), term(), keyword() | map()) :: t()
  def put_idempotency(command_id, result, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.merge(%{key: command_id, value: result})
    |> then(&new(:put_idempotency, &1))
  end

  @doc """
  Writes or replaces a durable claim fence.
  """
  @spec put_claim(String.t(), map(), keyword() | map()) :: t()
  def put_claim(claim_id, claim_info, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.merge(%{key: claim_id, value: claim_info})
    |> then(&new(:put_claim, &1))
  end

  @doc """
  Deletes a durable claim fence after complete, fail, release, or expiry.
  """
  @spec delete_claim(String.t(), keyword() | map()) :: t()
  def delete_claim(claim_id, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.put(:key, claim_id)
    |> then(&new(:delete_claim, &1))
  end

  @doc """
  Writes or replaces a durable queue shard lease.
  """
  @spec put_shard_lease(String.t() | atom(), non_neg_integer(), map(), keyword() | map()) :: t()
  def put_shard_lease(queue, shard, lease, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.merge(%{key: shard_key(queue, shard), value: lease})
    |> then(&new(:put_shard_lease, &1))
  end

  @doc """
  Deletes a durable queue shard lease after release or expiry.
  """
  @spec delete_shard_lease(String.t() | atom(), non_neg_integer(), keyword() | map()) :: t()
  def delete_shard_lease(queue, shard, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.put(:key, shard_key(queue, shard))
    |> then(&new(:delete_shard_lease, &1))
  end

  @doc """
  Inserts a durable outbox entry.
  """
  @spec put_outbox(String.t(), map(), keyword() | map()) :: t()
  def put_outbox(entry_id, entry, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.merge(%{key: entry_id, value: entry})
    |> then(&new(:put_outbox, &1))
  end

  @doc """
  Acknowledges a durable outbox entry.
  """
  @spec ack_outbox(String.t(), keyword() | map()) :: t()
  def ack_outbox(entry_id, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.put(:key, entry_id)
    |> then(&new(:ack_outbox, &1))
  end

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Store.Write.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  defp shard_key(queue, shard), do: "shard:" <> to_string(queue) <> ":" <> to_string(shard)

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs
end
