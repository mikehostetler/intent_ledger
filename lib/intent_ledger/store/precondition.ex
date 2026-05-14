defmodule IntentLedger.Store.Precondition do
  @moduledoc """
  Preconditions that must hold before a store commit can be applied.
  """

  @type kind ::
          :stream_version
          | :command_absent
          | :command_replay
          | :claim_fence
          | :shard_lease
          | :intent_status
          | :outbox_unacked

  @kinds [
    :stream_version,
    :command_absent,
    :command_replay,
    :claim_fence,
    :shard_lease,
    :intent_status,
    :outbox_unacked
  ]

  @schema Zoi.struct(__MODULE__, %{
            type: Zoi.enum(@kinds) |> Zoi.default(:stream_version) |> Zoi.optional(),
            key: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            stream: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            expected: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            metadata: Zoi.map() |> Zoi.default(%{}) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  @derive Jason.Encoder
  defstruct Zoi.Struct.struct_fields(@schema)

  @doc """
  Returns supported precondition kinds.
  """
  @spec kinds() :: [kind()]
  def kinds, do: @kinds

  @doc """
  Builds a precondition struct.
  """
  @spec new(kind(), keyword() | map()) :: t()
  def new(type, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.put(:type, type)
    |> then(&struct!(__MODULE__, &1))
  end

  @doc """
  Requires a stream to still be at the expected version.
  """
  @spec stream_version(String.t(), non_neg_integer()) :: t()
  def stream_version(stream, expected_version) do
    new(:stream_version, stream: stream, expected: expected_version)
  end

  @doc """
  Requires a command id to be absent before writing a new result.
  """
  @spec command_absent(String.t()) :: t()
  def command_absent(command_id) do
    new(:command_absent, key: command_id)
  end

  @doc """
  Requires a command id to replay an existing deterministic result.
  """
  @spec command_replay(String.t()) :: t()
  def command_replay(command_id) do
    new(:command_replay, key: command_id)
  end

  @doc """
  Requires an intent lifecycle state to have one of the expected statuses.

  Claim acquisition uses this precondition to fence the transition from an
  available or retry-scheduled state into a claimed state.
  """
  @spec intent_status(String.t(), atom() | [atom()], keyword() | map()) :: t()
  def intent_status(intent_id, expected_statuses, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.merge(%{key: intent_id, expected: List.wrap(expected_statuses)})
    |> then(&new(:intent_status, &1))
  end

  @doc """
  Requires a claim row to match the expected fencing token hash.

  Heartbeat, complete, fail, and release operations use this precondition to
  reject stale claim owners after release, expiry, or takeover.
  """
  @spec claim_fence(String.t(), String.t(), keyword() | map()) :: t()
  def claim_fence(claim_id, token_hash, attrs \\ %{}) do
    expected = %{status: :claimed, token_hash: token_hash}

    attrs
    |> normalize_attrs()
    |> Map.merge(%{key: claim_id, expected: expected})
    |> then(&new(:claim_fence, &1))
  end

  @doc """
  Requires a queue shard lease to be absent or expired at `now`.

  Shard acquire uses this precondition before writing a new lease owner.
  """
  @spec shard_available(String.t() | atom(), non_neg_integer(), DateTime.t(), keyword() | map()) :: t()
  def shard_available(queue, shard, %DateTime{} = now, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.merge(%{key: shard_key(queue, shard), expected: %{available_at: now}})
    |> then(&new(:shard_lease, &1))
  end

  @doc """
  Requires a queue shard lease to be current and owned by `owner_id`.

  Shard renew and release use this precondition to prevent stale shard owners
  from extending or releasing leases they no longer own.
  """
  @spec shard_lease(String.t() | atom(), non_neg_integer(), String.t(), keyword() | map()) :: t()
  def shard_lease(queue, shard, owner_id, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.merge(%{key: shard_key(queue, shard), expected: %{owner_id: to_string(owner_id), status: :current}})
    |> then(&new(:shard_lease, &1))
  end

  @doc """
  Requires a queue shard lease to be expired at or before `now`.

  Shard expiry and takeover use this precondition to make ownership transfer
  deterministic across competing nodes.
  """
  @spec shard_expired(String.t() | atom(), non_neg_integer(), DateTime.t(), keyword() | map()) :: t()
  def shard_expired(queue, shard, %DateTime{} = now, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.merge(%{key: shard_key(queue, shard), expected: %{expired_at_or_before: now}})
    |> then(&new(:shard_lease, &1))
  end

  @doc """
  Requires an outbox entry to exist and remain unacknowledged.
  """
  @spec outbox_unacked(String.t(), keyword() | map()) :: t()
  def outbox_unacked(entry_id, attrs \\ %{}) do
    attrs
    |> normalize_attrs()
    |> Map.merge(%{key: entry_id, expected: :unacked})
    |> then(&new(:outbox_unacked, &1))
  end

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Store.Precondition.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  defp shard_key(queue, shard), do: "shard:" <> to_string(queue) <> ":" <> to_string(shard)

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs
end
