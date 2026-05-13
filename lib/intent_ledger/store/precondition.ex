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
  Returns the Zoi schema for `t:IntentLedger.Store.Precondition.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs
end
