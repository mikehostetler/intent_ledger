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
  Builds a stream-version conflict.
  """
  @spec stream_version(String.t(), non_neg_integer(), non_neg_integer()) :: t()
  def stream_version(stream, expected, actual) do
    new(:stream_version,
      key: stream,
      expected: expected,
      actual: actual,
      message: "stream version conflict"
    )
  end

  @doc """
  Builds a command replay marker for an existing deterministic result.
  """
  @spec command_replay(String.t(), term()) :: t()
  def command_replay(command_id, result) do
    new(:command_replay,
      key: command_id,
      actual: result,
      message: "command result already recorded"
    )
  end

  @doc """
  Builds a command conflict for a command id reused with different semantics.
  """
  @spec command_conflict(String.t(), term(), term()) :: t()
  def command_conflict(command_id, expected, actual) do
    new(:command_conflict,
      key: command_id,
      expected: expected,
      actual: actual,
      message: "command id reused for a different command"
    )
  end

  @doc """
  Builds an intent-status conflict for conditional lifecycle transitions.
  """
  @spec intent_status(String.t(), atom() | [atom()], atom()) :: t()
  def intent_status(intent_id, expected, actual) do
    new(:intent_status,
      key: intent_id,
      expected: List.wrap(expected),
      actual: actual,
      message: "intent status conflict"
    )
  end

  @doc """
  Builds a claim-fence conflict for stale owners, tokens, or leases.
  """
  @spec claim_fence(String.t(), term(), term()) :: t()
  def claim_fence(claim_id, expected, actual) do
    new(:claim_fence,
      key: claim_id,
      expected: expected,
      actual: actual,
      message: "claim fence conflict"
    )
  end

  @doc """
  Builds a shard-lease conflict for stale, unexpired, or owner-mismatched leases.
  """
  @spec shard_lease(String.t() | atom(), non_neg_integer(), term(), term()) :: t()
  def shard_lease(queue, shard, expected, actual) do
    new(:shard_lease,
      key: shard_key(queue, shard),
      expected: expected,
      actual: actual,
      message: "shard lease conflict"
    )
  end

  @doc """
  Builds an outbox conflict for missing, already acknowledged, or duplicate entries.
  """
  @spec outbox(String.t(), term(), term()) :: t()
  def outbox(entry_id, expected, actual) do
    new(:outbox,
      key: entry_id,
      expected: expected,
      actual: actual,
      message: "outbox conflict"
    )
  end

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Store.Conflict.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  defp shard_key(queue, shard), do: "shard:" <> to_string(queue) <> ":" <> to_string(shard)

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs
end
