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
  Returns the Zoi schema for `t:IntentLedger.Store.Precondition.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs
end
