defmodule IntentLedger.ReplayEntry do
  @moduledoc """
  Replay metadata for one durable lifecycle signal.

  `replay/2` keeps the ergonomic API and returns bare `Jido.Signal` structs.
  `replay_entries/2` returns this richer shape for repair tools, projection
  catch-up, and forensic inspection that need stream cursor metadata.
  """

  @schema Zoi.struct(__MODULE__, %{
            stream: Zoi.string(),
            cursor: Zoi.integer() |> Zoi.positive(),
            signal: Zoi.any(),
            recorded_at: Zoi.datetime() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  @doc """
  Builds a replay entry from persisted stream data.
  """
  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, term()}
  def new(%__MODULE__{} = entry), do: {:ok, entry}
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    attrs = normalize_keys(attrs)

    %__MODULE__{
      stream: Map.get(attrs, :stream),
      cursor: Map.get(attrs, :cursor),
      signal: Map.get(attrs, :signal),
      recorded_at: Map.get(attrs, :recorded_at)
    }
    |> then(&Zoi.parse(@schema, &1))
    |> case do
      {:ok, entry} -> {:ok, entry}
      {:error, errors} -> {:error, {:invalid_replay_entry, errors}}
    end
  end

  @doc """
  Returns the Zoi schema for `t:IntentLedger.ReplayEntry.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  defp normalize_keys(attrs) do
    Map.new(attrs, fn
      {key, value} when is_binary(key) -> {normalize_key(key), value}
      pair -> pair
    end)
  end

  defp normalize_key("stream"), do: :stream
  defp normalize_key("cursor"), do: :cursor
  defp normalize_key("signal"), do: :signal
  defp normalize_key("recorded_at"), do: :recorded_at
  defp normalize_key(key), do: key
end
