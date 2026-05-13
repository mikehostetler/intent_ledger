defmodule IntentLedger.Claim do
  @moduledoc """
  Lease token returned to a worker that has claimed an intent.
  """

  alias IntentLedger.ID

  @schema Zoi.struct(__MODULE__, %{
            id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            intent_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            owner_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            token: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            attempt: Zoi.integer() |> Zoi.positive() |> Zoi.default(1) |> Zoi.optional(),
            lease_until: Zoi.datetime() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  @doc """
  Creates a claim for an intent and owner.
  """
  @spec new(String.t(), String.t(), pos_integer(), DateTime.t()) :: t()
  def new(intent_id, owner_id, attempt, %DateTime{} = lease_until) do
    %__MODULE__{
      id: ID.generate("clm"),
      intent_id: intent_id,
      owner_id: to_string(owner_id),
      token: ID.generate("tok"),
      attempt: attempt,
      lease_until: lease_until
    }
  end

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Claim.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  @doc """
  Returns a stable SHA-256 hash for a claim token.
  """
  @spec token_hash(String.t()) :: String.t()
  def token_hash(token) when is_binary(token) do
    :crypto.hash(:sha256, token)
    |> Base.encode16(case: :lower)
  end
end
