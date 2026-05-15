defmodule IntentLedger.BedrockStore.Codec do
  @moduledoc false

  @spec encode(term()) :: binary()
  @doc false
  def encode(term), do: :erlang.term_to_binary(term)

  @spec decode(binary()) :: term()
  @doc false
  def decode(binary), do: :erlang.binary_to_term(binary)
end
