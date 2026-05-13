defmodule IntentLedger.ID do
  @moduledoc false

  @doc false
  @spec generate(String.t()) :: String.t()
  def generate(prefix) when is_binary(prefix) do
    suffix =
      12
      |> :crypto.strong_rand_bytes()
      |> Base.url_encode64(padding: false)

    prefix <> "_" <> suffix
  end
end
