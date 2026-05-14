defmodule IntentLedger.BedrockStore.Options do
  @moduledoc false

  @spec non_negative_integer(keyword(), atom(), non_neg_integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  @doc false
  def non_negative_integer(opts, key, default) do
    value = Keyword.get(opts, key, default)

    if is_integer(value) and value >= 0 do
      {:ok, value}
    else
      {:error, {:invalid_option, key, value}}
    end
  end

  @spec positive_integer(keyword(), atom(), pos_integer()) :: {:ok, pos_integer()} | {:error, term()}
  @doc false
  def positive_integer(opts, key, default) do
    value = Keyword.get(opts, key, default)

    if is_integer(value) and value > 0 do
      {:ok, value}
    else
      {:error, {:invalid_option, key, value}}
    end
  end
end
