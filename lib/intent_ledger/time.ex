defmodule IntentLedger.Time do
  @moduledoc false

  @doc false
  @spec utc_now() :: DateTime.t()
  def utc_now do
    DateTime.utc_now()
  end

  @doc false
  @spec normalize(DateTime.t() | String.t() | nil, DateTime.t() | nil) ::
          {:ok, DateTime.t()} | {:error, term()}
  def normalize(nil, nil), do: {:ok, utc_now()}
  def normalize(nil, %DateTime{} = default), do: {:ok, default}
  def normalize(%DateTime{} = datetime, _default), do: {:ok, datetime}

  def normalize(value, _default) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, reason} -> {:error, {:invalid_datetime, value, reason}}
    end
  end

  def normalize(value, _default), do: {:error, {:invalid_datetime, value}}

  @doc false
  @spec add_ms(DateTime.t(), integer()) :: DateTime.t()
  def add_ms(%DateTime{} = datetime, ms) when is_integer(ms) do
    DateTime.add(datetime, ms, :millisecond)
  end
end
