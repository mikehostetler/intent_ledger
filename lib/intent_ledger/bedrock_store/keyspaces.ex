defmodule IntentLedger.BedrockStore.Keyspaces do
  @moduledoc false

  alias Bedrock.Encoding.Tuple, as: TupleEncoding
  alias Bedrock.Keyspace

  @spec root(module()) :: Keyspace.t()
  @doc false
  def root(ledger), do: Keyspace.new("intent_ledger/#{module_key(ledger)}/")

  @spec module_key(module()) :: String.t()
  @doc false
  def module_key(module) do
    module
    |> Module.split()
    |> Enum.join("_")
    |> Macro.underscore()
  end

  @spec intent(Keyspace.t()) :: Keyspace.t()
  @doc false
  def intent(root), do: Keyspace.partition(root, "intents/")

  @spec key_index(Keyspace.t()) :: Keyspace.t()
  @doc false
  def key_index(root), do: Keyspace.partition(root, "keys/")

  @spec stream_version(Keyspace.t()) :: Keyspace.t()
  @doc false
  def stream_version(root), do: Keyspace.partition(root, "stream_versions/")

  @spec outbox_version(Keyspace.t()) :: Keyspace.t()
  @doc false
  def outbox_version(root), do: Keyspace.partition(root, "outbox_versions/")

  @spec outbox(Keyspace.t()) :: Keyspace.t()
  @doc false
  def outbox(root), do: Keyspace.partition(root, "outbox/", key_encoding: TupleEncoding)

  @spec outbox_consumer(Keyspace.t()) :: Keyspace.t()
  @doc false
  def outbox_consumer(root), do: Keyspace.partition(root, "outbox_consumers/", key_encoding: TupleEncoding)

  @spec status_index(Keyspace.t(), atom()) :: Keyspace.t()
  @doc false
  def status_index(root, status), do: Keyspace.partition(root, "intent_status/#{status}/")

  @spec projection_offset(Keyspace.t()) :: Keyspace.t()
  @doc false
  def projection_offset(root), do: Keyspace.partition(root, "projection_offsets/", key_encoding: TupleEncoding)

  @spec stream(Keyspace.t(), String.t()) :: Keyspace.t()
  @doc false
  def stream(root, stream), do: Keyspace.partition(root, "streams/#{stream}/", key_encoding: TupleEncoding)

  @spec range_from_cursor(Keyspace.t(), non_neg_integer()) :: {binary(), binary()}
  @doc false
  def range_from_cursor(keyspace, cursor) when is_integer(cursor) and cursor >= 0 do
    prefix = Keyspace.prefix(keyspace)
    {Keyspace.pack(keyspace, cursor + 1), prefix <> <<0xFF>>}
  end

  @spec encode(term()) :: binary()
  @doc false
  def encode(term), do: :erlang.term_to_binary(term)

  @spec decode(binary()) :: term()
  @doc false
  def decode(binary), do: :erlang.binary_to_term(binary)
end
