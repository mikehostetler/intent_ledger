defmodule IntentLedger.Store.Bedrock.Keyspace do
  @moduledoc """
  Versioned Bedrock key layout for `IntentLedger.Store.Bedrock`.

  The adapter stores all data under a schema-versioned ledger prefix:

      intent_ledger/<schema_version>/<ledger>/<table>/...

  Bedrock tuple encoding is used for dynamic components so range scans preserve
  numeric ordering for stream versions, outbox sequences, shards, and queue
  visibility indexes. Table tags are stable strings because Bedrock tuple
  encoding does not support atoms.

  ## Key Families

  - `intent/<intent_id>` stores immutable intent data.
  - `state/<intent_id>` stores materialized lifecycle state.
  - `command/<command_id>` stores deterministic command replay results.
  - `stream/<stream_id>/<version>` stores lifecycle signal entries.
  - `queue/<queue>/<shard>/<visible_at>/<priority>/<intent_id>` stores due-intent indexes.
  - `claim/<claim_id>` stores claim fencing rows.
  - `shard/<queue>/<shard>` stores durable shard lease rows.
  - `outbox/<sequence>` stores durable outbox entries.
  - `projection/<name>` stores projection offsets.
  """

  alias IntentLedger.Error
  alias IntentLedger.Store.Bedrock, as: BedrockStore

  @schema_version 1
  @namespace "intent_ledger/"
  @tables ~w(intent state command stream queue claim shard outbox projection)
  @table_tags Map.new(@tables, &{String.to_atom(&1), &1})
  @bedrock_keyspace Module.concat(Bedrock, Keyspace)
  @bedrock_key_range Module.concat(Bedrock, KeyRange)
  @bedrock_tuple Module.concat([Bedrock, Encoding, Tuple])

  @type key :: binary()
  @type key_range :: {binary(), binary()}
  @type keyspace :: struct()
  @type table :: :intent | :state | :command | :stream | :queue | :claim | :shard | :outbox | :projection

  @doc """
  Returns the key schema version embedded in every ledger prefix.
  """
  @spec schema_version() :: pos_integer()
  def schema_version, do: @schema_version

  @doc """
  Returns the Bedrock keyspace prefix for a ledger.
  """
  @spec ledger(atom() | String.t()) :: keyspace()
  def ledger(ledger) do
    root()
    |> add(@schema_version)
    |> add(normalize_name!(ledger, :ledger))
  end

  @doc """
  Returns the keyspace prefix for a ledger table.
  """
  @spec table(atom() | String.t(), table()) :: keyspace()
  def table(ledger, table), do: ledger |> ledger() |> add(table_tag!(table))

  @doc """
  Returns the key range for every key in a ledger.
  """
  @spec ledger_range(atom() | String.t()) :: key_range()
  def ledger_range(ledger), do: range(ledger(ledger))

  @doc """
  Returns the key range for a table in a ledger.
  """
  @spec table_range(atom() | String.t(), table()) :: key_range()
  def table_range(ledger, table), do: ledger |> table(table) |> range()

  @doc """
  Encodes an immutable intent key.
  """
  @spec intent(atom() | String.t(), String.t()) :: key()
  def intent(ledger, intent_id), do: ledger |> table(:intent) |> add_id(intent_id, :intent_id) |> prefix()

  @doc """
  Encodes a materialized intent state key.
  """
  @spec state(atom() | String.t(), String.t()) :: key()
  def state(ledger, intent_id), do: ledger |> table(:state) |> add_id(intent_id, :intent_id) |> prefix()

  @doc """
  Encodes a command replay/idempotency key.
  """
  @spec command(atom() | String.t(), String.t()) :: key()
  def command(ledger, command_id), do: ledger |> table(:command) |> add_id(command_id, :command_id) |> prefix()

  @doc """
  Encodes a lifecycle stream entry key.
  """
  @spec stream(atom() | String.t(), String.t(), non_neg_integer()) :: key()
  def stream(ledger, stream_id, version) do
    ledger
    |> stream_keyspace(stream_id)
    |> add_non_neg_integer!(version, :version)
    |> prefix()
  end

  @doc """
  Returns the key range for a single lifecycle stream.
  """
  @spec stream_range(atom() | String.t(), String.t()) :: key_range()
  def stream_range(ledger, stream_id), do: ledger |> stream_keyspace(stream_id) |> range()

  @doc """
  Encodes a due-intent queue index key.

  The priority component is inverted so a normal ascending range scan returns
  higher-priority intents before lower-priority ones.
  """
  @spec queue(atom() | String.t(), String.t() | atom(), non_neg_integer(), DateTime.t(), integer(), String.t()) :: key()
  def queue(ledger, queue, shard, visible_at, priority, intent_id) do
    ledger
    |> queue_keyspace(queue, shard)
    |> add_datetime!(visible_at, :visible_at)
    |> add_integer!(-priority, :priority)
    |> add_id(intent_id, :intent_id)
    |> prefix()
  end

  @doc """
  Returns the key range for one queue, across all shards.
  """
  @spec queue_range(atom() | String.t(), String.t() | atom()) :: key_range()
  def queue_range(ledger, queue), do: ledger |> table(:queue) |> add(normalize_name!(queue, :queue)) |> range()

  @doc """
  Returns the key range for one queue shard.
  """
  @spec queue_range(atom() | String.t(), String.t() | atom(), non_neg_integer()) :: key_range()
  def queue_range(ledger, queue, shard), do: ledger |> queue_keyspace(queue, shard) |> range()

  @doc """
  Encodes a claim fencing key.
  """
  @spec claim(atom() | String.t(), String.t()) :: key()
  def claim(ledger, claim_id), do: ledger |> table(:claim) |> add_id(claim_id, :claim_id) |> prefix()

  @doc """
  Encodes a queue shard lease key.
  """
  @spec shard_lease(atom() | String.t(), String.t() | atom(), non_neg_integer()) :: key()
  def shard_lease(ledger, queue, shard), do: ledger |> queue_shard_table(:shard, queue, shard) |> prefix()

  @doc """
  Returns the key range for all shard lease rows for one queue.
  """
  @spec shard_lease_range(atom() | String.t(), String.t() | atom()) :: key_range()
  def shard_lease_range(ledger, queue), do: ledger |> table(:shard) |> add(normalize_name!(queue, :queue)) |> range()

  @doc """
  Encodes a durable outbox sequence key.
  """
  @spec outbox(atom() | String.t(), non_neg_integer()) :: key()
  def outbox(ledger, sequence), do: ledger |> table(:outbox) |> add_non_neg_integer!(sequence, :sequence) |> prefix()

  @doc """
  Returns the key range for all outbox records in a ledger.
  """
  @spec outbox_range(atom() | String.t()) :: key_range()
  def outbox_range(ledger), do: table_range(ledger, :outbox)

  @doc """
  Encodes a projection offset key.
  """
  @spec projection(atom() | String.t(), String.t() | atom()) :: key()
  def projection(ledger, name), do: ledger |> table(:projection) |> add(normalize_name!(name, :projection)) |> prefix()

  @doc """
  Returns true when `key` falls inside a range returned by this module.
  """
  @spec contains?(key_range(), key()) :: boolean()
  def contains?({start_key, end_key}, key) when is_binary(key), do: key >= start_key and key < end_key

  defp root do
    ensure_bedrock_keyspace!()
    apply(@bedrock_keyspace, :new, [@namespace])
  end

  defp stream_keyspace(ledger, stream_id), do: ledger |> table(:stream) |> add_id(stream_id, :stream_id)

  defp queue_keyspace(ledger, queue, shard), do: queue_shard_table(ledger, :queue, queue, shard)

  defp queue_shard_table(ledger, table, queue, shard) do
    ledger
    |> table(table)
    |> add(normalize_name!(queue, :queue))
    |> add_non_neg_integer!(shard, :shard)
  end

  defp add_id(keyspace, id, field), do: add(keyspace, normalize_id!(id, field))

  defp add_non_neg_integer!(keyspace, value, field), do: add(keyspace, non_neg_integer!(value, field))

  defp add_integer!(keyspace, value, _field) when is_integer(value), do: add(keyspace, value)

  defp add_integer!(_keyspace, value, field) do
    raise ArgumentError, "#{field} must be an integer, got: #{inspect(value)}"
  end

  defp add_datetime!(keyspace, %DateTime{} = value, _field), do: add(keyspace, DateTime.to_unix(value, :microsecond))

  defp add_datetime!(_keyspace, value, field) do
    raise ArgumentError, "#{field} must be a DateTime, got: #{inspect(value)}"
  end

  defp add(keyspace, value) do
    ensure_bedrock_keyspace!()
    apply(@bedrock_keyspace, :add, [keyspace, value])
  end

  defp range(keyspace) do
    ensure_bedrock_keyspace!()
    apply(@bedrock_key_range, :from_prefix, [prefix(keyspace)])
  end

  defp prefix(keyspace) do
    ensure_bedrock_keyspace!()
    apply(@bedrock_keyspace, :prefix, [keyspace])
  end

  defp table_tag!(table) do
    case Map.fetch(@table_tags, table) do
      {:ok, tag} -> tag
      :error -> raise ArgumentError, "unknown Bedrock keyspace table: #{inspect(table)}"
    end
  end

  defp normalize_name!(value, _field) when is_atom(value) do
    value
    |> Atom.to_string()
    |> String.trim_leading("Elixir.")
  end

  defp normalize_name!(value, field) when is_binary(value), do: non_empty_binary!(value, field)

  defp normalize_name!(value, field) do
    raise ArgumentError, "#{field} must be an atom or non-empty binary, got: #{inspect(value)}"
  end

  defp normalize_id!(value, field) when is_binary(value), do: non_empty_binary!(value, field)

  defp normalize_id!(value, field) do
    raise ArgumentError, "#{field} must be a non-empty binary, got: #{inspect(value)}"
  end

  defp non_empty_binary!(value, field) do
    if value == "" do
      raise ArgumentError, "#{field} must be a non-empty binary"
    end

    value
  end

  defp non_neg_integer!(value, _field) when is_integer(value) and value >= 0, do: value

  defp non_neg_integer!(value, field) do
    raise ArgumentError, "#{field} must be a non-negative integer, got: #{inspect(value)}"
  end

  defp ensure_bedrock_keyspace! do
    case BedrockStore.ensure_available([@bedrock_keyspace, @bedrock_key_range, @bedrock_tuple]) do
      :ok ->
        :ok

      {:error, error} ->
        raise Error.adapter_runtime(Exception.message(error),
                adapter: __MODULE__,
                dependency: :bedrock,
                reason: :missing_dependency
              )
    end
  end
end
