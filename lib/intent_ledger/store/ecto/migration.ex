if Code.ensure_loaded?(Ecto.Migration) do
  defmodule IntentLedger.Store.Ecto.Migration do
    @moduledoc """
    Migration helpers for the local Ecto/Postgres store adapter.

    Call `up/1` and `down/1` from an application migration, or call `change/1`
    when your migration can rely on Ecto's reversible create/drop commands.
    Options accept a repo module for parity with `IntentLedger.Store.Ecto`,
    a Postgres schema prefix, and table-name overrides.
    """

    use Ecto.Migration

    @tables %{
      intents: :intent_ledger_intents,
      states: :intent_ledger_states,
      signals: :intent_ledger_signals,
      streams: :intent_ledger_streams,
      commands: :intent_ledger_commands,
      claims: :intent_ledger_claims,
      shard_leases: :intent_ledger_shard_leases,
      outbox: :intent_ledger_outbox,
      projection_offsets: :intent_ledger_projection_offsets
    }

    @type table ::
            :intents
            | :states
            | :signals
            | :streams
            | :commands
            | :claims
            | :shard_leases
            | :outbox
            | :projection_offsets
    @type option ::
            {:repo, module()}
            | {:prefix, String.t() | nil}
            | {:tables, keyword() | map()}

    @doc """
    Creates all Intent Ledger Ecto/Postgres tables and indexes.
    """
    @spec up([option()]) :: term()
    def up(opts \\ []) do
      create_tables(opts)
      create_indexes(opts)
    end

    @doc """
    Drops all Intent Ledger Ecto/Postgres tables.
    """
    @spec down([option()]) :: term()
    def down(opts \\ []) do
      opts
      |> table_names()
      |> Map.keys()
      |> Enum.reverse()
      |> Enum.each(&drop_if_exists(table(table_name(&1, opts), prefix: prefix(opts))))
    end

    @doc """
    Creates all tables and indexes using reversible Ecto migration commands.
    """
    @spec change([option()]) :: term()
    def change(opts \\ []) do
      up(opts)
    end

    @doc """
    Returns the repo option supplied to migration helpers.
    """
    @spec repo([option()]) :: module() | nil
    def repo(opts \\ []), do: Keyword.get(opts, :repo)

    @doc """
    Returns the Postgres schema prefix supplied to migration helpers.
    """
    @spec prefix([option()]) :: String.t() | nil
    def prefix(opts \\ []), do: Keyword.get(opts, :prefix)

    @doc """
    Returns the configured physical table name for a logical table.
    """
    @spec table_name(table(), [option()]) :: atom()
    def table_name(table, opts \\ []) when is_atom(table) do
      opts
      |> table_names()
      |> Map.fetch!(table)
    end

    @doc """
    Returns logical table names merged with any `:tables` overrides.
    """
    @spec table_names([option()]) :: %{required(table()) => atom()}
    def table_names(opts \\ []) do
      overrides =
        opts
        |> Keyword.get(:tables, %{})
        |> Map.new()

      Map.merge(@tables, overrides)
    end

    defp create_tables(opts) do
      create table(table_name(:intents, opts), primary_key: false, prefix: prefix(opts)) do
        add(:ledger, :string, null: false)
        add(:intent_id, :string, null: false)
        add(:intent, :map, null: false)
        timestamps(type: :utc_datetime_usec)
      end

      create table(table_name(:states, opts), primary_key: false, prefix: prefix(opts)) do
        add(:ledger, :string, null: false)
        add(:intent_id, :string, null: false)
        add(:status, :string, null: false)
        add(:queue, :string, null: false)
        add(:shard, :integer, null: false)
        add(:priority, :integer, null: false)
        add(:visible_at, :utc_datetime_usec)
        add(:attempt, :integer, null: false)
        add(:claim_id, :string)
        add(:token_hash, :string)
        add(:lease_until, :utc_datetime_usec)
        add(:state, :map, null: false)
        timestamps(type: :utc_datetime_usec)
      end

      create table(table_name(:streams, opts), primary_key: false, prefix: prefix(opts)) do
        add(:ledger, :string, null: false)
        add(:stream, :string, null: false)
        add(:version, :bigint, null: false)
        timestamps(type: :utc_datetime_usec)
      end

      create table(table_name(:signals, opts), primary_key: false, prefix: prefix(opts)) do
        add(:ledger, :string, null: false)
        add(:stream, :string, null: false)
        add(:version, :bigint, null: false)
        add(:signal, :map, null: false)
        timestamps(type: :utc_datetime_usec)
      end

      create table(table_name(:commands, opts), primary_key: false, prefix: prefix(opts)) do
        add(:ledger, :string, null: false)
        add(:command_id, :string, null: false)
        add(:operation, :string, null: false)
        add(:command, :map, null: false)
        add(:result, :map, null: false)
        timestamps(type: :utc_datetime_usec)
      end

      create table(table_name(:claims, opts), primary_key: false, prefix: prefix(opts)) do
        add(:ledger, :string, null: false)
        add(:claim_id, :string, null: false)
        add(:intent_id, :string, null: false)
        add(:owner_id, :string, null: false)
        add(:token_hash, :string, null: false)
        add(:lease_until, :utc_datetime_usec, null: false)
        add(:claim, :map, null: false)
        timestamps(type: :utc_datetime_usec)
      end

      create table(table_name(:shard_leases, opts), primary_key: false, prefix: prefix(opts)) do
        add(:ledger, :string, null: false)
        add(:queue, :string, null: false)
        add(:shard, :integer, null: false)
        add(:owner_id, :string, null: false)
        add(:lease_until, :utc_datetime_usec, null: false)
        add(:lease, :map, null: false)
        timestamps(type: :utc_datetime_usec)
      end

      create table(table_name(:outbox, opts), primary_key: false, prefix: prefix(opts)) do
        add(:ledger, :string, null: false)
        add(:key, :string, null: false)
        add(:sequence, :bigint, null: false)
        add(:stream, :string, null: false)
        add(:signal_id, :string)
        add(:signal_type, :string)
        add(:subject, :string)
        add(:signal, :map, null: false)
        add(:entry, :map, null: false)
        add(:acked_at, :utc_datetime_usec)
        add(:consumer, :string)
        add(:metadata, :map)
        timestamps(type: :utc_datetime_usec)
      end

      create table(table_name(:projection_offsets, opts), primary_key: false, prefix: prefix(opts)) do
        add(:ledger, :string, null: false)
        add(:name, :string, null: false)
        add(:cursor, :bigint)
        add(:metadata, :map)
        timestamps(type: :utc_datetime_usec)
      end
    end

    defp create_indexes(opts) do
      create(unique_index(table_name(:intents, opts), [:ledger, :intent_id], prefix: prefix(opts)))
      create(unique_index(table_name(:states, opts), [:ledger, :intent_id], prefix: prefix(opts)))

      create(
        index(table_name(:states, opts), [:ledger, :queue, :shard, :status, :visible_at, :priority, :intent_id],
          prefix: prefix(opts)
        )
      )

      create(
        index(table_name(:states, opts), [:ledger, :queue, :shard, :status, :lease_until, :intent_id],
          prefix: prefix(opts)
        )
      )

      create(unique_index(table_name(:streams, opts), [:ledger, :stream], prefix: prefix(opts)))
      create(unique_index(table_name(:signals, opts), [:ledger, :stream, :version], prefix: prefix(opts)))
      create(unique_index(table_name(:commands, opts), [:ledger, :command_id], prefix: prefix(opts)))
      create(unique_index(table_name(:claims, opts), [:ledger, :claim_id], prefix: prefix(opts)))
      create(index(table_name(:claims, opts), [:ledger, :intent_id], prefix: prefix(opts)))
      create(unique_index(table_name(:shard_leases, opts), [:ledger, :queue, :shard], prefix: prefix(opts)))
      create(unique_index(table_name(:outbox, opts), [:ledger, :key], prefix: prefix(opts)))
      create(unique_index(table_name(:outbox, opts), [:ledger, :sequence], prefix: prefix(opts)))
      create(index(table_name(:outbox, opts), [:ledger, :acked_at, :sequence], prefix: prefix(opts)))
      create(index(table_name(:outbox, opts), [:ledger, :stream, :sequence], prefix: prefix(opts)))
      create(unique_index(table_name(:projection_offsets, opts), [:ledger, :name], prefix: prefix(opts)))
    end
  end
else
  defmodule IntentLedger.Store.Ecto.Migration do
    @moduledoc """
    Migration helpers for the local Ecto/Postgres store adapter.

    This fallback module keeps the core package compilable when optional Ecto
    dependencies are not installed. Applications must add `:ecto_sql` and
    `:postgrex` before running these migration helpers.
    """

    alias IntentLedger.Error

    @tables %{
      intents: :intent_ledger_intents,
      states: :intent_ledger_states,
      signals: :intent_ledger_signals,
      streams: :intent_ledger_streams,
      commands: :intent_ledger_commands,
      claims: :intent_ledger_claims,
      shard_leases: :intent_ledger_shard_leases,
      outbox: :intent_ledger_outbox,
      projection_offsets: :intent_ledger_projection_offsets
    }

    @type table ::
            :intents
            | :states
            | :signals
            | :streams
            | :commands
            | :claims
            | :shard_leases
            | :outbox
            | :projection_offsets
    @type option ::
            {:repo, module()}
            | {:prefix, String.t() | nil}
            | {:tables, keyword() | map()}

    @doc """
    Raises a normalized adapter error because Ecto is not installed.
    """
    @spec up([option()]) :: no_return()
    def up(opts \\ []), do: raise_missing_dependency(opts)

    @doc """
    Raises a normalized adapter error because Ecto is not installed.
    """
    @spec down([option()]) :: no_return()
    def down(opts \\ []), do: raise_missing_dependency(opts)

    @doc """
    Raises a normalized adapter error because Ecto is not installed.
    """
    @spec change([option()]) :: no_return()
    def change(opts \\ []), do: raise_missing_dependency(opts)

    @doc """
    Returns the repo option supplied to migration helpers.
    """
    @spec repo([option()]) :: module() | nil
    def repo(opts \\ []), do: Keyword.get(opts, :repo)

    @doc """
    Returns the Postgres schema prefix supplied to migration helpers.
    """
    @spec prefix([option()]) :: String.t() | nil
    def prefix(opts \\ []), do: Keyword.get(opts, :prefix)

    @doc """
    Returns the configured physical table name for a logical table.
    """
    @spec table_name(table(), [option()]) :: atom()
    def table_name(table, opts \\ []) when is_atom(table) do
      opts
      |> table_names()
      |> Map.fetch!(table)
    end

    @doc """
    Returns logical table names merged with any `:tables` overrides.
    """
    @spec table_names([option()]) :: %{required(table()) => atom()}
    def table_names(opts \\ []) do
      overrides =
        opts
        |> Keyword.get(:tables, %{})
        |> Map.new()

      Map.merge(@tables, overrides)
    end

    defp raise_missing_dependency(opts) do
      raise Error.adapter_runtime(
              "Ecto SQL and Postgrex dependencies are required to use IntentLedger.Store.Ecto.Migration",
              adapter: __MODULE__,
              details: opts,
              reason: :missing_dependency,
              dependencies: [:ecto_sql, :postgrex]
            )
    end
  end
end
