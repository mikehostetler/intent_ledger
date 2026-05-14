if Code.ensure_loaded?(Ecto.Query) do
  defmodule IntentLedger.Store.Ecto.Query do
    @moduledoc """
    Query builders for the Ecto/Postgres Store V1 adapter.
    """

    import Ecto.Query

    alias IntentLedger.Store.Ecto.{Migration, Schema}
    alias IntentLedger.Store.Listing

    @type table :: Migration.table()
    @type option :: Migration.option()

    @doc """
    Builds a query for rows in a logical table matching a ledger and field filters.
    """
    @spec by_fields(table(), atom() | String.t(), keyword(), [option()]) :: Ecto.Query.t()
    def by_fields(table, ledger, filters, opts \\ []) when is_list(filters) do
      source = Schema.source(table, opts)
      prefix = Migration.prefix(opts)
      ledger = ledger_key(ledger)

      filters
      |> Enum.reduce(from(row in source, prefix: ^prefix, where: row.ledger == ^ledger), fn {field_name, value},
                                                                                            query ->
        from(row in query, where: field(row, ^field_name) == ^value)
      end)
    end

    @doc """
    Builds the SQL query for a Store V1 listing request.
    """
    @spec listing(Listing.t(), atom() | String.t(), [option()]) :: Ecto.Query.t()
    def listing(listing, ledger, opts \\ [])

    def listing(%Listing{type: :due_intents} = listing, ledger, opts) do
      listing
      |> base_listing_query(ledger, opts)
      |> where([row], row.status in ["available", "retry_scheduled"])
      |> where([row], row.visible_at <= ^listing.at)
      |> order_by([row], desc: row.priority, asc: row.visible_at, asc: row.intent_id)
      |> limit(^listing.limit)
    end

    def listing(%Listing{type: :expired_claims} = listing, ledger, opts) do
      listing
      |> base_listing_query(ledger, opts)
      |> where([row], row.status == "claimed")
      |> where([row], row.lease_until <= ^listing.at)
      |> order_by([row], asc: row.lease_until, asc: row.intent_id)
      |> limit(^listing.limit)
    end

    defp base_listing_query(%Listing{} = listing, ledger, opts) do
      source = Schema.source(:states, opts)
      prefix = Migration.prefix(opts)
      ledger = ledger_key(ledger)

      query =
        from(row in source,
          prefix: ^prefix,
          where: row.ledger == ^ledger,
          where: row.queue == ^listing.queue
        )

      case listing.shard do
        nil -> query
        shard -> from(row in query, where: row.shard == ^shard)
      end
    end

    @doc """
    Builds the SQL query for ordered outbox entries.
    """
    @spec outbox_entries(atom() | String.t(), [option()]) :: Ecto.Query.t()
    def outbox_entries(ledger, opts \\ []) do
      source = Schema.source(:outbox, opts)
      prefix = Migration.prefix(opts)
      ledger = ledger_key(ledger)

      from(row in source,
        prefix: ^prefix,
        where: row.ledger == ^ledger,
        order_by: [asc: row.sequence]
      )
    end

    defp ledger_key(ledger), do: ledger |> inspect() |> String.trim_leading("Elixir.")
  end
else
  defmodule IntentLedger.Store.Ecto.Query do
    @moduledoc """
    Query builders for the Ecto/Postgres Store V1 adapter.

    This fallback keeps the package compilable without optional Ecto
    dependencies. Configure `:ecto_sql` and `:postgrex` before using these
    helpers.
    """

    alias IntentLedger.Error
    alias IntentLedger.Store.Ecto.Migration
    alias IntentLedger.Store.Listing

    @type table :: Migration.table()
    @type option :: Migration.option()

    @doc """
    Raises a normalized adapter error because Ecto is not installed.
    """
    @spec by_fields(table(), atom() | String.t(), keyword(), [option()]) :: no_return()
    def by_fields(_table, _ledger, _filters, _opts \\ []) do
      raise Error.adapter_runtime(
              "Ecto SQL and Postgrex dependencies are required to use IntentLedger.Store.Ecto.Query",
              adapter: __MODULE__,
              reason: :missing_dependency,
              dependencies: [:ecto_sql, :postgrex]
            )
    end

    @doc """
    Raises a normalized adapter error because Ecto is not installed.
    """
    @spec listing(Listing.t(), atom() | String.t(), [option()]) :: no_return()
    def listing(_listing, _ledger, _opts \\ []) do
      raise Error.adapter_runtime(
              "Ecto SQL and Postgrex dependencies are required to use IntentLedger.Store.Ecto.Query",
              adapter: __MODULE__,
              reason: :missing_dependency,
              dependencies: [:ecto_sql, :postgrex]
            )
    end

    @doc """
    Raises a normalized adapter error because Ecto is not installed.
    """
    @spec outbox_entries(atom() | String.t(), [option()]) :: no_return()
    def outbox_entries(_ledger, _opts \\ []) do
      raise Error.adapter_runtime(
              "Ecto SQL and Postgrex dependencies are required to use IntentLedger.Store.Ecto.Query",
              adapter: __MODULE__,
              reason: :missing_dependency,
              dependencies: [:ecto_sql, :postgrex]
            )
    end
  end
end
