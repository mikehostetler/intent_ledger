if Code.ensure_loaded?(Ecto.Query) do
  defmodule IntentLedger.Store.Ecto.Query do
    @moduledoc """
    Query builders for the Ecto/Postgres Store V1 adapter.
    """

    import Ecto.Query

    alias IntentLedger.Store.Ecto.{Migration, Schema}

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
  end
end
