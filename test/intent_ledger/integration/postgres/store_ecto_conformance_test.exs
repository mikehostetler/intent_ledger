defmodule IntentLedger.StoreEctoConformanceTest.Repo do
  @moduledoc false

  use Agent

  def start_link(_opts), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  def __adapter__, do: Ecto.Adapters.Postgres

  def reset, do: Agent.update(__MODULE__, fn _tables -> %{} end)

  def transaction(fun, _opts) do
    Agent.get_and_update(__MODULE__, fn tables ->
      Process.put(:tables, tables)

      try do
        result = fun.()
        next_tables = if match?({:error, _reason}, result), do: tables, else: Process.get(:tables, tables)
        {{:ok, result}, next_tables}
      after
        Process.delete(:tables)
      end
    end)
  end

  def one(query), do: query |> all() |> List.first()

  def all(%Ecto.Query{} = query) do
    query
    |> query_table()
    |> table_rows()
    |> Enum.filter(&query_match?(&1, query))
  end

  def insert_all(source, rows, opts) do
    table = source_table(source)

    update_table(table, fn existing_rows ->
      Enum.reduce(rows, existing_rows, &upsert_row(&1, &2, opts))
    end)

    {length(rows), nil}
  end

  def update_all(%Ecto.Query{} = query, opts) do
    table = query_table(query)
    updates = opts |> Keyword.fetch!(:set) |> Map.new()
    {rows, updated_count} = update_matching_rows(table_rows(table), query, updates)

    replace_table(table, rows)
    {updated_count, nil}
  end

  def delete_all(%Ecto.Query{} = query, _opts) do
    table = query_table(query)
    {deleted, kept} = Enum.split_with(table_rows(table), &query_match?(&1, query))

    replace_table(table, kept)
    {length(deleted), nil}
  end

  def rollback(reason), do: {:error, reason}

  defp update_matching_rows(rows, query, updates) do
    Enum.map_reduce(rows, 0, fn row, count ->
      if query_match?(row, query) do
        {Map.merge(row, updates), count + 1}
      else
        {row, count}
      end
    end)
  end

  defp upsert_row(row, existing_rows, opts) do
    conflict_target = Keyword.get(opts, :conflict_target, [])

    case find_conflict(existing_rows, row, conflict_target) do
      nil ->
        existing_rows ++ [row]

      index ->
        resolve_conflict(existing_rows, index, row, Keyword.get(opts, :on_conflict))
    end
  end

  defp resolve_conflict(existing_rows, _index, _row, :nothing), do: existing_rows

  defp resolve_conflict(existing_rows, index, row, {:replace, fields}) do
    List.update_at(existing_rows, index, &Map.merge(&1, Map.take(row, fields)))
  end

  defp resolve_conflict(existing_rows, index, row, _on_conflict) do
    List.replace_at(existing_rows, index, row)
  end

  defp find_conflict(_existing_rows, _row, []), do: nil

  defp find_conflict(existing_rows, row, conflict_target) do
    Enum.find_index(existing_rows, fn existing ->
      Enum.all?(conflict_target, &(field(existing, &1) == field(row, &1)))
    end)
  end

  defp query_match?(row, %Ecto.Query{wheres: wheres}) do
    Enum.all?(wheres, &where_match?(row, &1))
  end

  defp where_match?(row, %{expr: expr, params: params}) do
    eval_expr(expr, row, params)
  end

  defp eval_expr({:==, _meta, [field_expr, {:^, _param_meta, [index]}]}, row, params) do
    field(row, field_name(field_expr)) == param_value(params, index)
  end

  defp eval_expr({:<=, _meta, [field_expr, {:^, _param_meta, [index]}]}, row, params) do
    less_or_equal?(field(row, field_name(field_expr)), param_value(params, index))
  end

  defp eval_expr({:in, _meta, [field_expr, %Ecto.Query.Tagged{value: values}]}, row, _params) do
    field(row, field_name(field_expr)) in values
  end

  defp eval_expr(_expr, _row, _params), do: true

  defp less_or_equal?(%DateTime{} = left, %DateTime{} = right), do: DateTime.compare(left, right) != :gt
  defp less_or_equal?(nil, _right), do: false
  defp less_or_equal?(left, right), do: left <= right

  defp field_name({{:., _meta, [{:&, _binding_meta, [0]}, name]}, _call_meta, []}), do: name

  defp param_value(params, index) do
    params
    |> Enum.at(index)
    |> elem(0)
  end

  defp update_table(table, fun), do: replace_table(table, fun.(table_rows(table)))

  defp replace_table(table, rows) do
    Process.put(:tables, Map.put(Process.get(:tables, %{}), table, rows))
  end

  defp table_rows(table), do: Map.get(Process.get(:tables, %{}), table, [])

  defp query_table(%Ecto.Query{from: %{source: source}}), do: source_table(source)

  defp source_table({table, _schema}), do: table
  defp source_table(table), do: table

  defp field(value, key, default \\ nil)
  defp field(nil, _key, default), do: default
  defp field(%{} = value, key, default), do: Map.get(value, key, Map.get(value, Atom.to_string(key), default))
  defp field(value, key, default) when is_struct(value), do: value |> Map.from_struct() |> field(key, default)
  defp field(_value, _key, default), do: default
end

defmodule IntentLedger.StoreEctoConformanceTest do
  use IntentLedger.StoreCase,
    async: false,
    store_module: IntentLedger.Store.Ecto,
    store_opts: [name: __MODULE__.Store, repo: IntentLedger.StoreEctoConformanceTest.Repo]

  @moduletag :integration
  @moduletag :postgres

  use IntentLedger.StoreCase.AtomicCommitTests
  use IntentLedger.StoreCase.SemanticTests

  alias IntentLedger.StoreEctoConformanceTest.Repo

  setup_all do
    start_supervised!(Repo)
    :ok
  end

  setup do
    Repo.reset()
    :ok
  end
end
