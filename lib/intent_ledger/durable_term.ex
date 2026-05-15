defmodule IntentLedger.DurableTerm do
  @moduledoc false

  @max_binary_bytes 1_024
  @max_collection_items 50
  @max_depth 6

  @type limits :: %{
          max_binary_bytes: pos_integer(),
          max_collection_items: pos_integer(),
          max_depth: pos_integer()
        }

  @doc false
  @spec summarize(term(), keyword()) :: term()
  def summarize(term, opts \\ []) do
    limits = %{
      max_binary_bytes: Keyword.get(opts, :max_binary_bytes, @max_binary_bytes),
      max_collection_items: Keyword.get(opts, :max_collection_items, @max_collection_items),
      max_depth: Keyword.get(opts, :max_depth, @max_depth)
    }

    summarize(term, limits, 0)
  end

  defp summarize(term, _limits, _depth) when is_nil(term) or is_boolean(term), do: term
  defp summarize(term, _limits, _depth) when is_atom(term) or is_number(term), do: term

  defp summarize(term, limits, _depth) when is_binary(term) do
    if byte_size(term) <= limits.max_binary_bytes do
      term
    else
      %{type: :binary, bytes: byte_size(term), redacted: true}
    end
  end

  defp summarize(%DateTime{} = term, _limits, _depth), do: term
  defp summarize(%NaiveDateTime{} = term, _limits, _depth), do: term
  defp summarize(%Date{} = term, _limits, _depth), do: term
  defp summarize(%Time{} = term, _limits, _depth), do: term

  defp summarize(%{__exception__: true, __struct__: module}, _limits, _depth) do
    %{type: :exception, module: inspect(module), redacted: true}
  end

  defp summarize(%{__struct__: module}, _limits, _depth) do
    %{type: :struct, module: inspect(module), redacted: true}
  end

  defp summarize(term, _limits, _depth) when is_function(term) do
    %{type: :function, redacted: true}
  end

  defp summarize(term, _limits, _depth) when is_pid(term) do
    %{type: :pid, redacted: true}
  end

  defp summarize(term, _limits, _depth) when is_port(term) do
    %{type: :port, redacted: true}
  end

  defp summarize(term, _limits, _depth) when is_reference(term) do
    %{type: :reference, redacted: true}
  end

  defp summarize(term, limits, depth) when is_map(term) do
    if depth >= limits.max_depth do
      redacted(:map, map_size(term))
    else
      entries = Enum.take(term, limits.max_collection_items)
      omitted = max(map_size(term) - length(entries), 0)

      entries
      |> Map.new(fn {key, value} ->
        {summarize_key(key), summarize(value, limits, depth + 1)}
      end)
      |> maybe_put_omitted(omitted)
    end
  end

  defp summarize(term, limits, depth) when is_list(term) do
    if depth >= limits.max_depth do
      redacted(:list, length(term))
    else
      entries = Enum.take(term, limits.max_collection_items)
      omitted = max(length(term) - length(entries), 0)
      summarized = Enum.map(entries, &summarize(&1, limits, depth + 1))

      if omitted == 0 do
        summarized
      else
        %{type: :list, items: summarized, omitted: omitted}
      end
    end
  end

  defp summarize(term, limits, depth) when is_tuple(term) do
    size = tuple_size(term)

    cond do
      depth >= limits.max_depth ->
        redacted(:tuple, size)

      size > limits.max_collection_items ->
        %{
          type: :tuple,
          size: size,
          elements:
            term
            |> Tuple.to_list()
            |> Enum.take(limits.max_collection_items)
            |> Enum.map(&summarize(&1, limits, depth + 1)),
          omitted: size - limits.max_collection_items
        }

      true ->
        term
        |> Tuple.to_list()
        |> Enum.map(&summarize(&1, limits, depth + 1))
        |> List.to_tuple()
    end
  end

  defp summarize(term, _limits, _depth), do: %{type: :term, value: inspect(term), redacted: true}

  defp summarize_key(key) when is_atom(key) or is_binary(key) or is_integer(key), do: key
  defp summarize_key(key), do: inspect(key)

  defp maybe_put_omitted(map, 0), do: map
  defp maybe_put_omitted(map, omitted), do: Map.put(map, :__intent_ledger_omitted__, omitted)

  defp redacted(type, size), do: %{type: type, size: size, redacted: true}
end
