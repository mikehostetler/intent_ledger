defmodule IntentLedger.Store.Lineage do
  @moduledoc false

  @final_statuses [:completed, :failed, :cancelled, "completed", "failed", "cancelled"]

  @type counts :: %{
          children: non_neg_integer(),
          open_descendants: non_neg_integer()
        }

  @doc false
  @spec counts(Enumerable.t(), Enumerable.t(), keyword() | map()) :: counts()
  def counts(intents, states, attrs) do
    attrs = normalize_attrs(attrs)
    parent_intent_id = attrs |> field(:parent_intent_id) |> normalize_id()
    root_intent_id = attrs |> field(:root_intent_id) |> normalize_id()
    states_by_intent_id = Map.new(states, &{&1 |> field(:intent_id) |> normalize_id(), &1})

    %{
      children: count_children(intents, parent_intent_id),
      open_descendants: count_open_descendants(intents, states_by_intent_id, root_intent_id)
    }
  end

  defp count_children(_intents, nil), do: 0

  defp count_children(intents, parent_intent_id) do
    Enum.count(intents, fn intent ->
      intent |> field(:parent_intent_id) |> normalize_id() == parent_intent_id
    end)
  end

  defp count_open_descendants(_intents, _states_by_intent_id, nil), do: 0

  defp count_open_descendants(intents, states_by_intent_id, root_intent_id) do
    Enum.count(intents, fn intent ->
      intent_id = intent |> field(:id) |> normalize_id()

      intent |> field(:root_intent_id) |> normalize_id() == root_intent_id and
        intent_id != root_intent_id and
        states_by_intent_id |> Map.get(intent_id) |> open_state?()
    end)
  end

  defp open_state?(nil), do: false
  defp open_state?(state), do: field(state, :status) not in @final_statuses

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs

  defp normalize_id(nil), do: nil
  defp normalize_id(value), do: to_string(value)

  defp field(value, key, default \\ nil)
  defp field(nil, _key, default), do: default
  defp field(%{} = value, key, default), do: Map.get(value, key, Map.get(value, Atom.to_string(key), default))
  defp field(value, key, default) when is_struct(value), do: value |> Map.from_struct() |> field(key, default)
  defp field(_value, _key, default), do: default
end
