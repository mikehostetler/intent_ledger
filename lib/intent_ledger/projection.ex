defmodule IntentLedger.Projection do
  @moduledoc """
  Behaviour and helpers for rebuilding disposable projections from lifecycle signals.

  Projection modules are intentionally storage-neutral. They define how to
  initialize projection state and how each replayed lifecycle signal advances
  that state. Host applications own persistence for query projections and can
  drop/rebuild those projections from `IntentLedger.replay_*` APIs.
  """

  @type signal :: Jido.Signal.t() | map()
  @type projection :: term()
  @type context :: %{
          required(:projection) => module(),
          required(:opts) => keyword(),
          required(:index) => non_neg_integer()
        }
  @type apply_result :: projection() | {:ok, projection()} | :ignore | {:error, term()}

  @callback init(keyword()) :: projection() | {:ok, projection()} | {:error, term()}
  @callback apply_signal(signal(), projection(), context()) :: apply_result()

  @optional_callbacks init: 1

  @doc """
  Rebuilds a projection module from the beginning of a signal enumerable.

  If the projection module does not implement `init/1`, rebuild starts from an
  empty map. `apply_signal/3` may return the next projection directly, wrap it
  in `{:ok, projection}`, return `:ignore` to leave state unchanged, or return
  `{:error, reason}` to halt the rebuild.
  """
  @spec rebuild(module(), Enumerable.t(), keyword()) :: {:ok, projection()} | {:error, term()}
  def rebuild(module, signals, opts \\ []) when is_atom(module) do
    with {:ok, projection} <- init_projection(module, opts) do
      catch_up(module, projection, signals, opts)
    end
  end

  @doc """
  Applies replayed signals to an existing projection state.

  This is useful when a durable projection records its own offset and needs to
  catch up from a later replay cursor without rebuilding from scratch.
  """
  @spec catch_up(module(), projection(), Enumerable.t(), keyword()) :: {:ok, projection()} | {:error, term()}
  def catch_up(module, projection, signals, opts \\ []) when is_atom(module) do
    signals
    |> Stream.with_index()
    |> Enum.reduce_while({:ok, projection}, fn {signal, index}, {:ok, acc} ->
      case apply(module, signal, acc, Keyword.put(opts, :index, index)) do
        {:ok, next_projection} -> {:cont, {:ok, next_projection}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @doc """
  Applies one signal to a projection module.
  """
  @spec apply(module(), signal(), projection(), keyword()) :: {:ok, projection()} | {:error, term()}
  def apply(module, signal, projection, opts \\ []) when is_atom(module) do
    context = %{
      projection: module,
      opts: opts,
      index: opts |> Keyword.get(:index, 0) |> non_negative_or(0)
    }

    case call_apply(module, signal, projection, context) do
      {:ok, next_projection} -> {:ok, next_projection}
      :ignore -> {:ok, projection}
      {:error, reason} -> {:error, {module, reason}}
      next_projection -> {:ok, next_projection}
    end
  end

  defp init_projection(module, opts) do
    if function_exported?(module, :init, 1) do
      case module.init(opts) do
        {:ok, projection} -> {:ok, projection}
        {:error, reason} -> {:error, {module, reason}}
        projection -> {:ok, projection}
      end
    else
      {:ok, %{}}
    end
  catch
    kind, reason -> {:error, {module, {kind, reason}}}
  end

  defp call_apply(module, signal, projection, context) do
    module.apply_signal(signal, projection, context)
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp non_negative_or(value, _default) when is_integer(value) and value >= 0, do: value
  defp non_negative_or(_value, default), do: default
end
