defmodule IntentLedger.SignalHandler do
  @moduledoc """
  Behaviour for durable outbox signal handlers.

  Handlers are registered on an `IntentLedger.SignalDispatcher` with the
  `:signal_handlers` ledger option. A handler receives the durable outbox entry
  and a context map containing the ledger, consumer, and handler-specific opts.
  """

  @type entry :: map()
  @type context :: %{
          required(:ledger) => atom(),
          required(:consumer) => String.t(),
          required(:handler) => module(),
          required(:opts) => keyword()
        }
  @type result :: :ok | {:error, term()}
  @type spec :: module() | {module(), keyword()}
  @type normalized :: %{module: module(), opts: keyword()}

  @callback handle_signal(entry(), context()) :: result()

  @doc false
  @spec normalize([spec()] | nil) :: [normalized()]
  def normalize(nil), do: []

  def normalize(handlers) when is_list(handlers) do
    Enum.map(handlers, &normalize_handler/1)
  end

  defp normalize_handler(module) when is_atom(module), do: %{module: module, opts: []}

  defp normalize_handler({module, opts}) when is_atom(module) and is_list(opts) do
    %{module: module, opts: opts}
  end
end
