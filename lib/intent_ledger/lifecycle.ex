defmodule IntentLedger.Lifecycle do
  @moduledoc """
  Optional lifecycle hooks for ledger instances.

  A callback module can be supplied with `:lifecycle` when starting a ledger.
  `before_submit/2` remains a synchronous validation/enrichment boundary.

  `after_transition/2` is retained as a synchronous, best-effort compatibility
  observer. It runs on the local server process after a lifecycle commit has
  succeeded; callback errors are logged by the server and must not be used for
  durable delivery or correctness. Use `IntentLedger.SignalDispatcher` with
  `IntentLedger.SignalHandler` modules for at-least-once signal delivery.
  """

  alias IntentLedger.{Intent, Record}

  @type context :: map()

  @callback before_submit(Intent.t(), context()) :: {:ok, Intent.t()} | {:error, term()}
  @callback classify_failure(Record.t(), term(), context()) ::
              :retry | :fail | :ambiguous | {:retry, DateTime.t()} | {:error, term()}
  @callback classify_expired_claim(Record.t(), context()) ::
              :retry | :ambiguous | {:error, term()}

  @doc """
  Best-effort local observation hook for committed lifecycle signals.

  This callback is compatibility-only. A return value of `{:error, reason}`
  stops the local callback loop for the current commit, but the lifecycle commit
  has already succeeded and is not rolled back.
  """
  @callback after_transition(Jido.Signal.t(), context()) :: :ok | {:error, term()}

  @optional_callbacks before_submit: 2,
                      classify_failure: 3,
                      classify_expired_claim: 2,
                      after_transition: 2

  @doc false
  @spec before_submit(module() | nil, Intent.t(), context()) ::
          {:ok, Intent.t()} | {:error, term()}
  def before_submit(nil, %Intent{} = intent, _context), do: {:ok, intent}

  def before_submit(module, %Intent{} = intent, context) do
    if function_exported?(module, :before_submit, 2) do
      module.before_submit(intent, context)
    else
      {:ok, intent}
    end
  end

  @doc false
  @spec classify_failure(module() | nil, Record.t(), term(), context()) ::
          :default | :retry | :fail | :ambiguous | {:retry, DateTime.t()} | {:error, term()}
  def classify_failure(nil, %Record{}, _error, _context), do: :default

  def classify_failure(module, %Record{} = record, error, context) do
    if function_exported?(module, :classify_failure, 3) do
      module.classify_failure(record, error, context)
    else
      :default
    end
  end

  @doc false
  @spec after_transition(module() | nil, [Jido.Signal.t()], context()) :: :ok | {:error, term()}
  def after_transition(nil, _signals, _context), do: :ok

  def after_transition(module, signals, context) when is_list(signals) do
    if function_exported?(module, :after_transition, 2) do
      Enum.reduce_while(signals, :ok, fn signal, :ok ->
        case module.after_transition(signal, context) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    else
      :ok
    end
  end
end
