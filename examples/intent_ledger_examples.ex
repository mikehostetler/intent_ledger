defmodule IntentLedger.Examples.MemoryWorkflow do
  @moduledoc """
  Memory-backed workflow example compiled by the test suite.
  """

  @default_ledger Module.concat(__MODULE__, Ledger)

  @doc """
  Returns a supervisor child spec for a manual-claim memory ledger.
  """
  @spec child_spec(atom(), keyword()) :: Supervisor.child_spec()
  def child_spec(name \\ @default_ledger, opts \\ []) when is_atom(name) do
    {IntentLedger,
     [
       name: name,
       queues: Keyword.get(opts, :queues, []),
       lease_ms: Keyword.get(opts, :lease_ms, 30_000),
       store: IntentLedger.Store.Memory,
       lifecycle: Keyword.get(opts, :lifecycle, IntentLedger.Examples.RetryLifecycle),
       signal_handlers: Keyword.get(opts, :signal_handlers, [])
     ]}
  end

  @doc """
  Submits, claims, and completes one example intent.
  """
  @spec run_once(GenServer.server(), keyword()) ::
          {:ok,
           %{submitted: IntentLedger.Record.t(), claimed: IntentLedger.Claimed.t(), completed: IntentLedger.Record.t()}}
          | {:error, term()}
          | :empty
  def run_once(ledger, opts \\ []) do
    invoice_id = Keyword.get(opts, :invoice_id, 123)

    intent = %{
      key: "invoice:#{invoice_id}",
      kind: "invoice.send",
      payload: %{invoice_id: invoice_id},
      idempotency_key: "invoice:#{invoice_id}:send"
    }

    with {:ok, submitted} <-
           IntentLedger.submit(ledger, intent, command_id: "cmd:invoice:#{invoice_id}:submit"),
         {:ok, claimed} <- IntentLedger.claim(ledger, :default, "example-worker"),
         {:ok, completed} <-
           IntentLedger.complete(
             ledger,
             claimed.claim.id,
             claimed.claim.token,
             %{sent: true},
             command_id: "cmd:invoice:#{invoice_id}:complete"
           ) do
      {:ok, %{submitted: submitted, claimed: claimed, completed: completed}}
    end
  end
end

defmodule IntentLedger.Examples.RetryLifecycle do
  @moduledoc """
  Example lifecycle classifier for retry and ambiguity decisions.
  """

  @behaviour IntentLedger.Lifecycle

  @impl true
  def before_submit(%IntentLedger.Intent{} = intent, _context) do
    metadata = Map.put(intent.metadata, :example, true)
    {:ok, %{intent | metadata: metadata}}
  end

  @impl true
  def classify_failure(_record, %{temporary: true}, _context) do
    {:retry, DateTime.add(DateTime.utc_now(), 60, :second)}
  end

  def classify_failure(_record, :timeout, _context), do: :ambiguous
  def classify_failure(_record, _error, _context), do: :fail

  @impl true
  def classify_expired_claim(record, _context) do
    if record.intent.ambiguity_policy == :retry do
      :retry
    else
      :ambiguous
    end
  end
end

defmodule IntentLedger.Examples.SignalAuditHandler do
  @moduledoc """
  Example durable outbox signal handler.
  """

  @behaviour IntentLedger.SignalHandler

  @impl true
  def handle_signal(entry, %{opts: opts}) do
    if receiver = Keyword.get(opts, :send_to) do
      send(receiver, {:intent_ledger_example_signal, entry})
    end

    :ok
  end
end

defmodule IntentLedger.Examples.StatusProjection do
  @moduledoc """
  Example projection that tracks the latest lifecycle status per intent.
  """

  @behaviour IntentLedger.Projection

  @impl true
  def init(_opts), do: %{statuses: %{}, counts: %{}, version: 0}

  @impl true
  def apply_signal(%{subject: "intent:" <> intent_id, type: "intent_ledger.intent." <> event}, projection, _context) do
    status = event_status(event)

    projection
    |> Map.update(:statuses, %{intent_id => status}, &Map.put(&1, intent_id, status))
    |> Map.update(:counts, %{status => 1}, &Map.update(&1, status, 1, fn count -> count + 1 end))
    |> Map.update(:version, 1, fn version -> version + 1 end)
  end

  def apply_signal(_signal, projection, _context), do: projection

  defp event_status("marked_ambiguous"), do: "ambiguous"
  defp event_status("released"), do: "available"
  defp event_status(event), do: event
end
