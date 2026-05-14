defmodule IntentLedger.ExamplesTest do
  use ExUnit.Case, async: false

  alias IntentLedger.Examples.{MemoryWorkflow, SignalAuditHandler, StatusProjection}

  test "memory workflow example submits claims completes and rebuilds a projection" do
    ledger = unique_ledger()
    start_supervised!(MemoryWorkflow.child_spec(ledger))

    assert {:ok, %{submitted: submitted, claimed: claimed, completed: completed}} =
             MemoryWorkflow.run_once(ledger, invoice_id: 123)

    assert submitted.intent.id == completed.intent.id
    assert claimed.intent.id == completed.intent.id
    assert completed.state.status == :completed
    assert completed.state.result == %{sent: true}

    assert {:ok, projection} =
             IntentLedger.rebuild_projection(ledger, StatusProjection, source: {:intent, completed.intent.id})

    assert projection.statuses[completed.intent.id] == "completed"
    assert projection.counts["submitted"] == 1
    assert projection.counts["completed"] == 1
    assert projection.version >= 3
  end

  test "signal handler example acknowledges by returning ok" do
    entry = %{key: "out:1", signal: %{type: "intent_ledger.intent.completed"}}
    context = %{ledger: unique_ledger(), consumer: "test", handler: SignalAuditHandler, opts: [send_to: self()]}

    assert :ok = SignalAuditHandler.handle_signal(entry, context)
    assert_receive {:intent_ledger_example_signal, ^entry}
  end

  defp unique_ledger do
    Module.concat(__MODULE__, :"Ledger#{System.unique_integer([:positive])}")
  end
end
