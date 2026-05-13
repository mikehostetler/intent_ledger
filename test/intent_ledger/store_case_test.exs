defmodule IntentLedger.StoreCaseHarnessStore do
  @behaviour IntentLedger.Store

  alias IntentLedger.Store.Commit

  def child_spec(opts) do
    %{
      id: {__MODULE__, make_ref()},
      start: {Agent, :start_link, [fn -> Map.new(opts) end]}
    }
  end

  def commit(ref, ledger, request, _opts) do
    Agent.get(ref, fn state ->
      {:ok,
       Commit.new(
         command_id: request.command_id,
         result: %{ledger: ledger, operation: request.operation, state: state},
         writes: request.writes
       )}
    end)
  end

  def read(ref, ledger, request, _opts), do: result(ref, ledger, request)
  def lease(ref, ledger, request, _opts), do: result(ref, ledger, request)
  def listing(ref, ledger, request, _opts), do: result(ref, ledger, request)
  def outbox(ref, ledger, request, _opts), do: result(ref, ledger, request)

  defp result(ref, ledger, request) do
    Agent.get(ref, fn state -> {:ok, %{ledger: ledger, request: request, state: state}} end)
  end
end

defmodule IntentLedger.StoreCaseTest do
  use IntentLedger.StoreCase,
    async: true,
    store_module: IntentLedger.StoreCaseHarnessStore,
    store_opts: [adapter: :harness]

  test "starts the configured store and exposes adapter context", context do
    assert context.store_module == IntentLedger.StoreCaseHarnessStore
    assert is_pid(context.store_ref)
    assert context.store_opts == [adapter: :harness]

    assert context.ledger ==
             IntentLedger.StoreCaseTest.Ledgertest_starts_the_configured_store_and_exposes_adapter_context
  end

  test "wraps store v1 callbacks through the configured adapter", context do
    request =
      CommitRequest.new(
        command_id: "cmd_1",
        operation: :submit,
        writes: [Write.put_outbox("out_1", %{type: "intent_ledger.intent.submitted"})]
      )

    assert {:ok, commit} = commit(context, request)
    assert commit.command_id == "cmd_1"
    assert commit.result.operation == :submit
    assert commit.result.state == %{adapter: :harness}
    assert [write] = commit.writes
    assert write.type == :put_outbox

    assert {:ok, read_result} = read(context, {:intent, "int_1"})
    assert read_result.request == {:intent, "int_1"}

    assert {:ok, lease_result} = lease(context, {:shard, :acquire, %{queue: "default", shard: 0}})
    assert lease_result.request == {:shard, :acquire, %{queue: "default", shard: 0}}

    listing_request = Listing.due_intents(:default, 0, ~U[2026-01-01 00:00:00Z])
    assert {:ok, listing_result} = listing(context, listing_request)
    assert listing_result.request == listing_request

    outbox_request = Outbox.read("dispatcher")
    assert {:ok, outbox_result} = outbox(context, outbox_request)
    assert outbox_result.request == outbox_request
  end
end
