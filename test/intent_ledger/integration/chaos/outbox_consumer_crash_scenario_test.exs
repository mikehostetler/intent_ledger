defmodule IntentLedger.Chaos.OutboxConsumerCrashScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :chaos

  alias Bedrock.JobQueue.Internal

  defmodule DispatchableHandler do
    use IntentLedger.Handler, topic: "outbox.dispatchable"

    @impl true
    def handle(%{test_pid: test_pid}, ctx) do
      send(test_pid, {:dispatchable_handled, ctx.intent.id})
      {:ok, %{handled: true}}
    end
  end

  defmodule OutboxIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{"outbox.dispatchable" => [handler: DispatchableHandler]}
  end

  setup do
    IntentLedger.RealBedrock.setup!()
  end

  test "consumer crash before ack redelivers the same outbox batch after restart" do
    start_runtime!()

    assert {:ok, intent} = OutboxIntents.enqueue("outbox.dispatchable", %{test_pid: self()})
    assert_receive {:dispatchable_handled, intent_id}
    assert intent_id == intent.id

    assert_eventually(fn ->
      case OutboxIntents.fetch(intent.id) do
        {:ok, completed} -> completed.status == :completed and completed.result == %{handled: true}
        _other -> false
      end
    end)

    assert {:ok, first_batch} = OutboxIntents.read_outbox("dispatcher-a", limit: 10)
    first_facts = Enum.map(first_batch.entries, &{&1.cursor, &1.signal.type, &1.signal.subject})

    assert [
             {1, "intent.enqueued", ^intent_id},
             {2, "intent.started", ^intent_id},
             {3, "intent.completed", ^intent_id}
           ] = first_facts

    assert {:ok, redelivered_batch} = OutboxIntents.read_outbox("dispatcher-a", limit: 10)
    assert Enum.map(redelivered_batch.entries, &{&1.cursor, &1.signal.type, &1.signal.subject}) == first_facts

    assert {:ok, %{cursor: 3}} = OutboxIntents.ack_outbox("dispatcher-a", redelivered_batch.next_cursor)
    assert {:ok, drained} = OutboxIntents.read_outbox("dispatcher-a", limit: 10)
    assert drained.entries == []
    assert drained.acked_cursor == 3
  end

  defp start_runtime!, do: start_supervised!({OutboxIntents, root: queue_root(), scan_interval: 10})

  defp queue_root, do: Internal.root_keyspace(OutboxIntents.JobQueue)

  defp assert_eventually(fun, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(fun, deadline)
  end

  defp do_assert_eventually(fun, deadline) do
    if fun.() do
      assert true
    else
      if System.monotonic_time(:millisecond) >= deadline do
        assert fun.()
      else
        Process.sleep(20)
        do_assert_eventually(fun, deadline)
      end
    end
  end
end
