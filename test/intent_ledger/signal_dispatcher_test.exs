defmodule IntentLedger.SignalDispatcherTest do
  use ExUnit.Case, async: false

  alias IntentLedger.{Names, SignalDispatcher}

  test "polls durable outbox entries for a supervised ledger" do
    name = Module.concat(__MODULE__, "Ledger#{System.unique_integer([:positive])}")

    start_supervised!(
      {IntentLedger,
       name: name, queues: [default: [shards: 1]], dispatcher_interval_ms: 10_000, dispatcher_batch_size: 10}
    )

    assert {:ok, record} =
             IntentLedger.submit(name, %{
               key: "dispatcher:poll",
               kind: "test.signal_dispatcher",
               shard: 0
             })

    dispatcher = Process.whereis(Names.signal_dispatcher(name))

    assert {:ok, entries} = SignalDispatcher.poll_once(dispatcher)
    assert Enum.map(entries, & &1.signal.type) == ["intent_ledger.intent.submitted", "intent_ledger.intent.available"]
    assert Enum.all?(entries, &(&1.stream == "intent:" <> record.intent.id))

    state = SignalDispatcher.state(dispatcher)
    assert state.poll_count == 1
    assert state.read_count == 2
    assert Enum.map(state.last_entries, & &1.key) == Enum.map(entries, & &1.key)
    assert state.last_error == nil
  end
end
