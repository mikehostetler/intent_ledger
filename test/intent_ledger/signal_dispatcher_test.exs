defmodule IntentLedger.SignalDispatcherTest do
  use ExUnit.Case, async: false

  alias IntentLedger.{Names, SignalDispatcher}
  alias IntentLedger.Store.Outbox

  defmodule TestHandler do
    @behaviour IntentLedger.SignalHandler

    @impl true
    def handle_signal(entry, context) do
      send(Keyword.fetch!(context.opts, :test_pid), {:handled_signal, entry.signal.type, context})
      :ok
    end
  end

  defmodule FailingHandler do
    @behaviour IntentLedger.SignalHandler

    @impl true
    def handle_signal(entry, context) do
      send(Keyword.fetch!(context.opts, :test_pid), {:failed_signal_attempt, entry.key})
      {:error, :boom}
    end
  end

  defmodule RaisingHandler do
    @behaviour IntentLedger.SignalHandler

    @impl true
    def handle_signal(entry, context) do
      send(Keyword.fetch!(context.opts, :test_pid), {:raising_signal_attempt, entry.key})
      raise "handler boom"
    end
  end

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

  test "dispatches polled entries to registered signal handlers" do
    name = Module.concat(__MODULE__, "HandlerLedger#{System.unique_integer([:positive])}")

    start_supervised!(
      {IntentLedger,
       name: name,
       queues: [default: [shards: 1]],
       dispatcher_interval_ms: 10_000,
       signal_handlers: [{TestHandler, test_pid: self()}]}
    )

    assert {:ok, _record} =
             IntentLedger.submit(name, %{
               key: "dispatcher:handler",
               kind: "test.signal_handler",
               shard: 0
             })

    dispatcher = Process.whereis(Names.signal_dispatcher(name))

    assert {:ok, [_submitted, _available]} = SignalDispatcher.poll_once(dispatcher)

    assert_receive {:handled_signal, "intent_ledger.intent.submitted", %{ledger: ^name, handler: TestHandler}}
    assert_receive {:handled_signal, "intent_ledger.intent.available", %{ledger: ^name, handler: TestHandler}}

    state = SignalDispatcher.state(dispatcher)
    assert [%{module: TestHandler, opts: [test_pid: _pid]}] = state.handlers
    assert state.dispatched_count == 2
    assert state.acked_count == 2
    assert state.failed_count == 0
    assert state.last_errors == []

    assert {:ok, []} =
             IntentLedger.Store.Memory.outbox(
               Names.store(name),
               name,
               Outbox.read("intent_ledger.signal_dispatcher"),
               []
             )
  end

  test "supervised dispatcher restart resumes unacked outbox entries" do
    name = Module.concat(__MODULE__, "RestartLedger#{System.unique_integer([:positive])}")

    start_supervised!(
      {IntentLedger,
       name: name,
       queues: [default: [shards: 1]],
       dispatcher_interval_ms: 10_000,
       signal_handlers: [{TestHandler, test_pid: self()}]}
    )

    assert {:ok, _record} =
             IntentLedger.submit(name, %{
               key: "dispatcher:restart",
               kind: "test.signal_restart",
               shard: 0
             })

    old_dispatcher = Process.whereis(Names.signal_dispatcher(name))
    ref = Process.monitor(old_dispatcher)
    Process.exit(old_dispatcher, :kill)

    assert_receive {:DOWN, ^ref, :process, ^old_dispatcher, :killed}

    dispatcher = wait_for_restarted_dispatcher(name, old_dispatcher)

    assert {:ok, [_submitted, _available]} = SignalDispatcher.poll_once(dispatcher)

    assert_receive {:handled_signal, "intent_ledger.intent.submitted", %{ledger: ^name, handler: TestHandler}}
    assert_receive {:handled_signal, "intent_ledger.intent.available", %{ledger: ^name, handler: TestHandler}}

    assert {:ok, []} =
             IntentLedger.Store.Memory.outbox(
               Names.store(name),
               name,
               Outbox.read("intent_ledger.signal_dispatcher"),
               []
             )
  end

  test "leaves failed handler entries unacked and backs off retries" do
    name = Module.concat(__MODULE__, "RetryLedger#{System.unique_integer([:positive])}")

    start_supervised!(
      {IntentLedger,
       name: name,
       queues: [default: [shards: 1]],
       dispatcher_interval_ms: 10_000,
       dispatcher_retry_ms: 20,
       dispatcher_max_retry_ms: 20,
       signal_handlers: [{FailingHandler, test_pid: self()}]}
    )

    assert {:ok, _record} =
             IntentLedger.submit(name, %{
               key: "dispatcher:retry",
               kind: "test.signal_retry",
               shard: 0
             })

    dispatcher = Process.whereis(Names.signal_dispatcher(name))

    assert {:ok, entries} = SignalDispatcher.poll_once(dispatcher)
    keys = Enum.map(entries, & &1.key)

    assert_receive {:failed_signal_attempt, first_key}
    assert_receive {:failed_signal_attempt, second_key}
    assert Enum.sort([first_key, second_key]) == Enum.sort(keys)

    state = SignalDispatcher.state(dispatcher)
    assert state.failed_count == 2
    assert map_size(state.retries) == 2
    assert Enum.all?(Map.values(state.retries), &(&1.retry_count == 1))

    assert {:ok, _entries} = SignalDispatcher.poll_once(dispatcher)
    refute_receive {:failed_signal_attempt, _key}, 30

    state = SignalDispatcher.state(dispatcher)
    assert state.skipped_count == 2
    assert Enum.map(state.last_skipped, & &1.key) == keys

    Process.sleep(25)

    assert {:ok, _entries} = SignalDispatcher.poll_once(dispatcher)
    assert_receive {:failed_signal_attempt, _first_retry}
    assert_receive {:failed_signal_attempt, _second_retry}

    state = SignalDispatcher.state(dispatcher)
    assert state.failed_count == 4
    assert Enum.all?(Map.values(state.retries), &(&1.retry_count == 2))
  end

  test "captures raised handler failures without acknowledging entries" do
    name = Module.concat(__MODULE__, "RaisingLedger#{System.unique_integer([:positive])}")

    start_supervised!(
      {IntentLedger,
       name: name,
       queues: [default: [shards: 1]],
       dispatcher_interval_ms: 10_000,
       signal_handlers: [{RaisingHandler, test_pid: self()}]}
    )

    assert {:ok, _record} =
             IntentLedger.submit(name, %{
               key: "dispatcher:raising",
               kind: "test.signal_raising",
               shard: 0
             })

    dispatcher = Process.whereis(Names.signal_dispatcher(name))

    assert {:ok, entries} = SignalDispatcher.poll_once(dispatcher)
    keys = Enum.map(entries, & &1.key)

    assert_receive {:raising_signal_attempt, first_key}
    assert_receive {:raising_signal_attempt, second_key}
    assert Enum.sort([first_key, second_key]) == Enum.sort(keys)

    state = SignalDispatcher.state(dispatcher)
    assert state.failed_count == 2
    assert map_size(state.retries) == 2

    assert Enum.all?(state.last_errors, fn
             {RaisingHandler, {:error, %RuntimeError{message: "handler boom"}}} -> true
             _error -> false
           end)

    assert {:ok, unacked} =
             IntentLedger.Store.Memory.outbox(
               Names.store(name),
               name,
               Outbox.read("intent_ledger.signal_dispatcher"),
               []
             )

    assert Enum.map(unacked, & &1.key) == keys
  end

  defp wait_for_restarted_dispatcher(name, old_pid, attempts \\ 50)

  defp wait_for_restarted_dispatcher(name, old_pid, attempts) when attempts > 0 do
    case Process.whereis(Names.signal_dispatcher(name)) do
      pid when is_pid(pid) and pid != old_pid ->
        pid

      _not_restarted ->
        Process.sleep(10)
        wait_for_restarted_dispatcher(name, old_pid, attempts - 1)
    end
  end

  defp wait_for_restarted_dispatcher(name, old_pid, 0) do
    flunk("expected #{inspect(Names.signal_dispatcher(name))} to restart after #{inspect(old_pid)} stopped")
  end
end
