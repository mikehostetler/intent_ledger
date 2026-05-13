defmodule IntentLedger.NotifierTest do
  use ExUnit.Case, async: false

  alias IntentLedger.{Names, Notifier, QueueShardServer}

  test "wakes the local shard worker when visible work is submitted" do
    name = Module.concat(__MODULE__, "RuntimeLedger#{System.unique_integer([:positive])}")

    start_supervised!(
      {IntentLedger,
       name: name,
       queues: [default: [shards: 1]],
       lease_ms: 100,
       lease_renew_ms: 20,
       poll_interval_ms: 10_000,
       recovery_interval_ms: 10_000,
       wakeups?: true}
    )

    [{shard_pid, _}] = Registry.lookup(Names.registry(name), Names.queue_shard(:default, 0))
    wait_until(fn -> QueueShardServer.state(shard_pid).lease_until end)

    assert {:ok, record} =
             IntentLedger.submit(name, %{
               key: "notifier:wakeup",
               kind: "test.notifier",
               shard: 0
             })

    claimed =
      wait_until(fn ->
        case IntentLedger.get(name, record.intent.id) do
          {:ok, record} when record.state.status == :claimed -> record
          _not_claimed_yet -> nil
        end
      end)

    assert claimed.state.claim_id
    assert QueueShardServer.state(shard_pid).claimed_count >= 1
  end

  test "ignores wakeups when a notifier is not running" do
    assert :ok = Notifier.wake(Module.concat(__MODULE__, :MissingLedger), :default, 0)
  end

  defp wait_until(fun, attempts \\ 50)

  defp wait_until(fun, attempts) when attempts > 0 do
    case fun.() do
      nil ->
        Process.sleep(5)
        wait_until(fun, attempts - 1)

      value ->
        value
    end
  end

  defp wait_until(_fun, 0), do: flunk("timed out waiting for notifier")
end
