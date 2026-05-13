defmodule IntentLedger.RecoveryServerTest do
  use ExUnit.Case, async: false

  alias IntentLedger.{Names, RecoveryServer}

  @store IntentLedger.Store.Memory

  test "recovers expired claims on its timer" do
    name = Module.concat(__MODULE__, "RuntimeLedger#{System.unique_integer([:positive])}")
    now = DateTime.add(DateTime.utc_now(), -50, :millisecond)

    start_supervised!(
      {IntentLedger,
       name: name, queues: [default: [shards: 1]], lease_ms: 100, poll_interval_ms: 10_000, recovery_interval_ms: 10}
    )

    assert {:ok, record} =
             IntentLedger.submit(
               name,
               %{key: "recovery:expired-claim", kind: "test.recovery", shard: 0},
               now: now
             )

    assert {:ok, claimed} = IntentLedger.claim(name, :default, "worker-a", now: now, lease_ms: 1)
    assert claimed.intent.id == record.intent.id

    recovered =
      wait_until(fn ->
        case IntentLedger.get(name, record.intent.id) do
          {:ok, record} when record.state.status == :available -> record
          _not_recovered_yet -> nil
        end
      end)

    assert recovered.state.claim_id == nil

    recovery = RecoveryServer.state(Process.whereis(Names.recovery_server(name)))
    assert recovery.recovered_count >= 1
  end

  test "expires stale shard lease rows on its timer" do
    name = Module.concat(__MODULE__, "LeaseLedger#{System.unique_integer([:positive])}")
    store_name = Names.store(name)
    now = DateTime.add(DateTime.utc_now(), -50, :millisecond)

    start_supervised!({@store, name: store_name})

    start_supervised!(
      {IntentLedger.Server, name: name, store: {@store, store_name}, queues: [default: [shards: 1]], lease_ms: 100}
    )

    assert {:ok, _lease} =
             @store.lease(
               store_name,
               name,
               {:shard, :acquire,
                %{
                  queue: :default,
                  shard: 0,
                  owner_id: "dead-owner",
                  lease_until: DateTime.add(now, 1, :millisecond),
                  now: now
                }},
               []
             )

    {:ok, recovery} =
      RecoveryServer.start_link(
        name: name,
        store: {@store, store_name},
        queues: [default: [shards: 1]],
        recovery_interval_ms: 10
      )

    recovered = wait_until(fn -> RecoveryServer.state(recovery).expired_leases_count end)
    assert recovered >= 1
  end

  defp wait_until(fun, attempts \\ 50)

  defp wait_until(fun, attempts) when attempts > 0 do
    case fun.() do
      nil ->
        Process.sleep(5)
        wait_until(fun, attempts - 1)

      0 ->
        Process.sleep(5)
        wait_until(fun, attempts - 1)

      value ->
        value
    end
  end

  defp wait_until(_fun, 0), do: flunk("timed out waiting for recovery server")
end
