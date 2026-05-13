defmodule IntentLedger.QueueShardServerTest do
  use ExUnit.Case, async: false

  alias IntentLedger.{Names, QueueShardServer}

  @store IntentLedger.Store.Memory

  test "acquires and renews its queue shard lease" do
    name = Module.concat(__MODULE__, "Ledger#{System.unique_integer([:positive])}")
    store_name = Names.store(name)

    start_supervised!({Registry, keys: :unique, name: Names.registry(name)})
    start_supervised!({@store, name: store_name})

    {:ok, pid} =
      QueueShardServer.start_link(
        name: name,
        store: {@store, store_name},
        queue: :default,
        shard: 0,
        owner_id: "owner-a",
        lease_ms: 100,
        lease_renew_ms: 10,
        lease_retry_ms: 10
      )

    first = wait_until(fn -> QueueShardServer.state(pid).lease_until end)

    assert {:error, %IntentLedger.Store.Conflict{type: :shard_lease}} =
             @store.lease(
               store_name,
               name,
               {:shard, :acquire,
                %{
                  queue: :default,
                  shard: 0,
                  owner_id: "owner-b",
                  lease_until: DateTime.add(DateTime.utc_now(), 100, :millisecond),
                  now: DateTime.utc_now()
                }},
               []
             )

    renewed =
      wait_until(fn ->
        case QueueShardServer.state(pid).lease_until do
          lease_until when lease_until != first -> lease_until
          _same_or_missing -> nil
        end
      end)

    assert DateTime.compare(renewed, first) == :gt
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

  defp wait_until(_fun, 0), do: flunk("timed out waiting for queue shard server")
end
