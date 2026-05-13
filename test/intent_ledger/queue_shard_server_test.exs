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

  test "polls due intents and claims a batch while holding the shard lease" do
    name = Module.concat(__MODULE__, "RuntimeLedger#{System.unique_integer([:positive])}")

    start_supervised!(
      {IntentLedger,
       name: name,
       queues: [default: [shards: 1]],
       lease_ms: 100,
       lease_renew_ms: 20,
       poll_interval_ms: 10,
       claim_batch_size: 2}
    )

    assert {:ok, first} =
             IntentLedger.submit(name, %{
               key: "runtime-poll:1",
               kind: "test.runtime_poll",
               shard: 0
             })

    assert {:ok, second} =
             IntentLedger.submit(name, %{
               key: "runtime-poll:2",
               kind: "test.runtime_poll",
               shard: 0
             })

    claimed =
      wait_until(fn ->
        with {:ok, first_record} <- IntentLedger.get(name, first.intent.id),
             {:ok, second_record} <- IntentLedger.get(name, second.intent.id),
             true <- first_record.state.status == :claimed,
             true <- second_record.state.status == :claimed do
          [first_record, second_record]
        else
          _not_claimed_yet -> nil
        end
      end)

    assert Enum.all?(claimed, &is_binary(&1.state.claim_id))

    [{shard_pid, _}] = Registry.lookup(Names.registry(name), Names.queue_shard(:default, 0))
    assert QueueShardServer.state(shard_pid).claimed_count >= 2
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
