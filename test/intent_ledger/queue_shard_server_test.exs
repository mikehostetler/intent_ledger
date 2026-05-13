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

  test "polling recovers work when an early wakeup is lost before lease ownership" do
    name = Module.concat(__MODULE__, "LostWakeupLedger#{System.unique_integer([:positive])}")
    store_name = Names.store(name)

    start_supervised!({Registry, keys: :unique, name: Names.registry(name)})
    start_supervised!({@store, name: store_name})

    start_supervised!(
      {IntentLedger.Server, name: name, store: {@store, store_name}, queues: [default: [shards: 1]], lease_ms: 250}
    )

    assert {:ok, _lease} = shard_lease(name, :acquire, "other-owner")

    shard_pid =
      start_supervised!(
        {QueueShardServer,
         name: name,
         store: {@store, store_name},
         queue: :default,
         shard: 0,
         owner_id: "runtime-owner",
         lease_ms: 250,
         lease_retry_ms: 10,
         poll_interval_ms: 10}
      )

    wait_until(fn ->
      case QueueShardServer.state(shard_pid).lease_until do
        nil -> true
        _lease_until -> nil
      end
    end)

    assert {:ok, record} =
             IntentLedger.submit(name, %{
               key: "runtime-lost-wakeup",
               kind: "test.runtime_lost_wakeup",
               shard: 0
             })

    QueueShardServer.wake(shard_pid)
    QueueShardServer.state(shard_pid)

    assert {:ok, available} = IntentLedger.get(name, record.intent.id)
    assert available.state.status == :available

    assert {:ok, _released} = shard_lease(name, :release, "other-owner")

    claimed =
      wait_until(fn ->
        case IntentLedger.get(name, record.intent.id) do
          {:ok, record} when record.state.status == :claimed -> record
          _not_claimed_yet -> nil
        end
      end)

    assert claimed.state.claim_id

    shard_state = QueueShardServer.state(shard_pid)
    assert shard_state.claimed_count >= 1
    assert [%{owner_id: "runtime-owner"} | _rest] = shard_state.last_claimed
  end

  test "delayed visibility is not claimed before its visible time" do
    name = Module.concat(__MODULE__, "DelayedVisibilityLedger#{System.unique_integer([:positive])}")

    start_supervised!(
      {IntentLedger,
       name: name,
       queues: [default: [shards: 1]],
       lease_ms: 250,
       lease_renew_ms: 50,
       poll_interval_ms: 10,
       wakeups?: true}
    )

    [{shard_pid, _}] = Registry.lookup(Names.registry(name), Names.queue_shard(:default, 0))
    wait_until(fn -> QueueShardServer.state(shard_pid).lease_until end)

    visible_at = DateTime.add(DateTime.utc_now(), 250, :millisecond)

    assert {:ok, record} =
             IntentLedger.submit(name, %{
               key: "runtime-delayed-visibility",
               kind: "test.runtime_delayed_visibility",
               shard: 0,
               visible_at: visible_at
             })

    QueueShardServer.wake(shard_pid)
    QueueShardServer.state(shard_pid)

    assert {:ok, available} = IntentLedger.get(name, record.intent.id)
    assert available.state.status == :available

    claimed =
      wait_until(
        fn ->
          case IntentLedger.get(name, record.intent.id) do
            {:ok, record} when record.state.status == :claimed -> record
            _not_claimed_yet -> nil
          end
        end,
        150
      )

    assert DateTime.compare(claimed.state.updated_at, visible_at) in [:eq, :gt]
    assert claimed.state.claim_id
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

  defp shard_lease(name, operation, owner_id) do
    now = DateTime.utc_now()

    @store.lease(
      Names.store(name),
      name,
      {:shard, operation,
       %{
         queue: :default,
         shard: 0,
         owner_id: owner_id,
         lease_until: DateTime.add(now, 250, :millisecond),
         now: now
       }},
      []
    )
  end
end
