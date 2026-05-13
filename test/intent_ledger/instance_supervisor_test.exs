defmodule IntentLedger.InstanceSupervisorTest do
  use ExUnit.Case, async: false

  alias IntentLedger.{Instance, InstanceSupervisor, Names, QueueShardServer}

  @store IntentLedger.Store.Memory

  test "builds a stable child spec for a named ledger" do
    name = unique_name("ChildSpecLedger")

    assert %{
             id: {InstanceSupervisor, ^name},
             start: {InstanceSupervisor, :start_link, [[name: ^name, shutdown: 1_000]]},
             type: :supervisor,
             restart: :permanent,
             shutdown: 1_000
           } = InstanceSupervisor.child_spec(name: name, shutdown: 1_000)
  end

  test "supervises a named ledger with private registry and store processes" do
    name = unique_name("RuntimeLedger")
    on_exit(fn -> Instance.stop(name) end)

    assert {:ok, supervisor} =
             InstanceSupervisor.start_link(
               name: name,
               queues: [default: [shards: 2]],
               lease_ms: 10_000
             )

    assert Process.whereis(Names.supervisor(name)) == supervisor
    assert Process.whereis(Names.registry(name))
    assert Process.whereis(Names.notifier(name))
    assert Process.whereis(Names.store(name))
    assert Process.whereis(Names.queue_supervisor(name))
    assert Process.whereis(Names.recovery_server(name))
    assert Process.whereis(name)
    assert [{_default_0, _}] = Registry.lookup(Names.registry(name), Names.queue_shard(:default, 0))
    assert [{_default_1, _}] = Registry.lookup(Names.registry(name), Names.queue_shard(:default, 1))

    assert {:ok, record} =
             IntentLedger.submit(name, %{
               key: "job:instance-supervisor",
               kind: "test.instance_supervisor"
             })

    assert {:ok, fetched} = IntentLedger.get(name, record.intent.id)
    assert fetched.intent.id == record.intent.id
    assert fetched.state.status == :available
  end

  test "releases queue shard leases on shutdown and reacquires after restart" do
    name = unique_name("RestartLedger")

    start_supervised!(
      {IntentLedger,
       name: name, queues: [default: [shards: 1]], lease_ms: 250, lease_renew_ms: 50, poll_interval_ms: 10_000}
    )

    [{shard_pid, _}] = Registry.lookup(Names.registry(name), Names.queue_shard(:default, 0))
    wait_until(fn -> QueueShardServer.state(shard_pid).lease_until end)

    queue_supervisor = Process.whereis(Names.queue_supervisor(name))
    child_id = {QueueShardServer, name, "default", 0}

    assert :ok = Supervisor.terminate_child(queue_supervisor, child_id)
    assert [] = Registry.lookup(Names.registry(name), Names.queue_shard(:default, 0))

    assert {:ok, probe_lease} = acquire_probe_lease(name)
    assert probe_lease.owner_id == "probe-owner"
    assert {:ok, _released} = release_probe_lease(name)

    assert {:ok, restarted_pid} = Supervisor.restart_child(queue_supervisor, child_id)
    refute restarted_pid == shard_pid

    restarted_pid =
      wait_until(fn ->
        case Registry.lookup(Names.registry(name), Names.queue_shard(:default, 0)) do
          [{pid, _}] when pid == restarted_pid -> pid
          _missing_or_old -> nil
        end
      end)

    wait_until(fn -> QueueShardServer.state(restarted_pid).lease_until end)

    assert {:ok, record} =
             IntentLedger.submit(name, %{
               key: "job:restarted-shard",
               kind: "test.instance_supervisor.restart",
               shard: 0
             })

    QueueShardServer.wake(restarted_pid)

    claimed =
      wait_until(fn ->
        case IntentLedger.get(name, record.intent.id) do
          {:ok, record} when record.state.status == :claimed -> record
          _not_claimed_yet -> nil
        end
      end)

    assert claimed.state.claim_id
  end

  defp unique_name(prefix) do
    Module.concat(__MODULE__, "#{prefix}#{System.unique_integer([:positive])}")
  end

  defp acquire_probe_lease(name) do
    now = DateTime.utc_now()

    @store.lease(
      Names.store(name),
      name,
      {:shard, :acquire,
       %{
         queue: :default,
         shard: 0,
         owner_id: "probe-owner",
         lease_until: DateTime.add(now, 250, :millisecond),
         now: now
       }},
      []
    )
  end

  defp release_probe_lease(name) do
    now = DateTime.utc_now()

    @store.lease(
      Names.store(name),
      name,
      {:shard, :release,
       %{
         queue: :default,
         shard: 0,
         owner_id: "probe-owner",
         lease_until: DateTime.add(now, 250, :millisecond),
         now: now
       }},
      []
    )
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

  defp wait_until(_fun, 0), do: flunk("timed out waiting for instance supervisor")
end
