defmodule IntentLedger.InstanceSupervisorTest do
  use ExUnit.Case, async: false

  alias IntentLedger.{Instance, InstanceSupervisor, Names}

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

  defp unique_name(prefix) do
    Module.concat(__MODULE__, "#{prefix}#{System.unique_integer([:positive])}")
  end
end
