defmodule IntentLedger.QueueSupervisorTest do
  use ExUnit.Case, async: true

  alias IntentLedger.{QueueShardServer, QueueSupervisor}

  test "builds a shard child spec for each configured queue shard" do
    name = Module.concat(__MODULE__, :SpecLedger)

    specs =
      QueueSupervisor.shard_child_specs(
        name: name,
        store: {IntentLedger.Store.Memory, :store_ref},
        queues: [default: [shards: 2], critical: [shards: 1]],
        lease_ms: 15_000
      )

    assert specs |> Enum.map(& &1.id) |> Enum.sort() ==
             [
               {QueueShardServer, name, "critical", 0},
               {QueueShardServer, name, "default", 0},
               {QueueShardServer, name, "default", 1}
             ]

    assert Enum.all?(specs, &match?(%{start: {QueueShardServer, :start_link, [_]}}, &1))
  end
end
