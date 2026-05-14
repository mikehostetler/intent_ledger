defmodule IntentLedger.TelemetryTest do
  use ExUnit.Case, async: true

  alias IntentLedger.Store.CommitRequest
  alias IntentLedger.Telemetry

  def handle_event(event, measurements, metadata, parent) do
    send(parent, {:telemetry, event, measurements, metadata})
  end

  @events [
    :command_start,
    :command_stop,
    :command_exception,
    :store_commit_start,
    :store_commit_stop,
    :store_commit_exception,
    :store_conflict,
    :claim_stop,
    :shard_lease_stop,
    :recovery_stop,
    :outbox_read_stop,
    :outbox_ack_stop,
    :dispatcher_stop,
    :replay_stop,
    :projection_stop,
    :inspection_stop
  ]

  test "catalogue defines stable telemetry event names" do
    assert Telemetry.events() == @events
    assert Telemetry.default_prefix() == [:intent_ledger]

    assert Telemetry.fetch!(:command_start).name == [:command, :start]
    assert Telemetry.fetch!(:command_stop).name == [:command, :stop]
    assert Telemetry.fetch!(:store_commit_stop).name == [:store, :commit, :stop]
    assert Telemetry.fetch!(:store_conflict).name == [:store, :conflict]
    assert Telemetry.fetch!(:inspection_stop).name == [:inspection, :stop]

    for definition <- Telemetry.all() do
      assert Telemetry.fetch!(definition.event) == definition
      assert Telemetry.fetch!(definition.name) == definition
      assert Telemetry.event_name(definition.event) == [:intent_ledger | definition.name]
      assert is_list(definition.measurements)
      assert is_list(definition.required_metadata)
      assert is_list(definition.optional_metadata)
    end
  end

  test "metadata policy allows operational identifiers and excludes sensitive fields" do
    policy = Telemetry.metadata_policy()

    assert :ledger in policy.required
    assert :ledger in policy.allowed
    assert :command_id in policy.allowed
    assert :intent_id in policy.allowed
    assert :correlation_id in policy.lineage
    assert :root_intent_id in policy.lineage
    assert :duration in Map.keys(policy.measurement_units)
    assert policy.measurement_units.duration == :native
    assert policy.measurement_units.lag_ms == :millisecond

    for sensitive <- [:payload, :command_payload, :result, :error, :token, :token_hash, :headers] do
      assert sensitive in policy.sensitive
      refute sensitive in policy.allowed
    end
  end

  test "event definitions only advertise allowed metadata fields" do
    allowed = MapSet.new(Telemetry.allowed_metadata_fields())

    for definition <- Telemetry.all() do
      metadata_fields = definition.required_metadata ++ definition.optional_metadata

      assert Enum.all?(metadata_fields, &MapSet.member?(allowed, &1))
      assert Enum.all?(definition.required_metadata, &(&1 in metadata_fields))
    end
  end

  test "execute emits catalogue events under a configured prefix" do
    event_name = Telemetry.event_name(:command_stop, telemetry_prefix: [:my_app, :intent_ledger])
    handler_id = {__MODULE__, self(), :command_stop}
    parent = self()

    :ok = :telemetry.attach(handler_id, event_name, &__MODULE__.handle_event/4, parent)

    try do
      :ok =
        Telemetry.execute(
          [telemetry_prefix: [:my_app, :intent_ledger]],
          :command_stop,
          %{duration: 10, count: 1},
          %{ledger: MyApp.IntentLedger, operation: :submit, status: :ok}
        )

      assert_receive {:telemetry, ^event_name, %{duration: 10, count: 1},
                      %{ledger: MyApp.IntentLedger, operation: :submit, status: :ok}}
    after
      :telemetry.detach(handler_id)
    end
  end

  test "public commands emit command handling telemetry" do
    ledger = unique_atom(:command_telemetry_ledger)
    prefix = [unique_atom(:command_telemetry_prefix)]
    start_event = Telemetry.event_name(:command_start, telemetry_prefix: prefix)
    stop_event = Telemetry.event_name(:command_stop, telemetry_prefix: prefix)
    handler_id = attach_events([start_event, stop_event])

    start_supervised!({IntentLedger, name: ledger, store: IntentLedger.Store.Memory, telemetry_prefix: prefix})

    try do
      assert {:ok, _record} =
               IntentLedger.submit(
                 ledger,
                 %{key: "job:telemetry", kind: "job.run"},
                 command_id: "cmd_telemetry_submit",
                 actor: "worker-1",
                 correlation_id: "corr_1"
               )

      assert_receive {:telemetry, ^start_event, %{system_time: system_time}, start_metadata}
      assert is_integer(system_time)
      assert start_metadata.ledger == ledger
      assert start_metadata.operation == :submit
      assert start_metadata.command_id == "cmd_telemetry_submit"
      assert start_metadata.actor == "worker-1"
      assert start_metadata.correlation_id == "corr_1"
      assert start_metadata.depth == 0
      refute Map.has_key?(start_metadata, :payload)

      assert_receive {:telemetry, ^stop_event, %{duration: duration, count: 1}, stop_metadata}
      assert is_integer(duration)
      assert duration >= 0
      assert stop_metadata.status == :ok
      assert stop_metadata.replayed? == false
      assert stop_metadata.ledger == ledger
      assert stop_metadata.operation == :submit
      assert stop_metadata.command_id == "cmd_telemetry_submit"
      refute Map.has_key?(stop_metadata, :result)
    after
      :telemetry.detach(handler_id)
    end
  end

  test "store v1 commits emit commit telemetry" do
    store_name = unique_atom(:store_telemetry)
    prefix = [unique_atom(:store_telemetry_prefix)]
    start_event = Telemetry.event_name(:store_commit_start, telemetry_prefix: prefix)
    stop_event = Telemetry.event_name(:store_commit_stop, telemetry_prefix: prefix)
    handler_id = attach_events([start_event, stop_event])

    start_supervised!({IntentLedger.Store.Memory, name: store_name})

    request =
      CommitRequest.new(
        command_id: "cmd_store_telemetry",
        operation: :submit
      )

    try do
      assert {:ok, _commit} =
               IntentLedger.Store.Memory.commit(
                 store_name,
                 MyApp.IntentLedger,
                 request,
                 telemetry_prefix: prefix
               )

      assert_receive {:telemetry, ^start_event, %{system_time: system_time}, start_metadata}
      assert is_integer(system_time)
      assert start_metadata.ledger == MyApp.IntentLedger
      assert start_metadata.store == IntentLedger.Store.Memory
      assert start_metadata.operation == :submit
      assert start_metadata.command_id == "cmd_store_telemetry"

      assert_receive {:telemetry, ^stop_event, %{duration: duration, writes: 0, signals: 0, outbox_entries: 0},
                      stop_metadata}

      assert is_integer(duration)
      assert duration >= 0
      assert stop_metadata.status == :ok
      assert stop_metadata.replayed? == false
      assert stop_metadata.store == IntentLedger.Store.Memory
      refute Map.has_key?(stop_metadata, :result)
    after
      :telemetry.detach(handler_id)
    end
  end

  test "claims emit claim telemetry" do
    ledger = unique_atom(:claim_telemetry_ledger)
    prefix = [unique_atom(:claim_telemetry_prefix)]
    claim_event = Telemetry.event_name(:claim_stop, telemetry_prefix: prefix)
    handler_id = attach_events([claim_event])

    start_supervised!({IntentLedger, name: ledger, store: IntentLedger.Store.Memory, telemetry_prefix: prefix})

    assert {:ok, _record} =
             IntentLedger.submit(ledger, %{key: "job:claim-telemetry", kind: "job.run"},
               command_id: "cmd_claim_telemetry_submit"
             )

    try do
      assert {:ok, claimed} = IntentLedger.claim(ledger, :default, "worker-1", command_id: "cmd_claim_telemetry")

      assert_receive {:telemetry, ^claim_event, %{duration: duration, count: 1}, metadata}
      assert is_integer(duration)
      assert duration >= 0
      assert metadata.ledger == ledger
      assert metadata.queue == "default"
      assert metadata.owner_id == "worker-1"
      assert metadata.status == :ok
      assert metadata.limit == 1
      assert metadata.intent_id == claimed.intent.id
      assert metadata.claim_id == claimed.claim.id
    after
      :telemetry.detach(handler_id)
    end
  end

  test "recover emits recovery telemetry" do
    ledger = unique_atom(:recovery_telemetry_ledger)
    prefix = [unique_atom(:recovery_telemetry_prefix)]
    recovery_event = Telemetry.event_name(:recovery_stop, telemetry_prefix: prefix)
    handler_id = attach_events([recovery_event])

    start_supervised!({IntentLedger, name: ledger, store: IntentLedger.Store.Memory, telemetry_prefix: prefix})

    try do
      assert {:ok, []} = IntentLedger.recover(ledger, :default, command_id: "cmd_recovery_telemetry", limit: 1)

      assert_receive {:telemetry, ^recovery_event, %{duration: duration, count: 0}, metadata}
      assert is_integer(duration)
      assert duration >= 0
      assert metadata.ledger == ledger
      assert metadata.queue == "default"
      assert metadata.status == :ok
      assert metadata.limit == 1
    after
      :telemetry.detach(handler_id)
    end
  end

  test "shard leases emit lease and conflict telemetry" do
    store_name = unique_atom(:lease_telemetry_store)
    prefix = [unique_atom(:lease_telemetry_prefix)]
    lease_event = Telemetry.event_name(:shard_lease_stop, telemetry_prefix: prefix)
    conflict_event = Telemetry.event_name(:store_conflict, telemetry_prefix: prefix)
    handler_id = attach_events([lease_event, conflict_event])

    start_supervised!({IntentLedger.Store.Memory, name: store_name})

    now = DateTime.utc_now()

    acquire =
      {:shard, :acquire,
       %{
         queue: "default",
         shard: 0,
         owner_id: "owner-1",
         lease_until: DateTime.add(now, 30_000, :millisecond),
         now: now
       }}

    conflicting_acquire =
      {:shard, :acquire,
       %{
         queue: "default",
         shard: 0,
         owner_id: "owner-2",
         lease_until: DateTime.add(now, 30_000, :millisecond),
         now: now
       }}

    try do
      assert {:ok, _lease} =
               IntentLedger.Store.Memory.lease(store_name, MyApp.IntentLedger, acquire, telemetry_prefix: prefix)

      assert_receive {:telemetry, ^lease_event, %{duration: ok_duration}, ok_metadata}
      assert is_integer(ok_duration)
      assert ok_duration >= 0
      assert ok_metadata.ledger == MyApp.IntentLedger
      assert ok_metadata.store == IntentLedger.Store.Memory
      assert ok_metadata.operation == :acquire
      assert ok_metadata.queue == "default"
      assert ok_metadata.shard == 0
      assert ok_metadata.owner_id == "owner-1"
      assert ok_metadata.status == :ok

      assert {:error, _conflict} =
               IntentLedger.Store.Memory.lease(store_name, MyApp.IntentLedger, conflicting_acquire,
                 telemetry_prefix: prefix
               )

      assert_receive {:telemetry, ^lease_event, %{duration: conflict_duration}, conflict_metadata}
      assert is_integer(conflict_duration)
      assert conflict_duration >= 0
      assert conflict_metadata.status == :error
      assert conflict_metadata.conflict == :shard_lease
      assert conflict_metadata.owner_id == "owner-2"

      assert_receive {:telemetry, ^conflict_event, %{count: 1}, store_conflict_metadata}
      assert store_conflict_metadata.ledger == MyApp.IntentLedger
      assert store_conflict_metadata.store == IntentLedger.Store.Memory
      assert store_conflict_metadata.operation == :acquire
      assert store_conflict_metadata.conflict == :shard_lease
      assert store_conflict_metadata.queue == "default"
      assert store_conflict_metadata.shard == 0
    after
      :telemetry.detach(handler_id)
    end
  end

  defp attach_events(events) do
    handler_id = {__MODULE__, self(), make_ref()}

    :ok = :telemetry.attach_many(handler_id, events, &__MODULE__.handle_event/4, self())

    handler_id
  end

  defp unique_atom(prefix) do
    String.to_atom("#{prefix}_#{System.unique_integer([:positive])}")
  end
end
