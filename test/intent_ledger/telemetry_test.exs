defmodule IntentLedger.TelemetryTest do
  use ExUnit.Case, async: true

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
end
