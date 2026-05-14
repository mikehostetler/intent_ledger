defmodule IntentLedger.TelemetryBedrockTest do
  use ExUnit.Case, async: true

  @moduletag :integration
  @moduletag :bedrock

  alias IntentLedger.Store.CommitRequest
  alias IntentLedger.Telemetry

  defmodule BedrockTelemetryRepo do
    @moduledoc false

    use Agent

    def start_link(_opts), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

    def transact(fun, _opts) do
      Agent.get_and_update(__MODULE__, fn values ->
        try do
          Process.put(:values, values)

          result = fun.(__MODULE__)

          next_values =
            case result do
              {:error, _reason} -> values
              _success -> Process.get(:values, values)
            end

          {result, next_values}
        after
          Process.delete(:values)
        end
      end)
    end

    def put(key, value) do
      Process.put(:values, Map.put(Process.get(:values, %{}), key, value))
      :ok
    end

    def clear(key) do
      Process.put(:values, Map.delete(Process.get(:values, %{}), key))
      :ok
    end

    def get(key), do: Map.get(Process.get(:values, %{}), key)

    def get_range({start_key, end_key}) do
      Process.get(:values, %{})
      |> Enum.filter(fn {key, _value} -> key >= start_key and key < end_key end)
      |> Enum.sort_by(fn {key, _value} -> key end)
    end

    def add_read_conflict_key(_key), do: :ok
    def add_write_conflict_range(_range), do: :ok
  end

  test "bedrock store v1 commits emit commit telemetry" do
    store_name = unique_atom(:bedrock_store_telemetry)
    prefix = [unique_atom(:bedrock_store_telemetry_prefix)]
    start_event = Telemetry.event_name(:store_commit_start, telemetry_prefix: prefix)
    stop_event = Telemetry.event_name(:store_commit_stop, telemetry_prefix: prefix)
    handler_id = attach_events([start_event, stop_event])

    start_supervised!(BedrockTelemetryRepo)
    start_supervised!({IntentLedger.Store.Bedrock, name: store_name, repo: BedrockTelemetryRepo})

    request =
      CommitRequest.new(
        command_id: "cmd_bedrock_store_telemetry",
        operation: :inspection_seed
      )

    try do
      assert {:ok, _commit} =
               IntentLedger.Store.Bedrock.commit(
                 store_name,
                 MyApp.IntentLedger,
                 request,
                 telemetry_prefix: prefix
               )

      assert_receive {:telemetry, ^start_event, %{system_time: system_time}, start_metadata}
      assert is_integer(system_time)
      assert start_metadata.ledger == MyApp.IntentLedger
      assert start_metadata.store == IntentLedger.Store.Bedrock
      assert start_metadata.operation == :inspection_seed
      assert start_metadata.command_id == "cmd_bedrock_store_telemetry"

      assert_receive {:telemetry, ^stop_event, %{duration: duration, writes: 0, signals: 0, outbox_entries: 0},
                      stop_metadata}

      assert is_integer(duration)
      assert duration >= 0
      assert stop_metadata.status == :ok
      assert stop_metadata.store == IntentLedger.Store.Bedrock
      assert stop_metadata.command_id == "cmd_bedrock_store_telemetry"
    after
      :telemetry.detach(handler_id)
    end
  end

  def handle_event(event, measurements, metadata, parent) do
    send(parent, {:telemetry, event, measurements, metadata})
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
