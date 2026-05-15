defmodule IntentLedger.ReplayProjectionCrashScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias IntentLedger.Projection

  defmodule ProjectionCrashHandler do
    use IntentLedger.Handler, topic: "projection.crash"

    @impl true
    def handle(_payload, _ctx), do: :ok
  end

  defmodule ProjectionCrashIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{"projection.crash" => [handler: ProjectionCrashHandler]}
  end

  defmodule CountingProjection do
    @behaviour IntentLedger.Projection

    @impl true
    def init(_opts), do: %{count: 0, subjects: MapSet.new()}

    @impl true
    def apply_signal(%Jido.Signal{} = signal, projection, _ctx) do
      %{
        projection
        | count: projection.count + 1,
          subjects: MapSet.put(projection.subjects, signal.subject)
      }
    end
  end

  setup do
    IntentLedger.RealBedrock.setup!()
  end

  test "projection cursor only advances after durable projection work succeeds" do
    assert {:ok, first} = ProjectionCrashIntents.enqueue("projection.crash", %{id: 1})
    assert {:ok, second} = ProjectionCrashIntents.enqueue("projection.crash", %{id: 2})

    assert {:ok, [first_entry | _rest] = entries} = ProjectionCrashIntents.replay_entries(:ledger, limit: 10)

    assert {:ok, partial_projection} =
             Projection.catch_up(CountingProjection, %{count: 0, subjects: MapSet.new()}, [first_entry.signal])

    assert partial_projection.count == 1
    assert {:ok, nil} = ProjectionCrashIntents.projection_cursor(CountingProjection)

    assert {:ok, replayed_entries} = ProjectionCrashIntents.replay_entries(:ledger, cursor: 0, limit: 10)
    assert Enum.map(replayed_entries, & &1.cursor) == Enum.map(entries, & &1.cursor)

    assert {:ok, rebuilt_projection} =
             Projection.rebuild(CountingProjection, Enum.map(replayed_entries, & &1.signal))

    assert rebuilt_projection.count == 2
    assert MapSet.equal?(rebuilt_projection.subjects, MapSet.new([first.id, second.id]))

    last_cursor = replayed_entries |> List.last() |> Map.fetch!(:cursor)
    assert :ok = ProjectionCrashIntents.put_projection_cursor(CountingProjection, last_cursor)
    assert {:ok, ^last_cursor} = ProjectionCrashIntents.projection_cursor(CountingProjection)
  end
end
