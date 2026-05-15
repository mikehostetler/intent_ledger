defmodule IntentLedger.Bedrock.RestartDurabilityScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :bedrock_restart

  defmodule RestartHandler do
    use IntentLedger.Handler, topic: "restart.intent"

    @impl true
    def handle(_payload, _ctx), do: :ok
  end

  defmodule RestartProjection do
    @moduledoc false
  end

  defmodule RestartIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{"restart.intent" => [handler: RestartHandler]}
  end

  setup do
    IntentLedger.RealBedrock.reset!()
  end

  test "ledger, outbox, indexes, and cursors survive a real Bedrock cluster restart" do
    cluster = IntentLedger.RealBedrock.start_cluster_link!()

    assert {:ok, first} = RestartIntents.enqueue("restart.intent", %{n: 1}, key: "restart:1")
    assert {:ok, second} = RestartIntents.enqueue("restart.intent", %{n: 2}, key: "restart:2")
    assert {:ok, canceled} = RestartIntents.cancel(second.id, :restart_probe)
    assert canceled.status == :canceled

    assert {:ok, before_entries} = RestartIntents.replay_entries(:ledger, limit: 10)
    last_cursor = before_entries |> List.last() |> Map.fetch!(:cursor)
    assert :ok = RestartIntents.put_projection_cursor(RestartProjection, last_cursor)
    assert {:ok, %{cursor: 2}} = RestartIntents.ack_outbox("restart-dispatcher", 2)

    IntentLedger.RealBedrock.stop_cluster_link!(cluster)

    restarted = IntentLedger.RealBedrock.start_cluster_link!(ready_timeout_ms: 3_000)
    on_exit(fn -> IntentLedger.RealBedrock.stop_cluster_link!(restarted) end)

    assert {:ok, fetched_first} = bounded_call(fn -> RestartIntents.fetch(first.id) end)
    assert fetched_first.status == :enqueued
    assert fetched_first.payload == %{n: 1}

    assert {:ok, fetched_second} = bounded_call(fn -> RestartIntents.fetch(second.id) end)
    assert fetched_second.status == :canceled
    assert fetched_second.cancel_reason == :restart_probe

    assert {:ok, after_entries} = bounded_call(fn -> RestartIntents.replay_entries(:ledger, limit: 10) end)

    assert Enum.map(after_entries, &{&1.cursor, &1.signal.type, &1.signal.subject}) ==
             Enum.map(before_entries, &{&1.cursor, &1.signal.type, &1.signal.subject})

    assert {:ok, ^last_cursor} = bounded_call(fn -> RestartIntents.projection_cursor(RestartProjection) end)

    assert {:ok, outbox} = bounded_call(fn -> RestartIntents.read_outbox("restart-dispatcher", limit: 10) end)
    assert outbox.acked_cursor == 2
    assert Enum.map(outbox.entries, &{&1.cursor, &1.signal.type}) == [{3, "intent.canceled"}]

    assert {:ok, [queued]} = bounded_call(fn -> RestartIntents.view(:intents, status: :enqueued) end)
    assert queued.id == first.id

    assert {:ok, [canceled_view]} = bounded_call(fn -> RestartIntents.view(:intents, status: :canceled) end)
    assert canceled_view.id == second.id
  end

  defp bounded_call(fun, timeout \\ 2_000) do
    task = Task.async(fun)

    case Task.yield(task, timeout) do
      {:ok, result} ->
        result

      nil ->
        Task.shutdown(task, :brutal_kill)
        :timeout
    end
  end
end
