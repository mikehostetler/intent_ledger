defmodule IntentLedger.Chaos.ConcurrencyInvariantScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :chaos

  defmodule LoadHandler do
    use IntentLedger.Handler, topic: "load.intent"

    @impl true
    def handle(_payload, _ctx), do: :ok
  end

  defmodule LoadIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{"load.intent" => [handler: LoadHandler]}
  end

  setup do
    IntentLedger.RealBedrock.setup!()
  end

  test "high-volume signal redelivery collapses to one Intent and keeps indexes repair-clean" do
    assert {:ok, signal} =
             LoadIntents.command_signal(:enqueue,
               topic: "load.intent",
               payload: %{batch: "signal-redelivery"}
             )

    results =
      1..500
      |> Task.async_stream(fn _ -> LoadIntents.submit(signal) end,
        max_concurrency: 50,
        timeout: 10_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.all?(results, &match?({:ok, _intent}, &1))

    intent_ids =
      results
      |> Enum.map(fn {:ok, intent} -> intent.id end)
      |> Enum.uniq()

    assert [_intent_id] = intent_ids
    assert {:ok, %{"default" => %{pending_count: 1, processing_count: 0}}} = LoadIntents.view(:queues)

    assert {:ok, report} = IntentLedger.Repair.verify(LoadIntents)
    assert report.valid?
  end
end
