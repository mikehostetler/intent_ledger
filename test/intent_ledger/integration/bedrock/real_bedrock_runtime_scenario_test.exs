defmodule IntentLedger.Bedrock.RealBedrockRuntimeScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :bedrock
  @moduletag :real_bedrock

  defmodule NotifyHandler do
    use IntentLedger.Handler, topic: "real_bedrock.notify"

    @impl true
    def handle(%{label: label, test_pid: test_pid}, ctx) do
      send(test_pid, {:real_bedrock_handled, label, ctx.intent.id, ctx.queue, ctx.attempt})
      {:ok, %{label: label, queue: ctx.queue}}
    end
  end

  defmodule RealBedrockIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{
        "real_bedrock.notify" => [handler: NotifyHandler, queue: "default"]
      }
  end

  setup do
    IntentLedger.RealBedrock.reset!()
    IntentLedger.RealBedrock.start_cluster!()
    :ok
  end

  test "configured Intents module runs against a real Bedrock repo without a precomputed queue root" do
    start_supervised!({RealBedrockIntents, concurrency: 1, batch_size: 1})

    assert {:ok, intent} =
             RealBedrockIntents.enqueue("real_bedrock.notify", %{
               label: "real",
               test_pid: self()
             })

    assert_receive {:real_bedrock_handled, "real", intent_id, "default", 1}, 10_000
    assert intent_id == intent.id

    assert_eventually(fn ->
      case RealBedrockIntents.fetch(intent.id) do
        {:ok, completed} ->
          completed.status == :completed and completed.result == %{label: "real", queue: "default"}

        _other ->
          false
      end
    end)

    assert {:ok, history} = RealBedrockIntents.history(intent.id)
    assert Enum.map(history, & &1.type) == ["intent.enqueued", "intent.started", "intent.completed"]

    assert {:ok, replay} = RealBedrockIntents.replay(:ledger)
    assert Enum.map(replay, & &1.type) == ["intent.enqueued", "intent.started", "intent.completed"]

    assert {:ok, %{entries: outbox_entries, acked_cursor: 0, head_cursor: 3}} =
             RealBedrockIntents.read_outbox("dispatcher")

    assert Enum.map(outbox_entries, & &1.signal.type) == ["intent.enqueued", "intent.started", "intent.completed"]
  end

  defp assert_eventually(fun, timeout \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(fun, deadline)
  end

  defp do_assert_eventually(fun, deadline) do
    if fun.() do
      assert true
    else
      if System.monotonic_time(:millisecond) >= deadline do
        assert fun.()
      else
        Process.sleep(50)
        do_assert_eventually(fun, deadline)
      end
    end
  end
end
