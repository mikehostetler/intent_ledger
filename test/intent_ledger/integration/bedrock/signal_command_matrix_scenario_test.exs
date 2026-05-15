defmodule IntentLedger.Bedrock.SignalCommandMatrixScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :bedrock

  alias Bedrock.JobQueue.Internal

  defmodule FailingHandler do
    use IntentLedger.Handler, topic: "signal.fail"

    @impl true
    def handle(%{test_pid: test_pid}, ctx) do
      send(test_pid, {:signal_failed, ctx.intent.id, ctx.attempt})
      {:error, :planned_failure}
    end
  end

  defmodule PassiveHandler do
    use IntentLedger.Handler, topic: "signal.passive"

    @impl true
    def handle(_payload, _ctx), do: :ok
  end

  defmodule SignalIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{
        "signal.fail" => [handler: FailingHandler],
        "signal.passive" => [handler: PassiveHandler]
      }
  end

  setup do
    IntentLedger.RealBedrock.setup!()
  end

  test "cancel and mark_ambiguous command signals are redelivery-safe on real Bedrock" do
    assert {:ok, cancelable} = SignalIntents.enqueue("signal.passive", %{n: 1})

    assert {:ok, cancel_signal} =
             SignalIntents.command_signal(:cancel,
               intent_id: cancelable.id,
               reason: :customer_request
             )

    assert {:ok, canceled} = SignalIntents.submit(cancel_signal)
    assert canceled.status == :canceled
    assert canceled.cancel_reason == :customer_request

    assert {:ok, redelivered_cancel} = SignalIntents.submit(cancel_signal)
    assert redelivered_cancel.id == cancelable.id
    assert redelivered_cancel.status == :canceled

    assert {:ok, history} = SignalIntents.history(cancelable.id)
    assert Enum.map(history, & &1.type) == ["intent.enqueued", "intent.canceled"]
    assert List.last(history).data.command_signal_id == cancel_signal.id

    assert {:ok, ambiguous_candidate} = SignalIntents.enqueue("signal.passive", %{n: 2})

    assert {:ok, ambiguous_signal} =
             SignalIntents.command_signal(:mark_ambiguous,
               intent_id: ambiguous_candidate.id,
               reason: :operator_review
             )

    assert {:ok, ambiguous} = SignalIntents.submit(ambiguous_signal)
    assert ambiguous.status == :ambiguous
    assert ambiguous.error == :operator_review

    assert {:ok, redelivered_ambiguous} = SignalIntents.submit(ambiguous_signal)
    assert redelivered_ambiguous.id == ambiguous_candidate.id
    assert redelivered_ambiguous.status == :ambiguous

    assert {:ok, history} = SignalIntents.history(ambiguous_candidate.id)
    assert Enum.map(history, & &1.type) == ["intent.enqueued", "intent.ambiguous"]
    assert List.last(history).data.command_signal_id == ambiguous_signal.id
  end

  test "requeue command signal moves a failed Intent back to retry_scheduled" do
    runtime =
      start_supervised!({SignalIntents, root: Internal.root_keyspace(SignalIntents.JobQueue), scan_interval: 10})

    assert {:ok, intent} = SignalIntents.enqueue("signal.fail", %{test_pid: self()}, max_attempts: 1)
    assert_receive {:signal_failed, intent_id, 1}, 1_000
    assert intent_id == intent.id
    assert_eventually(fn -> status?(intent.id, :failed, error: :planned_failure) end)

    Supervisor.stop(runtime)

    assert {:ok, requeue_signal} =
             SignalIntents.command_signal(:requeue,
               intent_id: intent.id,
               reason: :manual_retry
             )

    assert {:ok, requeued} = SignalIntents.submit(requeue_signal)
    assert requeued.status == :retry_scheduled
    assert requeued.error == nil

    assert {:ok, history} = SignalIntents.history(intent.id)

    assert Enum.map(history, & &1.type) == [
             "intent.enqueued",
             "intent.started",
             "intent.failed",
             "intent.retry_scheduled"
           ]

    assert List.last(history).data.command_signal_id == requeue_signal.id
  end

  defp status?(intent_id, status, expected) do
    case SignalIntents.fetch(intent_id) do
      {:ok, intent} ->
        intent.status == status and Enum.all?(expected, fn {field, value} -> Map.fetch!(intent, field) == value end)

      _other ->
        false
    end
  end

  defp assert_eventually(fun, timeout \\ 2_000) do
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
        Process.sleep(20)
        do_assert_eventually(fun, deadline)
      end
    end
  end
end
