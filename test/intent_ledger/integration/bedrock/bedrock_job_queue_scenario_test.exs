defmodule IntentLedger.BedrockJobQueueScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :bedrock

  alias Bedrock.JobQueue.Internal

  defmodule ArchiveInvoice do
    use IntentLedger.Handler, topic: "invoice.archive"

    @impl true
    def handle(%{test_pid: test_pid, attachment: attachment}, ctx) do
      send(test_pid, {:archived, attachment, ctx.intent.id})
      {:ok, %{archived: true}}
    end
  end

  defmodule RetryInvoice do
    use IntentLedger.Handler, topic: "invoice.retry"

    @impl true
    def handle(%{test_pid: test_pid}, ctx) do
      send(test_pid, {:retry_attempted, ctx.intent.id, ctx.attempt})
      {:error, :retryable}
    end
  end

  defmodule DiscardInvoice do
    use IntentLedger.Handler, topic: "invoice.discard"

    @impl true
    def handle(%{test_pid: test_pid}, ctx) do
      send(test_pid, {:discard_attempted, ctx.intent.id, ctx.attempt})
      {:discard, :not_actionable}
    end
  end

  defmodule SnoozeInvoice do
    use IntentLedger.Handler, topic: "invoice.snooze"

    @impl true
    def handle(%{test_pid: test_pid}, ctx) do
      send(test_pid, {:snoozed, ctx.intent.id, ctx.attempt})
      {:snooze, 60_000}
    end
  end

  defmodule CancelableInvoice do
    use IntentLedger.Handler, topic: "invoice.cancelable"

    @impl true
    def handle(%{test_pid: test_pid}, ctx) do
      send(test_pid, {:cancelable_ran, ctx.intent.id})
      :ok
    end
  end

  defmodule ScenarioIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{
        "invoice.archive" => [handler: ArchiveInvoice],
        "invoice.retry" => [handler: RetryInvoice],
        "invoice.discard" => [handler: DiscardInvoice],
        "invoice.snooze" => [handler: SnoozeInvoice],
        "invoice.cancelable" => [handler: CancelableInvoice]
      }
  end

  setup do
    IntentLedger.RealBedrock.setup!()
  end

  test "IntentLedger stores the full Intent while bedrock_job_queue carries only a pointer" do
    start_runtime!()
    attachment = {:pdf, "invoice-123.pdf", <<1, 2, 3, 4>>}

    assert {:ok, intent} =
             ScenarioIntents.enqueue("invoice.archive", %{
               invoice_id: 123,
               attachment: attachment,
               test_pid: self()
             })

    assert_receive {:archived, ^attachment, intent_id}
    assert intent_id == intent.id

    assert_eventually(fn ->
      case ScenarioIntents.fetch(intent.id) do
        {:ok, completed} ->
          completed.payload.attachment == attachment and completed.status == :completed

        _other ->
          false
      end
    end)
  end

  test "Bedrock queue actions drive success retry failure discard snooze and cancel lifecycle" do
    start_runtime!()

    assert {:ok, success} =
             ScenarioIntents.enqueue("invoice.archive", %{
               attachment: {:pdf, "success.pdf", <<1>>},
               test_pid: self()
             })

    assert_receive {:archived, {:pdf, "success.pdf", <<1>>}, success_id}
    assert success_id == success.id
    assert_eventually(fn -> status?(success.id, :completed, result: %{archived: true}) end)

    assert {:ok, retrying} = ScenarioIntents.enqueue("invoice.retry", %{test_pid: self()}, max_attempts: 3)
    assert_receive {:retry_attempted, retrying_id, 1}
    assert retrying_id == retrying.id
    assert_eventually(fn -> status?(retrying.id, :retry_scheduled, error: :retryable) end)
    assert {:ok, retry_scheduled} = ScenarioIntents.fetch(retrying.id)
    assert retry_scheduled.status == :retry_scheduled
    assert retry_scheduled.error == :retryable
    assert {:ok, [retry_view]} = ScenarioIntents.view(:retries)
    assert retry_view.id == retrying.id

    assert {:ok, failing} = ScenarioIntents.enqueue("invoice.retry", %{test_pid: self()}, max_attempts: 1)
    assert_receive {:retry_attempted, failing_id, 1}
    assert failing_id == failing.id
    assert_eventually(fn -> status?(failing.id, :failed, error: :retryable) end)

    assert {:ok, discarded} = ScenarioIntents.enqueue("invoice.discard", %{test_pid: self()})
    assert_receive {:discard_attempted, discarded_id, 1}
    assert discarded_id == discarded.id
    assert_eventually(fn -> status?(discarded.id, :discarded, error: :not_actionable) end)

    assert {:ok, snoozed} = ScenarioIntents.enqueue("invoice.snooze", %{test_pid: self()})
    assert_receive {:snoozed, snoozed_id, 1}
    assert snoozed_id == snoozed.id
    assert_eventually(fn -> status?(snoozed.id, :retry_scheduled, error: {:snooze, 60_000}) end)
  end

  test "canceled pending Intent does not execute when the supervised runtime starts" do
    assert {:ok, canceled} = ScenarioIntents.enqueue("invoice.cancelable", %{test_pid: self()})
    assert {:ok, canceled} = ScenarioIntents.cancel(canceled.id, :not_needed)
    assert canceled.status == :canceled

    start_runtime!()
    refute_receive {:cancelable_ran, _intent_id}, 50

    assert {:ok, still_canceled} = ScenarioIntents.fetch(canceled.id)
    assert still_canceled.status == :canceled

    assert {:ok, outbox_signals} = ScenarioIntents.replay(:outbox, limit: 100)
    signal_types = Enum.map(outbox_signals, & &1.type)
    assert "intent.canceled" in signal_types
    refute "intent.started" in signal_types
    refute "intent.completed" in signal_types
    refute "intent.failed" in signal_types
  end

  test "outbox replay remains deterministic on a real Bedrock repo" do
    start_runtime!()

    assert {:ok, intent} =
             ScenarioIntents.enqueue("invoice.archive", %{
               attachment: {:pdf, "restart.pdf", <<2>>},
               test_pid: self()
             })

    intent_id = intent.id
    assert_receive {:archived, {:pdf, "restart.pdf", <<2>>}, ^intent_id}
    assert_eventually(fn -> status?(intent.id, :completed) end)

    assert {:ok, before_restart} = ScenarioIntents.replay(:outbox, limit: 100)
    before_facts = Enum.map(before_restart, &{&1.type, &1.subject})
    assert {"intent.completed", intent.id} in before_facts

    assert {:ok, after_restart} = ScenarioIntents.replay(:outbox, limit: 100)
    assert Enum.map(after_restart, &{&1.type, &1.subject}) == before_facts
  end

  defp start_runtime! do
    start_supervised!({ScenarioIntents, root: Internal.root_keyspace(ScenarioIntents.JobQueue), scan_interval: 10})
  end

  defp status?(intent_id, status, expected \\ []) do
    case ScenarioIntents.fetch(intent_id) do
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
