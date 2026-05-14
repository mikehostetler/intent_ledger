defmodule IntentLedger.BedrockJobQueueScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :bedrock

  alias Bedrock.JobQueue.Consumer.Worker
  alias Bedrock.JobQueue.Internal
  alias Bedrock.JobQueue.Store

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
      repo: IntentLedger.FakeRepo,
      intents: %{
        "invoice.archive" => ArchiveInvoice,
        "invoice.retry" => RetryInvoice,
        "invoice.discard" => DiscardInvoice,
        "invoice.snooze" => SnoozeInvoice,
        "invoice.cancelable" => CancelableInvoice
      }
  end

  setup do
    IntentLedger.FakeRepo.reset!()
    :ok
  end

  test "IntentLedger stores the full Intent while bedrock_job_queue carries only a pointer" do
    attachment = {:pdf, "invoice-123.pdf", <<1, 2, 3, 4>>}

    assert {:ok, intent} =
             ScenarioIntents.enqueue("invoice.archive", %{
               invoice_id: 123,
               attachment: attachment,
               test_pid: self()
             })

    queue_root = Internal.root_keyspace(ScenarioIntents.JobQueue)
    [item] = Store.peek(IntentLedger.FakeRepo, queue_root, "default")

    assert item.id == intent.id
    assert item.topic == "invoice.archive"
    assert :erlang.binary_to_term(item.payload) == %{ledger: ScenarioIntents, intent_id: intent.id}

    assert {:ok, lease} =
             Store.obtain_lease(IntentLedger.FakeRepo, queue_root, item, "test-holder", 30_000)

    assert {:ok, %{archived: true}} =
             result =
             Worker.execute(item, %{"invoice.archive" => ArchiveInvoice})

    assert :ok =
             IntentLedger.FakeRepo.transact(fn ->
               queue_result = Store.complete(IntentLedger.FakeRepo, queue_root, lease)

               IntentLedger.JobQueueHook.apply(
                 IntentLedger.FakeRepo,
                 queue_root,
                 lease,
                 :complete,
                 result,
                 queue_result,
                 ScenarioIntents
               )
             end)

    assert_receive {:archived, ^attachment, intent_id}
    assert intent_id == intent.id

    assert {:ok, completed} = ScenarioIntents.fetch(intent.id)
    assert completed.payload.attachment == attachment
    assert completed.status == :completed
  end

  test "Bedrock queue actions drive success retry failure discard snooze and cancel lifecycle" do
    assert {:ok, success} =
             ScenarioIntents.enqueue("invoice.archive", %{
               attachment: {:pdf, "success.pdf", <<1>>},
               test_pid: self()
             })

    assert {:ok, %{archived: true}} = run_visible_job("invoice.archive")
    assert_receive {:archived, {:pdf, "success.pdf", <<1>>}, success_id}
    assert success_id == success.id
    assert {:ok, completed} = ScenarioIntents.fetch(success.id)
    assert completed.status == :completed
    assert completed.result == %{archived: true}

    assert {:ok, retrying} = ScenarioIntents.enqueue("invoice.retry", %{test_pid: self()}, max_attempts: 3)
    assert {:error, :retryable} = run_visible_job("invoice.retry")
    assert_receive {:retry_attempted, retrying_id, 1}
    assert retrying_id == retrying.id
    assert {:ok, retry_scheduled} = ScenarioIntents.fetch(retrying.id)
    assert retry_scheduled.status == :retry_scheduled
    assert retry_scheduled.error == :retryable
    assert {:ok, [retry_view]} = ScenarioIntents.inspect(:retries)
    assert retry_view.id == retrying.id

    assert {:ok, failing} = ScenarioIntents.enqueue("invoice.retry", %{test_pid: self()}, max_attempts: 1)
    assert {:error, :retryable} = run_visible_job("invoice.retry")
    assert_receive {:retry_attempted, failing_id, 1}
    assert failing_id == failing.id
    assert {:ok, failed} = ScenarioIntents.fetch(failing.id)
    assert failed.status == :failed
    assert failed.error == :retryable

    assert {:ok, discarded} = ScenarioIntents.enqueue("invoice.discard", %{test_pid: self()})
    assert {:discard, :not_actionable} = run_visible_job("invoice.discard")
    assert_receive {:discard_attempted, discarded_id, 1}
    assert discarded_id == discarded.id
    assert {:ok, discarded} = ScenarioIntents.fetch(discarded.id)
    assert discarded.status == :discarded
    assert discarded.error == :not_actionable

    assert {:ok, snoozed} = ScenarioIntents.enqueue("invoice.snooze", %{test_pid: self()})
    assert {:snooze, 60_000} = run_visible_job("invoice.snooze")
    assert_receive {:snoozed, snoozed_id, 1}
    assert snoozed_id == snoozed.id
    assert {:ok, retry_scheduled} = ScenarioIntents.fetch(snoozed.id)
    assert retry_scheduled.status == :retry_scheduled
    assert retry_scheduled.error == {:snooze, 60_000}

    assert {:ok, canceled} = ScenarioIntents.enqueue("invoice.cancelable", %{test_pid: self()})
    assert {:ok, canceled} = ScenarioIntents.cancel(canceled.id, :not_needed)
    assert canceled.status == :canceled
    assert :ok = run_visible_job("invoice.cancelable")
    refute_receive {:cancelable_ran, _intent_id}, 50
    assert {:ok, still_canceled} = ScenarioIntents.fetch(canceled.id)
    assert still_canceled.status == :canceled

    assert {:ok, outbox_signals} = ScenarioIntents.replay(:outbox, limit: 100)
    signal_types = Enum.map(outbox_signals, & &1.type)
    assert "intent.completed" in signal_types
    assert "intent.retry_scheduled" in signal_types
    assert "intent.failed" in signal_types
    assert "intent.discarded" in signal_types
    assert "intent.canceled" in signal_types
  end

  test "outbox replay remains deterministic across storage process restart" do
    assert {:ok, intent} =
             ScenarioIntents.enqueue("invoice.archive", %{
               attachment: {:pdf, "restart.pdf", <<2>>},
               test_pid: self()
             })

    assert {:ok, %{archived: true}} = run_visible_job("invoice.archive")
    intent_id = intent.id
    assert_receive {:archived, {:pdf, "restart.pdf", <<2>>}, ^intent_id}

    assert {:ok, before_restart} = ScenarioIntents.replay(:outbox, limit: 100)
    before_facts = Enum.map(before_restart, &{&1.type, &1.subject})
    assert {"intent.completed", intent.id} in before_facts

    IntentLedger.FakeRepo.snapshot!()
    |> IntentLedger.FakeRepo.restart!()

    assert {:ok, after_restart} = ScenarioIntents.replay(:outbox, limit: 100)
    assert Enum.map(after_restart, &{&1.type, &1.subject}) == before_facts
  end

  defp run_visible_job(topic) do
    queue_root = Internal.root_keyspace(ScenarioIntents.JobQueue)
    [item] = Store.peek(IntentLedger.FakeRepo, queue_root, "default", limit: 1)
    assert item.topic == topic

    assert {:ok, lease} =
             Store.obtain_lease(IntentLedger.FakeRepo, queue_root, item, "test-holder", 30_000)

    result = Worker.execute(item, workers())
    action = action_for_result(result)

    assert :ok =
             IntentLedger.FakeRepo.transact(fn ->
               queue_result = apply_queue_action(queue_root, lease, action)

               IntentLedger.JobQueueHook.apply(
                 IntentLedger.FakeRepo,
                 queue_root,
                 lease,
                 action,
                 result,
                 queue_result,
                 ScenarioIntents
               )
             end)

    result
  end

  defp apply_queue_action(queue_root, lease, :complete), do: Store.complete(IntentLedger.FakeRepo, queue_root, lease)

  defp apply_queue_action(queue_root, lease, :requeue),
    do: Store.requeue(IntentLedger.FakeRepo, queue_root, lease, backoff_fn: fn _attempt -> 60_000 end)

  defp apply_queue_action(queue_root, lease, {:snooze, delay_ms}) do
    Store.requeue(IntentLedger.FakeRepo, queue_root, lease,
      base_delay: delay_ms,
      max_delay: delay_ms
    )
  end

  defp action_for_result(:ok), do: :complete
  defp action_for_result({:ok, _result}), do: :complete
  defp action_for_result({:discard, _reason}), do: :complete
  defp action_for_result({:error, _reason}), do: :requeue
  defp action_for_result({:snooze, delay_ms}), do: {:snooze, delay_ms}

  defp workers do
    %{
      "invoice.archive" => ArchiveInvoice,
      "invoice.retry" => RetryInvoice,
      "invoice.discard" => DiscardInvoice,
      "invoice.snooze" => SnoozeInvoice,
      "invoice.cancelable" => CancelableInvoice
    }
  end
end
