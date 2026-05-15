defmodule IntentLedger.Runtime.QueueLifecycleTest do
  use IntentLedger.TestCase, async: false

  test "queue action hooks keep raw bedrock_job_queue return values" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123, test_pid: self()})

    queue_payload = queue_payload(TestIntents, intent.id)

    assert {:ok, %{sent: true}} =
             result =
             SendInvoice.perform(queue_payload, %{
               topic: "invoice.send",
               queue_id: "default",
               item_id: intent.id,
               attempt: 1
             })

    assert :ok =
             IntentLedger.Runtime.QueueLifecycle.apply(
               IntentLedger.FakeRepo,
               nil,
               lease_for(intent),
               :complete,
               result,
               :ok,
               TestIntents
             )
  end

  test "queue lifecycle duplicate hooks do not duplicate terminal facts" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123, test_pid: self()})
    assert {:ok, %{sent: true}} = result = SendInvoice.perform(queue_payload(TestIntents, intent.id), job_meta(intent))

    finalize_perform(TestIntents, intent, result)

    assert :ok =
             IntentLedger.Runtime.QueueLifecycle.apply_queue_action(
               TestIntents,
               IntentLedger.FakeRepo,
               lease_for(intent),
               :complete,
               result,
               :ok
             )

    assert_history(TestIntents, intent, ["intent.enqueued", "intent.started", "intent.completed"])
  end

  test "stale queue failures do not mutate Intent lifecycle state" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123, test_pid: self()})
    assert {:ok, %{sent: true}} = result = SendInvoice.perform(queue_payload(TestIntents, intent.id), job_meta(intent))

    assert {:error, :stale_lease} =
             IntentLedger.Runtime.QueueLifecycle.apply_queue_action(
               TestIntents,
               IntentLedger.FakeRepo,
               lease_for(intent),
               :complete,
               result,
               {:error, :stale_lease}
             )

    assert {:ok, started} = TestIntents.fetch(intent.id)
    assert started.status == :started
    assert_history(TestIntents, intent, ["intent.enqueued", "intent.started"])
  end

  test "unexpected runnable queue action combinations fail instead of silently no-oping" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123, test_pid: self()})
    assert {:ok, %{sent: true}} = SendInvoice.perform(queue_payload(TestIntents, intent.id), job_meta(intent))

    assert {:error, {:unexpected_queue_action, :complete, {:error, :boom}, :ok}} =
             IntentLedger.Runtime.QueueLifecycle.apply_queue_action(
               TestIntents,
               IntentLedger.FakeRepo,
               lease_for(intent),
               :complete,
               {:error, :boom},
               :ok
             )

    assert {:ok, started} = TestIntents.fetch(intent.id)
    assert started.status == :started
    assert_history(TestIntents, intent, ["intent.enqueued", "intent.started"])
  end

  test "terminal Intents are immutable through repeated queue actions" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123, test_pid: self()})
    assert {:ok, %{sent: true}} = result = SendInvoice.perform(queue_payload(TestIntents, intent.id), job_meta(intent))
    finalize_perform(TestIntents, intent, result)

    assert :ok =
             IntentLedger.Runtime.QueueLifecycle.apply_queue_action(
               TestIntents,
               IntentLedger.FakeRepo,
               lease_for(intent),
               :requeue,
               {:error, :late_error},
               {:ok, :requeued}
             )

    assert {:ok, completed} = TestIntents.fetch(intent.id)
    assert completed.status == :completed
    assert completed.result == %{sent: true}
    assert_history(TestIntents, intent, ["intent.enqueued", "intent.started", "intent.completed"])
  end

  test "failed handlers schedule retry until max attempts are exhausted" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.fail", %{}, max_attempts: 2)

    queue_payload = queue_payload(TestIntents, intent.id)

    assert {:error, :boom} =
             result =
             FailingIntent.perform(queue_payload, %{
               topic: "invoice.fail",
               queue_id: "default",
               item_id: intent.id,
               attempt: 1
             })

    finalize_perform(TestIntents, intent, result, action: :requeue, queue_result: {:ok, :requeued})

    assert {:ok, retrying} = TestIntents.fetch(intent.id)
    assert retrying.status == :retry_scheduled

    assert {:error, :boom} =
             result =
             FailingIntent.perform(queue_payload, %{
               topic: "invoice.fail",
               queue_id: "default",
               item_id: intent.id,
               attempt: 2
             })

    finalize_perform(TestIntents, intent, result, action: :requeue, queue_result: {:ok, :dead_lettered})

    assert {:ok, failed} = TestIntents.fetch(intent.id)
    assert failed.status == :failed
  end

  test "ambiguous intents are parked and not handed to handlers" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123, test_pid: self()})
    assert {:ok, ambiguous} = TestIntents.mark_ambiguous(intent.id, :manual_review)
    assert ambiguous.status == :ambiguous
    assert {:ok, %{"default" => %{pending_count: 0, processing_count: 0}}} = TestIntents.stats(queue: "default")

    assert :ok =
             SendInvoice.perform(queue_payload(TestIntents, intent.id), %{
               topic: "invoice.send",
               queue_id: "default",
               item_id: intent.id,
               attempt: 1
             })

    refute_receive {:handled, _, _, _}
    assert {:ok, still_ambiguous} = TestIntents.fetch(intent.id)
    assert still_ambiguous.status == :ambiguous
  end

  defp job_meta(intent) do
    %{
      topic: "invoice.send",
      queue_id: "default",
      item_id: intent.id,
      attempt: 1
    }
  end
end
