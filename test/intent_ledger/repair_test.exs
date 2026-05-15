defmodule IntentLedger.RepairTest do
  use IntentLedger.TestCase, async: false

  alias Bedrock.JobQueue.{Internal, Store}
  alias IntentLedger.BedrockStore
  alias IntentLedger.BedrockStore.Keyspaces
  alias IntentLedger.Repair

  test "verify confirms command-side projections match lifecycle replay" do
    assert {:ok, first} =
             TestIntents.enqueue("invoice.send", %{invoice_id: 1},
               key: "invoice:1",
               queue: "tenant:acme"
             )

    assert {:ok, successful} =
             TestIntents.enqueue(
               "invoice.send",
               %{invoice_id: 3, test_pid: self()},
               key: "invoice:3",
               queue: "tenant:acme"
             )

    assert {:ok, %{sent: true}} =
             success_result =
             SendInvoice.perform(queue_payload(TestIntents, successful.id), %{
               topic: "invoice.send",
               queue_id: "tenant:acme",
               item_id: successful.id,
               attempt: 1
             })

    success_lease = lease_intent(successful, "repair-success")
    commit_queue_action(success_lease, :complete, success_result)

    assert {:ok, second} = TestIntents.enqueue("invoice.fail", %{invoice_id: 2}, max_attempts: 1)
    queue_payload = queue_payload(TestIntents, second.id)

    assert {:error, :boom} =
             result =
             FailingIntent.perform(queue_payload, %{
               topic: "invoice.fail",
               queue_id: "default",
               item_id: second.id,
               attempt: 1
             })

    failure_lease = lease_intent(second, "repair-failure")
    commit_queue_action(failure_lease, :requeue, result)

    assert {:ok, report} = Repair.verify(TestIntents)

    assert report.valid?
    assert Enum.all?(report.checks, &(&1.status == :ok))

    assert {:ok, [first_entry]} = TestIntents.replay_entries({:intent, first.id})
    assert first_entry.signal.subject == first.id
  end

  test "verify reports drift when a command-side index is corrupted" do
    assert {:ok, intent} =
             TestIntents.enqueue("invoice.send", %{invoice_id: 1},
               key: "invoice:corrupt",
               queue: "tenant:acme"
             )

    BedrockStore.transact(TestIntents, fn repo, root ->
      repo.clear(Keyspaces.status_index(root, :enqueued), intent.id)
      repo.clear(Keyspaces.key_index(root), "invoice:corrupt")
      :ok
    end)

    assert {:ok, report} = Repair.verify(TestIntents)

    refute report.valid?
    assert %{status: :drift} = check(report, :status_indexes)
    assert %{status: :drift} = check(report, :key_index)
    assert check(report, :ledger_head).status == :ok
    assert check(report, :outbox_mirror).status == :ok
  end

  test "verify reports drift when queue state is corrupted" do
    assert {:ok, intent} =
             TestIntents.enqueue("invoice.send", %{invoice_id: 1},
               key: "invoice:queue-corrupt",
               queue: "tenant:acme"
             )

    BedrockStore.transact(TestIntents, fn repo, _root ->
      queue_root = Internal.root_keyspace(TestIntents.JobQueue)
      keyspaces = Store.queue_keyspaces(queue_root, intent.queue)
      [{item_key, _value}] = repo.get_range(keyspaces.items, limit: 1)
      repo.clear(keyspaces.items, item_key)
      :ok
    end)

    assert {:ok, report} = Repair.verify(TestIntents)

    refute report.valid?
    assert %{status: :drift, details: details} = check(report, :queue_consistency)
    assert details.missing_runnable == [intent.id]
    assert [%{queue: "tenant:acme"}] = details.stat_mismatches
  end

  defp check(report, name), do: Enum.find(report.checks, &(&1.name == name))

  defp lease_intent(intent, holder) do
    queue_root = Internal.root_keyspace(TestIntents.JobQueue)

    item =
      IntentLedger.FakeRepo
      |> Store.peek(queue_root, intent.queue, limit: 10)
      |> Enum.find(&(&1.id == intent.id))

    assert item
    assert {:ok, lease} = Store.obtain_lease(IntentLedger.FakeRepo, queue_root, item, holder, 30_000)
    lease
  end

  defp commit_queue_action(lease, action, handler_result) do
    queue_root = Internal.root_keyspace(TestIntents.JobQueue)

    assert :ok =
             IntentLedger.FakeRepo.transact(fn ->
               queue_result = apply_queue_action(queue_root, lease, action)

               IntentLedger.Runtime.QueueLifecycle.apply(
                 IntentLedger.FakeRepo,
                 queue_root,
                 lease,
                 action,
                 handler_result,
                 queue_result,
                 TestIntents
               )
             end)
  end

  defp apply_queue_action(queue_root, lease, :complete), do: Store.complete(IntentLedger.FakeRepo, queue_root, lease)

  defp apply_queue_action(queue_root, lease, :requeue),
    do: Store.requeue(IntentLedger.FakeRepo, queue_root, lease, backoff_fn: fn _attempt -> 60_000 end)
end
