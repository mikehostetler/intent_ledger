defmodule IntentLedger.RepairTest do
  use IntentLedger.TestCase, async: false

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

    finalize_perform(TestIntents, successful, success_result)

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

    finalize_perform(TestIntents, second, result, action: :requeue, queue_result: {:ok, :dead_lettered})

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

  defp check(report, name), do: Enum.find(report.checks, &(&1.name == name))
end
