defmodule IntentLedger.BedrockStore.OutboxTest do
  use IntentLedger.TestCase, async: false

  test "outbox replay uses the outbox cursor window" do
    assert {:ok, first} = TestIntents.enqueue("invoice.send", %{invoice_id: 1})
    assert {:ok, second} = TestIntents.enqueue("invoice.send", %{invoice_id: 2})
    assert {:ok, third} = TestIntents.enqueue("invoice.send", %{invoice_id: 3})

    assert {:ok, entries} = TestIntents.inspect(:outbox, cursor: 0, limit: 3)
    assert Enum.map(entries, & &1.cursor) == [1, 2, 3]
    assert Enum.map(entries, & &1.signal.subject) == [first.id, second.id, third.id]

    assert {:ok, [signal]} = TestIntents.replay(:outbox, cursor: 1, limit: 1)
    assert signal.subject == second.id
    assert signal.type == "intent.enqueued"

    assert {:ok, []} = TestIntents.replay(:outbox, cursor: 3, limit: 1)
  end

  test "outbox consumers read and ack durable cursors monotonically" do
    assert {:ok, first} = TestIntents.enqueue("invoice.send", %{invoice_id: 1})
    assert {:ok, second} = TestIntents.enqueue("invoice.send", %{invoice_id: 2})
    assert {:ok, third} = TestIntents.enqueue("invoice.send", %{invoice_id: 3})

    assert {:ok, nil} = TestIntents.outbox_cursor("webhook-dispatcher")

    attach_telemetry(:outbox, self())

    assert {:ok, batch} = TestIntents.read_outbox("webhook-dispatcher", limit: 2)
    assert batch.consumer == "name:webhook-dispatcher"
    assert batch.acked_cursor == 0
    assert batch.next_cursor == 2
    assert batch.head_cursor == 3
    assert batch.lag == 1
    assert Enum.map(batch.entries, & &1.signal.subject) == [first.id, second.id]

    assert_receive {:telemetry, [:intent_ledger, :outbox, :stop], measurements, metadata}
    assert is_integer(measurements.duration)
    assert measurements.count == 2
    assert metadata.operation == :read
    assert metadata.consumer == "webhook-dispatcher"

    assert {:ok, ack} = TestIntents.ack_outbox("webhook-dispatcher", batch.next_cursor)
    assert ack.cursor == 2
    assert {:ok, 2} = TestIntents.outbox_cursor("webhook-dispatcher")

    assert {:ok, next_batch} = TestIntents.read_outbox("webhook-dispatcher", limit: 10)
    assert next_batch.acked_cursor == 2
    assert next_batch.next_cursor == 3
    assert next_batch.lag == 0
    assert Enum.map(next_batch.entries, & &1.signal.subject) == [third.id]

    assert {:error, %IntentLedger.Error.ConflictError{reason: :stale_outbox_ack}} =
             TestIntents.ack_outbox("webhook-dispatcher", 1)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :cursor, value: 4}} =
             TestIntents.ack_outbox("webhook-dispatcher", 4)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :consumer, value: ""}} =
             TestIntents.read_outbox("")
  end

  test "outbox dispatcher example processes and acks one deterministic batch" do
    assert {:ok, first} = TestIntents.enqueue("invoice.send", %{invoice_id: 1})
    assert {:ok, second} = TestIntents.enqueue("invoice.send", %{invoice_id: 2})

    assert {:ok, batch} = TestIntents.read_outbox("dispatcher", limit: 10)
    delivered = Enum.map(batch.entries, &{&1.cursor, &1.signal.subject})

    assert delivered == [{1, first.id}, {2, second.id}]
    assert {:ok, %{cursor: 2}} = TestIntents.ack_outbox("dispatcher", batch.next_cursor)
    assert {:ok, %{entries: []}} = TestIntents.read_outbox("dispatcher", limit: 10)
  end
end
