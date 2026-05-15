defmodule IntentLedger.Runtime.InspectionTest do
  use IntentLedger.TestCase, async: false

  test "queue stats default to all configured queues" do
    assert {:ok, _intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123})

    assert {:ok, queues} = TestIntents.stats()

    assert queues |> Map.keys() |> Enum.sort() == ["default", "tenant:acme", "tenant:beta"]
    assert queues["default"].pending_count == 1
    assert queues["tenant:acme"].pending_count == 0
    assert queues["tenant:beta"].pending_count == 0
  end

  test "ledger replay starts from the target stream cursor, not the global stream keyspace" do
    for invoice_id <- 1..12 do
      assert {:ok, _intent} = TestIntents.enqueue("invoice.send", %{invoice_id: invoice_id})
    end

    assert {:ok, [first, second]} = TestIntents.replay(:ledger, cursor: 10, limit: 2)
    assert first.type == "intent.enqueued"
    assert second.type == "intent.enqueued"
  end

  test "replay_entries returns stream cursor metadata without replacing simple replay" do
    assert {:ok, first_intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 1})
    assert {:ok, second_intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 2})

    assert {:ok, [first_signal, second_signal]} = TestIntents.replay(:ledger, cursor: 0, limit: 2)
    assert Enum.map([first_signal, second_signal], & &1.subject) == [first_intent.id, second_intent.id]

    assert {:ok, [first_entry, second_entry]} = TestIntents.replay_entries(:ledger, cursor: 0, limit: 2)

    assert %IntentLedger.ReplayEntry{} = first_entry
    assert first_entry.stream == "ledger"
    assert first_entry.cursor == 1
    assert first_entry.signal.subject == first_intent.id
    assert %DateTime{} = first_entry.recorded_at

    assert second_entry.stream == "ledger"
    assert second_entry.cursor == 2
    assert second_entry.signal.subject == second_intent.id

    assert {:ok, [intent_entry]} = TestIntents.replay_entries({:intent, first_intent.id})
    assert intent_entry.stream == "intent:#{first_intent.id}"
    assert intent_entry.cursor == 1
    assert intent_entry.signal.subject == first_intent.id

    assert {:ok, [outbox_entry]} = TestIntents.replay_entries(:outbox, cursor: 1, limit: 1)
    assert outbox_entry.stream == "outbox"
    assert outbox_entry.cursor == 2
    assert outbox_entry.signal.subject == second_intent.id
  end

  test "unsupported replay sources return public invalid input errors" do
    assert {:error, %IntentLedger.Error.InvalidInputError{field: :source, value: :unknown}} =
             TestIntents.replay(:unknown)
  end

  test "inspection views expose intents retries ambiguous outbox and projections" do
    assert {:ok, enqueued} =
             TestIntents.enqueue("invoice.send", %{invoice_id: 1},
               key: "invoice:1",
               queue: "tenant:acme"
             )

    assert {:ok, retrying} = TestIntents.enqueue("invoice.fail", %{invoice_id: 2}, max_attempts: 3)
    retry_payload = queue_payload(TestIntents, retrying.id)

    assert {:error, :boom} =
             retry_result =
             FailingIntent.perform(retry_payload, %{
               topic: "invoice.fail",
               queue_id: "default",
               item_id: retrying.id,
               attempt: 1
             })

    finalize_perform(TestIntents, retrying, retry_result, action: :requeue, queue_result: {:ok, :requeued})

    assert {:ok, ambiguous} = TestIntents.enqueue("invoice.send", %{invoice_id: 3})
    assert {:ok, _ambiguous} = TestIntents.mark_ambiguous(ambiguous.id, :manual_review)
    assert :ok = TestIntents.put_projection_cursor(StatusProjection, 2)

    assert {:ok, intents} = TestIntents.view(:intents)
    intent_ids = intents |> Enum.map(& &1.id) |> MapSet.new()
    assert MapSet.subset?(MapSet.new([enqueued.id, retrying.id, ambiguous.id]), intent_ids)

    assert {:ok, [filtered]} = TestIntents.view(:intents, queue: "tenant:acme")
    assert filtered.id == enqueued.id

    assert {:ok, [retry_view]} = TestIntents.view(:retries)
    assert retry_view.id == retrying.id

    assert {:ok, [ambiguous_view]} = TestIntents.view(:ambiguous)
    assert ambiguous_view.id == ambiguous.id

    assert {:ok, outbox} = TestIntents.view(:outbox, limit: 20)
    assert Enum.any?(outbox, &(&1.signal.type == "intent.retry_scheduled"))
    assert Enum.any?(outbox, &(&1.signal.type == "intent.ambiguous"))

    assert {:ok, [projection]} = TestIntents.view(:projections)
    assert projection.projection == "module:intent_ledger__test_fixtures__status_projection"
    assert projection.cursor == 2
    assert projection.head_cursor >= 2
    assert projection.lag == projection.head_cursor - projection.cursor
  end

  test "inspection views normalize invalid options through public errors" do
    assert {:error, %IntentLedger.Error.InvalidInputError{field: :view, value: :missing}} =
             TestIntents.view(:missing)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :cursor, value: -1}} =
             TestIntents.view(:outbox, cursor: -1)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :limit, value: 0}} =
             TestIntents.view(:projections, limit: 0)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :status, value: :unknown}} =
             TestIntents.view(:intents, status: :unknown)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :queue, value: ""}} =
             TestIntents.view(:intents, queue: "")
  end

  test "health exposes the configured runtime" do
    attach_telemetry(:health, self())

    assert {:ok, _intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 123})
    assert {:ok, health} = TestIntents.health()

    assert health.status == :ok
    assert health.repo == IntentLedger.FakeRepo
    assert health.queues == ["default", "tenant:acme", "tenant:beta"]
    assert health.default_queue == "default"
    assert health.topics == ["invoice.fail", "invoice.send"]
    assert health.queue_stats["default"].pending_count == 1
    assert health.cursors == %{ledger: 1, outbox: 1}
    assert health.errors == []

    assert_receive {:telemetry, [:intent_ledger, :health, :stop], measurements, metadata}
    assert is_integer(measurements.duration)
    assert metadata.ledger == TestIntents
    assert metadata.status == :ok
  end
end
