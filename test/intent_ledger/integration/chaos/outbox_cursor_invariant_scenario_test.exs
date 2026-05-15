defmodule IntentLedger.Chaos.OutboxCursorInvariantScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :chaos

  defmodule CursorHandler do
    use IntentLedger.Handler, topic: "outbox.cursor"

    @impl true
    def handle(_payload, _ctx), do: :ok
  end

  defmodule CursorIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{"outbox.cursor" => [handler: CursorHandler]}
  end

  setup do
    IntentLedger.RealBedrock.setup!()
  end

  test "outbox consumers page independently and reject stale or impossible acknowledgements" do
    assert {:ok, first} = CursorIntents.enqueue("outbox.cursor", %{n: 1})
    assert {:ok, second} = CursorIntents.enqueue("outbox.cursor", %{n: 2})
    assert first.id != second.id

    assert {:ok, first_page} = CursorIntents.read_outbox("consumer-a", limit: 1)
    assert first_page.acked_cursor == 0
    assert first_page.head_cursor == 2
    assert first_page.next_cursor == 1
    assert first_page.lag == 1
    assert Enum.map(first_page.entries, & &1.cursor) == [1]

    assert {:ok, untouched_b} = CursorIntents.read_outbox("consumer-b", limit: 10)
    assert untouched_b.acked_cursor == 0
    assert Enum.map(untouched_b.entries, & &1.cursor) == [1, 2]

    assert {:ok, %{cursor: 1}} = CursorIntents.ack_outbox("consumer-a", 1)

    assert {:error, %IntentLedger.Error.ConflictError{reason: :stale_outbox_ack, details: stale_details}} =
             CursorIntents.ack_outbox("consumer-a", 0)

    assert stale_details == %{consumer: "name:consumer-a", cursor: 0, current: 1}

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :cursor, value: 99, details: past_head_details}} =
             CursorIntents.ack_outbox("consumer-a", 99)

    assert past_head_details == %{consumer: "name:consumer-a", field: :cursor, head: 2, value: 99}

    assert {:ok, second_page} = CursorIntents.read_outbox("consumer-a", limit: 10)
    assert second_page.acked_cursor == 1
    assert Enum.map(second_page.entries, & &1.cursor) == [2]

    assert {:ok, %{cursor: 2}} = CursorIntents.ack_outbox("consumer-a", 2)
    assert {:ok, drained_a} = CursorIntents.read_outbox("consumer-a", limit: 10)
    assert drained_a.entries == []
    assert drained_a.lag == 0

    assert {:ok, still_unacked_b} = CursorIntents.read_outbox("consumer-b", limit: 10)
    assert Enum.map(still_unacked_b.entries, & &1.cursor) == [1, 2]
  end
end
