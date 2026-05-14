defmodule IntentLedger.CommandTest do
  use ExUnit.Case, async: true

  alias IntentLedger.Command

  test "constructors normalize direct commands" do
    assert {:ok, %Command{type: :enqueue, topic: "invoice.send", payload: %{id: 1}, opts: [key: "k"]}} =
             Command.enqueue("invoice.send", %{id: 1}, key: "k")

    assert {:ok, %Command{type: :cancel, intent_id: "intent-1", reason: :done}} =
             Command.cancel("intent-1", :done)

    assert {:ok, %Command{type: :requeue, intent_id: "intent-1", opts: [reason: :manual]}} =
             Command.requeue("intent-1", reason: :manual)

    assert {:ok, %Command{type: :mark_ambiguous, intent_id: "intent-1", reason: :manual_review}} =
             Command.mark_ambiguous("intent-1", :manual_review)

    assert is_map(Command.schema())
  end

  test "to_signal supports atom, command signal string, and invalid command inputs" do
    assert {:ok, cancel} =
             Command.to_signal(IntentLedger.TestFixtures.TestIntents, "intent.command.cancel",
               intent_id: "intent-1",
               reason: :done
             )

    assert cancel.type == "intent.command.cancel"
    assert cancel.subject == "intent-1"

    assert {:ok, requeue} =
             Command.to_signal(IntentLedger.TestFixtures.TestIntents, :requeue,
               intent_id: "intent-1",
               reason: :manual
             )

    assert requeue.type == "intent.command.requeue"
    assert requeue.data.reason == :manual

    assert {:error, {:unsupported_command, :missing}} =
             Command.to_signal(IntentLedger.TestFixtures.TestIntents, "missing", %{})
  end

  test "from_signal normalizes requeue and mark ambiguous command envelopes" do
    assert {:ok, requeue_signal} =
             Jido.Signal.new("intent.command.requeue", %{
               "intent_id" => "intent-1",
               "reason" => "manual",
               "scheduled_at" => "2026-05-14T12:00:00Z"
             })

    assert {:ok, %Command{type: :requeue, intent_id: "intent-1", signal: ^requeue_signal, opts: opts}} =
             Command.from_signal(requeue_signal, metadata: [existing: true])

    assert opts[:reason] == "manual"
    assert opts[:scheduled_at] == "2026-05-14T12:00:00Z"
    assert opts[:metadata].existing == true
    assert opts[:metadata].command_signal_id == requeue_signal.id

    assert {:ok, ambiguous_signal} =
             Jido.Signal.new("intent.command.mark_ambiguous", %{"intent_id" => "intent-1", "reason" => "manual"})

    assert {:ok, %Command{type: :mark_ambiguous, reason: "manual", signal: ^ambiguous_signal}} =
             Command.from_signal(ambiguous_signal)
  end

  test "from_signal validates command envelope shape" do
    assert {:error, {:invalid_command_signal, :not_a_signal}} = Command.from_signal(:not_a_signal)

    assert {:ok, bad_data} = Jido.Signal.new("intent.command.enqueue", "not-a-map")
    assert {:error, {:invalid_command_signal_data, "not-a-map"}} = Command.from_signal(bad_data)

    assert {:ok, missing_topic} = Jido.Signal.new("intent.command.enqueue", %{})
    assert {:error, {:missing_command_field, :topic}} = Command.from_signal(missing_topic)

    assert {:ok, missing_reason} = Jido.Signal.new("intent.command.cancel", %{intent_id: "intent-1"})
    assert {:error, {:missing_command_field, :reason}} = Command.from_signal(missing_reason)
  end
end
