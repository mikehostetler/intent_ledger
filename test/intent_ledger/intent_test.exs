defmodule IntentLedger.IntentTest do
  use ExUnit.Case, async: true

  alias IntentLedger.Intent

  @uuid7_pattern ~r/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

  test "normalizes the release Intent shape" do
    assert {:ok, intent} =
             Intent.new(
               topic: :invoice_send,
               queue: :tenant_acme,
               payload: {:binary, :erlang.term_to_binary(%{invoice_id: 123})},
               key: "invoice:123:send",
               max_attempts: 5,
               priority: 50
             )

    assert intent.topic == "invoice_send"
    assert intent.queue == "tenant_acme"
    assert intent.id =~ @uuid7_pattern
    assert Jido.Signal.ID.valid?(intent.id)
    assert intent.status == :enqueued
    assert intent.max_attempts == 5
    assert intent.priority == 50
    assert intent.root_intent_id == intent.id
    assert intent.correlation_id == intent.id
  end

  test "allows arbitrary payload terms" do
    attachment = {:attachment, "invoice.pdf", <<0, 1, 2, 3>>}

    assert {:ok, %Intent{payload: ^attachment}} =
             Intent.new(topic: "invoice.archive", payload: attachment)
  end
end
