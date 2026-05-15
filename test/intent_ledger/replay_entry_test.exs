defmodule IntentLedger.ReplayEntryTest do
  use ExUnit.Case, async: true

  alias IntentLedger.ReplayEntry

  test "new accepts atom, string, keyword, and existing replay entry shapes" do
    signal = signal()

    assert {:ok, entry} =
             ReplayEntry.new(%{
               "stream" => "ledger",
               "cursor" => 1,
               "signal" => signal,
               "recorded_at" => DateTime.utc_now()
             })

    assert entry.stream == "ledger"
    assert entry.cursor == 1
    assert entry.signal == signal
    assert %DateTime{} = entry.recorded_at

    assert {:ok, keyword_entry} =
             ReplayEntry.new(stream: "outbox", cursor: 2, signal: signal, recorded_at: nil)

    assert keyword_entry.stream == "outbox"
    assert keyword_entry.recorded_at == nil

    assert {:ok, ^keyword_entry} = ReplayEntry.new(keyword_entry)
  end

  test "new rejects invalid replay entry data" do
    assert {:error, {:invalid_replay_entry, _errors}} =
             ReplayEntry.new(stream: "ledger", cursor: 0, signal: signal())

    assert {:error, {:invalid_replay_entry, _errors}} =
             ReplayEntry.new(cursor: 1, signal: signal())
  end

  test "schema exposes the replay entry contract" do
    assert {:ok, %ReplayEntry{}} =
             Zoi.parse(ReplayEntry.schema(), %ReplayEntry{
               stream: "ledger",
               cursor: 1,
               signal: signal(),
               recorded_at: nil
             })
  end

  defp signal do
    Jido.Signal.new!("intent.enqueued", %{},
      source: "/intent_ledger/Test",
      subject: "intent-1",
      datacontenttype: "application/x-erlang-term"
    )
  end
end
