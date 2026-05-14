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

  defmodule ScenarioIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.FakeRepo,
      handlers: %{"invoice.archive" => ArchiveInvoice}
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

    assert {:ok, %{archived: true}} =
             Worker.execute(item, %{"invoice.archive" => ArchiveInvoice})

    assert_receive {:archived, ^attachment, intent_id}
    assert intent_id == intent.id

    assert {:ok, completed} = ScenarioIntents.fetch(intent.id)
    assert completed.payload.attachment == attachment
    assert completed.status == :completed
  end
end
