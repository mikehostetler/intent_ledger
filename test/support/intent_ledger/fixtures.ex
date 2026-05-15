defmodule IntentLedger.TestFixtures.SendInvoice do
  @moduledoc false

  use IntentLedger.Handler,
    topic: "invoice.send",
    payload_schema: Zoi.map(),
    result_schema: Zoi.map(),
    timeout: 1_000

  @impl true
  def handle(%{invoice_id: invoice_id, test_pid: test_pid}, ctx) do
    send(test_pid, {:handled, invoice_id, ctx.intent.id, ctx.attempt})
    {:ok, %{sent: true}}
  end
end

defmodule IntentLedger.TestFixtures.FailingIntent do
  @moduledoc false

  use IntentLedger.Handler, topic: "invoice.fail"

  @impl true
  def handle(_payload, _ctx), do: {:error, :boom}
end

defmodule IntentLedger.TestFixtures.EdgeIntent do
  @moduledoc false

  use IntentLedger.Handler,
    topic: "intent.edge",
    payload_schema: Zoi.map(),
    result_schema: Zoi.map()

  @impl true
  def handle(%{mode: mode, test_pid: test_pid}, ctx) do
    send(test_pid, {:edge_handled, mode, ctx.intent.id, ctx.attempt})

    case mode do
      :ok -> :ok
      :result -> {:ok, %{handled: true}}
      :error -> {:error, :boom}
      :discard -> {:discard, :not_useful}
      :snooze -> {:snooze, 5_000}
      :invalid_result -> {:ok, :not_a_map}
      :invalid_return -> {:unexpected, :shape}
      :exception -> raise RuntimeError, "handler exploded"
      :throw -> throw(:handler_threw)
    end
  end
end

defmodule IntentLedger.TestFixtures.EdgeIntents do
  @moduledoc false

  use IntentLedger,
    otp_app: :intent_ledger,
    repo: IntentLedger.FakeRepo,
    intents: %{
      "intent.edge" => [handler: IntentLedger.TestFixtures.EdgeIntent, queue: "default"]
    }
end

defmodule IntentLedger.TestFixtures.StatusProjection do
  @moduledoc false
end

defmodule IntentLedger.TestFixtures.TestIntents do
  @moduledoc false

  use IntentLedger,
    otp_app: :intent_ledger,
    repo: IntentLedger.FakeRepo,
    queues: ["tenant:acme", "tenant:beta"],
    intents: %{
      "invoice.send" => [handler: IntentLedger.TestFixtures.SendInvoice, queue: "default"],
      "invoice.fail" => [handler: IntentLedger.TestFixtures.FailingIntent, queue: "default"]
    }
end

defmodule IntentLedger.TestFixtures.CriticalIntents do
  @moduledoc false

  use IntentLedger,
    otp_app: :intent_ledger,
    repo: IntentLedger.FakeRepo,
    intents: %{
      "invoice.send" => [handler: IntentLedger.TestFixtures.SendInvoice, queue: :critical]
    }
end

defmodule IntentLedger.TestSupport do
  @moduledoc false

  import ExUnit.Assertions

  alias IntentLedger.TestFixtures.{EdgeIntent, EdgeIntents}

  def attach_telemetry(event, test_pid) do
    id = {__MODULE__, event, make_ref()}

    :telemetry.attach(
      id,
      [:intent_ledger, event, :stop],
      &__MODULE__.handle_telemetry/4,
      test_pid
    )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(id) end)
  end

  def handle_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry, event, measurements, metadata})
  end

  def enqueue_edge(mode, opts \\ []) do
    EdgeIntents.enqueue("intent.edge", %{mode: mode, test_pid: self()}, opts)
  end

  def perform_edge(intent, meta) do
    result =
      EdgeIntent.perform(queue_payload(EdgeIntents, intent.id), %{
        topic: "intent.edge",
        queue_id: "default",
        item_id: intent.id,
        attempt: Keyword.get(meta, :attempt, 1)
      })

    if Keyword.get(meta, :finalize, true) do
      finalize_perform(EdgeIntents, intent, result, meta)
    end

    result
  end

  def queue_payload(ledger, intent_id), do: %{raw: :erlang.term_to_binary(%{ledger: ledger, intent_id: intent_id})}

  def finalize_perform(ledger, intent, handler_result, opts \\ []) do
    action = Keyword.get(opts, :action, action_for_result(handler_result))
    queue_result = Keyword.get(opts, :queue_result, queue_result_for_action(action))

    assert :ok =
             IntentLedger.Runtime.QueueLifecycle.apply_queue_action(
               ledger,
               IntentLedger.FakeRepo,
               lease_for(intent),
               action,
               handler_result,
               queue_result
             )
  end

  def lease_for(intent) do
    %Bedrock.JobQueue.Lease{
      id: "test-lease",
      item_id: intent.id,
      queue_id: intent.queue,
      holder: "test",
      obtained_at: 0,
      expires_at: 1,
      item_key: nil
    }
  end

  def assert_history(ledger, intent, expected_types) do
    assert {:ok, signals} = ledger.history(intent.id)
    assert Enum.map(signals, & &1.type) == expected_types
  end

  def assert_handler_telemetry(status, intent, opts \\ []) do
    expected_attempt = Keyword.get(opts, :attempt, 1)

    assert_receive {:telemetry, [:intent_ledger, :handler, :stop], measurements, metadata}
    assert is_integer(measurements.duration)
    assert measurements.count == 1
    assert metadata.ledger == EdgeIntents
    assert metadata.handler == EdgeIntent
    assert metadata.intent_id == intent.id
    assert metadata.topic == "intent.edge"
    assert metadata.queue == "default"
    assert metadata.item_id == intent.id
    assert metadata.attempt == expected_attempt
    assert metadata.status == status
    refute Map.has_key?(metadata, :payload)

    case Keyword.fetch(opts, :error_kind) do
      {:ok, error_kind} -> assert metadata.error_kind == error_kind
      :error -> refute Map.has_key?(metadata, :error_kind)
    end
  end

  defp action_for_result(:ok), do: :complete
  defp action_for_result({:ok, _result}), do: :complete
  defp action_for_result({:discard, _reason}), do: :complete
  defp action_for_result({:error, _reason}), do: :requeue
  defp action_for_result({:snooze, delay_ms}), do: {:snooze, delay_ms}

  defp queue_result_for_action(:complete), do: :ok
  defp queue_result_for_action(:requeue), do: {:ok, :requeued}
  defp queue_result_for_action({:snooze, _delay_ms}), do: {:ok, :requeued}
end

defmodule IntentLedger.TestCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  using do
    quote do
      import IntentLedger.TestSupport

      alias IntentLedger.TestFixtures.{
        CriticalIntents,
        EdgeIntent,
        EdgeIntents,
        FailingIntent,
        SendInvoice,
        StatusProjection,
        TestIntents
      }
    end
  end

  setup do
    IntentLedger.FakeRepo.reset!()
    :ok
  end
end
