defmodule IntentLedger.Bedrock.SupervisedWorkerScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :bedrock

  alias Bedrock.JobQueue.Internal

  defmodule NotifyHandler do
    use IntentLedger.Handler, topic: "runtime.supervised.notify"

    @impl true
    def handle(%{test_pid: test_pid, label: label}, ctx) do
      send(test_pid, {:supervised_handled, label, ctx.intent.id, ctx.queue, ctx.attempt})
      {:ok, %{label: label, queue: ctx.queue}}
    end
  end

  defmodule BulkHandler do
    use IntentLedger.Handler, topic: "runtime.supervised.bulk"

    @impl true
    def handle(%{test_pid: test_pid, label: label}, ctx) do
      send(test_pid, {:bulk_handled, label, ctx.intent.id, ctx.queue, ctx.attempt})
      :ok
    end
  end

  defmodule PayloadProbeHandler do
    use IntentLedger.Handler, topic: "runtime.supervised.payload"

    @impl true
    def handle(%{test_pid: test_pid, attachment: attachment}, ctx) do
      send(test_pid, {:payload_probe, ctx.intent.id, byte_size(attachment), ctx.queue})
      {:ok, %{stored: true, bytes: byte_size(attachment)}}
    end
  end

  defmodule RuntimeIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{
        "runtime.supervised.notify" => [handler: NotifyHandler, queue: "fast"],
        "runtime.supervised.bulk" => [handler: BulkHandler, queue: "bulk"],
        "runtime.supervised.payload" => [handler: PayloadProbeHandler, queue: "fast"]
      },
      queues: ["fast", "bulk"]
  end

  setup do
    IntentLedger.RealBedrock.setup!()
  end

  test "generated child_spec drives handler execution through the supervised job_queue runtime" do
    start_runtime!()

    assert {:ok, intent} =
             RuntimeIntents.enqueue("runtime.supervised.notify", %{
               label: "supervised",
               test_pid: self()
             })

    assert_receive {:supervised_handled, "supervised", intent_id, "fast", 1}, 1_000
    assert intent_id == intent.id

    assert_eventually(fn ->
      case RuntimeIntents.fetch(intent.id) do
        {:ok, completed} -> completed.status == :completed and completed.result == %{label: "supervised", queue: "fast"}
        _other -> false
      end
    end)

    assert {:ok, %{"fast" => %{pending_count: 0, processing_count: 0}}} =
             RuntimeIntents.stats(queue: "fast")
  end

  test "configured queues route independently under the supervised runtime" do
    start_runtime!()

    assert {:ok, fast} =
             RuntimeIntents.enqueue("runtime.supervised.notify", %{
               label: "fast",
               test_pid: self()
             })

    assert {:ok, bulk} =
             RuntimeIntents.enqueue("runtime.supervised.bulk", %{
               label: "bulk",
               test_pid: self()
             })

    assert_receive {:supervised_handled, "fast", fast_id, "fast", 1}, 1_000
    assert_receive {:bulk_handled, "bulk", bulk_id, "bulk", 1}, 1_000
    assert fast_id == fast.id
    assert bulk_id == bulk.id

    assert_eventually(fn ->
      with {:ok, %{"fast" => fast_stats}} <- RuntimeIntents.stats(queue: "fast"),
           {:ok, %{"bulk" => bulk_stats}} <- RuntimeIntents.stats(queue: "bulk") do
        fast_stats.pending_count == 0 and fast_stats.processing_count == 0 and
          bulk_stats.pending_count == 0 and bulk_stats.processing_count == 0
      else
        _other -> false
      end
    end)
  end

  test "signal-native enqueue reaches the same supervised execution path as direct enqueue" do
    start_runtime!()

    assert {:ok, signal} =
             RuntimeIntents.command_signal(:enqueue,
               topic: "runtime.supervised.notify",
               payload: %{label: "signal", test_pid: self()}
             )

    assert {:ok, intent} = RuntimeIntents.submit(signal)
    assert intent.key == "signal:#{signal.id}"

    assert_receive {:supervised_handled, "signal", intent_id, "fast", 1}, 1_000
    assert intent_id == intent.id

    assert_eventually(fn ->
      case RuntimeIntents.fetch(intent.id) do
        {:ok, completed} -> completed.status == :completed
        _other -> false
      end
    end)
  end

  test "payload provenance keeps full payload on the Intent and only a pointer in the queue" do
    start_runtime!()
    attachment = :crypto.strong_rand_bytes(2_048)
    telemetry_ref = attach_enqueue_probe()

    assert {:ok, intent} =
             RuntimeIntents.enqueue("runtime.supervised.payload", %{
               attachment: attachment,
               test_pid: self()
             })

    assert_receive {:enqueue_telemetry, ^telemetry_ref, metadata}, 500
    refute Map.has_key?(metadata, :payload)
    refute Map.has_key?(metadata, "payload")
    assert_receive {:payload_probe, intent_id, 2_048, "fast"}, 1_000
    assert intent_id == intent.id

    assert {:ok, stored} = RuntimeIntents.fetch(intent.id)
    assert stored.payload.attachment == attachment

    assert_eventually(fn ->
      case RuntimeIntents.fetch(intent.id) do
        {:ok, completed} -> completed.status == :completed and completed.result == %{stored: true, bytes: 2_048}
        _other -> false
      end
    end)

    :telemetry.detach(telemetry_ref)
  end

  defp start_runtime! do
    start_supervised!({RuntimeIntents, concurrency: 2, batch_size: 2, root: queue_root()})
  end

  defp queue_root, do: Internal.root_keyspace(RuntimeIntents.JobQueue)

  defp attach_enqueue_probe do
    ref = "intent-ledger-supervised-enqueue-#{System.unique_integer([:positive])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        ref,
        [:intent_ledger, :enqueue, :stop],
        fn _event, _measurements, metadata, ^ref ->
          send(test_pid, {:enqueue_telemetry, ref, metadata})
        end,
        ref
      )

    ref
  end

  defp assert_eventually(fun, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(fun, deadline)
  end

  defp do_assert_eventually(fun, deadline) do
    if fun.() do
      assert true
    else
      if System.monotonic_time(:millisecond) >= deadline do
        assert fun.()
      else
        Process.sleep(20)
        do_assert_eventually(fun, deadline)
      end
    end
  end
end
