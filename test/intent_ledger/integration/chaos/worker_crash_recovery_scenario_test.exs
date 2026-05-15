defmodule IntentLedger.Chaos.WorkerCrashRecoveryScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :chaos

  alias Bedrock.JobQueue.Internal

  @ets_table :intent_ledger_worker_crash_counts

  defmodule CrashAfterSideEffectHandler do
    use IntentLedger.Handler, topic: "worker.crash.after_side_effect"

    @impl true
    def handle(%{test_pid: test_pid}, ctx) do
      run_count = :ets.update_counter(:intent_ledger_worker_crash_counts, ctx.intent.id, {2, 1}, {ctx.intent.id, 0})
      send(test_pid, {:worker_side_effect, ctx.intent.id, run_count, ctx.attempt})

      if run_count == 1 do
        receive do
          :release -> {:ok, %{released: true}}
        after
          30_000 -> {:ok, %{timed_out: true}}
        end
      else
        {:ok, %{recovered: true, run_count: run_count}}
      end
    end
  end

  defmodule CrashRecoveryIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{"worker.crash.after_side_effect" => [handler: CrashAfterSideEffectHandler]}
  end

  setup do
    IntentLedger.RealBedrock.setup!()
    reset_ets!()
    :ok
  end

  test "supervised worker crash after side effect recovers through lease expiry" do
    previous_flag = Process.flag(:trap_exit, true)

    try do
      {:ok, supervisor} = start_runtime()

      assert {:ok, intent} =
               CrashRecoveryIntents.enqueue("worker.crash.after_side_effect", %{
                 test_pid: self()
               })

      assert_receive {:worker_side_effect, intent_id, 1, 1}, 1_000
      assert intent_id == intent.id

      Supervisor.stop(supervisor, :shutdown, 1_000)
      assert_receive {:EXIT, ^supervisor, :shutdown}

      assert {:ok, started} = CrashRecoveryIntents.fetch(intent.id)
      assert started.status == :started

      {:ok, recovered_supervisor} = start_runtime()

      assert_receive {:worker_side_effect, ^intent_id, 2, _attempt}, 1_000

      assert_eventually(fn ->
        case CrashRecoveryIntents.fetch(intent.id) do
          {:ok, completed} ->
            completed.status == :completed and completed.result == %{recovered: true, run_count: 2}

          _other ->
            false
        end
      end)

      Supervisor.stop(recovered_supervisor, :shutdown, 1_000)
      assert_receive {:EXIT, ^recovered_supervisor, :shutdown}
    after
      Process.flag(:trap_exit, previous_flag)
    end
  end

  defp start_runtime do
    CrashRecoveryIntents.start_link(
      root: queue_root(),
      concurrency: 1,
      batch_size: 1,
      scan_interval: 10,
      lease_duration: 10
    )
  end

  defp queue_root, do: Internal.root_keyspace(CrashRecoveryIntents.JobQueue)

  defp reset_ets! do
    if :ets.whereis(@ets_table) != :undefined do
      :ets.delete(@ets_table)
    end

    :ets.new(@ets_table, [:named_table, :public, :set])
    :ok
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
