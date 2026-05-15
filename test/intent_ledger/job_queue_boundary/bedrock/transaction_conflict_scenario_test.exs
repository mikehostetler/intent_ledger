defmodule IntentLedger.Bedrock.TransactionConflictScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :bedrock
  @moduletag :real_bedrock
  @moduletag :job_queue_boundary

  alias Bedrock.JobQueue.{Internal, Store}
  alias IntentLedger.Repair
  alias IntentLedger.Runtime.QueueLifecycle

  defmodule ConflictHandler do
    use IntentLedger.Handler, topic: "real_bedrock.conflict"

    @impl true
    def handle(_payload, _ctx), do: :ok
  end

  defmodule ConflictIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{
        "real_bedrock.conflict" => [handler: ConflictHandler, queue: "default"]
      }
  end

  setup do
    IntentLedger.RealBedrock.reset!()
    IntentLedger.RealBedrock.start_cluster!()
    :ok
  end

  test "concurrent queue completions conflict through Bedrock and leave one terminal lifecycle fact" do
    assert {:ok, intent} = ConflictIntents.enqueue("real_bedrock.conflict", %{invoice_id: 123})
    lease = lease_intent(intent)

    parent = self()

    tasks =
      for label <- [:a, :b] do
        Task.async(fn -> safe_complete_after_barrier(label, parent, lease) end)
      end

    assert_receive {:completion_ready, :a}
    assert_receive {:completion_ready, :b}
    Enum.each(tasks, &send(&1.pid, :go))

    results =
      Enum.map(tasks, fn task ->
        Task.yield(task, 10_000) || Task.shutdown(task, :brutal_kill)
      end)

    assert Enum.count(results, &(&1 == {:ok, :ok})) == 1
    assert Enum.count(results, &match?({:ok, {:raised, %RuntimeError{}}}, &1)) == 1

    assert {:ok, completed} = ConflictIntents.fetch(intent.id)
    assert completed.status == :completed

    assert {:ok, history} = ConflictIntents.history(intent.id)
    assert Enum.map(history, & &1.type) == ["intent.enqueued", "intent.completed"]

    assert {:ok, report} = Repair.verify(ConflictIntents)
    assert report.valid?
  end

  defp safe_complete_after_barrier(label, parent, lease) do
    complete_after_barrier(label, parent, lease)
  rescue
    exception -> {:raised, exception}
  catch
    kind, reason -> {:caught, kind, reason}
  end

  defp complete_after_barrier(label, parent, lease) do
    repo = IntentLedger.RealBedrock.Repo
    queue_root = queue_root()

    repo.transact(
      fn ->
        keyspaces = Store.queue_keyspaces(queue_root, lease.queue_id)
        _stored_lease = repo.get(keyspaces.leases, lease.item_id)
        send(parent, {:completion_ready, label})

        receive do
          :go -> :ok
        after
          5_000 -> raise "completion barrier timed out"
        end

        queue_result = Store.complete(repo, queue_root, lease)

        case QueueLifecycle.apply(
               repo,
               queue_root,
               lease,
               :complete,
               {:ok, %{committed_by: label}},
               queue_result,
               ConflictIntents
             ) do
          :ok -> :ok
          {:error, reason} -> repo.rollback(reason)
        end
      end,
      retry_limit: 0
    )
  end

  defp lease_intent(intent) do
    repo = IntentLedger.RealBedrock.Repo

    repo.transact(fn ->
      [item] = Store.peek(repo, queue_root(), intent.queue, limit: 1)
      Store.obtain_lease(repo, queue_root(), item, "conflict-test", 30_000)
    end)
    |> then(fn
      {:ok, lease} -> lease
      other -> flunk("expected lease, got: #{inspect(other)}")
    end)
  end

  defp queue_root, do: Internal.root_keyspace(ConflictIntents.JobQueue)
end
