defmodule IntentLedger.ReplayProjectionRepairScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias IntentLedger.BedrockStore
  alias IntentLedger.BedrockStore.Keyspaces
  alias IntentLedger.Projection
  alias IntentLedger.Repair

  defmodule ProjectionHandler do
    use IntentLedger.Handler, topic: "projection.invoice"

    @impl true
    def handle(_payload, _ctx), do: :ok
  end

  defmodule ProjectionIntents do
    use IntentLedger,
      otp_app: :intent_ledger,
      repo: IntentLedger.RealBedrock.Repo,
      intents: %{"projection.invoice" => [handler: ProjectionHandler]}
  end

  defmodule StatusProjection do
    @behaviour IntentLedger.Projection

    @impl true
    def init(_opts), do: %{counts: %{}, subjects: []}

    @impl true
    def apply_signal(%Jido.Signal{} = signal, projection, _ctx) do
      status = signal.type |> String.replace_prefix("intent.", "") |> String.to_atom()

      projection
      |> update_in([:counts, status], &((&1 || 0) + 1))
      |> update_in([:subjects], &[signal.subject | &1])
    end
  end

  setup do
    IntentLedger.RealBedrock.setup!()
  end

  test "projection rebuild after restart derives state from replay and records a monotonic cursor" do
    assert {:ok, first} =
             ProjectionIntents.enqueue("projection.invoice", %{invoice_id: 1}, key: "projection:1")

    assert {:ok, second} =
             ProjectionIntents.enqueue("projection.invoice", %{invoice_id: 2}, key: "projection:2")

    assert {:ok, canceled} = ProjectionIntents.cancel(second.id, :duplicate)
    assert canceled.status == :canceled

    assert {:ok, entries} = ProjectionIntents.replay_entries(:ledger, limit: 100)
    signals = Enum.map(entries, & &1.signal)

    assert {:ok, projection} = Projection.rebuild(StatusProjection, signals)
    assert projection.counts.enqueued == 2
    assert projection.counts.canceled == 1
    assert first.id in projection.subjects
    assert second.id in projection.subjects

    last_cursor = entries |> List.last() |> Map.fetch!(:cursor)
    assert :ok = ProjectionIntents.put_projection_cursor(StatusProjection, last_cursor)
    assert {:ok, ^last_cursor} = ProjectionIntents.projection_cursor(StatusProjection)
  end

  test "repair verifier reports outbox mirror drift after an outbox entry is lost" do
    assert {:ok, intent} =
             ProjectionIntents.enqueue("projection.invoice", %{invoice_id: 3}, key: "projection:repair")

    BedrockStore.transact(ProjectionIntents, fn repo, root ->
      repo.clear(Keyspaces.outbox(root), 1)
      :ok
    end)

    assert {:ok, report} = Repair.verify(ProjectionIntents)

    refute report.valid?
    assert %{status: :drift, details: details} = check(report, :outbox_mirror)
    assert details.ledger_count == 1
    assert details.outbox_count == 0

    assert {:ok, [history_signal]} = ProjectionIntents.history(intent.id)
    assert history_signal.type == "intent.enqueued"
  end

  defp check(report, name), do: Enum.find(report.checks, &(&1.name == name))
end
