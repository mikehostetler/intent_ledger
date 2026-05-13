defmodule IntentLedger.ProjectionTest do
  use ExUnit.Case, async: false

  defmodule StatusProjection do
    @behaviour IntentLedger.Projection

    @impl true
    def init(opts) do
      %{
        version: 0,
        prefix: Keyword.get(opts, :prefix),
        subject: nil,
        status: nil,
        transitions: []
      }
    end

    @impl true
    def apply_signal(%{type: "intent_ledger.intent.submitted", subject: subject}, projection, context) do
      send(Keyword.fetch!(context.opts, :test_pid), {:projected, context.index, :submitted})

      advance(projection,
        subject: subject,
        status: :submitted,
        transitions: projection.transitions ++ [:submitted]
      )
    end

    def apply_signal(%{type: "intent_ledger.intent.available"}, projection, _context) do
      advance(projection, status: :available, transitions: projection.transitions ++ [:available])
    end

    def apply_signal(%{type: "intent_ledger.intent.claimed"}, projection, _context) do
      advance(projection, status: :claimed, transitions: projection.transitions ++ [:claimed])
    end

    def apply_signal(%{type: "intent_ledger.intent.completed"}, projection, _context) do
      advance(projection, status: :completed, transitions: projection.transitions ++ [:completed])
    end

    def apply_signal(_signal, _projection, _context), do: :ignore

    defp advance(projection, attrs) do
      projection
      |> Map.merge(Map.new(attrs))
      |> Map.update!(:version, &(&1 + 1))
    end
  end

  defmodule FailingProjection do
    @behaviour IntentLedger.Projection

    @impl true
    def apply_signal(_signal, _projection, _context), do: {:error, :boom}
  end

  test "rebuild applies projection hooks over replayed signals" do
    name = Module.concat(__MODULE__, "Ledger#{System.unique_integer([:positive])}")

    start_supervised!({IntentLedger, name: name, queues: [default: [shards: 1]], dispatcher_interval_ms: 10_000})

    {:ok, record} =
      IntentLedger.submit(name, %{
        key: "projection:1",
        kind: "projection.test",
        shard: 0
      })

    {:ok, claimed} = IntentLedger.claim(name, :default, "projection-worker")
    assert {:ok, _completed} = IntentLedger.complete(name, claimed.claim.id, claimed.claim.token, %{ok: true})

    assert {:ok, projection} =
             IntentLedger.rebuild_projection(name, StatusProjection,
               source: {:intent, record.intent.id},
               projection: [test_pid: self(), prefix: :intent]
             )

    assert projection == %{
             version: 4,
             prefix: :intent,
             subject: "intent:" <> record.intent.id,
             status: :completed,
             transitions: [:submitted, :available, :claimed, :completed]
           }

    assert_receive {:projected, 0, :submitted}
  end

  test "catch_up applies a replay window to an existing projection" do
    partial = %{version: 1, prefix: nil, subject: "intent:int_1", status: :submitted, transitions: [:submitted]}

    signals = [
      %{type: "intent_ledger.intent.available", subject: "int_1"},
      %{type: "intent_ledger.intent.claimed", subject: "int_1"}
    ]

    assert {:ok, projection} = IntentLedger.Projection.catch_up(StatusProjection, partial, signals)

    assert projection.status == :claimed
    assert projection.version == 3
    assert projection.transitions == [:submitted, :available, :claimed]
  end

  test "projection errors halt rebuilds" do
    assert {:error, {FailingProjection, :boom}} =
             IntentLedger.Projection.rebuild(FailingProjection, [%{type: "intent_ledger.intent.submitted"}])
  end
end
