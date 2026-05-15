defmodule IntentLedger.BedrockStore.ProjectionsTest do
  use IntentLedger.TestCase, async: false

  defmodule CountingProjection do
    @moduledoc false

    def init(_opts), do: %{count: 0, subjects: []}

    def apply_signal(signal, projection, context) do
      assert context.projection == __MODULE__
      assert is_integer(context.index)

      %{
        projection
        | count: projection.count + 1,
          subjects: [signal.subject | projection.subjects]
      }
    end
  end

  defmodule IgnoreProjection do
    @moduledoc false

    def apply_signal(_signal, _projection, _context), do: :ignore
  end

  defmodule ErrorProjection do
    @moduledoc false

    def apply_signal(_signal, _projection, _context), do: {:error, :bad_signal}
  end

  defmodule ThrowProjection do
    @moduledoc false

    def apply_signal(_signal, _projection, _context), do: throw(:bad_projection)
  end

  test "projection cursors are durable per configured ledger" do
    assert {:ok, _first} = TestIntents.enqueue("invoice.send", %{invoice_id: 1})
    assert {:ok, _second} = TestIntents.enqueue("invoice.send", %{invoice_id: 2})

    assert {:ok, nil} = TestIntents.projection_cursor(StatusProjection)
    assert :ok = TestIntents.put_projection_cursor(StatusProjection, 2)
    assert {:ok, 2} = TestIntents.projection_cursor(StatusProjection)

    attach_telemetry(:projection, self())

    assert {:error, %IntentLedger.Error.ConflictError{reason: :stale_projection_cursor}} =
             TestIntents.put_projection_cursor(StatusProjection, 1)

    assert_receive {:telemetry, [:intent_ledger, :projection, :stop], measurements, metadata}
    assert is_integer(measurements.duration)
    assert metadata.operation == :put_cursor
    assert metadata.status == :error
    assert metadata.error_kind == :stale_projection_cursor

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :cursor, value: 3}} =
             TestIntents.put_projection_cursor(StatusProjection, 3)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :force}} =
             TestIntents.put_projection_cursor(StatusProjection, 1, force: true)

    assert :ok = TestIntents.put_projection_cursor(StatusProjection, 1, force: true, repair: true)
    assert {:ok, 1} = TestIntents.projection_cursor(StatusProjection)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :allow_ahead}} =
             TestIntents.put_projection_cursor(StatusProjection, 3, allow_ahead: true)

    assert :ok = TestIntents.put_projection_cursor(StatusProjection, 3, allow_ahead: true, repair: true)
    assert {:ok, 3} = TestIntents.projection_cursor(StatusProjection)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :cursor, value: -1}} =
             TestIntents.put_projection_cursor(StatusProjection, -1)

    assert {:error, %IntentLedger.Error.InvalidInputError{field: :projection, value: ""}} =
             TestIntents.projection_cursor("")
  end

  test "Projection rebuild and catch_up apply lifecycle signals deterministically" do
    assert {:ok, first} = TestIntents.enqueue("invoice.send", %{invoice_id: 1})
    assert {:ok, second} = TestIntents.enqueue("invoice.send", %{invoice_id: 2})
    assert {:ok, signals} = TestIntents.replay(:ledger)

    assert {:ok, projection} = IntentLedger.Projection.rebuild(CountingProjection, signals)
    assert projection.count == 2
    assert Enum.reverse(projection.subjects) == [first.id, second.id]

    assert {:ok, caught_up} = IntentLedger.Projection.catch_up(CountingProjection, %{count: 10, subjects: []}, signals)
    assert caught_up.count == 12
  end

  test "Projection apply supports ignore and error paths" do
    assert {:ok, intent} = TestIntents.enqueue("invoice.send", %{invoice_id: 1})
    assert {:ok, [signal]} = TestIntents.replay(:ledger)

    assert {:ok, %{existing: true}} = IntentLedger.Projection.apply(IgnoreProjection, signal, %{existing: true})
    assert {:error, {ErrorProjection, :bad_signal}} = IntentLedger.Projection.apply(ErrorProjection, signal, %{})

    assert {:error, {ThrowProjection, {:throw, :bad_projection}}} =
             IntentLedger.Projection.apply(ThrowProjection, signal, %{})

    assert signal.subject == intent.id
  end
end
