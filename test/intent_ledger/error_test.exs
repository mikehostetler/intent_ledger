defmodule IntentLedger.ErrorTest do
  use ExUnit.Case, async: true

  alias IntentLedger.Error

  test "constructors normalize details" do
    assert %Error.InvalidInputError{field: :topic, value: :bad} =
             Error.invalid("bad", field: :topic, value: :bad)

    assert %Error.ConflictError{reason: :not_cancelable, details: %{status: :completed}} =
             Error.conflict(:not_cancelable, status: :completed)

    assert %Error.RuntimeError{message: "boom", details: %{reason: :bad}} = Error.runtime("boom", :bad)
  end

  test "from_reason maps public validation and conflict reasons" do
    assert %Error.InvalidInputError{field: :topic, value: "missing"} = Error.from_reason({:unknown_topic, "missing"})
    assert %Error.InvalidInputError{field: :command, value: :bad} = Error.from_reason({:unsupported_command, :bad})
    assert %Error.InvalidInputError{field: :signal, value: :bad} = Error.from_reason({:invalid_command_signal, :bad})
    assert %Error.InvalidInputError{field: :data, value: :bad} = Error.from_reason({:invalid_command_signal_data, :bad})
    assert %Error.InvalidInputError{field: :intent_id} = Error.from_reason({:missing_command_field, :intent_id})
    assert %Error.InvalidInputError{field: :datetime, value: "bad"} = Error.from_reason({:invalid_datetime, "bad"})

    assert %Error.InvalidInputError{field: :datetime, value: "bad"} =
             Error.from_reason({:invalid_datetime, "bad", :bad})

    assert %Error.ConflictError{reason: :not_ambiguousable} = Error.from_reason({:not_ambiguousable, :completed})
  end

  test "from_reason maps runtime and unknown reasons" do
    exception = RuntimeError.exception("already good")

    assert Error.from_reason(exception) == exception
    assert %Error.RuntimeError{details: %{ledger: BadLedger}} = Error.from_reason({:unknown_ledger, BadLedger})
    assert %Error.RuntimeError{} = Error.from_reason(:invalid_queue_payload)
    assert %Error.InvalidInputError{} = Error.from_reason(:not_found)
    assert %Error.RuntimeError{details: %{reason: :other}} = Error.from_reason(:other)
  end

  test "normalize_result converts only error tuples" do
    assert {:error, %Error.InvalidInputError{}} = Error.normalize_result({:error, {:required, :topic}})
    assert {:ok, :value} = Error.normalize_result({:ok, :value})
  end
end
