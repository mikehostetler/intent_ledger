defmodule IntentLedger.TimeTest do
  use ExUnit.Case, async: true

  alias IntentLedger.Time

  test "normalizes nil, DateTime, and ISO8601 values" do
    default = ~U[2026-05-14 12:00:00Z]

    assert {:ok, ^default} = Time.normalize(nil, default)
    assert {:ok, ^default} = Time.normalize(default, nil)
    assert {:ok, ^default} = Time.normalize("2026-05-14T12:00:00Z", nil)

    assert {:ok, %DateTime{}} = Time.normalize(nil, nil)
  end

  test "rejects invalid datetime values and adds milliseconds" do
    assert {:error, {:invalid_datetime, "bad", _reason}} = Time.normalize("bad", nil)
    assert {:error, {:invalid_datetime, 123}} = Time.normalize(123, nil)

    assert Time.add_ms(~U[2026-05-14 12:00:00Z], 1_500) == ~U[2026-05-14 12:00:01.500Z]
  end
end
