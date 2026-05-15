defmodule IntentLedger.DurableTermTest do
  use ExUnit.Case, async: true

  alias IntentLedger.DurableTerm

  test "summarizes unsafe durable values without losing simple terms" do
    assert DurableTerm.summarize(%{ok: true, reason: {:error, :boom}}) == %{ok: true, reason: {:error, :boom}}

    assert DurableTerm.summarize({:exception, %RuntimeError{message: "secret"}}) ==
             {:exception, %{type: :exception, module: "RuntimeError", redacted: true}}

    assert DurableTerm.summarize(:crypto.strong_rand_bytes(1_100)) == %{type: :binary, bytes: 1_100, redacted: true}
  end
end
