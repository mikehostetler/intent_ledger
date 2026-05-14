defmodule IntentLedger.StoreBedrockGuardrailTest do
  use ExUnit.Case, async: true

  @moduletag :integration
  @moduletag :bedrock

  alias IntentLedger.Error
  alias IntentLedger.Store.Bedrock

  test "declares Bedrock as an optional dependency" do
    assert {:bedrock, "~> 0.5.0", opts} =
             Enum.find(Mix.Project.config()[:deps], &match?({:bedrock, _, _}, &1))

    assert opts[:optional]
  end

  test "does not start Bedrock as a core package application" do
    refute :bedrock in IntentLedger.MixProject.application()[:extra_applications]
  end

  test "normalizes missing dependency failures through IntentLedger.Error" do
    missing = Module.concat([IntentLedger, Store, MissingBedrock])

    assert {:error,
            %Error.AdapterRuntimeError{
              adapter: Bedrock,
              details: %{
                dependency: :bedrock,
                missing_modules: [^missing],
                reason: :missing_dependency
              }
            }} = Bedrock.ensure_available([missing])
  end

  test "exposes a store child spec without forcing Bedrock to load" do
    assert %{
             id: {Bedrock, :my_store},
             start: {Bedrock, :start_link, [[name: :my_store]]},
             type: :worker
           } = Bedrock.child_spec(name: :my_store)
  end
end
