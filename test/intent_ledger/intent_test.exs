defmodule IntentLedger.IntentTest do
  use ExUnit.Case, async: true

  alias IntentLedger.Intent

  test "adds first-class lineage fields with root defaults" do
    assert {:ok, intent} =
             Intent.new(%{
               id: "int_root",
               key: "job:root",
               kind: "job.run"
             })

    assert intent.correlation_id == "int_root"
    assert intent.causation_id == nil
    assert intent.root_intent_id == "int_root"
    assert intent.parent_intent_id == nil
    assert intent.depth == 0
    assert intent.actor == nil

    assert intent.metadata.correlation_id == "int_root"
    assert intent.metadata.root_intent_id == "int_root"
    assert intent.metadata.depth == 0
  end

  test "normalizes explicit lineage fields and mirrors them into metadata" do
    assert {:ok, intent} =
             Intent.new(%{
               "id" => "int_child",
               "key" => "job:child",
               "kind" => "job.run",
               "correlation_id" => "corr_1",
               "causation_id" => "cmd_parent",
               "root_intent_id" => "int_root",
               "parent_intent_id" => "int_parent",
               "depth" => "2",
               "actor" => :agent,
               "metadata" => %{"custom" => true}
             })

    assert intent.correlation_id == "corr_1"
    assert intent.causation_id == "cmd_parent"
    assert intent.root_intent_id == "int_root"
    assert intent.parent_intent_id == "int_parent"
    assert intent.depth == 2
    assert intent.actor == "agent"

    assert intent.metadata.correlation_id == "corr_1"
    assert intent.metadata.causation_id == "cmd_parent"
    assert intent.metadata.root_intent_id == "int_root"
    assert intent.metadata.parent_intent_id == "int_parent"
    assert intent.metadata.depth == 2
    assert intent.metadata.actor == "agent"
    assert intent.metadata["custom"]
  end

  test "accepts lineage from legacy metadata" do
    assert {:ok, intent} =
             Intent.new(%{
               id: "int_child",
               key: "job:child",
               kind: "job.run",
               metadata: %{
                 correlation_id: "corr_1",
                 causation_id: "cmd_parent",
                 root_intent_id: "int_root",
                 parent_intent_id: "int_parent",
                 depth: 3,
                 actor: "agent"
               }
             })

    assert intent.correlation_id == "corr_1"
    assert intent.causation_id == "cmd_parent"
    assert intent.root_intent_id == "int_root"
    assert intent.parent_intent_id == "int_parent"
    assert intent.depth == 3
    assert intent.actor == "agent"
  end

  test "rejects invalid lineage fields" do
    assert {:error, {:invalid_non_negative_integer, :depth, -1}} =
             Intent.new(%{
               key: "job:bad-depth",
               kind: "job.run",
               depth: -1
             })

    assert {:error, {:invalid_string, :actor, ""}} =
             Intent.new(%{
               key: "job:bad-actor",
               kind: "job.run",
               actor: ""
             })
  end
end
