defmodule IntentLedger.MultiNode.PeerNetSplitScenarioTest do
  use ExUnit.Case, async: false

  @moduletag :multi_node
  @moduletag :peer_net_split
  @moduletag skip: "requires the real :peer multi-node Bedrock harness"

  test "real peer node partition cannot mutate terminal state after heal" do
    flunk("""
    TDD peer-net-split acceptance:

    1. Start three `:peer` Erlang nodes connected to one real Bedrock cluster.
    2. Node A submits an `intent.command.enqueue` signal.
    3. Node B leases and starts the handler.
    4. Disconnect Node B before the queue action/lifecycle transaction commits.
    5. Node C observes recovery after lease expiry and completes the Intent.
    6. Reconnect Node B and prove its stale completion cannot append another
       terminal lifecycle fact or change queue state.
    """)
  end
end
