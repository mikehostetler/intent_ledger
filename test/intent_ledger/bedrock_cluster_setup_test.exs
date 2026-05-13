defmodule IntentLedger.BedrockClusterSetupTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :bedrock
  @moduletag :multi_node
  @moduletag :bedrock_cluster

  alias IntentLedger.BedrockClusterSetup
  alias IntentLedger.PeerNodes

  test "starts peer Bedrock nodes with a shared descriptor and object storage" do
    cluster =
      BedrockClusterSetup.start_cluster!(3,
        peer_opts: [prefix: :intent_ledger_bedrock_cluster],
        coordinator_ping_timeout_in_ms: 100,
        gateway_ping_timeout_in_ms: 100
      )

    assert File.dir?(cluster.object_storage_path)

    assert {:ok, descriptor} = Bedrock.Cluster.Descriptor.read_from_file(cluster.descriptor_path)
    assert descriptor.cluster_name == cluster.cluster.name()
    assert descriptor.coordinator_nodes == cluster.nodes

    assert cluster.nodes == Enum.uniq(cluster.nodes)
    assert map_size(cluster.supervisors) == 3

    [first, second | _] = cluster.peers

    assert :ok = PeerNodes.call(first, BedrockClusterSetup, :put_object, [cluster.cluster, "probe/key", "value"])
    assert {:ok, "value"} = PeerNodes.call(second, BedrockClusterSetup, :get_object, [cluster.cluster, "probe/key"])

    for peer <- cluster.peers do
      status = PeerNodes.call(peer, BedrockClusterSetup, :node_status, [cluster.cluster])

      assert status.node == peer.node
      assert {:ok, %{coordinator_nodes: coordinator_nodes}} = status.descriptor
      assert coordinator_nodes == cluster.nodes
      assert status.config[:path_to_descriptor] == cluster.descriptor_path
      assert status.config[:object_storage] == cluster.object_storage
      assert status.config[:capabilities] == [:coordination]
      assert is_pid(status.services.coordinator)
      assert is_pid(status.services.link)
    end
  end
end
