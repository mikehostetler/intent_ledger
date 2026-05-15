defmodule IntentLedger.PeerFixtures.ProcessInvoice do
  @moduledoc false

  use IntentLedger.Handler, topic: "peer.invoice"

  @impl true
  def handle(%{test_pid: test_pid, node_label: node_label}, ctx) do
    send(test_pid, {:peer_handled, node_label, node(), ctx.intent.id, ctx.attempt})
    {:ok, %{handled_by: node_label, node: node()}}
  end
end

defmodule IntentLedger.PeerFixtures.PeerIntents do
  @moduledoc false

  use IntentLedger,
    otp_app: :intent_ledger,
    repo: IntentLedger.RealBedrock.Repo,
    intents: %{
      "peer.invoice" => [handler: IntentLedger.PeerFixtures.ProcessInvoice, queue: "default"]
    }
end

defmodule IntentLedger.PeerFixtures.Runtime do
  @moduledoc false

  alias Bedrock.JobQueue.Internal
  alias IntentLedger.PeerFixtures.PeerIntents

  def start_cluster_and_runtime!(opts \\ []) do
    start_owned(:runtime, opts)
  end

  def start_cluster!(coordinator_node) do
    start_owned(:cluster, coordinator_node: coordinator_node)
  end

  def stop!(%{owner: owner}) when is_pid(owner) do
    ref = make_ref()
    send(owner, {:stop, self(), ref})

    receive do
      {^ref, :ok} -> :ok
    after
      15_000 -> {:error, :stop_timeout}
    end
  end

  def stop!(_state), do: :ok

  def enqueue_from_peer(topic, payload, opts \\ []), do: PeerIntents.enqueue(topic, payload, with_layout(opts))

  def fetch(intent_id) do
    {:ok, layout} = IntentLedger.RealBedrock.Cluster.fetch_transaction_system_layout()

    PeerIntents
    |> IntentLedger.BedrockStore.transact(
      fn repo, root -> IntentLedger.BedrockStore.fetch(repo, root, intent_id) end,
      transaction_system_layout: layout
    )
    |> IntentLedger.Error.normalize_result()
  end

  def replay(source, opts \\ []), do: PeerIntents.replay(source, with_layout(opts))

  def stats(opts \\ []), do: PeerIntents.stats(with_layout(opts))

  defp start_owned(kind, opts) do
    caller = self()
    ref = make_ref()

    owner =
      spawn(fn ->
        Process.flag(:trap_exit, true)
        send(caller, {ref, start_inside_owner(kind, opts)})
        await_stop()
      end)

    receive do
      {^ref, {:ok, state}} -> {:ok, Map.put(state, :owner, owner)}
      {^ref, error} -> error
    after
      15_000 ->
        Process.exit(owner, :kill)
        {:error, :start_timeout}
    end
  end

  defp start_inside_owner(kind, opts) do
    {:ok, _} = Application.ensure_all_started(:telemetry)
    {:ok, _} = Application.ensure_all_started(:jido_signal)
    coordinator_node = Keyword.fetch!(opts, :coordinator_node)

    cluster =
      IntentLedger.RealBedrock.start_cluster_link!(
        capabilities: [],
        coordinator_node: coordinator_node
      )

    runtime =
      case kind do
        :cluster ->
          nil

        :runtime ->
          {:ok, pid} = PeerIntents.start_link(runtime_opts(opts))
          pid
      end

    {:ok, %{cluster: cluster, runtime: runtime, node: node()}}
  catch
    kind, reason -> {kind, reason}
  end

  defp await_stop do
    receive do
      {:stop, caller, ref} ->
        stop_linked_children()
        send(caller, {ref, :ok})
    end
  end

  defp stop_linked_children do
    Process.info(self(), :links)
    |> case do
      {:links, links} -> links
      _other -> []
    end
    |> Enum.each(fn
      pid when is_pid(pid) ->
        if Process.alive?(pid), do: Supervisor.stop(pid)

      _other ->
        :ok
    end)
  end

  defp runtime_opts(opts) do
    Keyword.merge(
      [
        root: Internal.root_keyspace(PeerIntents.JobQueue),
        concurrency: 1,
        batch_size: 1,
        scan_interval: 10,
        lease_duration: 30_000
      ],
      opts
    )
  end

  defp with_layout(opts) do
    {:ok, layout} = IntentLedger.RealBedrock.Cluster.fetch_transaction_system_layout()
    Keyword.put(opts, :transaction_system_layout, layout)
  end
end
