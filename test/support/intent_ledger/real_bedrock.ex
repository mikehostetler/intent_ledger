defmodule IntentLedger.RealBedrock.Cluster do
  @moduledoc false

  @working_dir Path.join(System.tmp_dir!(), "intent_ledger_real_bedrock")

  use Bedrock.Cluster,
    otp_app: :intent_ledger,
    name: "intent_ledger_real_bedrock"

  def working_dir, do: @working_dir
end

defmodule IntentLedger.RealBedrock.Repo do
  @moduledoc false

  use Bedrock.Repo, cluster: IntentLedger.RealBedrock.Cluster
end

defmodule IntentLedger.RealBedrock do
  @moduledoc false

  import ExUnit.Callbacks, only: [on_exit: 1, start_supervised!: 1]

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias IntentLedger.RealBedrock.Cluster
  alias IntentLedger.RealBedrock.Repo

  @ready_timeout_ms 15_000
  @active_capabilities [:coordination, :log, :materializer]

  def reset! do
    previous_object_storage = Application.get_env(:bedrock, ObjectStorage)
    working_dir = Cluster.working_dir()

    File.rm_rf!(working_dir)
    File.mkdir_p!(working_dir)

    configure_object_storage!()
    configure_cluster!(@active_capabilities)

    on_exit(fn ->
      case previous_object_storage do
        nil -> Application.delete_env(:bedrock, ObjectStorage)
        value -> Application.put_env(:bedrock, ObjectStorage, value)
      end
    end)

    :ok
  end

  def setup! do
    reset!()
    start_cluster!()
    :ok
  end

  def configure_object_storage! do
    Application.put_env(:bedrock, ObjectStorage,
      backend: {LocalFilesystem, root: Path.join(Cluster.working_dir(), "objects")},
      bootstrap_key: "bootstrap"
    )

    :ok
  end

  def repo, do: Repo

  def configure_cluster!(capabilities \\ @active_capabilities) when is_list(capabilities) do
    object_storage = {LocalFilesystem, root: Path.join(Cluster.working_dir(), "objects")}

    Application.put_env(:intent_ledger, Cluster,
      capabilities: capabilities,
      durability_mode: :relaxed,
      object_storage: object_storage,
      trace: [],
      coordinator: [path: Cluster.working_dir()],
      materializer: [path: Cluster.working_dir(), object_storage: object_storage],
      log: [path: Cluster.working_dir(), object_storage: object_storage]
    )

    :ok
  end

  def start_cluster! do
    ensure_node_started!()
    configure_cluster!(@active_capabilities)
    write_descriptor!(Node.self())
    pid = start_supervised!({Cluster, []})
    wait_until_ready!()
    pid
  end

  def start_cluster_link!(opts \\ []) do
    ensure_node_started!()
    configure_object_storage!()
    configure_cluster!(Keyword.get(opts, :capabilities, @active_capabilities))
    write_descriptor!(Keyword.get(opts, :coordinator_node, Node.self()))

    %{start: {module, function, args}} = Cluster.child_spec([])
    {:ok, pid} = apply(module, function, args)
    wait_until_ready!(Keyword.get(opts, :ready_timeout_ms, @ready_timeout_ms))
    pid
  end

  def stop_cluster_link!(pid) when is_pid(pid) do
    Supervisor.stop(pid)
    :ok
  end

  def start_peer!(name) when is_atom(name) do
    ensure_node_started!()

    args =
      [~c"-setcookie", Atom.to_charlist(Node.get_cookie())] ++
        Enum.flat_map(:code.get_path(), &[~c"-pa", &1])

    {:ok, peer, node} =
      :peer.start_link(%{
        name: name,
        host: ~c"127.0.0.1",
        longnames: true,
        connection: 0,
        wait_boot: 15_000,
        args: args
      })

    true = Node.connect(node)
    {peer, node}
  end

  defp ensure_node_started! do
    if Node.alive?() do
      :ok
    else
      case Node.start(:"intent_ledger_real_bedrock@127.0.0.1") do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
        {:error, reason} -> raise "could not start local distributed node for Bedrock tests: #{inspect(reason)}"
      end
    end
  end

  defp write_descriptor!(coordinator_node) do
    path = Cluster.path_to_descriptor()
    File.mkdir_p!(Path.dirname(path))
    Descriptor.write_to_file!(path, Descriptor.new(Cluster.name(), [coordinator_node]))
  end

  defp cache_layout!(layout) do
    :sys.replace_state(Cluster.otp_name(:link), fn state ->
      %{state | transaction_system_layout: layout}
    end)
  catch
    :exit, _reason -> :ok
  end

  def wait_until_ready!(timeout_ms \\ @ready_timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait_for_layout(deadline)
  end

  defp wait_for_layout(deadline) do
    case Cluster.fetch_transaction_system_layout() do
      {:ok, %{epoch: _epoch, proxies: proxies} = layout} when is_list(proxies) ->
        cache_layout!(layout)
        layout

      _other ->
        if System.monotonic_time(:millisecond) >= deadline do
          raise "Bedrock transaction system layout did not become ready"
        end

        Process.sleep(50)
        wait_for_layout(deadline)
    end
  end
end
