defmodule IntentLedger.BedrockTestCluster do
  @moduledoc false

  use Bedrock.Cluster, name: "intent_ledger_test", otp_app: :intent_ledger
end

defmodule IntentLedger.BedrockTestRepo do
  @moduledoc false

  use Bedrock.Repo, cluster: IntentLedger.BedrockTestCluster
end

defmodule IntentLedger.BedrockClusterSetup do
  @moduledoc false

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias IntentLedger.PeerNodes

  @default_cluster IntentLedger.BedrockTestCluster
  @default_repo IntentLedger.BedrockTestRepo
  @default_capabilities [:coordination]
  @default_timeout 15_000

  @type t :: %{
          base_path: Path.t(),
          descriptor_path: Path.t(),
          object_storage: ObjectStorage.backend(),
          object_storage_path: Path.t(),
          peers: [PeerNodes.peer()],
          nodes: [node()],
          supervisors: %{node() => pid()},
          cluster: module(),
          repo: module()
        }

  @spec start_cluster!(pos_integer(), keyword()) :: t()
  def start_cluster!(count \\ 3, opts \\ []) do
    case start_cluster(count, opts) do
      {:ok, cluster} ->
        ExUnit.Callbacks.on_exit(fn -> stop_cluster(cluster) end)
        cluster

      {:error, reason} ->
        raise "failed to start Bedrock cluster: #{inspect(reason)}"
    end
  end

  @spec start_cluster(pos_integer(), keyword()) :: {:ok, t()} | {:error, term()}
  def start_cluster(count, opts \\ []) when is_integer(count) and count > 0 do
    base_path = Keyword.get_lazy(opts, :base_path, &unique_base_path/0)
    object_storage_path = Path.join(base_path, "object_storage")
    object_storage = ObjectStorage.backend(LocalFilesystem, root: object_storage_path)
    descriptor_path = Path.join(base_path, Bedrock.Cluster.default_descriptor_file_name())
    cluster = Keyword.get(opts, :cluster, @default_cluster)
    repo = Keyword.get(opts, :repo, @default_repo)
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    peer_opts = Keyword.get(opts, :peer_opts, [])

    with :ok <- prepare_base_path(base_path, object_storage_path),
         {:ok, peers} <- PeerNodes.start_peers(count, peer_opts),
         :ok <- PeerNodes.connect_all(peers, timeout),
         nodes = PeerNodes.nodes(peers),
         :ok <- write_descriptor(cluster.name(), nodes, descriptor_path),
         {:ok, supervisors} <-
           start_cluster_supervisors(peers, cluster, descriptor_path, object_storage, base_path, opts),
         :ok <-
           wait_for_cluster_services(peers, cluster, timeout, Keyword.get(opts, :capabilities, @default_capabilities)) do
      {:ok,
       %{
         base_path: base_path,
         descriptor_path: descriptor_path,
         object_storage: object_storage,
         object_storage_path: object_storage_path,
         peers: peers,
         nodes: nodes,
         supervisors: supervisors,
         cluster: cluster,
         repo: repo
       }}
    else
      {:error, reason} ->
        File.rm_rf(base_path)
        {:error, reason}
    end
  end

  @spec stop_cluster(t()) :: :ok
  def stop_cluster(%{peers: peers, supervisors: supervisors, base_path: base_path}) do
    Enum.each(peers, fn peer ->
      case Map.fetch(supervisors, peer.node) do
        {:ok, supervisor} -> call(peer, __MODULE__, :stop_supervisor, [supervisor], 5_000)
        :error -> :ok
      end
    end)

    PeerNodes.stop_peers(peers)
    File.rm_rf(base_path)
    :ok
  end

  @spec node_status(module()) :: map()
  def node_status(cluster \\ @default_cluster) do
    %{
      node: Node.self(),
      config: cluster.node_config(),
      descriptor: Descriptor.read_from_file(cluster.path_to_descriptor()),
      services: %{
        coordinator: Process.whereis(cluster.otp_name(:coordinator)),
        foreman: Process.whereis(cluster.otp_name(:foreman)),
        link: Process.whereis(cluster.otp_name(:link))
      }
    }
  end

  @spec put_object(module(), String.t(), iodata()) :: :ok | {:error, term()}
  def put_object(cluster \\ @default_cluster, key, value) when is_binary(key) do
    cluster
    |> object_storage!()
    |> ObjectStorage.put(key, value)
  end

  @spec get_object(module(), String.t()) :: {:ok, iodata()} | {:error, term()}
  def get_object(cluster \\ @default_cluster, key) when is_binary(key) do
    cluster
    |> object_storage!()
    |> ObjectStorage.get(key)
  end

  @spec configure_and_start_cluster(module(), keyword()) :: {:ok, pid()} | {:error, term()}
  def configure_and_start_cluster(cluster, config) when is_atom(cluster) and is_list(config) do
    _ = Application.ensure_all_started(:bedrock)
    Application.put_env(:intent_ledger, cluster, config)

    case Supervisor.start_link([cluster], strategy: :one_for_one) do
      {:ok, pid} ->
        Process.unlink(pid)
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec stop_supervisor(pid()) :: :ok
  def stop_supervisor(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      Supervisor.stop(pid, :shutdown, 5_000)
    end

    :ok
  catch
    :exit, _reason -> :ok
  end

  defp start_cluster_supervisors(peers, cluster, descriptor_path, object_storage, base_path, opts) do
    Enum.reduce_while(peers, {:ok, %{}}, fn peer, {:ok, supervisors} ->
      config = node_config(peer, descriptor_path, object_storage, base_path, opts)

      case call(peer, __MODULE__, :configure_and_start_cluster, [cluster, config]) do
        {:ok, supervisor} ->
          {:cont, {:ok, Map.put(supervisors, peer.node, supervisor)}}

        {:error, reason} ->
          stop_started_supervisors(peers, supervisors)
          {:halt, {:error, {:cluster_start_failed, peer.node, reason}}}

        other ->
          stop_started_supervisors(peers, supervisors)
          {:halt, {:error, {:cluster_start_failed, peer.node, other}}}
      end
    end)
  end

  defp stop_started_supervisors(peers, supervisors) do
    Enum.each(peers, fn peer ->
      case Map.fetch(supervisors, peer.node) do
        {:ok, supervisor} -> call(peer, __MODULE__, :stop_supervisor, [supervisor], 5_000)
        :error -> :ok
      end
    end)
  end

  defp wait_for_cluster_services(peers, cluster, timeout, capabilities) do
    wait_until(
      fn ->
        Enum.all?(peers, fn peer ->
          case call(peer, __MODULE__, :node_status, [cluster], 1_000) do
            %{services: services} ->
              required_services_available?(services, capabilities)

            _other ->
              false
          end
        end)
      end,
      timeout,
      {:cluster_services_unavailable, PeerNodes.nodes(peers)}
    )
  end

  defp required_services_available?(services, capabilities) do
    is_pid(services.link) and
      coordination_services_available?(services, capabilities) and
      worker_services_available?(services, capabilities)
  end

  defp coordination_services_available?(services, capabilities) do
    :coordination not in capabilities or is_pid(services.coordinator)
  end

  defp worker_services_available?(services, capabilities) do
    (:log not in capabilities and :materializer not in capabilities) or is_pid(services.foreman)
  end

  defp wait_until(fun, timeout, error) do
    deadline = System.monotonic_time(:millisecond) + timeout
    wait_until_deadline(fun, deadline, error)
  end

  defp wait_until_deadline(fun, deadline, error) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, error}

      true ->
        Process.sleep(50)
        wait_until_deadline(fun, deadline, error)
    end
  end

  defp node_config(peer, descriptor_path, object_storage, base_path, opts) do
    node_path = Path.join([base_path, "nodes", safe_node_name(peer.node)])

    [
      capabilities: Keyword.get(opts, :capabilities, @default_capabilities),
      coordinator_ping_timeout_in_ms: Keyword.get(opts, :coordinator_ping_timeout_in_ms, 500),
      durability_mode: Keyword.get(opts, :durability_mode, :relaxed),
      gateway_ping_timeout_in_ms: Keyword.get(opts, :gateway_ping_timeout_in_ms, 500),
      object_storage: object_storage,
      path: node_path,
      path_to_descriptor: descriptor_path,
      worker: [
        object_storage: object_storage,
        path: Path.join(node_path, "workers")
      ]
    ]
  end

  defp object_storage!(cluster) do
    cluster
    |> apply(:node_config, [])
    |> Keyword.fetch!(:object_storage)
  end

  defp call(peer, module, function, args, timeout \\ @default_timeout) do
    PeerNodes.call(peer, module, function, args, timeout)
  catch
    :exit, reason -> {:error, reason}
  end

  defp prepare_base_path(base_path, object_storage_path) do
    with {:ok, _} <- File.rm_rf(base_path),
         :ok <- File.mkdir_p(object_storage_path),
         :ok <- File.mkdir_p(Path.join(base_path, "nodes")) do
      :ok
    end
  end

  defp write_descriptor(cluster_name, nodes, descriptor_path) do
    descriptor_path
    |> Path.dirname()
    |> File.mkdir_p!()

    Descriptor.write_to_file!(descriptor_path, Descriptor.new(cluster_name, nodes))
  end

  defp safe_node_name(node) do
    node
    |> Atom.to_string()
    |> String.replace(["@", "."], "_")
  end

  defp unique_base_path do
    Path.join(System.tmp_dir!(), "intent_ledger_bedrock_cluster_#{System.unique_integer([:positive])}")
  end
end
