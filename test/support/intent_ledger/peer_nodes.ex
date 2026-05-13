defmodule IntentLedger.PeerNodes do
  @moduledoc false

  @default_cookie :intent_ledger_peer_test
  @default_host ~c"127.0.0.1"
  @default_wait_boot 15_000
  @default_shutdown :halt

  @type peer :: %{
          name: atom(),
          node: node(),
          pid: pid()
        }

  @spec start_peer(keyword()) :: {:ok, peer()} | {:error, term()}
  def start_peer(opts \\ []) do
    name = Keyword.get_lazy(opts, :name, fn -> unique_name(Keyword.get(opts, :prefix, :intent_ledger_peer)) end)

    peer_opts = %{
      name: name,
      host: opts |> Keyword.get(:host, @default_host) |> charlist_arg!(),
      longnames: Keyword.get(opts, :longnames, true),
      connection: Keyword.get(opts, :connection, 0),
      peer_down: Keyword.get(opts, :peer_down, :stop),
      wait_boot: Keyword.get(opts, :wait_boot, @default_wait_boot),
      shutdown: Keyword.get(opts, :shutdown, @default_shutdown),
      args: peer_args(opts),
      env: Keyword.get(opts, :env, [])
    }

    case start_peer_node(peer_opts) do
      {:ok, pid, node} -> {:ok, %{name: name, node: node, pid: pid}}
      {:ok, pid} -> {:ok, %{name: name, node: :undefined, pid: pid}}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec start_peers(pos_integer(), keyword()) :: {:ok, [peer()]} | {:error, term()}
  def start_peers(count, opts \\ []) when is_integer(count) and count > 0 do
    Enum.reduce_while(1..count, {:ok, []}, fn index, {:ok, peers} ->
      prefix = Keyword.get(opts, :prefix, :intent_ledger_peer)
      peer_opts = Keyword.put_new(opts, :name, unique_name(:"#{prefix}_#{index}"))

      case start_peer(peer_opts) do
        {:ok, peer} -> {:cont, {:ok, [peer | peers]}}
        {:error, reason} -> {:halt, {:error, reason, peers}}
      end
    end)
    |> case do
      {:ok, peers} ->
        {:ok, Enum.reverse(peers)}

      {:error, reason, peers} ->
        stop_peers(peers)
        {:error, reason}
    end
  end

  @spec start_peers!(pos_integer(), keyword()) :: [peer()]
  def start_peers!(count, opts \\ []) do
    case start_peers(count, opts) do
      {:ok, peers} ->
        ExUnit.Callbacks.on_exit(fn -> stop_peers(peers) end)
        peers

      {:error, reason} ->
        raise "failed to start peer nodes: #{inspect(reason)}"
    end
  end

  @spec stop_peer(peer() | pid()) :: :ok
  def stop_peer(%{pid: pid}), do: stop_peer(pid)

  def stop_peer(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      try do
        :peer.stop(pid)
      catch
        :exit, _reason -> :ok
      end
    end

    :ok
  end

  @spec stop_peers([peer()]) :: :ok
  def stop_peers(peers) when is_list(peers) do
    peers
    |> Enum.reverse()
    |> Enum.each(&stop_peer/1)

    :ok
  end

  @spec call(peer() | pid(), module(), atom(), [term()], timeout()) :: term()
  def call(peer_or_pid, module, function, args, timeout \\ 5_000)
  def call(%{pid: pid}, module, function, args, timeout), do: call(pid, module, function, args, timeout)
  def call(pid, module, function, args, timeout), do: :peer.call(pid, module, function, args, timeout)

  @spec cast(peer() | pid(), module(), atom(), [term()]) :: :ok
  def cast(%{pid: pid}, module, function, args), do: cast(pid, module, function, args)
  def cast(pid, module, function, args), do: :peer.cast(pid, module, function, args)

  @spec send_message(peer() | pid(), pid() | atom(), term()) :: :ok
  def send_message(%{pid: pid}, destination, message), do: send_message(pid, destination, message)
  def send_message(pid, destination, message), do: :peer.send(pid, destination, message)

  @spec connect_all([peer()], timeout()) :: :ok | {:error, term()}
  def connect_all(peers, timeout \\ 5_000) when is_list(peers) do
    Enum.reduce_while(peers, :ok, fn peer, :ok ->
      case connect_peer(peer, peers, timeout) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @spec nodes([peer()]) :: [node()]
  def nodes(peers), do: Enum.map(peers, & &1.node)

  defp start_peer_node(peer_opts) do
    try do
      :peer.start_link(peer_opts)
    catch
      :exit, reason -> {:error, reason}
    end
  end

  defp connect_peer(peer, peers, timeout) do
    peers
    |> Enum.reject(&(&1.node == peer.node))
    |> Enum.reduce_while(:ok, fn other, :ok ->
      case call(peer, Node, :connect, [other.node], timeout) do
        true -> {:cont, :ok}
        false -> {:halt, {:error, {:connect_failed, peer.node, other.node}}}
      end
    end)
  end

  defp peer_args(opts) do
    opts
    |> Keyword.get(:cookie, @default_cookie)
    |> cookie_args()
    |> Kernel.++(code_path_args(Keyword.get(opts, :code_paths, :code.get_path())))
    |> Kernel.++(Enum.map(Keyword.get(opts, :args, []), &charlist_arg!/1))
  end

  defp cookie_args(nil), do: []
  defp cookie_args(cookie), do: [~c"-setcookie", charlist_arg!(cookie)]

  defp code_path_args(paths) do
    Enum.flat_map(paths, fn path -> [~c"-pa", charlist_arg!(path)] end)
  end

  defp unique_name(prefix) do
    :"#{prefix}_#{System.unique_integer([:positive])}"
  end

  defp charlist_arg!(value) when is_atom(value), do: Atom.to_charlist(value)
  defp charlist_arg!(value) when is_binary(value), do: String.to_charlist(value)
  defp charlist_arg!(value) when is_list(value), do: value
end
