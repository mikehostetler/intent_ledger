defmodule IntentLedger.Store.Ecto do
  @moduledoc """
  Ecto/Postgres-backed local store adapter scaffold.

  Ecto SQL and Postgrex are optional dependencies so applications that use the
  memory or Bedrock adapters do not pull in an Ecto stack. Projects that
  configure this adapter must include `:ecto_sql` and `:postgrex`, and must pass
  an Ecto repo configured with the Postgres adapter.

  This module currently owns dependency and repo guardrails. The SQL Store V1
  operations are implemented by later Ecto adapter tasks.
  """

  @behaviour IntentLedger.Store

  use GenServer

  alias IntentLedger.{Error, Store}
  alias IntentLedger.Store.{CommitRequest, Outbox}

  @dependencies [:ecto_sql, :postgrex]
  @postgres_adapter Module.concat([Ecto, Adapters, Postgres])
  @required_modules [
    Ecto,
    Ecto.Changeset,
    Ecto.Multi,
    Ecto.Query,
    Ecto.Schema,
    Ecto.Adapters.SQL,
    Ecto.Adapters.Postgres,
    Postgrex
  ]

  @type option ::
          {:name, GenServer.name()}
          | {:repo, module()}
          | {:prefix, String.t() | nil}

  defstruct repo: nil,
            prefix: nil

  @type t :: %__MODULE__{
          repo: module(),
          prefix: String.t() | nil
        }

  @doc false
  @impl true
  @spec child_spec([option()]) :: Supervisor.child_spec()
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5000
    }
  end

  @doc false
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)

    with :ok <- ensure_available(),
         {:ok, repo} <- fetch_repo(opts) do
      GenServer.start_link(__MODULE__, %{repo: repo, prefix: Keyword.get(opts, :prefix)}, name: name)
    end
  end

  @doc false
  @spec available?() :: boolean()
  def available?, do: ensure_available() == :ok

  @doc false
  @spec ensure_available([module()]) :: :ok | {:error, Exception.t()}
  def ensure_available(required_modules \\ @required_modules) when is_list(required_modules) do
    case Enum.reject(required_modules, &loaded?/1) do
      [] ->
        :ok

      missing_modules ->
        {:error,
         adapter_error("Ecto SQL and Postgrex dependencies are required to use IntentLedger.Store.Ecto",
           reason: :missing_dependency,
           dependencies: @dependencies,
           missing_modules: missing_modules
         )}
    end
  end

  @doc false
  @impl true
  @spec init(map()) :: {:ok, t()}
  def init(state), do: {:ok, struct!(__MODULE__, state)}

  @doc false
  @impl true
  @spec commit(Store.ref(), atom(), CommitRequest.t(), keyword()) :: Store.commit_result()
  def commit(ref, ledger, %CommitRequest{} = request, opts), do: GenServer.call(ref, {:commit, ledger, request, opts})

  @doc false
  @impl true
  @spec read(Store.ref(), atom(), Store.read_request(), keyword()) :: Store.result()
  def read(ref, ledger, request, opts), do: GenServer.call(ref, {:read, ledger, request, opts})

  @doc false
  @impl true
  @spec lease(Store.ref(), atom(), Store.lease_request(), keyword()) :: Store.result()
  def lease(ref, ledger, request, opts), do: GenServer.call(ref, {:lease, ledger, request, opts})

  @doc false
  @impl true
  @spec listing(Store.ref(), atom(), Store.listing_request(), keyword()) :: Store.result()
  def listing(ref, ledger, request, opts), do: GenServer.call(ref, {:listing, ledger, request, opts})

  @doc false
  @impl true
  @spec outbox(Store.ref(), atom(), Store.outbox_request(), keyword()) :: Store.result()
  def outbox(ref, ledger, request, opts), do: GenServer.call(ref, {:outbox, ledger, request, opts})

  @impl true
  def handle_call({operation, _ledger, request, _opts}, _from, %__MODULE__{} = state)
      when operation in [:commit, :read, :lease, :listing, :outbox] do
    {:reply, not_implemented(operation, request), state}
  end

  defp fetch_repo(opts) do
    case Keyword.fetch(opts, :repo) do
      {:ok, repo} when is_atom(repo) ->
        validate_repo(repo)

      {:ok, repo} ->
        {:error, adapter_error("Ecto store repo must be a module", reason: :invalid_repo, repo: repo)}

      :error ->
        {:error, adapter_error("Ecto store requires a :repo option", reason: :missing_repo)}
    end
  end

  defp validate_repo(repo) do
    cond do
      not loaded?(repo) ->
        {:error, adapter_error("Ecto store repo module is not available", reason: :invalid_repo, repo: repo)}

      not function_exported?(repo, :__adapter__, 0) ->
        {:error, adapter_error("Ecto store repo must expose an Ecto adapter", reason: :invalid_repo, repo: repo)}

      repo.__adapter__() != @postgres_adapter ->
        {:error,
         adapter_error("Ecto store requires a Postgres repo",
           reason: :unsupported_repo_adapter,
           repo: repo,
           repo_adapter: repo.__adapter__()
         )}

      true ->
        {:ok, repo}
    end
  end

  defp not_implemented(operation, request) do
    {:error,
     adapter_error("Ecto store operation is not implemented yet",
       reason: :not_implemented,
       operation: operation,
       request: compact_request(request)
     )}
  end

  defp compact_request(%CommitRequest{} = request), do: %{operation: request.operation, command_id: request.command_id}
  defp compact_request(%Outbox{} = request), do: %{type: request.type, key: request.key, consumer: request.consumer}
  defp compact_request(request), do: request

  defp loaded?(module), do: match?({:module, _module}, Code.ensure_loaded(module))

  defp adapter_error(message, details) do
    Error.adapter_runtime(message, Keyword.put(details, :adapter, __MODULE__))
  end
end
