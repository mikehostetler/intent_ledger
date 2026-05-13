defmodule IntentLedger.Store.Bedrock do
  @moduledoc """
  Bedrock-backed durable store adapter.

  Bedrock is an optional dependency so Hex consumers can use the core package
  and the memory reference adapter without pulling in a Bedrock runtime. Projects
  that configure this adapter must include `:bedrock`; the adapter checks for
  the dependency at startup and returns a normalized
  `IntentLedger.Error.AdapterRuntimeError` when it is missing.
  """

  @behaviour IntentLedger.Store

  alias IntentLedger.{Error, Store}

  @dependency :bedrock
  @required_modules [Bedrock, Bedrock.Repo]

  @type option ::
          {:name, GenServer.name()}
          | {:repo, term()}
          | {:cluster, term()}
          | {:keyspace, term()}
          | {:ledger, term()}

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
    with :ok <- ensure_available() do
      {:error,
       adapter_error("Bedrock store adapter is not configured yet",
         reason: :not_configured,
         opts: Keyword.keys(opts)
       )}
    end
  end

  @doc false
  @impl true
  @spec commit(Store.ref(), atom(), Store.CommitRequest.t(), keyword()) :: Store.commit_result()
  def commit(_ref, _ledger, _request, _opts), do: unavailable()

  @doc false
  @impl true
  @spec read(Store.ref(), atom(), Store.read_request(), keyword()) :: Store.result()
  def read(_ref, _ledger, _request, _opts), do: unavailable()

  @doc false
  @impl true
  @spec lease(Store.ref(), atom(), Store.lease_request(), keyword()) :: Store.result()
  def lease(_ref, _ledger, _request, _opts), do: unavailable()

  @doc false
  @impl true
  @spec listing(Store.ref(), atom(), Store.listing_request(), keyword()) :: Store.result()
  def listing(_ref, _ledger, _request, _opts), do: unavailable()

  @doc false
  @impl true
  @spec outbox(Store.ref(), atom(), Store.outbox_request(), keyword()) :: Store.result()
  def outbox(_ref, _ledger, _request, _opts), do: unavailable()

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
         adapter_error("Bedrock dependency is required to use IntentLedger.Store.Bedrock",
           reason: :missing_dependency,
           dependency: @dependency,
           missing_modules: missing_modules
         )}
    end
  end

  defp unavailable do
    with :ok <- ensure_available() do
      {:error, adapter_error("Bedrock store adapter is not configured yet", reason: :not_configured)}
    end
  end

  defp loaded?(module), do: match?({:module, _module}, Code.ensure_loaded(module))

  defp adapter_error(message, details) do
    Error.adapter_runtime(message, Keyword.put(details, :adapter, __MODULE__))
  end
end
