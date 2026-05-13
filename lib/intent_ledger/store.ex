defmodule IntentLedger.Store do
  @moduledoc """
  Persistence contract for intent lifecycle state.

  Store V1 is a semantic adapter contract. Stores own atomic lifecycle commits,
  reads, shard leases, listing indexes, and durable outbox operations without
  exposing backend-specific transaction machinery.

  The bundled `IntentLedger.Store.Memory` adapter is intended for tests, local
  development, and as the executable contract for durable adapters.
  """

  alias IntentLedger.Store.{Commit, CommitRequest, Conflict}

  @type ref :: GenServer.server()
  @type result :: {:ok, term()} | {:error, term()}
  @type commit_result :: {:ok, Commit.t()} | {:error, Conflict.t() | term()}
  @type read_request ::
          {:intent, String.t()}
          | {:history, String.t()}
          | {:stream, String.t(), keyword()}
          | map()
  @type lease_request :: {:shard, atom(), map()} | map()
  @type listing_request :: {:due_intents, map()} | {:expired_claims, map()} | map()
  @type outbox_request :: {:insert, map()} | {:read, map()} | {:ack, map()} | {:replay, map()} | map()

  @callback child_spec(keyword()) :: Supervisor.child_spec()
  @callback commit(ref(), atom(), CommitRequest.t(), keyword()) :: commit_result()
  @callback read(ref(), atom(), read_request(), keyword()) :: result()
  @callback lease(ref(), atom(), lease_request(), keyword()) :: result()
  @callback listing(ref(), atom(), listing_request(), keyword()) :: result()
  @callback outbox(ref(), atom(), outbox_request(), keyword()) :: result()

  @doc false
  @spec normalize_spec(module() | {module(), keyword()} | nil) :: {module(), keyword()}
  def normalize_spec(nil), do: {IntentLedger.Store.Memory, []}
  def normalize_spec(module) when is_atom(module), do: {module, []}
  def normalize_spec({module, opts}) when is_atom(module) and is_list(opts), do: {module, opts}
end
