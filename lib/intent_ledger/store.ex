defmodule IntentLedger.Store do
  @moduledoc """
  Persistence contract for intent lifecycle state.

  Store V1 is a semantic adapter contract. Stores own atomic lifecycle commits,
  reads, shard leases, listing indexes, and durable outbox operations without
  exposing backend-specific transaction machinery.

  ## Stream And Replay Semantics

  Stream versions are monotonically increasing counters scoped to semantic
  ledger streams such as ledger, queue, and intent streams. A commit request
  uses `IntentLedger.Store.Precondition.stream_version/2` to compare the
  caller's expected stream version with the adapter's current version. A
  mismatch must return `IntentLedger.Store.Conflict.stream_version/3` and must
  not apply any writes.

  Command idempotency is keyed by `command_id`. New commands should include a
  `IntentLedger.Store.Precondition.command_absent/1` precondition and a matching
  `IntentLedger.Store.Write.put_idempotency/3` write that records the
  deterministic command result in the same atomic commit as lifecycle state and
  signal writes.

  Replayed commands should use
  `IntentLedger.Store.Precondition.command_replay/1`. If the command id already
  has a compatible stored result, the adapter returns an
  `IntentLedger.Store.Commit` with `replayed: true` and the original result. If
  the command id exists for different command semantics, the adapter returns
  `IntentLedger.Store.Conflict.command_conflict/3`.

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
