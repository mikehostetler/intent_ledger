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

  ## Claim Fencing Semantics

  Claim ownership is fenced by a durable claim row keyed by `claim_id` and by
  the matching token hash copied onto intent state. Claim acquisition uses
  `IntentLedger.Store.Precondition.intent_status/3` to compare-and-set an
  eligible `:available` or `:retry_scheduled` intent into `:claimed`, then writes
  the claim row with `IntentLedger.Store.Write.put_claim/3` in the same commit
  as the state and signal writes.

  Heartbeat, complete, fail, and release operations must include
  `IntentLedger.Store.Precondition.claim_fence/3`. The adapter checks that the
  claim row exists, points at a `:claimed` intent, matches the expected token
  hash, and has not expired at the operation time. A stale token, missing claim,
  released claim, final intent, or expired lease must return
  `IntentLedger.Store.Conflict.claim_fence/3` without applying writes.

  Successful heartbeat commits update the claim lease and append a heartbeat
  signal atomically. Successful complete, fail, and release commits delete the
  claim row with `IntentLedger.Store.Write.delete_claim/2` in the same atomic
  commit as the state transition and lifecycle signals.

  ## Shard Lease Semantics

  Queue shard ownership is represented by a durable lease row keyed by queue and
  shard. Lease requests use the `{:shard, operation, attrs}` shape where
  operation is `:acquire`, `:renew`, `:release`, `:expire`, or `:takeover`.
  A lease is current only when its `lease_until` is strictly greater than the
  operation time.

  Acquire uses `IntentLedger.Store.Precondition.shard_available/4` and succeeds
  only when the lease row is absent or expired. Renew and release use
  `IntentLedger.Store.Precondition.shard_lease/4` and require the current owner
  to match. Expire and takeover use
  `IntentLedger.Store.Precondition.shard_expired/4`; takeover then writes the
  new owner lease in the same atomic operation.

  Failed acquire, renew, release, expire, or takeover requests return
  `IntentLedger.Store.Conflict.shard_lease/4` without changing ownership.
  Successful acquire, renew, and takeover write a lease row with
  `IntentLedger.Store.Write.put_shard_lease/4`; successful release and expiry
  delete it with `IntentLedger.Store.Write.delete_shard_lease/3`.

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
  @type shard_lease_operation :: :acquire | :renew | :release | :expire | :takeover
  @type lease_request :: {:shard, shard_lease_operation(), map()} | map()
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
