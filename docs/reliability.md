# Reliability Semantics

Intent Ledger gives applications a durable lifecycle around deferred work. It
does not make external side effects exactly-once. The reliability model is:

- lifecycle transitions are committed atomically by the configured store;
- workers claim work through fenced, expiring leases;
- failed or expired work can be retried or parked as ambiguous;
- lifecycle signals are appended with the state transition that produced them;
- outbox handlers receive lifecycle signals at least once;
- replay APIs are read-only and can rebuild disposable projections.

Application workers and signal handlers must still be idempotent around their
own external effects.

## Guarantees And Non-Guarantees

Intent Ledger guarantees:

- one current claim token can heartbeat, complete, fail, or release a claimed
  intent;
- stale claim tokens are rejected after release, completion, failure, expiry, or
  takeover;
- command replay can return the first deterministic result for the same command
  ID without appending duplicate lifecycle signals;
- lifecycle state and lifecycle signals are committed together by adapters that
  implement the Store V1 contract;
- durable outbox entries are acknowledged only after every configured signal
  handler succeeds.

Intent Ledger does not guarantee:

- exactly-once execution of worker side effects;
- exactly-once delivery to signal handlers;
- global ordering across different queues or shards;
- automatic reconciliation of side effects after a worker dies mid-operation;
- clock-independent recovery. Visibility, leases, and recovery compare
  `DateTime` values.

## Lifecycle States

| State | Meaning | Claimable? | Final? |
| --- | --- | --- | --- |
| `:available` | Work is due now or already released for another attempt. | yes | no |
| `:retry_scheduled` | Work is waiting until `visible_at`. | when due | no |
| `:claimed` | A worker owns a fenced lease. | no | no |
| `:completed` | Work completed and stored a result. | no | yes |
| `:failed` | Work exhausted or was classified as failed. | no | yes |
| `:cancelled` | Work was intentionally cancelled. | no | yes |
| `:ambiguous` | Intent needs manual or reconciliation handling. | no | no |

Ambiguous intents are intentionally parked outside normal claiming. Use
`IntentLedger.inspect_ambiguity/2` to find them, then decide whether to requeue
or cancel after application-specific reconciliation.

## Command Replay And Idempotency

Every mutating public API call is represented as an
`intent_ledger.command.*` signal. A command has a `:command_id`; when omitted,
the command signal ID is used.

Use stable command IDs when callers may retry the same request:

```elixir
IntentLedger.submit(
  MyApp.IntentLedger,
  %{key: "invoice:123", kind: "invoice.send"},
  command_id: "cmd:invoice:123:send"
)
```

A repeated command with the same ID and the same command shape returns the first
recorded result. A repeated command ID with different semantics is a conflict.

`idempotency_key` belongs to the intent itself. It prevents two submitted
intents from representing the same business work. It is complementary to
`command_id`: use command IDs for request replay, and idempotency keys for the
business identity of the work.

## Claim Fencing

Claiming moves a due intent into `:claimed` and returns a claim ID plus a secret
claim token. The token is stored only as a hash in the store. Heartbeat,
complete, fail, and release must present both the claim ID and token before the
lease expires.

Worker rules:

- heartbeat long-running work before `lease_until`;
- complete only after the external side effect has reached its own durable
  success boundary;
- fail with enough error information for a lifecycle classifier or operator to
  decide retry versus ambiguity;
- release only when no side effect has started or when the side effect is known
  to be safe to re-run;
- treat `:stale_claim` and `:lease_expired` responses as loss of ownership.

Claim fencing protects the ledger from stale owners. It cannot prevent a stale
worker from calling an external service after its lease has expired. External
systems should receive their own idempotency keys.

## At-Least-Once Work

Workers should assume a claimed intent can be attempted more than once. Common
redelivery paths are:

- the worker completes the external effect and dies before `complete/5`;
- the worker lease expires during a long operation;
- a node dies and recovery makes the intent available again;
- a worker calls `fail/5` and the lifecycle classifier schedules a retry;
- an operator manually requeues an ambiguous intent.

The safe worker shape is:

1. Claim work and persist a local execution record if the application needs one.
2. Execute the external effect with an idempotency key derived from the intent.
3. Record enough external evidence to reconcile retries.
4. Complete the claim with the current token.
5. If completion is rejected as stale or expired, reconcile before re-running
   the side effect.

## Failure Classification

When a worker calls `IntentLedger.fail/5`, the ledger asks the optional
`IntentLedger.Lifecycle` module to classify the failure. Without a lifecycle
module, the default behavior is:

- retry while `attempt < max_attempts`;
- use `:retry_at` or `:retry_ms` when supplied, otherwise retry immediately;
- after attempts are exhausted, mark the intent `:failed` for
  `ambiguity_policy: :retry`;
- after attempts are exhausted, mark the intent `:ambiguous` for
  `ambiguity_policy: :manual` or `:reconcile`.

A lifecycle module can override failure handling:

```elixir
defmodule MyApp.IntentLifecycle do
  @behaviour IntentLedger.Lifecycle

  @impl true
  def classify_failure(record, error, _context) do
    cond do
      transient?(error) -> {:retry, DateTime.add(DateTime.utc_now(), 60, :second)}
      externally_unknown?(record, error) -> :ambiguous
      true -> :fail
    end
  end
end
```

Valid classifications are `:retry`, `{:retry, retry_at}`, `:fail`,
`:ambiguous`, or `{:error, reason}`. Returning an error rejects the failure
transition so the claim remains unresolved until the worker retries, releases,
or the lease expires.

## Expired Claims And Recovery

The recovery server periodically scans configured queues for expired claims.
You can also call `IntentLedger.recover/3` manually.

For each expired claim, the ledger emits
`intent_ledger.claim.lease_expired` and then resolves the intent:

- default policy retries only when `ambiguity_policy: :retry` and attempts
  remain;
- otherwise the intent becomes `:ambiguous`;
- `classify_expired_claim/2` can prefer `:retry` when attempts remain or force
  `:ambiguous`;
- invalid classifications reject the recovery transition.

Expired-claim recovery is intentionally conservative. If a worker may have
completed an external effect but failed before completing the claim, ambiguity
is safer than blind retry unless the effect is idempotent.

The recovery server also expires stale shard lease rows so another shard worker
can take ownership after node death.

## Ambiguity

Ambiguity means the ledger cannot safely infer the real-world result. It is the
correct state when retrying could duplicate an effect and failing could hide a
successful effect.

Use ambiguity for cases such as:

- worker process died after calling an external service but before recording
  the result;
- external service returned an unknown, timeout, or partial result;
- retry budget was exhausted for work that requires human or automated
  reconciliation;
- an operator intentionally parks work before deciding the next step.

An ambiguity workflow usually looks like:

1. Monitor `IntentLedger.inspect_ambiguity/2`.
2. Read the intent, lifecycle history, and application-side execution evidence.
3. Check the external system using the intent key or idempotency key.
4. If the work should run again, call `IntentLedger.requeue/3`.
5. If no more ledger work should run, call `IntentLedger.cancel/4` with a
   reconciliation reason.

## Lifecycle Signals And Outbox Delivery

Lifecycle signals are facts about committed state transitions. Local
`after_transition/2` lifecycle callbacks are best-effort observers; their errors
are logged and do not roll back the already committed transition.

Use `IntentLedger.SignalDispatcher` and `IntentLedger.SignalHandler` modules for
durable signal handling. The dispatcher:

- reads unacknowledged outbox entries for its consumer;
- calls every configured handler;
- acknowledges the entry only after every handler returns `:ok`;
- retries failed handler or ack attempts with exponential backoff.

Handlers must be idempotent. They may receive the same signal more than once
after handler failure, ack failure, dispatcher restart, store conflict, or
consumer reconfiguration.

## Replay And Projections

Replay APIs do not mutate delivery state:

```elixir
{:ok, ledger_signals} = IntentLedger.replay_ledger(MyApp.IntentLedger, cursor: 0)
{:ok, outbox_entries} = IntentLedger.replay_outbox(MyApp.IntentLedger, cursor: 0)
```

Use replay to rebuild projections and audits from committed lifecycle history.
Projection code should be deterministic and safe to re-run from an earlier
cursor. When a durable projection stores a cursor, catch up from that cursor and
commit the new projection state and cursor in the projection's own store.

## Operational Checks

Track reliability through both telemetry and inspection:

- command conflicts by `:command_id`;
- `:claim_fence` and `:shard_lease` conflicts;
- expired claims and recovery counts;
- queue depth, retry depth, and ambiguous intent counts;
- outbox lag per consumer;
- signal handler failures and ack failures;
- clock skew across nodes that share a durable store.

The [Operations And Observability](operations.md) guide maps these checks to
telemetry events, metrics, dashboards, and runbooks.
