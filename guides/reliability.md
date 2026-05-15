# Reliability

Intent Ledger records what the application intended to do and what happened to
that Intent over time. `bedrock_job_queue` performs the queue mechanics that
make the work visible to handlers.

## Guarantees

Current guarantees:

- enqueue is durable and atomic across Intent state, lifecycle signals, outbox,
  and queue item insert;
- business keys are idempotent at the Intent boundary;
- signal-native enqueue commands are idempotent by signal ID when no explicit
  key is supplied;
- handler payloads are validated before execution when a Zoi schema is provided;
- successful handler results are validated when a result schema is provided;
- lifecycle transitions are replayable from ledger and per-intent streams.
- outbox consumers record durable monotonic ack cursors for at-least-once
  delivery;
- projection cursors are monotonic by default and report lag through inspection.

`intent.started` is deliberately an attempt observation rather than a unique
state transition. Consumers should expect repeated started facts when a worker
crashes, a lease expires, or stale execution races with recovery. Terminal facts
are the final lifecycle boundary.

Non-goals:

- exactly-once external side effects;
- hiding the fact that retries can run the same Intent more than once;
- generic workflow orchestration;
- non-Bedrock queue or Intent persistence.

## Handler Results

Handlers return one of:

```elixir
:ok
{:ok, result}
{:error, reason}
{:discard, reason}
{:snooze, delay_ms}
```

Intent Ledger maps those results to Intent lifecycle states. The queue layer maps
the same result to complete, requeue, discard, or snooze behavior.

Manual `requeue/2` currently accepts failed or discarded Intents only. That
avoids duplicating live queue items for Intents that are still pending,
processing, retry-scheduled, or parked as ambiguous.

## Current Caveat

Handler result handling uses the `bedrock_job_queue` action hook so queue state
and Intent lifecycle state commit in one Bedrock transaction for complete,
retry, max-attempt failure, discard, and snooze paths.

Duplicate or stale queue lifecycle callbacks are treated as boundary conditions:
terminal Intents are left unchanged, duplicate terminal hooks do not append
duplicate lifecycle facts, and failed queue actions do not advance Intent state.

Cancellation and ambiguity updates remove an unleased queue item in the same
transaction when the item is still pending. If the item is already leased or
executing, the worker observes the non-runnable Intent state and completes the
queue item without invoking the handler.

Application handlers should remain idempotent and should record external
side-effect evidence in the application domain whenever side effects matter.
Worker crash and expired-lease recovery scenarios are still being hardened with
`bedrock_job_queue` and are not part of the alpha contract yet.

## Chaos Scenarios

Failure-mode tests live behind the `:chaos` tag and run with:

```sh
mix test.chaos
```

The chaos suite is the right place for net splits, stale leases, duplicate queue
callbacks, crash-before-commit and crash-after-commit boundaries, and outbox or
projection recovery after restart. Deterministic simulations should come first;
true distributed partition tests should only assert behavior that Bedrock and
`bedrock_job_queue` expose as stable recovery contracts.
