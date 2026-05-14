# Reliability

Intent Ledger records what the application intended to do and what happened to
that Intent over time. `bedrock_job_queue` performs the queue mechanics that
make the work visible to handlers.

## Guarantees

Current guarantees:

- enqueue is durable and atomic across Intent state, lifecycle signals, outbox,
  and queue item insert;
- business keys are idempotent at the Intent boundary;
- handler payloads are validated before execution when a Zoi schema is provided;
- successful handler results are validated when a result schema is provided;
- lifecycle transitions are replayable from ledger and per-intent streams.

Non-goals:

- exactly-once external side effects;
- hiding the fact that retries can run the same Intent more than once;
- generic workflow orchestration;
- Postgres-backed queue or Intent persistence.

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

The alpha runtime still needs a transaction hook in `bedrock_job_queue`, or a
thin direct executor, so handler result handling can update queue state and
Intent lifecycle state in one Bedrock transaction. Until then, enqueue has the
strongest atomicity guarantee, while completion/retry lifecycle state is updated
by the bridge worker before the queue manager finalizes the queue item.

Application handlers should remain idempotent and should record external
side-effect evidence in the application domain whenever side effects matter.
