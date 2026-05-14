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

Application handlers should remain idempotent and should record external
side-effect evidence in the application domain whenever side effects matter.
Worker crash and expired-lease recovery scenarios are still being hardened with
`bedrock_job_queue` and are not part of the alpha contract yet.
