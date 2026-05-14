# Operations And Observability

Intent Ledger exposes two operational surfaces:

- stable `:telemetry` events for metrics, logs, and traces;
- read-only inspection APIs for point-in-time queue, claim, shard, outbox, and
  projection state.

Use telemetry for continuous dashboards and alerting. Use inspection APIs for
runbooks, admin views, and low-frequency health checks that need current
operational rows.

## Telemetry Setup

Events are emitted under `[:intent_ledger]` by default. Pass
`:telemetry_prefix` when starting a ledger to put events under an application
namespace:

```elixir
children = [
  {IntentLedger,
   name: MyApp.IntentLedger,
   store: IntentLedger.Store.Memory,
   telemetry_prefix: [:my_app, :intent_ledger]}
]
```

Attach handlers using the stable event catalogue:

```elixir
events =
  IntentLedger.Telemetry.all()
  |> Enum.map(&IntentLedger.Telemetry.event_name(&1.event, telemetry_prefix: [:my_app, :intent_ledger]))

:telemetry.attach_many(
  "my-app-intent-ledger-metrics",
  events,
  &MyApp.IntentLedgerMetrics.handle_event/4,
  %{}
)
```

Measurement units are part of `IntentLedger.Telemetry.metadata_policy/0`:

- `:duration` and `:system_time` use native VM units;
- `:lag_ms` is milliseconds;
- `:count`, `:writes`, `:signals`, `:outbox_entries`, and `:failed` are counts.

Metadata is intentionally operational. Payloads, command results, raw errors,
claim tokens, token hashes, headers, and common secret fields are redacted or
excluded by default. Treat `:intent_id`, `:claim_id`, and `:command_id` as
trace/log attributes rather than high-cardinality metric labels.

## Core Metrics

| Area | Events | Recommended Metrics |
| --- | --- | --- |
| Command handling | `[:command, :start]`, `[:command, :stop]`, `[:command, :exception]` | command rate, latency, exception rate, replay rate by ledger and operation |
| Store commits | `[:store, :commit, :start]`, `[:store, :commit, :stop]`, `[:store, :commit, :exception]` | commit latency, writes per commit, signals per commit, outbox entries per commit |
| Store conflicts | `[:store, :conflict]` | conflict rate by operation and conflict type |
| Claiming | `[:claim, :stop]` | claim latency, claimed count, empty claim rate, claim errors by queue |
| Shard leases | `[:shard_lease, :stop]` | acquire/renew/release latency, lease conflict rate, lease errors by queue/shard |
| Recovery | `[:recovery, :stop]` | recovered claim count, recovery latency, recovery errors by queue |
| Outbox reads | `[:outbox, :read, :stop]` | read batch size, read latency, oldest returned entry lag |
| Outbox acknowledgements | `[:outbox, :ack, :stop]` | ack latency, ack count, ack conflict/error rate |
| Dispatch | `[:dispatcher, :stop]` | dispatched count, failed handler count, dispatch latency |
| Replay | `[:replay, :stop]` | replay count, replay latency, replay errors by source |
| Projection rebuilds | `[:projection, :stop]` | rebuild/catch-up count, projection latency, projection errors |
| Inspection | `[:inspection, :stop]` | inspection latency, inspected row count, inspection errors |

For latency histograms, keep labels low-cardinality: `ledger`, `operation`,
`status`, `queue`, `shard`, `store`, `consumer`, `projection`, and `source` are
reasonable. Avoid labeling metrics with per-intent or per-claim identifiers.

## Inspection APIs

Inspection calls are read-only and return operational rows without payloads or
claim secrets:

```elixir
{:ok, queues} = IntentLedger.inspect_queues(MyApp.IntentLedger)
{:ok, shards} = IntentLedger.inspect_shards(MyApp.IntentLedger)
{:ok, claims} = IntentLedger.inspect_claims(MyApp.IntentLedger, queue: :default)
{:ok, retries} = IntentLedger.inspect_retries(MyApp.IntentLedger, limit: 50)
{:ok, ambiguous} = IntentLedger.inspect_ambiguity(MyApp.IntentLedger)
{:ok, outbox} = IntentLedger.inspect_outbox_lag(MyApp.IntentLedger, consumer: "intent_ledger.signal_dispatcher")
{:ok, projection} = IntentLedger.inspect_projection_lag(MyApp.IntentLedger, MyApp.IntentStatusProjection, cursor: 123)
```

Common options:

- `:queue` and `:shard` scope queue, shard, claim, retry, and ambiguity views.
- `:at` evaluates due work and expired claims at a specific `DateTime`.
- `:limit` bounds row-returning inspections.
- `:consumer` and `:cursor` scope outbox lag.
- `:stream` and `:cursor` scope projection lag. The stream defaults to the
  ledger-wide stream for the public API.

Key fields by inspection:

| API | Primary Fields |
| --- | --- |
| `inspect_queues/2` | `queue`, `shards`, `depth`, `available`, `retry_scheduled`, `claimed`, `expired_claims`, `ambiguous`, `total_open` |
| `inspect_shards/2` | `queue`, `shard`, `status`, `owner_id`, `lease_until`, `depth`, `claimed`, `expired_claims` |
| `inspect_claims/2` | `intent_id`, `claim_id`, `owner_id`, `lease_until`, `expired?`, `queue`, `shard`, `attempt` |
| `inspect_retries/2` | `intent_id`, `retry_at`, `due?`, `queue`, `shard`, `attempt` |
| `inspect_ambiguity/2` | `intent_id`, `queue`, `shard`, `attempt`, `updated_at`, `error_class` |
| `inspect_outbox_lag/2` | `cursor`, `max_sequence`, `lag`, `unacked`, `oldest_unacked_sequence`, `oldest_unacked_age_ms` |
| `inspect_projection_lag/3` | `projection`, `stream`, `cursor`, `stream_version`, `lag` |

## Dashboard Recommendations

Build dashboards around operational questions rather than individual events.

### Ledger Overview

- Command rate and command latency by operation/status.
- Open work by queue: queue `depth`, `claimed`, `retry_scheduled`, and
  `ambiguous`.
- Store commit latency and conflict rate.
- Outbox `lag`, unacked entries, and dispatcher failures.

### Queue And Claim Health

- Queue depth by queue and shard.
- Empty claim rate beside queue depth. A high empty rate with nonzero depth can
  indicate shard lease or visibility-time problems.
- Active claims and expired claims by queue/shard.
- Oldest expired claim age from `inspect_claims/2`.
- Recovery count and recovery error rate.

### Shard Lease Health

- Shard status from `inspect_shards/2`: `:owned`, `:unowned`, or `:expired`.
- Lease renew latency and lease conflict rate.
- Count of expired or unowned shards per queue.
- Owner changes for the same queue/shard, preferably as logs or traces.

### Outbox And Dispatch Health

- Outbox read batch size and read latency.
- Outbox lag from telemetry `:lag_ms` and `inspect_outbox_lag/2`.
- Dispatcher failed count by handler.
- Ack conflicts and ack error rate.
- Oldest unacked outbox age.

### Projection And Replay Health

- Projection rebuild/catch-up duration and count.
- Projection lag by projection and stream.
- Replay error rate by source.
- Replay count per rebuild to catch unexpectedly large rebuild windows.

## Alert Guidelines

Tune thresholds to workload and SLOs, but these conditions should page or at
least create high-priority tickets in production:

- Outbox `lag` or `oldest_unacked_age_ms` grows for multiple polling intervals.
- Dispatcher `failed` count remains nonzero for the same handler.
- Any queue has expired claims that recovery does not clear.
- Any configured shard remains `:expired` or `:unowned` while queue depth is
  nonzero.
- Store conflict rate spikes above the normal concurrency baseline.
- Command exception rate is nonzero for user-facing command paths.
- Ambiguous intents appear and remain unresolved beyond the business-defined
  manual review window.
- Projection lag grows while the source stream continues to advance.

## Runbook Checks

When work is not being claimed:

1. Check `inspect_queues/2` for queue depth and due retry count.
2. Check `inspect_shards/2` for expired or unowned shard leases.
3. Check `[:shard_lease, :stop]` errors and conflicts.
4. Check node clocks, because visibility, claims, leases, and recovery are
   time-based.

When handlers stop receiving lifecycle signals:

1. Check `inspect_outbox_lag/2` for unacked entries and oldest age.
2. Check dispatcher `failed` counts by handler.
3. Check `[:outbox, :ack, :stop]` conflicts.
4. Replay a bounded outbox window with `IntentLedger.replay_outbox/2` for
   diagnosis; replay does not acknowledge entries.

When projections look stale:

1. Compare the projection's stored cursor with `inspect_projection_lag/3`.
2. Check `[:projection, :stop]` latency and error events.
3. Replay from the stored cursor and apply
   `IntentLedger.Projection.catch_up/4`.

## Adapter Notes

- `IntentLedger.Store.Memory` is useful for local development and tests only.
  It is not durable and should not drive production dashboards.
- `IntentLedger.Store.Ecto` is suitable for local durable development and
  explicit single-node deployments. Do not use it as evidence of clustered
  correctness.
- `IntentLedger.Store.Bedrock` is the clustered durable adapter. For clustered
  deployments, dashboard queue/shard health from every participating node
  against the same ledger name and queue configuration.
