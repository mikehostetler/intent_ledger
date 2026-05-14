# Bedrock Adapter

`IntentLedger.Store.Bedrock` is the primary durable store adapter for
clustered deployments. It compiles Store V1 semantic requests into Bedrock repo
transactions and keeps all package APIs independent of Bedrock unless the
adapter is configured.

## Dependency And Startup

Bedrock is an optional dependency for Hex consumers. Applications that use this
adapter must add Bedrock themselves and provide a repo module:

```elixir
def deps do
  [
    {:intent_ledger, "~> 0.1"},
    {:bedrock, "~> 0.5"}
  ]
end
```

```elixir
defmodule MyApp.BedrockRepo do
  use Bedrock.Repo, cluster: MyApp.BedrockCluster
end
```

Start the Bedrock cluster and repo dependencies before the ledger:

```elixir
children = [
  MyApp.BedrockCluster,
  MyApp.BedrockRepo,
  {IntentLedger,
   name: MyApp.IntentLedger,
   queues: [default: [shards: 16]],
   lease_ms: 30_000,
   store: {IntentLedger.Store.Bedrock, repo: MyApp.BedrockRepo}}
]
```

The adapter checks for `:bedrock`, `Bedrock`, and `Bedrock.Repo` at startup and
returns an `IntentLedger.Error.AdapterRuntimeError` when the dependency or repo
module is missing.

### Setup Checklist

1. Add the optional `:bedrock` dependency to the host application.
2. Define the application-owned Bedrock cluster and repo modules.
3. Configure durable Bedrock storage, logs, coordinators, and materializers
   according to Bedrock's deployment requirements.
4. Start Bedrock infrastructure before any `IntentLedger` child that uses the
   Bedrock repo.
5. Use the same ledger name, queues, shard counts, lease settings, and repo
   module on every node that participates in claiming.
6. Keep node clocks synchronized. Intent visibility, claim leases, shard
   leases, and recovery decisions compare `DateTime` values written by
   different processes.
7. Attach telemetry handlers and add inspection-based health checks before
   enabling workers that claim production work.

### Store Options

The store child accepts:

- `:name` - process name for the adapter child, supplied by the ledger runtime
  when the store is configured through `IntentLedger`.
- `:repo` - required Bedrock repo module.
- `:transaction_opts` - optional keyword list merged into each Bedrock
  transaction call.

Pass transaction options through the store tuple when the application needs
repo-specific transaction behavior:

```elixir
{IntentLedger,
 name: MyApp.IntentLedger,
 store:
   {IntentLedger.Store.Bedrock,
    repo: MyApp.BedrockRepo,
    transaction_opts: [timeout: 15_000]}}
```

## Keyspace Layout

All keys are rooted under a versioned ledger prefix:

```text
intent_ledger/<schema_version>/<ledger>/<table>/...
```

The current schema version is `1`. Dynamic key parts are encoded through
`Bedrock.Keyspace` and Bedrock tuple encoding so integer and timestamp
components sort correctly in range scans. Table names are stable string tags.

| Table | Key Shape | Purpose |
| --- | --- | --- |
| `intent` | `intent/<intent_id>` | Immutable intent command envelope. |
| `state` | `state/<intent_id>` | Current materialized lifecycle state. |
| `command` | `command/<command_id>` | Deterministic command idempotency and replay result. |
| `stream` | `stream/<stream_id>/<version>` | Ordered lifecycle signal history. |
| `queue` | `queue/<queue>/<shard>/<visible_at>/<priority>/<intent_id>` | Due-intent listing index for `:available` and `:retry_scheduled` states. |
| `claim` | `claim/<claim_id>` | Claim fence token and lease row. |
| `shard` | `shard/<queue>/<shard>` | Queue shard lease row. |
| `outbox` | `outbox/<sequence>` | Durable outbox delivery entry. |
| `projection` | `projection/<name>` | Projection offset namespace reserved for consumers. |

Queue priority is stored inverted so higher-priority work sorts before lower
priority work for the same visibility time. Listing code still sorts decoded
rows by the Store V1 contract: priority descending, `visible_at` ascending, then
intent id ascending.

## Value Encoding

Every value is stored as an Erlang-term binary envelope:

```elixir
%{
  schema_version: 1,
  type: "state",
  value: %IntentLedger.IntentState{}
}
```

The type tag is checked when values are decoded. A type mismatch or unsupported
schema version is normalized into `IntentLedger.Error.AdapterRuntimeError`
rather than leaking raw Bedrock internals. These values are package-internal;
external integrations should consume the public Intent Ledger APIs or Store V1
semantic structs, not decode the Bedrock values directly.

## Transaction Semantics

The adapter performs each commit, listing, lease, read, and outbox request inside
a Bedrock repo transaction.

- Command idempotency uses `command/<command_id>` rows. Matching replay
  signatures return the original result without applying writes.
- Stream appends require either an explicit append version or a
  `stream_version` precondition. Stale stream versions return
  `IntentLedger.Store.Conflict.stream_version/3`.
- State writes update the materialized state row and maintain due queue indexes
  when states enter or leave `:available` or `:retry_scheduled`.
- Claim fences read the claim row and associated state row with Bedrock read
  conflict keys before allowing claim-sensitive writes.
- Shard leases use `shard/<queue>/<shard>` rows for acquire, renew, release,
  expiry, and takeover fencing.
- Outbox inserts allocate monotonically increasing sequences from the outbox
  range, add a write conflict range during allocation, and persist ack metadata
  in-place.

Rollback is delegated to Bedrock transaction semantics. Tests cover failed
preconditions preserving stream history, command replay rows, queue state, shard
leases, and outbox entries across adapter/repo restarts.

## Test Commands

The Bedrock integration harness runs against temporary local Erlang nodes and
local object storage. It does not require an external Bedrock service.

The harness tests use these ExUnit tags:

- `:integration` for local distributed-node tests;
- `:bedrock` for tests that start a local Bedrock-backed store;
- `:multi_node` for tests that start peer Erlang nodes;
- `:bedrock_cluster` for cluster/setup helper tests;
- `:bedrock_multi_node` for the Epic 9 Bedrock scenario matrix.

Useful local and CI commands:

```sh
mix test
mix test.integration
mix test.bedrock
mix test.multi_node
mix test --exclude flaky --only bedrock_multi_node
```

`mix test` remains the complete non-flaky suite. The narrower aliases are useful
when CI shards integration coverage separately from the faster unit and
single-process store tests.

## Operational Requirements

Production deployments must run a durable Bedrock cluster. At minimum:

- configure a `Bedrock.Cluster` and `Bedrock.Repo` module owned by the
  application;
- provide durable paths/object storage for Bedrock coordination, logs, and
  materializers;
- keep Bedrock durability mode strict for production;
- reserve relaxed durability for local development and test environments only;
- start the Bedrock cluster before the Intent Ledger instance that uses the
  repo;
- monitor Bedrock coordinator, log, materializer, object-storage, and disk
  health using Bedrock telemetry and application supervision.

The Intent Ledger adapter is stateless apart from its repo module. Restarting
the adapter process does not rebuild state from memory; all lifecycle history,
queue state, command replay records, leases, and outbox entries must already be
durably committed in Bedrock.

## Bedrock Operations

Use the general [Operations And Observability](operations.md) guide for event
names, metric units, dashboard recommendations, and runbooks. Bedrock
deployments should add the following adapter-specific views.

### Ledger Runtime Health

- Queue depth and retry depth from `IntentLedger.inspect_queues/2`.
- Shard ownership from `IntentLedger.inspect_shards/2`.
- Expired claims from `IntentLedger.inspect_claims/2`.
- Outbox lag from `IntentLedger.inspect_outbox_lag/2`.
- Projection lag from `IntentLedger.inspect_projection_lag/3`.
- Store conflict rate from `[:store, :conflict]`, especially
  `:stream_version`, `:claim_fence`, `:shard_lease`, and `:outbox` conflicts.

### Bedrock Infrastructure Health

Monitor the Bedrock cluster independently from Intent Ledger:

- coordinator availability and leadership changes;
- transaction latency and timeout rate;
- log and materializer health;
- object-storage availability and latency;
- disk usage and fsync latency for durable paths;
- process restarts for the repo, cluster, logs, and materializers.

Intent Ledger can report queue and lease symptoms, but Bedrock infrastructure
telemetry explains whether the durable substrate is healthy.

### Failure Modes

| Symptom | Likely Causes | First Checks |
| --- | --- | --- |
| Queue depth grows but claims are empty | shard lease not owned, clock skew, future `visible_at`, transaction errors | `inspect_shards/2`, `inspect_queues/2`, `[:shard_lease, :stop]`, node clocks |
| Shard ownership flaps | lease interval too short for transaction latency, node restarts, Bedrock timeouts | lease telemetry, Bedrock transaction latency, supervisor restarts |
| Expired claims accumulate | recovery not running, lifecycle classifier rejects recovery, Bedrock errors | recovery telemetry, `inspect_claims/2`, lifecycle logs |
| Outbox lag grows | dispatcher stopped, handler failures, ack conflicts, Bedrock write failures | `inspect_outbox_lag/2`, dispatcher telemetry, ack telemetry |
| Command replay conflicts spike | unstable command IDs or changed command semantics for the same ID | `[:store, :conflict]` by operation and command logs |

### Capacity Notes

Shard count controls local claim concurrency. More shards allow more shard
workers to claim in parallel, but also create more lease rows and more periodic
lease traffic. Start with enough shards for expected worker parallelism and
increase only when queue depth grows while each owned shard is claiming
successfully.

Tune these values together:

- `:lease_ms` - claim lease duration and base shard lease duration.
- `:lease_renew_ms` - how often shard workers renew ownership.
- `:lease_retry_ms` - retry delay after failed shard acquisition.
- `:poll_interval_ms` - periodic due-work scan interval.
- `:recovery_interval_ms` - expired-claim and stale-shard recovery interval.

Short intervals reduce failover time but increase transaction pressure. Long
intervals reduce pressure but increase recovery time after node or worker death.

## Cluster Formation Expectations

Intent Ledger does not manage Bedrock or BEAM cluster membership. The host
application must form the runtime topology before starting ledgers that depend
on it:

- start the Bedrock cluster, object-storage processes, and repo dependencies
  before the `IntentLedger` child that uses `IntentLedger.Store.Bedrock`;
- start the same named ledger, queue names, shard counts, lease settings, and
  Bedrock repo module on every BEAM node that should participate in claiming;
- keep nodes connected through the host release, deployment platform, or a
  clustering library such as `libcluster`;
- keep wall clocks synchronized across nodes because `visible_at`, claim lease,
  shard lease, and recovery checks compare timestamps written by different
  processes;
- run application workers that claim and complete work after the ledger process
  is available on that node.

Cross-node coordination happens through Bedrock transactions and Intent
Ledger's shard lease rows. The local notifier is intentionally best-effort and
does not publish wakeups to other nodes; periodic shard polling and recovery are
the progress mechanism when a submit happens on a different node, a wakeup is
lost, or a worker restarts.

If a node shuts down cleanly, its shard workers attempt to release their shard
leases during termination. If a node dies or the release path cannot reach
Bedrock, the lease remains fenced until it expires and the recovery loop or
another shard worker can take over. Size `lease_ms`, `lease_renew_ms`,
`lease_retry_ms`, `poll_interval_ms`, and `recovery_interval_ms` for the
deployment's expected transaction latency and failover target.

See [Clustering And Multi-Node Testing](clustering.md) for the production
topology checklist, lease tuning guidance, and local peer-node test harness.

## Compatibility Notes

The key schema version is embedded in the root prefix so a future migration can
run a new layout side by side with the current one. Do not write to these keys
outside `IntentLedger.Store.Bedrock` unless you also preserve the value envelope,
conflict semantics, and secondary indexes described above.
