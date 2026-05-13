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

```elixir
children = [
  {IntentLedger,
   name: MyApp.IntentLedger,
   store: {IntentLedger.Store.Bedrock, repo: MyApp.BedrockRepo}}
]
```

The adapter checks for `:bedrock`, `Bedrock`, and `Bedrock.Repo` at startup and
returns an `IntentLedger.Error.AdapterRuntimeError` when the dependency or repo
module is missing.

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

## Compatibility Notes

The key schema version is embedded in the root prefix so a future migration can
run a new layout side by side with the current one. Do not write to these keys
outside `IntentLedger.Store.Bedrock` unless you also preserve the value envelope,
conflict semantics, and secondary indexes described above.
