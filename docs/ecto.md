# Ecto/Postgres Adapter

`IntentLedger.Store.Ecto` is the optional SQL adapter for local durable
development and single-node deployments. It uses an application-owned Ecto repo
and Postgres tables for Store V1 data while keeping Ecto and Postgrex optional
for applications that use only the memory or Bedrock adapters.

This adapter is not a clustered production backend. SQL transactions provide
atomicity inside one Postgres repo, but they do not form a consensus layer,
coordinate BEAM cluster membership, or provide the distributed fencing expected
from `IntentLedger.Store.Bedrock`.

## Supported Scope

Use the Ecto/Postgres adapter for:

- local development when process restarts should preserve ledger state;
- test environments that need SQL persistence without Bedrock;
- single-node applications where one BEAM node owns queue processing;
- validating Store V1 behavior against a relational backend.

Avoid this adapter for:

- multiple BEAM nodes claiming work from the same ledger;
- cross-region or highly available production coordination;
- workflows that require distributed fencing beyond one SQL transaction
  boundary;
- replacing Bedrock in clustered production deployments.

## Current Adapter Surface

The adapter implements the Store V1 callbacks for:

- atomic commits with stream-version, command-replay, claim-fence, shard-lease,
  and outbox preconditions;
- shard lease acquire, renew, release, expire, and takeover operations;
- due-intent and expired-claim listings;
- durable outbox insert, read, ack, and replay;
- lineage counts and inspection reads;
- adapter availability checks and normalized runtime errors.

Unsupported Store V1 requests return
`IntentLedger.Error.AdapterRuntimeError` values instead of leaking Ecto or
Postgrex failures. Public runtime paths that still depend on
Memory-specific direct callbacks are outside this adapter's surface.

## Dependencies

Ecto SQL and Postgrex are optional dependencies for Hex consumers. Applications
that use this adapter must add them explicitly and provide a Postgres Ecto repo:

```elixir
def deps do
  [
    {:intent_ledger, "~> 0.1"},
    {:ecto_sql, "~> 3.13"},
    {:postgrex, "~> 0.22"}
  ]
end
```

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.Postgres
end
```

Configure the repo normally in the host application:

```elixir
config :my_app, MyApp.Repo,
  database: "my_app_dev",
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  pool_size: 10
```

## Setup Checklist

1. Add `:ecto_sql` and `:postgrex` to the host application's dependencies.
2. Define and configure an application-owned Postgres repo.
3. Add the Intent Ledger migration and run it against that repo.
4. Start the repo before any ledger that uses `IntentLedger.Store.Ecto`.
5. Pass the same `:prefix` and `:tables` options to both the migration helper
   and the store configuration.
6. Run one BEAM node as the queue-processing owner for the ledger.
7. Attach telemetry handlers and inspection checks before relying on the SQL
   store for local durable workflows.

## Supervision

Start the repo before the ledger:

```elixir
children = [
  MyApp.Repo,
  {IntentLedger,
   name: MyApp.IntentLedger,
   queues: [default: [shards: 4]],
   lease_ms: 30_000,
   store: {IntentLedger.Store.Ecto, repo: MyApp.Repo}}
]
```

At startup the adapter checks that Ecto SQL, Postgrex, and a Postgres repo are
available. Missing dependencies, a missing repo option, or a non-Postgres repo
adapter are returned as `IntentLedger.Error.AdapterRuntimeError` values.

## Store Options

The store child accepts:

- `:name` - process name for the adapter child, supplied by the ledger runtime
  when the store is configured through `IntentLedger`;
- `:repo` - required application-owned Postgres Ecto repo module;
- `:prefix` - optional Postgres schema prefix for all adapter tables;
- `:tables` - optional logical-to-physical table-name overrides.

When using table overrides, keep the option in one shared constant so migration
and runtime configuration cannot drift:

```elixir
@intent_ledger_tables [
  intents: :workflow_intents,
  states: :workflow_states,
  outbox: :workflow_outbox
]

IntentLedger.Store.Ecto.Migration.change(
  prefix: "intent_ledger",
  tables: @intent_ledger_tables
)

{IntentLedger,
 name: MyApp.IntentLedger,
 store:
   {IntentLedger.Store.Ecto,
    repo: MyApp.Repo,
    prefix: "intent_ledger",
    tables: @intent_ledger_tables}}
```

## Migrations

Use `IntentLedger.Store.Ecto.Migration` from an application migration to create
the Store V1 tables:

```elixir
defmodule MyApp.Repo.Migrations.CreateIntentLedgerTables do
  use Ecto.Migration

  def change do
    IntentLedger.Store.Ecto.Migration.change()
  end
end
```

For a dedicated Postgres schema, create the schema first and call the explicit
`up/1` and `down/1` helpers:

```elixir
defmodule MyApp.Repo.Migrations.CreateIntentLedgerTables do
  use Ecto.Migration

  def up do
    execute("CREATE SCHEMA IF NOT EXISTS intent_ledger")
    IntentLedger.Store.Ecto.Migration.up(prefix: "intent_ledger")
  end

  def down do
    IntentLedger.Store.Ecto.Migration.down(prefix: "intent_ledger")
    execute("DROP SCHEMA IF EXISTS intent_ledger")
  end
end
```

The current table set is:

- `intent_ledger_intents` - immutable intent records;
- `intent_ledger_states` - materialized lifecycle state and claim fields;
- `intent_ledger_streams` - per-stream version counters;
- `intent_ledger_signals` - lifecycle signal records;
- `intent_ledger_commands` - command idempotency and replay results;
- `intent_ledger_claims` - claim-fencing rows;
- `intent_ledger_shard_leases` - queue shard ownership leases;
- `intent_ledger_outbox` - durable outbox entries and acknowledgements;
- `intent_ledger_projection_offsets` - projection cursor checkpoints.

Tables are keyed by the logical ledger name, so one database can host multiple
ledgers as long as each ledger uses a stable `:name`.

## Runtime Notes

Store commits run inside `repo.transaction/2` and write SQL rows through Ecto
query and schema helpers. The adapter process itself is stateless apart from
its repo module, prefix, and table mapping.

Keep the runtime boundary simple:

- run one ledger node against the Ecto/Postgres store;
- start the repo before `IntentLedger`;
- keep system clocks stable because visibility checks and leases compare
  `DateTime` values;
- size the Ecto pool for the ledger, queue workers, recovery, dispatcher, and
  the rest of the application;
- use Bedrock when multiple nodes must claim, recover, or dispatch work for the
  same ledger.

For SQL sandbox tests, remember that ledger, queue, recovery, and dispatcher
processes are separate processes. Use a sandbox ownership strategy that allows
those processes to share the test connection, or run adapter integration tests
against an isolated test database.

## Operations

Use the general [Operations And Observability](operations.md) guide for event
names, metric units, dashboard recommendations, and runbooks. Add
Postgres-specific panels for:

- transaction latency and timeout rate;
- connection pool checkout latency and queue time;
- row counts for state, signals, command, claim, shard lease, and outbox
  tables;
- index size and bloat for high-churn tables such as states, claims, shard
  leases, and outbox;
- autovacuum activity and dead tuple counts;
- failed migrations or schema drift between environments.

The inspection APIs are available for SQL-backed state that is covered by the
adapter surface:

```elixir
{:ok, queues} = IntentLedger.inspect_queues(MyApp.IntentLedger)
{:ok, shards} = IntentLedger.inspect_shards(MyApp.IntentLedger)
{:ok, claims} = IntentLedger.inspect_claims(MyApp.IntentLedger)
{:ok, outbox_lag} = IntentLedger.inspect_outbox_lag(MyApp.IntentLedger)
```

## Failure Modes

| Symptom | Likely Causes | First Checks |
| --- | --- | --- |
| Adapter fails at startup | missing optional dependency, missing repo option, repo is not Postgres | dependency list, repo module, adapter availability check |
| Migration succeeds but runtime cannot find tables | store `:prefix` or `:tables` differs from migration options | migration module, store tuple, database search path |
| Queue depth grows without claims | single-node worker stopped, shard lease held, due listing blocked by clock or visibility time | `inspect_shards/2`, `inspect_queues/2`, repo transaction logs |
| Outbox lag grows | dispatcher stopped, handler failures, ack conflicts, SQL timeouts | `inspect_outbox_lag/2`, dispatcher telemetry, outbox table |
| Transaction conflicts or timeouts spike | small pool, slow queries, table bloat, long-running app transactions | Ecto pool telemetry, Postgres locks, index health |

## Migration Path

The public `IntentLedger` API is the intended boundary between Memory, Ecto, and
Bedrock-backed deployments. Keep application code on the public lifecycle APIs
and treat the store as supervision configuration.

Before moving a local Ecto setup to Bedrock:

- run the Bedrock setup checklist in [Bedrock Adapter](bedrock.md);
- start Bedrock cluster and repo infrastructure before the ledger;
- align queue names, shard counts, lease settings, and ledger names across
  nodes;
- replace SQL-specific dashboards with Bedrock infrastructure health checks;
- rehearse replay, outbox dispatch, recovery, and inspection workflows on the
  target adapter.
