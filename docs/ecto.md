# Ecto/Postgres Adapter

`IntentLedger.Store.Ecto` is the optional SQL adapter for local durable
development and single-node deployments. It uses an application-owned Ecto repo
and Postgres tables for Store V1 data while keeping Ecto and Postgrex optional
for applications that use only the memory or Bedrock adapters.

This adapter is not a clustered production backend. It does not form or manage
BEAM clusters, does not coordinate multiple ledger nodes through a distributed
consensus layer, and should not be used as the durability boundary for
multi-node claiming. Use `IntentLedger.Store.Bedrock` for clustered production
deployments.

## Supported Scope

Use the Ecto/Postgres adapter for:

- local development when process restarts should preserve ledger state;
- test environments that need SQL persistence without Bedrock;
- single-node applications where one BEAM node owns queue processing.

Avoid this adapter for:

- multiple BEAM nodes claiming work from the same ledger;
- cross-region or highly available production coordination;
- workflows that require distributed fencing beyond a single SQL repo
  transaction boundary.

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

Start the repo before the ledger:

```elixir
children = [
  MyApp.Repo,
  {IntentLedger,
   name: MyApp.IntentLedger,
   store: {IntentLedger.Store.Ecto, repo: MyApp.Repo}}
]
```

At startup the adapter checks that Ecto SQL, Postgrex, and a Postgres repo are
available. Missing dependencies or unsupported repo adapters are returned as
`IntentLedger.Error.AdapterRuntimeError` values.

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

The helper also accepts a Postgres schema prefix and table-name overrides:

```elixir
IntentLedger.Store.Ecto.Migration.change(
  prefix: "intent_ledger",
  tables: [intents: :workflow_intents]
)
```

The current table set stores intents, materialized states, stream counters,
signals, command idempotency rows, claim fences, shard leases, durable outbox
entries, and projection offsets.

## Runtime Notes

Store commits run inside `repo.transaction/2` and write SQL rows through Ecto
query and schema helpers. The adapter process itself is stateless apart from
its repo module, prefix, and table mapping.

Keep the runtime boundary simple:

- run one ledger node against the Ecto/Postgres store;
- start the repo before `IntentLedger`;
- keep system clocks stable because leases and visibility checks are
  time-based;
- use Bedrock instead when multiple nodes must claim, recover, or dispatch work
  for the same ledger.
