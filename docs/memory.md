# Memory Reference Adapter

`IntentLedger.Store.Memory` is the in-memory reference adapter for tests,
examples, and local development. It implements the Store V1 contract with one
GenServer process and keeps all lifecycle state, stream records, shard leases,
claim fences, command idempotency rows, and outbox entries in process memory.

This adapter is useful because it exercises the same semantic contract as the
durable adapters without requiring Bedrock, Ecto, Postgres, or local storage.
It is not durable and is not a clustered production backend.

## Supported Scope

Use the Memory adapter for:

- unit and integration tests that need a real ledger runtime;
- examples, documentation snippets, and local demos;
- adapter contract exploration before choosing a durable store;
- fast development workflows where state can be discarded.

Avoid this adapter for:

- work that must survive process, node, release, or VM restart;
- multiple BEAM nodes claiming from the same logical ledger;
- production outbox delivery, audit history, or recovery workflows;
- capacity testing of a durable backend.

## Setup

The Memory adapter has no optional dependencies. It is the default store when a
ledger is started without an explicit `:store` option:

```elixir
children = [
  {IntentLedger,
   name: MyApp.IntentLedger,
   queues: [default: [shards: 4]],
   lease_ms: 30_000}
]
```

You can also configure it explicitly:

```elixir
children = [
  {IntentLedger,
   name: MyApp.IntentLedger,
   queues: [default: [shards: 4]],
   store: IntentLedger.Store.Memory}
]
```

Start the ledger before workers that submit or claim intents. The store process
is supervised as part of the ledger instance and is reset when that process is
restarted.

## Store Options

When the store is configured through `IntentLedger`, the runtime supplies the
adapter process name. The adapter accepts:

- `:name` - required process name for the Memory store child.

Applications usually do not need to pass Memory-specific options. Tests that
start the adapter directly can provide a unique process name:

```elixir
start_supervised!({IntentLedger.Store.Memory, name: MyTest.Store})
```

## Runtime Semantics

The Memory adapter serializes every operation through a single GenServer call.
Within that process it preserves the important Store V1 semantics:

- lifecycle updates and lifecycle signals are committed together;
- command IDs provide deterministic replay for repeated mutating commands;
- claim acquisition, heartbeat, completion, failure, and release are fenced by
  claim IDs and token hashes;
- queue shard leases are compared and renewed with the same lease rules used by
  durable adapters;
- replay streams are versioned by ledger, queue shard, intent, and outbox
  stream;
- durable-outbox semantics are modeled in memory with ordered reads, acks, and
  replay;
- inspection APIs evaluate queue depth, shard ownership, claims, retries,
  ambiguity, outbox lag, and projection lag from the current store state.

These guarantees hold only while the Memory store process is alive. If the
process or VM exits, all stored state is lost.

## Test Usage

Use Memory when tests need the public ledger API and real lifecycle behavior:

```elixir
setup do
  name = Module.concat(__MODULE__, "Ledger#{System.unique_integer([:positive])}")

  start_supervised!(
    {IntentLedger,
     name: name,
     queues: [default: [shards: 2]],
     store: IntentLedger.Store.Memory}
  )

  {:ok, ledger: name}
end
```

Then call the public API from the test:

```elixir
{:ok, record} =
  IntentLedger.submit(ledger, %{
    key: "email:123",
    kind: "email.send",
    payload: %{to: "user@example.com"}
  })

{:ok, claimed} = IntentLedger.claim(ledger, :default, "test-worker")
{:ok, completed} = IntentLedger.complete(ledger, claimed.claim.id, claimed.claim.token, %{})

assert completed.intent.id == record.intent.id
```

Give each test a unique ledger name when tests run concurrently. A named ledger
maps to local processes, so reusing the same name across async tests can cause
process-registration conflicts or state leakage.

## Reference Adapter Role

`IntentLedger.Store.Memory` is the executable reference for bundled adapter
conformance. It runs the shared StoreCase suites that cover atomic commits,
semantic lifecycle transitions, inspection reads, listings, shard leases, and
outbox behavior.

When implementing another `IntentLedger.Store` adapter, compare its behavior
against Memory and the shared StoreCase tests. Differences should be backend
requirements, not semantic drift.

## Operations

Memory-backed ledgers emit the same public telemetry events as other adapters,
including command, commit, conflict, claim, shard lease, recovery, outbox,
replay, projection, and inspection events. The metrics are useful for local
debugging and test assertions, but they describe only the current process.

The inspection APIs are also available:

```elixir
{:ok, queues} = IntentLedger.inspect_queues(MyApp.IntentLedger)
{:ok, shards} = IntentLedger.inspect_shards(MyApp.IntentLedger)
{:ok, claims} = IntentLedger.inspect_claims(MyApp.IntentLedger)
{:ok, outbox_lag} = IntentLedger.inspect_outbox_lag(MyApp.IntentLedger)
```

Use the general [Operations And Observability](operations.md) guide for event
names and metric units. Do not treat Memory inspection output as evidence of
durable production health.

## Migration Path

The public `IntentLedger` API is the intended migration boundary. If tests and
application code call `IntentLedger.submit/3`, `IntentLedger.claim/4`, and the
other public lifecycle functions instead of calling Memory-specific functions
directly, switching stores is a supervision configuration change:

```elixir
{IntentLedger,
 name: MyApp.IntentLedger,
 store: {IntentLedger.Store.Bedrock, repo: MyApp.BedrockRepo}}
```

Before moving from Memory to a durable adapter, validate:

- the durable store or cluster starts before the ledger;
- migrations or Bedrock infrastructure are applied;
- queue names, shard counts, and lease settings match across nodes;
- telemetry and inspection dashboards are attached;
- tests that depend on empty state use isolated durable namespaces or cleanup.
