# Intent Ledger Bedrock Job Queue Milestone

Updated: 2026-05-14

This milestone replaces the earlier Store V1/Postgres/shard plan. The package
is moving to a simpler release architecture:

```text
IntentLedger public API
  -> IntentLedger.Handler
  -> bedrock_job_queue
  -> Bedrock
```

The current code remains useful as a state-machine and test reference, but it
is not the release architecture.

## Release Thesis

Publish `intent_ledger` as a low-level, durable Intent lifecycle package for
Elixir applications that need explicit deferred work semantics.

The package owns:

- the `Intent` domain model;
- the public configured `MyApp.Intents` API;
- `IntentLedger.Handler` and handler validation;
- lifecycle signals;
- command/idempotency records;
- durable outbox;
- replay;
- projections;
- operational inspection;
- lineage for recursive or agent-driven work.

The package does not own:

- a generic workflow engine;
- Phoenix integration;
- a worker DSL;
- a Postgres adapter;
- a generic store abstraction;
- a Jido runtime;
- `Jido.Action` execution;
- exactly-once external side effects.

## Non-Negotiable Decisions

- Use `Intent`, not `Job`, in the public API.
- Use `Handler`, not `Action`, in the public API.
- Do not depend on `jido_action`.
- Do not create a separate Jido adapter package.
- Build `IntentLedger.Handler` directly on Zoi.
- Keep dependency surface thin.
- Use `jido_signal` only for signal envelopes.
- Use `bedrock_job_queue` for queue visibility, leasing, scheduling, retry,
  backoff, and concurrent execution.
- Use Bedrock as the durable persistence substrate.
- Remove Ecto/Postgres modules, tests, aliases, docs, and dependencies.
- Remove public shard, claim, heartbeat, release, and recover APIs.
- Keep lifecycle facts, replay, outbox, projections, and lineage as first-class
  release concepts.

## Target Public API

Applications define a configured Intents module:

```elixir
defmodule MyApp.Intents do
  use IntentLedger,
    otp_app: :my_app,
    repo: MyApp.Bedrock,
    handlers: %{
      "invoice.send" => MyApp.Intents.SendInvoice
    }
end
```

Applications define handlers:

```elixir
defmodule MyApp.Intents.SendInvoice do
  use IntentLedger.Handler,
    topic: "invoice.send",
    payload_schema: Zoi.object(%{
      invoice_id: Zoi.integer()
    }),
    result_schema: Zoi.object(%{
      sent: Zoi.boolean()
    }),
    timeout: :timer.seconds(30)

  @impl true
  def handle(%{invoice_id: invoice_id}, ctx) do
    MyApp.Billing.send_invoice(invoice_id, ctx)
    {:ok, %{sent: true}}
  end
end
```

Applications enqueue and inspect Intents:

```elixir
{:ok, intent} =
  MyApp.Intents.enqueue("invoice.send", %{invoice_id: 123},
    key: "invoice:123:send",
    queue: "tenant:acme",
    priority: 50,
    max_attempts: 5
  )

{:ok, intent} = MyApp.Intents.fetch(intent.id)
{:ok, signals} = MyApp.Intents.history(intent.id)
{:ok, window} = MyApp.Intents.replay(:ledger, cursor: 0, limit: 100)
{:ok, queues} = MyApp.Intents.inspect(:queues)
```

The intended public functions are:

```elixir
MyApp.Intents.enqueue(topic, payload, opts)
MyApp.Intents.enqueue_many(entries, opts)

MyApp.Intents.fetch(intent_id)
MyApp.Intents.history(intent_id, opts)
MyApp.Intents.replay(source, opts)

MyApp.Intents.cancel(intent_id, reason, opts)
MyApp.Intents.requeue(intent_id, opts)
MyApp.Intents.mark_ambiguous(intent_id, reason, opts)

MyApp.Intents.inspect(view, opts)
MyApp.Intents.stats(opts)
MyApp.Intents.health(opts)
```

## Handler Contract

`IntentLedger.Handler` is the only public execution contract.

Callback:

```elixir
@callback handle(payload :: term(), context :: IntentLedger.Context.t()) ::
            :ok
            | {:ok, term()}
            | {:error, term()}
            | {:discard, term()}
            | {:snooze, non_neg_integer()}
```

Handler responsibilities:

- declare a topic;
- optionally declare `payload_schema`;
- optionally declare `result_schema`;
- optionally declare timeout and metadata;
- perform one unit of application work;
- return one of the IntentLedger result shapes.

IntentLedger responsibilities:

- validate payloads with Zoi before handler execution;
- validate successful results when a result schema is declared;
- provide context with the full Intent and execution metadata;
- map handler results to durable lifecycle transitions;
- emit telemetry;
- ensure queue state and Intent state move together.

`Jido.Action` is deliberately not used here. It is valuable for the Jido agent
runtime, command/tool composition, AI tool definitions, and richer execution
machinery. IntentLedger is lower-level and should not inherit that dependency
surface or vocabulary.

## Durable Model

An Intent is the durable application-level object.

Expected release fields:

- `id`;
- `key`;
- `topic`;
- `queue`;
- `payload`;
- `context`;
- `status`;
- `priority`;
- `attempt`;
- `max_attempts`;
- `scheduled_at`;
- `created_at`;
- `updated_at`;
- `completed_at`;
- `result`;
- `error`;
- `cancel_reason`;
- `root_intent_id`;
- `parent_intent_id`;
- `depth`;
- `correlation_id`;
- `causation_id`;
- `actor`;
- `metadata`.

`bedrock_job_queue` item ids should map to Intent ids. Queue payloads should be
minimal, ideally just enough to resolve the Intent:

```elixir
%{"intent_id" => intent.id}
```

The full application payload belongs to the Intent record, not to the queue
item. The queue is machinery; IntentLedger is the domain boundary.

## Lifecycle Signals

Lifecycle transitions are recorded as `Jido.Signal` envelopes.

Target lifecycle events:

- `intent.enqueued`
- `intent.started`
- `intent.completed`
- `intent.failed`
- `intent.retry_scheduled`
- `intent.discarded`
- `intent.canceled`
- `intent.ambiguous`

Signals are durable facts. They drive:

- `history/2`;
- `replay/2`;
- outbox dispatch;
- projection rebuild;
- audit and repair workflows.

## Persistence Story

The release persistence story is Bedrock-only.

Dependencies should become:

```elixir
{:bedrock, path: "../bedrock", override: true},
{:bedrock_job_queue, path: "../job_queue"}
```

During implementation, path dependencies are acceptable because the local
mono-folder contains sibling checkouts of `bedrock` and `job_queue`.

Before Hex release, decide whether `bedrock_job_queue` is available on Hex or
whether release timing must wait for that package.

Remove from the release path:

- `IntentLedger.Store.Ecto`;
- Ecto migration helpers;
- Ecto query/schema modules;
- Postgres tests;
- Postgres guide content;
- optional dependency load tests for Ecto/Postgres;
- `mix test.postgres`;
- any README references to Postgres as an adapter.

Memory may remain temporarily during the refactor only if it accelerates unit
testing. It is not a release runtime story unless there is a specific, narrow
test-support reason to keep it.

## Bedrock Job Queue Integration

`bedrock_job_queue` owns:

- queue partitioning;
- pointer indexes;
- queue leases;
- item leases;
- scheduling;
- retry/backoff;
- dead-letter mechanics;
- scanner/manager/worker concurrency.

IntentLedger owns:

- Intent records;
- lifecycle state;
- lifecycle signals;
- command/idempotency records;
- outbox;
- projection offsets;
- inspection views.

The key integration requirement is atomic lifecycle movement. When a handler
returns, the queue action and the IntentLedger lifecycle commit must succeed or
fail together.

Preferred upstream change:

```elixir
on_action: {IntentLedger.JobQueueHook, :apply}
```

The hook should run inside the same Bedrock transaction that completes,
requeues, snoozes, or dead-letters the queue item.

The hook needs access to:

- repo;
- root keyspace;
- queue item;
- lease;
- handler result;
- chosen queue action;
- timestamp/options.

If that hook is not accepted upstream quickly, the fallback is to build a thin
IntentLedger executor that uses `Bedrock.JobQueue.Store` primitives directly.
The fallback should still preserve the QuiCK queue data model instead of
reintroducing IntentLedger shards.

## Replay And Projection API

Replay is source-based:

```elixir
MyApp.Intents.replay({:intent, intent_id}, cursor: 0, limit: 100)
MyApp.Intents.replay(:ledger, cursor: 0, limit: 100)
MyApp.Intents.replay(:outbox, cursor: 0, limit: 100)
```

No public shard replay API.

Projection responsibilities:

- consume lifecycle signals;
- store their own durable cursor if they are durable;
- be rebuildable from replay;
- not participate in lifecycle correctness.

Inspection is view-based:

```elixir
MyApp.Intents.inspect(:queues)
MyApp.Intents.inspect(:intents)
MyApp.Intents.inspect(:retries)
MyApp.Intents.inspect(:ambiguous)
MyApp.Intents.inspect(:outbox)
MyApp.Intents.inspect(:projections)
```

## Release Gates

Fast gates:

- `mix format --check-formatted`
- `mix compile --warnings-as-errors`
- `mix test`
- `mix docs`
- `mix hex.build`

Dependency gates:

- no `ecto_sql`;
- no `postgrex`;
- no `jido_action`;
- no runtime dependency introduced only for optional integrations;
- dependency tree reviewed before release.

Unit gates:

- `Intent` schema tests;
- `IntentLedger.Handler` schema and callback tests;
- payload validation tests;
- result validation tests;
- lifecycle result mapping tests;
- command/idempotency tests;
- signal serialization tests;
- replay window tests;
- projection catch-up tests;
- outbox tests;
- public API examples compile.

Integration gates:

- opt-in local Bedrock scenario passes;
- enqueue writes Intent state and queue item atomically;
- handler success completes queue item and Intent state atomically;
- handler error schedules retry and records signal atomically;
- `{:discard, reason}` records discarded state and removes queue item;
- `{:snooze, ms}` reschedules queue item and records signal;
- cancel removes or neutralizes queue visibility and records signal;
- duplicate enqueue with same key is deterministic;
- outbox replay survives restart.

Multi-node gates:

- three Erlang nodes run one logical Intents module against Bedrock;
- Node A enqueues, Node B executes, Node C can inspect/replay;
- concurrent execution races resolve to one active lease;
- worker crash recovers through queue lease expiry;
- duplicate enqueue/command races resolve once;
- outbox dispatch resumes after dispatcher interruption;
- Bedrock restart preserves Intent and queue state.

## Implementation Epics

### Epic 0: Dependency And Surface Reset

Goal: remove old architecture before building the new one.

Scope:

- point `:bedrock` at `../bedrock`;
- add `:bedrock_job_queue` from `../job_queue`;
- remove Ecto/Postgres dependencies;
- remove Postgres aliases and tests;
- remove Ecto content from ExDoc config;
- remove public shard/claim/heartbeat/release/recover guide content;
- keep README aligned with the new alpha story.

Acceptance:

- `rg "Ecto|Postgres|postgrex|ecto_sql" mix.exs README.md guides lib test`
  returns only changelog or historical comments, if any;
- `mix deps.get`;
- `mix test` passes without Postgres exclusions being meaningful.

### Epic 1: Handler Contract

Goal: introduce the release execution contract.

Scope:

- add `IntentLedger.Handler`;
- add `IntentLedger.Context`;
- implement Zoi payload validation;
- implement Zoi result validation;
- normalize handler result values;
- document return semantics;
- add compile-time option validation where practical without adding
  `nimble_options` directly unless already pulled in by existing deps.

Acceptance:

- handler modules compile with `use IntentLedger.Handler`;
- invalid handler options fail clearly;
- payload validation failures record failed/discarded lifecycle according to
  policy;
- no dependency on `jido_action`.

### Epic 2: Bedrock Persistence Core

Goal: define the Bedrock keyspace and persistence functions IntentLedger owns.

Scope:

- versioned IntentLedger keyspace;
- Intent record put/fetch;
- lifecycle state put/fetch;
- lifecycle signal append;
- command/idempotency record put/fetch;
- outbox insert/read/ack/replay;
- projection offset put/fetch;
- inspection range reads.

Acceptance:

- operations run inside Bedrock transactions;
- key encoding is documented;
- signal append and state update are atomic;
- replay windows are deterministic.

### Epic 3: Job Queue Integration

Goal: bridge IntentLedger lifecycle commits with `bedrock_job_queue`.

Scope:

- configure an internal job queue module for each Intents module;
- use Intent id as queue item id;
- store minimal queue payload;
- map queue topic to IntentLedger handler topic;
- implement or upstream transaction hook;
- map handler result to queue action and Intent lifecycle commit.

Acceptance:

- enqueue writes Intent and queue item in one transaction;
- success completes queue item and Intent in one transaction;
- retry/snooze/discard paths are atomic;
- stale/duplicate execution cannot append duplicate terminal lifecycle facts.

### Epic 4: Public API Rewrite

Goal: replace the old top-level API with configured Intents modules.

Scope:

- implement `use IntentLedger`;
- generate `child_spec/1` and `start_link/1`;
- generate `enqueue/3`;
- generate `enqueue_many/2`;
- generate `fetch/1`;
- generate `history/2`;
- generate `replay/2`;
- generate `cancel/3`;
- generate `requeue/2`;
- generate `mark_ambiguous/3`;
- generate `inspect/2`;
- generate `stats/1`;
- generate `health/1`;
- remove no-backwards-compat public functions from `IntentLedger`.

Acceptance:

- README examples compile;
- public API reference and guides only show the configured module API;
- old claim/shard API is gone.

### Epic 5: Signals, Outbox, Replay, Projections

Goal: keep the durable audit and read-model story first-class.

Scope:

- finalize lifecycle signal schemas;
- append outbox entries in lifecycle transactions;
- implement source-based replay;
- implement projection catch-up helpers;
- implement projection lag inspection;
- keep signal dispatcher if still useful after runtime rewrite.

Acceptance:

- every lifecycle transition emits one canonical signal;
- replay supports intent, ledger, and outbox sources;
- projection rebuild works from replayed lifecycle signals;
- outbox delivery is at-least-once and acked durably.

### Epic 6: Integration And Multi-Node Scenarios

Goal: prove the new architecture with opt-in Bedrock tests.

Scope:

- keep heavy tests tagged;
- start simple local Bedrock scenario;
- add handler success/failure/snooze/discard scenarios;
- add restart scenario;
- add multi-node scenario after the single-node path is stable.

Acceptance:

- `mix test` remains fast by default;
- `mix test --include bedrock` runs Bedrock scenarios;
- multi-node tests are opt-in and isolated.

### Epic 7: Release Docs And Hex Prep

Goal: ship an honest alpha release when implementation catches up.

Scope:

- README reflects implemented API, not only target API;
- reliability guide documents at-least-once execution;
- Bedrock guide documents local filesystem and object storage considerations;
- operations guide documents telemetry and inspection;
- changelog has initial alpha release notes;
- package metadata no longer references removed adapters.

Acceptance:

- `mix docs`;
- `mix hex.build`;
- README warning clearly states alpha status;
- Discord link is present for design discussion.

## First Build Sequence

1. Reset dependencies and delete Postgres/Ecto surface.
2. Add `IntentLedger.Handler` and `IntentLedger.Context`.
3. Add Bedrock persistence functions for Intent state, signals, outbox, replay,
   and command/idempotency.
4. Prototype the `bedrock_job_queue` transaction hook or direct Store fallback.
5. Implement `use IntentLedger` configured module API.
6. Port tests from old lifecycle semantics to new Intent/Handler semantics.
7. Add first Bedrock queue integration scenario.
8. Expand into restart and multi-node scenarios.

## Open Questions

- Does `bedrock_job_queue` accept a transaction lifecycle hook, or do we build a
  direct Store-based executor first?
- Should command/idempotency be exposed publicly, or only as an internal
  duplicate-enqueue guarantee?
- Should Memory survive as a pure unit-test support module, or should tests use
  Bedrock fakes and local Bedrock only?
- What is the final Hex release timing relative to `bedrock_job_queue`
  availability on Hex?
