# Intent Ledger Architecture

This document restates the original IntentLedger CQRS vision in the current
Bedrock-first architecture. It is intended for design review, not as a release
checklist.

Source context:

- Original vision gist:
  <https://gist.github.com/mikehostetler/cc2f56822cf5611126f4462d7ed874c7>
- Current implementation plan: `MILESTONE.md`
- Current public package story: `README.md`

## Thesis

IntentLedger is a low-level, durable Intent lifecycle control plane.

It records what an application intends to do later, tracks how that work moves
through execution, and exposes lifecycle facts for replay, inspection, outbox
delivery, and projections.

The original gist framed this as a Signal-native CQRS system with built-in
claiming, leases, shard ownership, recovery, dispatch, and projection machinery.
The current architecture keeps the durable lifecycle and Signal/CQRS goals, but
delegates queue concurrency and lease mechanics to `bedrock_job_queue`.

The resulting shape is:

```text
Application
  -> MyApp.Intents public API
  -> IntentLedger command and lifecycle boundary
  -> bedrock_job_queue queue and lease machinery
  -> Bedrock transactional persistence
```

IntentLedger should feel like an Intent API, not a job API. Job terminology may
exist below the boundary because `bedrock_job_queue` uses that vocabulary, but
applications should model and inspect durable `Intent`s.

## Core Responsibilities

IntentLedger owns:

- the `IntentLedger.Intent` domain model;
- configured `MyApp.Intents` API modules;
- direct command APIs such as `enqueue/3`, `cancel/3`, `requeue/2`, and
  `mark_ambiguous/3`;
- signal-native command ingress through `submit/2`;
- command signal construction through `command_signal/3`;
- command normalization and idempotency semantics;
- lifecycle signals as durable `Jido.Signal` facts;
- intent history and replay;
- durable outbox entries and consumer cursors;
- projection cursor helpers and rebuild helpers;
- operational inspection views;
- lineage, causation, correlation, and actor metadata;
- error normalization and telemetry around the public API.

`bedrock_job_queue` owns:

- queue item visibility;
- queue partitioning;
- queue and item leases;
- worker concurrency;
- scheduling;
- retry backoff;
- snooze/delay mechanics;
- dead-letter decision mechanics;
- worker lease extension and recovery behavior exposed by the queue layer.

Bedrock owns:

- transactional durability;
- conflict detection;
- ordered keyspace storage;
- the consistency boundary for IntentLedger state, lifecycle streams, outbox
  entries, projection cursors, and queue data.

The application owns:

- business logic inside handlers;
- external side-effect idempotency;
- workflow orchestration;
- whether to run a web controller, Plug, bus subscriber, or Jido runtime on top;
- durable query projections beyond the cursors IntentLedger stores.

## Non-Goals

IntentLedger should not become:

- a generic CQRS framework;
- a workflow engine;
- a Phoenix or Plug integration package;
- a Jido runtime package;
- a `Jido.Action` execution package;
- a Postgres adapter package;
- a generic store abstraction;
- a worker DSL;
- an exactly-once side-effect system.

These exclusions are important. The package lives below workflow engines and
above queue mechanics. Its value is the durable Intent lifecycle boundary.

## Revised CQRS Model

The gist's CQRS model still applies, but the concrete ownership changes.

### Command Side

Command side responsibilities:

- accept direct Elixir commands and signal-native command envelopes;
- normalize inputs to `IntentLedger.Command`;
- validate topics, queues, payload shape, command fields, and lifecycle
  invariants;
- write Intent state;
- append lifecycle signals;
- write outbox entries;
- maintain idempotency and status indexes;
- enqueue or neutralize `bedrock_job_queue` items when required.

The primary command APIs are direct Elixir functions:

```elixir
MyApp.Intents.enqueue(topic, payload, opts)
MyApp.Intents.cancel(intent_id, reason, opts)
MyApp.Intents.requeue(intent_id, opts)
MyApp.Intents.mark_ambiguous(intent_id, reason, opts)
```

Signal-native ingress uses the same command path:

```elixir
MyApp.Intents.submit(%Jido.Signal{} = signal, opts)
MyApp.Intents.command_signal(:enqueue, attrs, opts)
```

Direct API calls are the preferred Elixir developer experience. Signals are the
transport boundary for systems that already move CloudEvents-style envelopes.

### Queue And Execution Side

The original gist described IntentLedger-owned claim and shard servers. In the
current architecture, those responsibilities move below the boundary.

`bedrock_job_queue` receives a small queue item whose payload points back to the
Intent:

```elixir
%{ledger: MyApp.Intents, intent_id: intent.id}
```

The full application payload remains on the Intent record. The queue item is
machinery, not the domain object.

When a queue worker executes a handler:

1. `bedrock_job_queue` leases a visible queue item.
2. The IntentLedger handler bridge loads the Intent by ID.
3. The bridge checks whether the Intent is runnable.
4. If runnable, IntentLedger records `intent.started`.
5. The application handler runs.
6. The handler result is returned to `bedrock_job_queue`.
7. The queue action and lifecycle transition commit together through the
   `bedrock_job_queue` action hook.

This is the replacement for the gist's custom `ClaimIntent`, `CompleteIntent`,
`FailIntent`, and expired-claim machinery.

### Signal Side

Lifecycle signals are durable facts. They are appended to:

- the global ledger stream;
- the per-Intent stream;
- the durable outbox stream.

Current lifecycle signal types are:

- `intent.enqueued`
- `intent.started`
- `intent.completed`
- `intent.failed`
- `intent.retry_scheduled`
- `intent.discarded`
- `intent.canceled`
- `intent.ambiguous`

Command signal types are:

- `intent.command.enqueue`
- `intent.command.cancel`
- `intent.command.requeue`
- `intent.command.mark_ambiguous`

The signal type namespace is not final. Before release, decide whether to keep
the short `intent.*` namespace or move to a more package-specific namespace
such as `intent_ledger.intent.completed`.

### Query Side

Query-side projections are not required for correctness. They may lag and may
be rebuilt from lifecycle replay.

IntentLedger currently provides:

- `history/2`;
- `replay(:ledger | :outbox | {:intent, id}, opts)`;
- `IntentLedger.Projection.rebuild/3`;
- `IntentLedger.Projection.catch_up/4`;
- durable projection cursor read/write helpers;
- `inspect(:projections)` for cursor, ledger head, and lag.

Application-owned projections can store their own materialized state elsewhere.
IntentLedger only needs to provide deterministic replay and durable cursor
tracking.

## Invariants

These invariants are retained from the original vision.

### Lifecycle Facts Are Canonical

Accepted lifecycle transitions must append a durable lifecycle signal. The
signal is the audit and replay fact.

Current implementation status: landed for enqueue, start, complete, fail,
retry, discard, cancel, and ambiguous transitions.

### Command-Side Updates And Lifecycle Append Are Atomic

Intent state, status indexes, lifecycle streams, and outbox entries must move in
one Bedrock transaction.

Current implementation status: landed for IntentLedger-owned commands and for
handler terminal/retry paths through the `bedrock_job_queue` action hook.

### Queue Action And Intent Lifecycle Commit Together

When a handler returns, the queue completion/requeue/dead-letter action and the
Intent lifecycle transition must commit together.

Current implementation status: landed through `bedrock_job_queue` `on_action`
hook integration.

### Terminal Intents Are Immutable

Late duplicate hooks or stale queue callbacks must not append duplicate
terminal facts or change terminal state.

Current implementation status: unit and opt-in chaos coverage exists for stale
and duplicate queue callbacks.

### Idempotent Command Replay Is Deterministic

Submitting the same command should produce the same durable outcome when it
uses the same idempotency key.

Current implementation status:

- direct enqueue idempotency is keyed by explicit `:key`;
- signal-native enqueue defaults to `data.key || "signal:#{signal.id}"`;
- cancel and ambiguous commands are idempotent once already in that state;
- invalid repeated lifecycle commands return normalized conflicts.

### Lost Wakeups Do Not Lose Work

Work visibility must be durable in Bedrock, not dependent on a process message.

Current implementation status: delegated to `bedrock_job_queue` queue indexes
and scanner/manager behavior.

### Store Authority Beats Distributed Erlang Membership

During node races or partitions, correctness must come from Bedrock
transactions and queue lease/fencing behavior, not from trusting local process
membership.

Current implementation status: deterministic local chaos tests exist. True
distributed net-split tests are still required.

## Persistence Layout

IntentLedger stores these categories in Bedrock keyspaces:

- Intent records;
- idempotency key index;
- status indexes;
- stream versions;
- global lifecycle stream;
- per-Intent lifecycle streams;
- durable outbox entries;
- outbox consumer cursors;
- projection cursors.

`bedrock_job_queue` stores queue data in its own keyspaces:

- queue items;
- pointer indexes;
- queue leases;
- item leases;
- queue stats;
- dead-letter state.

The split matters:

- IntentLedger state explains what the application intended and what happened.
- Queue state explains how work is made visible, leased, retried, and executed.

## Supervision Model

The package application should stay thin. Applications define a configured
Intents module and place it in their supervision tree:

```elixir
children = [
  MyApp.BedrockCluster,
  {MyApp.Intents, concurrency: 10, batch_size: 20}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

`MyApp.Intents.child_spec/1` delegates to the generated
`MyApp.Intents.JobQueue` child spec. This preserves the gist's requirement that
runtime state belongs to named ledger instances, while avoiding a separate
IntentLedger supervisor tree.

## Signal Boundary

The package should be Signal-aware, but not force Signals into every internal
function.

Current boundary:

- command ingress can be `%Jido.Signal{}`;
- lifecycle facts are `%Jido.Signal{}`;
- replay and history return lifecycle signals;
- outbox entries contain lifecycle signals;
- queue handoff is currently a private Bedrock/job_queue pointer payload;
- handler callbacks receive payload and `IntentLedger.Context`, not raw
  `Jido.Signal` envelopes.

This is a deliberate compromise. It keeps the public and durable boundaries
Signal-native while allowing `bedrock_job_queue` to remain the queue owner.

Decision:

- Keep queue handoff as a private pointer payload.
- Do not wrap `bedrock_job_queue` or force its internal execution protocol into
  a public `Jido.Signal` contract.
- Keep Signals at durable and transport boundaries: command ingress, lifecycle
  facts, replay, and outbox.

This is the simpler boundary. It preserves the original gist's durable Signal
goals without making queue internals part of the public integration contract.

## Outbox

IntentLedger currently exposes durable outbox primitives:

```elixir
MyApp.Intents.read_outbox("consumer", limit: 100)
MyApp.Intents.outbox_cursor("consumer")
MyApp.Intents.ack_outbox("consumer", cursor)
```

This is enough for applications to build dispatchers with at-least-once
delivery. It does not yet ship a managed dispatcher process.

Decision to make:

- Keep only the low-level durable cursor API; or
- add an optional dispatcher process that reads outbox entries, calls a
  configured dispatch function, and advances cursors after successful delivery.

The core package should not add Phoenix, Plug, PubSub, or Jido runtime
dependencies for dispatch. Any dispatcher should be transport-neutral.

## Projections

IntentLedger has two projection categories.

Command-side projections:

- Intent records;
- status indexes;
- idempotency indexes;
- stream versions;
- outbox and cursor state;
- projection cursor state.

These participate in correctness and are updated transactionally.

Query-side projections:

- application-owned dashboards;
- search views;
- lineage views;
- analytics summaries.

These should be rebuildable from replay and must not be used for command
correctness.

Gap from the original gist:

- there is no offline verifier that rebuilds command-side projections from
  lifecycle streams and compares them against current Bedrock state.

That verifier is the main remaining CQRS-hardening feature after real recovery
tests.

## Failure And Recovery Semantics

IntentLedger should prove correctness under:

- duplicate command submissions;
- duplicate signal delivery;
- stale queue callbacks;
- cancel or ambiguous while leased;
- handler failure;
- retry and max-attempt failure;
- snooze;
- discard;
- worker crash before handler starts;
- worker crash after side effect but before queue commit;
- lease expiry and reprocessing;
- Bedrock restart;
- distributed node partition and heal;
- outbox consumer crash before ack;
- projection cursor replay and monotonicity.

Current coverage:

- fast unit tests cover command, handler, lifecycle, replay, outbox, projection,
  error, and telemetry boundaries;
- `mix test.bedrock` covers single-node Bedrock/job_queue scenarios;
- `mix test.multi_node` covers simulated node roles over shared Bedrock state;
- `mix test.chaos` covers deterministic local partition-style edge cases;
- internal repair verification rebuilds expected command-side Intent state,
  status indexes, idempotency keys, queue consistency, queue stats, and outbox
  mirror facts from replay.

Missing coverage:

- real distributed net split;
- real lease-expiry recovery once `bedrock_job_queue` exposes a stable path;
- worker crash after side effect but before queue commit;
- public operational repair commands.

## Developer Experience

The public API should stay small and boring.

Applications should normally use:

```elixir
MyApp.Intents.enqueue("invoice.send", %{invoice_id: 123}, key: "invoice:123:send")
```

Signal-native integrations should use:

```elixir
{:ok, signal} =
  MyApp.Intents.command_signal(:enqueue,
    topic: "invoice.send",
    payload: %{invoice_id: 123}
  )

{:ok, intent} = MyApp.Intents.submit(signal)
```

Handlers should use `IntentLedger.Handler`, not `Jido.Action`:

```elixir
defmodule MyApp.Intents.SendInvoice do
  use IntentLedger.Handler, topic: "invoice.send"

  @impl true
  def handle(payload, ctx) do
    MyApp.Billing.send_invoice(payload.invoice_id, ctx)
    {:ok, %{sent: true}}
  end
end
```

The package can interoperate with Jido because it uses `Jido.Signal` envelopes,
but it should not expose Jido runtime concepts in the IntentLedger developer
experience.

## Architecture Decisions

### Keep Bedrock-Only Persistence For Alpha

The original gist allowed pluggable persistence. The current release should not.
Bedrock is the consistency substrate. Adding a generic store behavior now would
weaken the core contract and re-open the Postgres adapter surface too early.

### Delegate Queue Ownership To bedrock_job_queue

The original gist owned queues, shards, claims, heartbeats, and recovery. The
current package delegates those mechanics to `bedrock_job_queue`, which is
designed for high-concurrency queue processing.

IntentLedger should harden the boundary instead of reimplementing queue
machinery.

### Keep Handler Language

The package should keep `Handler`, not `Action`, as the execution contract.

`Jido.Action` is useful at the agent/runtime layer. IntentLedger is lower-level
and should keep its dependency surface thin.

### Keep Plug/Phoenix Out

No `IntentLedger.Plug` should ship in core. Web apps can decode a signal and
call `MyApp.Intents.submit(signal)`.

### Keep Direct API Primary

Signal-native command ingress is important, but direct Elixir APIs should remain
the best default developer experience.

## Boundary Refinement Plan

The architecture should stay simple. Do not create a generic queue adapter or
wrap `bedrock_job_queue` behind another abstraction. The boundary to tighten is
the developer-facing IntentLedger boundary, not the internal queue dependency.

### 1. Keep bedrock_job_queue Direct And Internal

`bedrock_job_queue` is not a plugin. It is the queue and lease engine for this
package.

The clean boundary is:

```text
Public API:       MyApp.Intents.*
Handler API:      IntentLedger.Handler.handle/2
Lifecycle API:    Jido.Signal facts, replay, outbox
Internal queue:   bedrock_job_queue
Durability:       Bedrock
```

Do not add `IntentLedger.Queue` or `IntentLedger.Queue.Adapter` unless there is
a concrete second queue implementation. Until then, an adapter would add
ceremony without improving the developer experience.

The rule is simpler:

- public docs should explain IntentLedger concepts;
- internal code may call `bedrock_job_queue` directly;
- queue structs, leases, and store calls should not appear in the public API;
- tests may use `bedrock_job_queue` primitives to prove the integration.

### 2. Keep The Handler DX Small

Application handlers should see only:

```elixir
def handle(payload, %IntentLedger.Context{} = ctx)
```

The fact that the handler module also satisfies `Bedrock.JobQueue.Job` can
remain an implementation detail generated by `use IntentLedger.Handler`. There
is no need to split that into a separate bridge module unless the public docs or
compiler errors start exposing job_queue concepts to application developers.

Tightening work:

- make handler errors mention IntentLedger handler concepts, not job_queue
  callback details;
- ensure generated docs show `IntentLedger.Handler` as the contract;
- keep `Bedrock.JobQueue.Job` out of examples and guides;
- keep context fields focused on Intent lifecycle data, not queue internals.

### 3. Keep The Public API Boring

The main API should remain:

```elixir
enqueue/3
enqueue_many/2
submit/2
command_signal/3
fetch/1
history/2
replay/2
cancel/3
requeue/2
mark_ambiguous/3
read_outbox/2
ack_outbox/3
projection_cursor/2
put_projection_cursor/3
inspect/2
stats/1
health/1
```

Avoid adding public functions that expose queue mechanics, claims, leases,
heartbeats, shard ownership, or recovery commands. Those are either owned by
`bedrock_job_queue` or should stay internal until there is a proven operational
need.

### 4. Keep Signals At Durable Boundaries

Current Signal boundary is the right simple shape:

- command ingress can be a `%Jido.Signal{}`;
- direct API commands are still first-class;
- lifecycle facts are `%Jido.Signal{}`;
- replay and outbox expose lifecycle signals;
- queue handoff remains a private pointer payload;
- handlers receive payload plus `IntentLedger.Context`.

Do not force queue handoff into a Signal just to satisfy the original gist
literally. The durable and integration-facing surfaces are Signal-native; the
internal queue payload can stay minimal.

### 5. Keep Rich Replay Explicit

`history/2` and `replay/2` returning bare `Jido.Signal` structs is a good DX.
Keep that as the default.

Repair, projection catch-up, and forensics sometimes need stream cursor/version
metadata. That richer shape exists as an explicit helper rather than
complicating the default:

```elixir
MyApp.Intents.replay_entries(:ledger, cursor: 0, limit: 100)
```

Do not replace the simple signal replay API.

### 6. Keep Repair Internal First

The original CQRS vision wants command-side projections to be rebuildable from
lifecycle signals. That is still a useful invariant, but it does not need a
large public API yet.

Target:

- internal or test-support verifier;
- replay lifecycle facts;
- rebuild expected Intent/index/outbox cursor state;
- compare against Bedrock keyspaces;
- report drift.

Current status: an internal verifier exists and is covered by focused tests.
Expose public repair commands only after the verifier proves useful and the
operator workflow is clear.

### 7. Keep Inspection Read-Only

`inspect/2`, `stats/1`, and `health/1` should stay read-only.

Do not turn inspection into repair, mutation, or queue control. If repair
exists later, keep it separate and explicit.

### 8. Keep Outbox Low-Level

The durable outbox cursor API is enough for alpha:

```elixir
read_outbox/2
outbox_cursor/2
ack_outbox/3
```

Do not add a managed dispatcher unless it remains optional, transport-neutral,
and obviously useful. A dispatcher can easily become a framework-shaped surface.

### 9. Prove The Queue Boundary With Tests

Since IntentLedger intentionally delegates queue mechanics to
`bedrock_job_queue`, hardening should happen through integration tests, not a
wrapper layer.

The key release tests are:

- worker crashes before handler starts;
- worker crashes after handler side effect but before queue commit;
- lease expires and another worker obtains the same queue item;
- stale worker completes after a newer worker has already committed;
- node partition heals and only one terminal Intent lifecycle survives.

## Open Decisions Before Stable Release

1. Should IntentLedger ship an optional managed outbox dispatcher?
2. What is the final signal type namespace?
3. What exact recovery guarantees does `bedrock_job_queue` expose for expired
   leases and worker crashes?
4. Should operational repair helpers become public, or stay test/internal
   tooling?
5. What is the Hex release order for `bedrock`, `bedrock_job_queue`, and
   `intent_ledger`?

## Review Checklist

Use this checklist when reviewing whether the implementation still matches the
architecture:

- Public docs use `Intent`, not `Job`.
- Public docs use `Handler`, not `Action`.
- No Plug, Phoenix, Postgres, Ecto, or Jido runtime dependency enters core.
- Direct API and signal-native API normalize through the same command path.
- Every accepted lifecycle transition appends a lifecycle signal.
- Intent state and lifecycle signal append commit atomically.
- Queue action and lifecycle transition commit atomically for handler results.
- Terminal Intents are immutable.
- Duplicate command delivery is deterministic.
- Stale queue callbacks cannot mutate terminal state.
- Outbox cursors are monotonic.
- Projection cursors are monotonic.
- Query projections are never used for runtime correctness.
- Real net-split and lease-expiry recovery are tested before stable release.
