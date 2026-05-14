# Intent Ledger

`intent_ledger` is an alpha Elixir package for durable, observable deferred work.
It models work as **Intents**: explicit records of something an application wants
to happen later, with lifecycle history, replay, projections, and operational
inspection built in.

The package has been hard-refactored around
[`bedrock_job_queue`](https://github.com/bedrock-kv/job_queue) and
[`bedrock`](https://github.com/bedrock-kv/bedrock). The examples below describe
the current alpha API for this direction. Expect breaking changes before any
stable release.

If you are interested in the design or want to build with it during alpha, join
the Jido Discord: <https://jido.run/discord>.

## Status

This project is **alpha**.

- The public API is not stable.
- Postgres/Ecto support has been removed from the runtime surface.
- Bedrock is the intended durable runtime.
- `bedrock_job_queue` provides scheduling, leasing, retry, and concurrent
  execution machinery.
- Intent Ledger owns the domain model: Intents, lifecycle signals, replay,
  outbox, projections, and developer-facing API.

The goal is not to expose another "job" abstraction. The lower queue layer may
use job terminology internally, but the application-facing model is simply:

```text
Intent -> Signal history -> Projection / outbox / inspection
```

## Why

Background work systems usually start as "put this job on a queue" and later
grow requirements around idempotency, auditability, retries, recovery,
observability, and business-level replay.

Intent Ledger starts with those requirements as the core object model:

- enqueue durable Intents with stable business keys;
- process them with application handlers;
- record every lifecycle transition as a signal;
- replay lifecycle streams for audit, repair, and projections;
- inspect queue health, retries, outbox lag, and projection lag;
- preserve lineage for recursive or agent-driven work.

## Installation

During the Bedrock refactor, use local path dependencies in this mono-folder:

```elixir
def deps do
  [
    {:bedrock, path: "../bedrock", override: true},
    {:bedrock_job_queue, path: "../job_queue"},
    {:intent_ledger, path: "../intent_ledger"}
  ]
end
```

Once the API settles, this package is expected to move back to normal Hex-based
installation.

## Quick Start

Define an application-facing Intents module:

```elixir
defmodule MyApp.Intents do
  use IntentLedger,
    otp_app: :my_app,
    repo: MyApp.Bedrock,
    intents: %{
      "invoice.send" => [
        handler: MyApp.Intents.SendInvoice,
        queue: "billing"
      ]
    },
    queues: ["tenant:acme", "bulk"]
end
```

`intents` is the ledger instance's routing contract. Each topic declares its
handler and may declare the queue used when producers omit `:queue`. Multiple
topics can share a queue. `queues` is optional and adds extra allowed lanes, such
as tenant or bulk partitions. `default_queue` is only needed for topics that do
not declare their own queue. Unknown queue IDs are rejected before Intent state is
written.

Define a handler:

```elixir
defmodule MyApp.Intents.SendInvoice do
  use IntentLedger.Handler, topic: "invoice.send"

  @impl true
  def handle(%{invoice_id: invoice_id}, ctx) do
    MyApp.Billing.send_invoice(invoice_id, ctx)
    {:ok, %{sent: true}}
  end
end
```

Start Bedrock and the Intents module under supervision:

```elixir
children = [
  MyApp.BedrockCluster,
  {MyApp.Intents, concurrency: 10, batch_size: 20}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Enqueue an Intent:

```elixir
{:ok, intent} =
  MyApp.Intents.enqueue("invoice.send", %{invoice_id: 123},
    key: "invoice:123:send",
    queue: "tenant:acme",
    priority: 50,
    max_attempts: 5
  )
```

Inspect and replay:

```elixir
{:ok, intent} = MyApp.Intents.fetch(intent.id)
{:ok, signals} = MyApp.Intents.history(intent.id)

{:ok, window} = MyApp.Intents.replay(:ledger, cursor: 0, limit: 100)
{:ok, outbox_signals} = MyApp.Intents.replay(:outbox, cursor: 0, limit: 100)
{:ok, queues} = MyApp.Intents.inspect(:queues)
{:ok, tenant_queue} = MyApp.Intents.stats(queue: "tenant:acme")
{:ok, outbox} = MyApp.Intents.inspect(:outbox)
```

## Public API Shape

The intended API is intentionally small:

```elixir
MyApp.Intents.enqueue(topic, payload, opts)
MyApp.Intents.enqueue_many(entries, opts)

MyApp.Intents.fetch(intent_id)
MyApp.Intents.history(intent_id, opts)
MyApp.Intents.replay(source, opts)

MyApp.Intents.read_outbox(consumer, opts)
MyApp.Intents.outbox_cursor(consumer, opts)
MyApp.Intents.ack_outbox(consumer, cursor, opts)

MyApp.Intents.projection_cursor(projection, opts)
MyApp.Intents.put_projection_cursor(projection, cursor, opts)

MyApp.Intents.cancel(intent_id, reason, opts)
MyApp.Intents.requeue(intent_id, opts)
MyApp.Intents.mark_ambiguous(intent_id, reason, opts)

MyApp.Intents.inspect(view, opts)
MyApp.Intents.stats(opts)
MyApp.Intents.health(opts)
```

Handler modules receive the Intent payload and an `IntentLedger.Context`:

```elixir
@callback handle(payload :: term(), context :: IntentLedger.Context.t()) ::
            :ok
            | {:ok, term()}
            | {:error, term()}
            | {:discard, term()}
            | {:snooze, non_neg_integer()}
```

## Intent Model

An Intent is the durable application-level object.

Expected fields include:

- `id` - generated Intent id;
- `key` - producer-provided business/idempotency key;
- `topic` - handler routing topic, such as `"invoice.send"`;
- `queue` - queue or tenant partition;
- `payload` - application payload;
- `status` - current lifecycle status;
- `attempt` and retry metadata;
- `created_at`, `scheduled_at`, and lifecycle timestamps;
- lineage fields such as `root_intent_id`, `parent_intent_id`,
  `correlation_id`, `causation_id`, and `actor`.

Payloads should be treated as application data. For large files, store the file
outside Intent Ledger and put references, checksums, size, and content type in
the payload.

## Lifecycle

The public lifecycle is expressed through Intents and signals:

```text
enqueued
started
completed
failed
retry_scheduled
discarded
canceled
ambiguous
```

`bedrock_job_queue` handles visibility, scheduling, leases, backoff, concurrent
execution, and dead-letter mechanics. Intent Ledger records the domain lifecycle
around those mechanics so application code can inspect, replay, and project what
happened.

Manual `requeue/2` is intentionally narrow in the current alpha: it accepts
failed or discarded Intents. Ambiguous Intents are parked for reconciliation and
should not be blindly requeued until the queue-state repair path is explicit.

## Signals And Replay

Every lifecycle transition is recorded as a durable signal.

Replay is source-based:

```elixir
MyApp.Intents.replay({:intent, intent_id}, cursor: 0, limit: 100)
MyApp.Intents.replay(:ledger, cursor: 0, limit: 100)
MyApp.Intents.replay(:outbox, cursor: 0, limit: 100)
```

Signals are the audit log and the source for rebuildable read models. They are
also the boundary for integrations that need durable outbox delivery.

## Durable Outbox

`replay(:outbox, ...)` gives raw signal replay. Integrations that need durable
delivery should use named outbox consumers:

```elixir
{:ok, batch} = MyApp.Intents.read_outbox("webhook-dispatcher", limit: 100)

Enum.each(batch.entries, fn entry ->
  deliver_webhook!(entry.signal)
end)

{:ok, _ack} = MyApp.Intents.ack_outbox("webhook-dispatcher", batch.next_cursor)
```

Consumer acks are monotonic by default. Re-acking the same cursor is idempotent;
acking behind the stored cursor returns a conflict. This is an at-least-once
consumer cursor API, not a managed dispatcher process.

## Projections

Projections are disposable read models built from Intent lifecycle signals.

```elixir
defmodule MyApp.IntentStatusProjection do
  @behaviour IntentLedger.Projection

  @impl true
  def init(_opts), do: %{cursor: 0, statuses: %{}}

  @impl true
  def apply_signal(signal, projection, _ctx) do
    put_in(projection, [:statuses, signal.subject], signal.type)
  end
end
```

The intended projection workflow is:

```elixir
{:ok, cursor} = MyApp.Intents.projection_cursor(MyApp.IntentStatusProjection)
cursor = cursor || 0
{:ok, signals} = MyApp.Intents.replay(:ledger, cursor: cursor)
{:ok, projection} = IntentLedger.Projection.catch_up(MyApp.IntentStatusProjection, projection, signals)
:ok = MyApp.Intents.put_projection_cursor(MyApp.IntentStatusProjection, cursor + length(signals))
```

Projection cursor writes are monotonic by default and cannot advance beyond the
current ledger head. Use `force: true` only for explicit repair or rebuild
workflows. `inspect(:projections)` includes each projection cursor, ledger head,
and lag.

## Persistence

Intent Ledger now uses a Bedrock-only durable persistence story.

`bedrock_job_queue` stores queue state inside Bedrock transactions. Intent Ledger
stores Intent state, lifecycle signals, and outbox entries in the same
Bedrock-backed system.

Postgres is intentionally not part of the new runtime plan. Teams that want a
familiar operational substrate can run Bedrock with local filesystem persistence
or object storage such as S3/MinIO as Bedrock support matures.

## Operations

The current inspection surface is view-based:

```elixir
MyApp.Intents.inspect(:queues)
MyApp.Intents.inspect(:intents, queue: "billing", status: :retry_scheduled)
MyApp.Intents.inspect(:retries)
MyApp.Intents.inspect(:ambiguous)
MyApp.Intents.inspect(:outbox)
MyApp.Intents.inspect(:projections)
```

Inspection views return `{:ok, value}` or normalized `IntentLedger.Error`
exceptions for unsupported views and invalid options. Queue callbacks still use
the raw `bedrock_job_queue` return protocol.

Telemetry currently emits stop events for:

- enqueue and command handling;
- handler execution;
- replay;
- outbox read/cursor/ack;
- projection cursor reads/writes;
- health checks.

The remaining telemetry surface will cover:

- lifecycle signal append;
- Bedrock transaction duration and conflicts;
- queue depth and retry pressure;
- managed outbox dispatch if that becomes part of the package;
- projection catch-up.

## Next Refactor Work

The remaining implementation work is:

1. Decide whether Intent Ledger should ship a managed outbox dispatcher process
   or keep the lower-level durable consumer cursor API only.
2. Add real worker crash and lease-expiry scenarios once `bedrock_job_queue`
   exposes a stable recovery path for those cases.
3. Publish once `bedrock_job_queue` has a Hex release path.

## Development

```sh
mix deps.get
mix test
mix quality
mix docs
```

Heavy integration tests are opt-in and tagged. Run the focused aliases for the
scenario you want:

```sh
mix test.integration
mix test.bedrock
mix test.multi_node
```

The package is changing quickly. For design discussion, implementation status,
or integration questions, join <https://jido.run/discord>.
