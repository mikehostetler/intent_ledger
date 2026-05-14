# Intent Ledger

`intent_ledger` is an OTP-native intent lifecycle ledger for deferred work. It
stores immutable intents, tracks claim/retry/completion state, and records every
transition as a `Jido.Signal` so work can be claimed, recovered, replayed, and
projected with durable semantics.

Use it when an application needs a small, explicit lifecycle around background
or agent work:

- submit deferred work with stable idempotency keys;
- claim work through fenced leases;
- heartbeat, complete, fail, release, cancel, requeue, or mark work ambiguous;
- recover expired claims;
- dispatch lifecycle signals through a durable outbox;
- replay intent, queue, ledger, and outbox streams;
- inspect queue depth, shard leases, claims, retries, ambiguity, outbox lag, and
  projection lag.

## Installation

```elixir
def deps do
  [
    {:intent_ledger, "~> 0.1"}
  ]
end
```

During local mono-folder development this project uses the sibling
`../jido_signal` path dependency when present. Outside this workspace it uses
the Hex dependency declared in `mix.exs`.

## Quick Start

Start a memory-backed ledger under your supervision tree:

```elixir
children = [
  {IntentLedger,
   name: MyApp.IntentLedger,
   queues: [default: [shards: 4]],
   lease_ms: 30_000,
   store: IntentLedger.Store.Memory}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

`IntentLedger.Store.Memory` is intended for tests, examples, and local
development. Use a durable adapter for work that must survive process restart.
Use `IntentLedger.Store.Bedrock` for clustered durable deployments. Use
`IntentLedger.Store.Ecto` only for local durable development or explicit
single-node deployments.

Submit and process one intent:

```elixir
{:ok, record} =
  IntentLedger.submit(MyApp.IntentLedger, %{
    key: "invoice:123",
    kind: "invoice.send",
    payload: %{invoice_id: 123},
    idempotency_key: "invoice:123:send"
  })

{:ok, claimed} = IntentLedger.claim(MyApp.IntentLedger, :default, "worker-1")

{:ok, completed} =
  IntentLedger.complete(
    MyApp.IntentLedger,
    claimed.claim.id,
    claimed.claim.token,
    %{sent: true}
  )

{:ok, history} = IntentLedger.history(MyApp.IntentLedger, record.intent.id)
Enum.map(history, & &1.type)
```

If the same mutating command is retried with the same `:command_id`, Intent
Ledger returns the first recorded result and does not append duplicate lifecycle
signals.

```elixir
IntentLedger.submit(
  MyApp.IntentLedger,
  %{key: "invoice:123", kind: "invoice.send"},
  command_id: "cmd_invoice_123_send"
)
```

## Runtime Shape

A named ledger instance starts the runtime pieces it needs:

- `IntentLedger.InstanceSupervisor` owns the instance.
- The ledger server process validates public API calls and delegates commits.
- `IntentLedger.QueueSupervisor` starts one shard worker for each configured
  queue shard.
- `IntentLedger.QueueShardServer` claims due work only while holding a durable
  store lease for its queue/shard.
- `IntentLedger.Notifier` provides local best-effort wakeups for newly
  claimable work.
- `IntentLedger.RecoveryServer` recovers expired claims and stale shard leases.
- `IntentLedger.SignalDispatcher` reads durable outbox entries and invokes
  configured signal handlers.
- `IntentLedger.Telemetry` and `IntentLedger.Inspection` expose operational
  metrics and read-only state views.
- `IntentLedger.Store` defines the persistence contract implemented by the
  bundled adapters.

Supervise the ledger after any durable store or cluster infrastructure it
depends on, and before workers that submit or claim intents. Use a stable atom
such as `MyApp.IntentLedger` as the `:name`; it is both the local process name
and the logical ledger identifier used in durable store keys.

For clustered operation, start the same named ledger configuration on every
participating node. Intent Ledger does not form BEAM clusters by itself. The
host release owns node connectivity, durable store startup, and consistent
queue/shard configuration. Keep node clocks synchronized because visibility
times, claim leases, shard leases, and recovery decisions are time-based.

## Lifecycle APIs

The public lifecycle API is intentionally narrow:

- `submit/3` and `submit_many/3`
- `claim/4`
- `heartbeat/4`
- `complete/5`
- `fail/5`
- `release/4`
- `cancel/4`
- `requeue/3`
- `mark_ambiguous/4`
- `recover/3`
- `get/2` and `history/2`

Failures can be classified through `IntentLedger.Lifecycle`. A lifecycle module
can enrich intents before submit and classify failed or expired work as retry,
failure, or ambiguity.

## Command Signals

Public mutating APIs are normalized through `IntentLedger.Command` before they
commit lifecycle state. Command envelopes are `Jido.Signal` structs with stable
`intent_ledger.command.*` signal types, `datacontenttype: "application/json"`,
a versioned `dataschema` URI, command data, and command metadata.

Call `IntentLedger.command/3` to execute a command signal directly.

The current command signal types are:

- `intent_ledger.command.submit`
- `intent_ledger.command.submit_many`
- `intent_ledger.command.claim`
- `intent_ledger.command.heartbeat`
- `intent_ledger.command.complete`
- `intent_ledger.command.fail`
- `intent_ledger.command.release`
- `intent_ledger.command.cancel`
- `intent_ledger.command.requeue`
- `intent_ledger.command.mark_ambiguous`
- `intent_ledger.command.recover`

## Lifecycle Signals And Replay

Every lifecycle transition appends durable `Jido.Signal` records with stable
`intent_ledger.*` types:

- `intent_ledger.intent.submitted`
- `intent_ledger.intent.available`
- `intent_ledger.intent.claimed`
- `intent_ledger.intent.completed`
- `intent_ledger.intent.failed`
- `intent_ledger.intent.retry_scheduled`
- `intent_ledger.intent.cancelled`
- `intent_ledger.intent.marked_ambiguous`
- `intent_ledger.intent.released`
- `intent_ledger.claim.heartbeat`
- `intent_ledger.claim.lease_expired`

Replay APIs return bounded signal windows without mutating delivery state:

```elixir
{:ok, intent_signals} = IntentLedger.replay_intent(MyApp.IntentLedger, record.intent.id)
{:ok, shard_signals} = IntentLedger.replay_queue(MyApp.IntentLedger, :default, 0, limit: 100)
{:ok, ledger_signals} = IntentLedger.replay_ledger(MyApp.IntentLedger, cursor: 0, limit: 100)
{:ok, outbox_entries} = IntentLedger.replay_outbox(MyApp.IntentLedger, cursor: 0, limit: 100)
```

## Projections

Query projections can be treated as disposable state derived from lifecycle
signals. Define a projection module with `IntentLedger.Projection`, then rebuild
it from an intent, queue shard, or whole-ledger replay window:

```elixir
defmodule MyApp.IntentStatusProjection do
  @behaviour IntentLedger.Projection

  @impl true
  def init(_opts), do: %{version: 0, statuses: %{}}

  @impl true
  def apply_signal(%{subject: intent_id, type: type}, projection, _context) do
    status = String.replace_prefix(type, "intent_ledger.intent.", "")

    projection
    |> update_in([:statuses], &Map.put(&1, intent_id, status))
    |> Map.update!(:version, &(&1 + 1))
  end
end

{:ok, projection} =
  IntentLedger.rebuild_projection(
    MyApp.IntentLedger,
    MyApp.IntentStatusProjection,
    source: :ledger
  )
```

When a durable projection stores its own offset, replay from that cursor and
pass returned signals to `IntentLedger.Projection.catch_up/4`.

## Recursive Work

Intent lineage is durable context, not an in-memory workflow runtime.
`root_intent_id`, `parent_intent_id`, `depth`, `causation_id`,
`correlation_id`, and `actor` make related work observable and enforceable.
They do not make child intents run synchronously, join back to a parent, inherit
cancellation automatically, or become a process dependency graph.

Use child intents as durable handoffs from already claimed work. Configure
`:max_depth`, `:max_children_per_intent` or `:max_children`, and
`:max_open_descendants` to reject unsafe recursive submissions before commit.

## Operations

Intent Ledger emits stable `:telemetry` events for command handling, store
commits, conflicts, claims, shard leases, recovery, outbox dispatch, replay,
projection refresh, and inspection calls. Runtime inspection APIs expose
read-only state views without returning payloads or claim secrets:

```elixir
{:ok, queues} = IntentLedger.inspect_queues(MyApp.IntentLedger)
{:ok, shards} = IntentLedger.inspect_shards(MyApp.IntentLedger)
{:ok, claims} = IntentLedger.inspect_claims(MyApp.IntentLedger)
{:ok, retries} = IntentLedger.inspect_retries(MyApp.IntentLedger)
{:ok, ambiguous} = IntentLedger.inspect_ambiguity(MyApp.IntentLedger)
{:ok, outbox_lag} = IntentLedger.inspect_outbox_lag(MyApp.IntentLedger)
{:ok, projection_lag} = IntentLedger.inspect_projection_lag(MyApp.IntentLedger, MyApp.IntentStatusProjection)
```

See [Operations And Observability](docs/operations.md) for metric mapping,
dashboard panels, alert guidelines, and runbook checks.

## Adapter Guides

- [Bedrock Adapter](docs/bedrock.md) covers the clustered durable adapter.
- [Ecto/Postgres Adapter](docs/ecto.md) covers local durable and single-node SQL
  usage.
- `IntentLedger.Store.Memory` is the executable reference adapter for tests and
  examples; it is not durable.

## Development

```sh
mix deps.get
mix test
mix quality
mix docs
```
