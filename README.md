# Intent Ledger

`intent_ledger` is an OTP-native package spike for durable deferred work. It turns
agent or workflow commands into immutable intents, tracks claim/retry/completion
state, and records every transition as a `Jido.Signal`.

This package is based on the design gist:
https://gist.github.com/mikehostetler/cc2f56822cf5611126f4462d7ed874c7

## Status

This is a package spike. The public API, lifecycle structs, supervision shape,
store behaviour, in-memory adapter, and optional Bedrock adapter are in place.
Ecto/Postgres remains a planned local durable adapter.

## Runtime Shape

- `IntentLedger` is the public API and child spec.
- `IntentLedger.InstanceSupervisor` owns a named ledger instance.
- The server process validates API calls and delegates atomic commits.
- `IntentLedger.QueueSupervisor` starts one local shard worker for each
  configured queue shard. Shard workers claim due work only while holding a
  durable store lease for that queue/shard.
- `IntentLedger.Notifier` provides local best-effort wakeups after newly
  claimable work is committed. Periodic polling and recovery remain the
  correctness path when wakeups are missed or work is submitted from another
  node.
- `IntentLedger.RecoveryServer` periodically recovers expired claims and stale
  queue-shard leases supported by the configured store.
- `IntentLedger.Projection` defines lightweight rebuild hooks for query models
  derived from replayed lifecycle signals.
- `IntentLedger.Store` defines the persistence contract.
- `IntentLedger.Store.Memory` is the executable in-memory reference adapter for
  tests and local examples. It is not durable and is not a clustered production
  backend.
- `IntentLedger.Store.Bedrock` is the optional durable adapter for Bedrock-backed
  clustered deployments. See [Bedrock Adapter](docs/bedrock.md).
- `IntentLedger.Lifecycle` provides optional hooks for submit enrichment and
  post-transition observation.

## Installation

During local mono-folder development this project uses the sibling
`../jido_signal` path dependency when present. Outside this workspace it falls
back to the Hex dependency declared in `mix.exs`.

```elixir
def deps do
  [
    {:intent_ledger, "~> 0.1"}
  ]
end
```

## Usage

Start a named ledger under your supervision tree:

The example below uses `IntentLedger.Store.Memory` so it can run without
external services. Use a durable `IntentLedger.Store` adapter for production or
for any workflow that must survive process restart.

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

In a host application, supervise the ledger after any durable store or cluster
infrastructure it depends on and before application workers that submit,
claim, complete, or recover intents. A named ledger instance starts its own
private registry, notifier, store adapter process, API server, queue shard
workers, and recovery server. Use a stable atom such as `MyApp.IntentLedger` as
the `name`; the same name is local to each BEAM node and is also the logical
ledger identifier used in durable store keys.

For clustered operation, start the same named ledger configuration on every
node that should participate in runtime claiming. Intent Ledger does not form
or discover BEAM clusters by itself. The host release is responsible for node
connectivity, durable store startup, and consistent queue/shard configuration
across participating nodes. Queue-shard ownership is fenced by store lease
rows, so only one local shard worker should claim a given queue/shard at a
time. Keep node clocks synchronized because visibility times, claim leases,
shard leases, and recovery decisions are time-based.

Submit and process work:

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

## Signal Compatibility

Public mutating APIs are normalized through `IntentLedger.Command` before they
commit lifecycle state. Command envelopes are `Jido.Signal` structs with:

- stable `intent_ledger.command.*` signal types;
- `datacontenttype: "application/json"`;
- a versioned `dataschema` URI;
- `data.schema_version`;
- command metadata fields for `command_id`, `idempotency_key`, `actor`,
  `causation_id`, `correlation_id`, `root_intent_id`, `parent_intent_id`, and
  `depth`.

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

`command_id` is the replay key. Reusing the same `command_id` returns the first
recorded result for the running ledger instance and does not append duplicate
lifecycle signals. Omit it when replay is not required.

## Lifecycle Signals

Lifecycle facts are emitted as `Jido.Signal` structs with stable
`intent_ledger.*` types, `datacontenttype: "application/json"`, a versioned
`dataschema` URI, and `data.schema_version`. The current lifecycle types are:

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

## Projection Rebuilds

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
    status =
      String.replace_prefix(type, "intent_ledger.intent.", "")

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
pass the returned signals to `IntentLedger.Projection.catch_up/4`.

## Development

```sh
mix deps.get
mix test
mix q
```
