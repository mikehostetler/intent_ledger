# Intent Ledger

`intent_ledger` is an OTP-native package spike for durable deferred work. It turns
agent or workflow commands into immutable intents, tracks claim/retry/completion
state, and records every transition as a `Jido.Signal`.

This package is based on the design gist:
https://gist.github.com/mikehostetler/cc2f56822cf5611126f4462d7ed874c7

## Status

This is a package spike. The public API, lifecycle structs, supervision shape,
store behaviour, and in-memory adapter are in place. Durable adapters such as
Ecto/Postgres or Bedrock can implement `Jido.IntentLedger.Store` without changing
callers.

## Runtime Shape

- `Jido.IntentLedger` is the public API and child spec.
- `Jido.IntentLedger.InstanceSupervisor` owns a named ledger instance.
- The server process validates API calls and delegates atomic commits.
- `Jido.IntentLedger.Store` defines the persistence contract.
- `Jido.IntentLedger.Store.Memory` is the executable in-memory contract for tests.
- `Jido.IntentLedger.Lifecycle` provides optional hooks for submit enrichment and
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

```elixir
children = [
  {Jido.IntentLedger,
   name: MyApp.IntentLedger,
   queues: [default: [shards: 4]],
   lease_ms: 30_000,
   store: Jido.IntentLedger.Store.Memory}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Submit and process work:

```elixir
{:ok, record} =
  Jido.IntentLedger.submit(MyApp.IntentLedger, %{
    key: "invoice:123",
    kind: "invoice.send",
    payload: %{invoice_id: 123},
    idempotency_key: "invoice:123:send"
  })

{:ok, claimed} = Jido.IntentLedger.claim(MyApp.IntentLedger, :default, "worker-1")

{:ok, completed} =
  Jido.IntentLedger.complete(
    MyApp.IntentLedger,
    claimed.claim.id,
    claimed.claim.token,
    %{sent: true}
  )

{:ok, history} = Jido.IntentLedger.history(MyApp.IntentLedger, record.intent.id)
Enum.map(history, & &1.type)
```

## Lifecycle Signals

The spike emits these `Jido.Signal` types:

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

## Development

```sh
mix deps.get
mix test
mix q
```
