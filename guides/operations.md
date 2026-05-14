# Operations

The alpha operational surface is intentionally small.

```elixir
MyApp.Intents.fetch(intent_id)
MyApp.Intents.submit(signal)
MyApp.Intents.command_signal(:enqueue, topic: "invoice.send", payload: %{id: 1})
MyApp.Intents.history(intent_id)
MyApp.Intents.replay(:ledger, cursor: 0, limit: 100)
MyApp.Intents.replay({:intent, intent_id}, cursor: 0, limit: 100)
MyApp.Intents.replay(:outbox, cursor: 0, limit: 100)
MyApp.Intents.read_outbox("webhook-dispatcher", limit: 100)
MyApp.Intents.outbox_cursor("webhook-dispatcher")
MyApp.Intents.ack_outbox("webhook-dispatcher", cursor)
MyApp.Intents.projection_cursor(MyApp.IntentStatusProjection)
MyApp.Intents.put_projection_cursor(MyApp.IntentStatusProjection, cursor)
MyApp.Intents.inspect(:queues)
MyApp.Intents.inspect(:queues, queue: "default")
MyApp.Intents.inspect(:intents, status: :enqueued)
MyApp.Intents.inspect(:retries)
MyApp.Intents.inspect(:ambiguous)
MyApp.Intents.inspect(:outbox, cursor: 0, limit: 100)
MyApp.Intents.inspect(:projections)
MyApp.Intents.stats()
MyApp.Intents.stats(queue: "default")
MyApp.Intents.health()
```

## Signal-Native Ingress

Intent Ledger stays transport-neutral. It does not expose `IntentLedger.Plug`
and does not depend on Plug or Phoenix. Web and bus integrations should decode
their inbound envelope into `%Jido.Signal{}` and submit it:

```elixir
with {:ok, signal} <- Jido.Signal.new(params),
     {:ok, intent} <- MyApp.Intents.submit(signal) do
  {:ok, intent}
end
```

The direct APIs and `submit/2` share the same command path. Signal-native
enqueue uses the signal ID as the default idempotency key when no key is present
in the signal data, so at-least-once delivery can safely redeliver the same
command envelope.

## Queue Stats

`stats/1` delegates to `bedrock_job_queue` and returns pending and processing
counts for configured queues. Without `:queue`, it returns every queue declared
by the ledger instance. With `:queue`, it returns one configured queue.

```elixir
{:ok, %{"default" => %{pending_count: 10, processing_count: 2}, "bulk" => _}} =
  MyApp.Intents.stats()

{:ok, %{"default" => %{pending_count: 10, processing_count: 2}}} =
  MyApp.Intents.stats(queue: "default")
```

## Inspection

`inspect/2` is view-based and intended for operator-facing dashboards,
diagnostics, and repair tooling:

```elixir
{:ok, intents} = MyApp.Intents.inspect(:intents, limit: 100)
{:ok, retrying} = MyApp.Intents.inspect(:retries)
{:ok, ambiguous} = MyApp.Intents.inspect(:ambiguous)
{:ok, outbox} = MyApp.Intents.inspect(:outbox, cursor: 0, limit: 100)
{:ok, cursors} = MyApp.Intents.inspect(:projections)
```

`:intents` accepts optional `:queue`, `:topic`, `:status`, and `:limit`
filters. `:retries` is the `:retry_scheduled` Intent view. `:ambiguous` is the
manual reconciliation view. `:projections` returns durable projection cursor
records written by `put_projection_cursor/3`, including ledger head and lag.

## Lifecycle Replay

Lifecycle signals are durable facts. Use replay for audit, repair tools, and
projection rebuilds:

```elixir
{:ok, signals} = MyApp.Intents.replay(:ledger, cursor: 0, limit: 500)
{:ok, outbox_signals} = MyApp.Intents.replay(:outbox, cursor: 0, limit: 500)
```

## Durable Outbox

Use `read_outbox/2` and `ack_outbox/3` for integrations that need durable
delivery progress:

```elixir
{:ok, batch} = MyApp.Intents.read_outbox("webhook-dispatcher", limit: 100)

Enum.each(batch.entries, fn entry ->
  dispatch!(entry.signal)
end)

{:ok, _ack} = MyApp.Intents.ack_outbox("webhook-dispatcher", batch.next_cursor)
```

Outbox acknowledgements are monotonic. A consumer can reread after its last
acknowledged cursor after a crash, which gives at-least-once delivery. Intent
Ledger does not currently ship a managed dispatcher process.

Signal types are:

- `intent.enqueued`;
- `intent.started`;
- `intent.completed`;
- `intent.failed`;
- `intent.retry_scheduled`;
- `intent.discarded`;
- `intent.canceled`;
- `intent.ambiguous`.

## Telemetry

The telemetry boundary emits stop events under `[:intent_ledger]` for
high-level runtime operations:

- `[:intent_ledger, :enqueue, :stop]`
- `[:intent_ledger, :handler, :stop]`
- `[:intent_ledger, :command, :stop]`
- `[:intent_ledger, :replay, :stop]`
- `[:intent_ledger, :outbox, :stop]`
- `[:intent_ledger, :projection, :stop]`
- `[:intent_ledger, :health, :stop]`

Measurements include `:duration` in native units and `:count`. Metadata includes
the ledger module, status, and operation-specific fields such as handler, topic,
queue, command, and intent ID. Payloads and handler results are intentionally
excluded.

The event surface will grow around Bedrock transaction duration, queue pressure,
managed outbox dispatch if added, and projection catch-up as those pieces
stabilize.

## Errors

Application-facing APIs return `{:error, exception}` using `IntentLedger.Error`
exceptions. Queue callbacks still return `:ok`, `{:ok, result}`,
`{:error, reason}`, `{:discard, reason}`, or `{:snooze, delay_ms}` because those
terms are the `bedrock_job_queue` execution protocol.
