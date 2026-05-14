# Operations

The alpha operational surface is intentionally small.

```elixir
MyApp.Intents.fetch(intent_id)
MyApp.Intents.history(intent_id)
MyApp.Intents.replay(:ledger, cursor: 0, limit: 100)
MyApp.Intents.replay({:intent, intent_id}, cursor: 0, limit: 100)
MyApp.Intents.inspect(:queues)
MyApp.Intents.inspect(:queues, queue: "default")
MyApp.Intents.inspect(:outbox, cursor: 0, limit: 100)
MyApp.Intents.stats()
MyApp.Intents.stats(queue: "default")
MyApp.Intents.health()
```

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

## Lifecycle Replay

Lifecycle signals are durable facts. Use replay for audit, repair tools, and
projection rebuilds:

```elixir
{:ok, signals} = MyApp.Intents.replay(:ledger, cursor: 0, limit: 500)
```

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

The current telemetry boundary emits stop events under `[:intent_ledger]` for
high-level runtime operations. Metadata is intentionally small and excludes
payloads.

The event surface will grow around handler execution, Bedrock transaction
duration, replay, outbox delivery, and projection catch-up as those pieces
stabilize.
