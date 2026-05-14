# Bedrock Runtime

Intent Ledger is Bedrock-backed. The package stores durable Intent state and
lifecycle streams directly in Bedrock keyspaces and uses `bedrock_job_queue` for
queue mechanics.

```text
MyApp.Intents
  -> IntentLedger.Runtime
  -> IntentLedger.BedrockStore
  -> Bedrock.JobQueue.Store
  -> Bedrock
```

## Dependencies

During alpha development in the mono-folder, use path dependencies:

```elixir
{:bedrock, path: "../bedrock", override: true},
{:bedrock_job_queue, path: "../job_queue"}
```

`bedrock_job_queue` is not wrapped as public API. Application modules call their
configured `MyApp.Intents` module, and Intent Ledger keeps job queue details
behind that boundary.

## Persistence Layout

Intent Ledger stores:

- full `IntentLedger.Intent` records;
- key-to-intent idempotency index entries;
- lifecycle signal streams for `:ledger` and `{:intent, intent_id}`;
- a durable outbox stream mirroring lifecycle signals;
- outbox consumer cursors;
- projection cursors.

The queue item payload is intentionally minimal:

```elixir
%{ledger: MyApp.Intents, intent_id: intent.id}
```

The full application payload remains on the Intent record and is encoded with
Erlang external term encoding, so arbitrary Elixir terms and binaries are
preserved. Large files should normally be stored externally with references and
checksums in the Intent payload.

## Current Atomicity Boundary

Enqueue is atomic: Intent state, lifecycle signal append, outbox append, and
queue item insert happen in one Bedrock transaction.

Handler completion uses the `bedrock_job_queue` action hook so queue
completion/requeue and Intent lifecycle state commit in the same Bedrock
transaction. Cancellation and ambiguity updates remove pending unleased queue
items directly; already leased items are neutralized when the worker observes
the non-runnable Intent state.

## Testing

Default tests use a small Bedrock-compatible fake repo. Opt-in Bedrock/job queue
scenarios are tagged:

```sh
mix test --include bedrock
```
