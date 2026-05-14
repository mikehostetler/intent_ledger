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
- a durable outbox stream mirroring lifecycle signals.

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

Handler completion currently records Intent lifecycle state before
`bedrock_job_queue` commits its queue completion/requeue transaction. The next
implementation step is an upstream job queue transaction hook or a thin direct
executor so handler result handling can update queue state and Intent state in
the same Bedrock transaction.

## Testing

Default tests use a small Bedrock-compatible fake repo. Opt-in Bedrock/job queue
scenarios are tagged:

```sh
mix test --include bedrock
```
