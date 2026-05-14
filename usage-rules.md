# Usage Rules

## IntentLedger

- Start a supervised ledger with `{IntentLedger, name: MyApp.IntentLedger}` before using the API.
- Use stable `:key` and `:idempotency_key` values for commands that must not be duplicated.
- Pass a stable `:command_id` when a caller needs deterministic replay of a mutating command.
- Treat child intents as durable handoffs, not synchronous workflow calls; propagate lineage fields and configure
  recursion guardrails before enabling recursive work.
- Workers must use the returned claim token when calling `complete/5`, `fail/5`, `release/4`, or `heartbeat/4`.
- Treat `IntentLedger.Store.Memory` as a development and test adapter. Production adapters should implement `IntentLedger.Store`.
- Use `history/2` when you need the lifecycle signal stream for an intent.
