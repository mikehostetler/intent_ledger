# Usage Rules

## Jido.IntentLedger

- Start a supervised ledger with `{Jido.IntentLedger, name: MyApp.IntentLedger}` before using the API.
- Use stable `:key` and `:idempotency_key` values for commands that must not be duplicated.
- Workers must use the returned claim token when calling `complete/5`, `fail/5`, `release/4`, or `heartbeat/4`.
- Treat `Jido.IntentLedger.Store.Memory` as a development and test adapter. Production adapters should implement `Jido.IntentLedger.Store`.
- Use `history/2` when you need the lifecycle signal stream for an intent.
