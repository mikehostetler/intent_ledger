# Usage Rules

## IntentLedger

- Define a configured module with `use IntentLedger`, `:repo`, and `:intents`.
- Configure each Intent topic with one `IntentLedger.Handler` module and, when
  useful, a queue name.
- Use stable `:key` values for work that must not be duplicated.
- Keep payloads ordinary Erlang terms. IntentLedger stores the full payload in
  Bedrock and gives `bedrock_job_queue` only an Intent pointer.
- Keep handlers idempotent. Retries are at-least-once and external side effects
  are outside IntentLedger's transaction.
- Use `fetch/1`, `history/2`, `replay/2`, and `inspect/2` for operational and
  repair tooling.
