# Changelog

All notable changes to this project will be documented in this file.

## 0.1.0 - Unreleased

Initial public release candidate for the `intent_ledger` package.

### Added

- Bedrock-first Intent runtime built around configured `use IntentLedger`
  modules.
- Topic-based Intents with `IntentLedger.Handler` callbacks, Zoi payload
  validation, and Zoi result validation.
- `bedrock_job_queue` integration for queue visibility, leasing, retry,
  discard, snooze, and completion mechanics.
- Atomic enqueue of Intent state, lifecycle signal, outbox entry, and queue
  pointer.
- Atomic queue action hook for committing queue completion/retry/discard/snooze
  together with Intent lifecycle transitions.
- Full Erlang-term payload storage in Bedrock while queue items carry only an
  Intent pointer.
- Idempotent enqueue keys and lifecycle commands for cancel, requeue, and
  ambiguous reconciliation.
- Durable lifecycle replay for `:ledger`, `{:intent, intent_id}`, and `:outbox`.
- Projection cursor read/write helpers for application-owned projections.
- View-based operational inspection for queues, Intents, retries, ambiguous
  Intents, outbox entries, and projection cursors.
- Splode-backed public error normalization and telemetry stop events.
- Opt-in Bedrock and simulated multi-node integration scenarios.
