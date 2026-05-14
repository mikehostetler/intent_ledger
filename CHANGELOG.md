# Changelog

All notable changes to this project will be documented in this file.

## 0.1.0 - Unreleased

Initial public release candidate for the `intent_ledger` package.

### Added

- Supervised named ledgers for durable intent submission, claim, completion,
  failure, retry, cancellation, ambiguity, recovery, and operational
  inspection workflows.
- Public lifecycle structs with Zoi schemas, typespecs, and JSON encoders for
  intents, intent state, claims, queue shard state, records, inspection
  requests, and Store V1 commit primitives.
- Jido.Signal command and lifecycle contracts for submit, claim, completion,
  retry, cancellation, ambiguity, and recovery events.
- Store V1 behaviour plus semantic commit, listing, outbox, precondition,
  conflict, and write request structs.
- In-memory reference store for tests, examples, and local development.
- Optional Bedrock adapter with value/keyspace helpers and clustered
  durability documentation.
- Optional Ecto/Postgres adapter with schema, query, and migration helpers for
  local durable storage.
- At-least-once reliability semantics covering idempotent commands, claim
  fencing, lease expiry, retry scheduling, outbox delivery, and ambiguity
  handling.
- Compile-tested examples for memory workflows, retry lifecycle handling,
  signal audit handlers, and status projections.
- Operations, reliability, clustering, Memory, Bedrock, and Ecto documentation
  included in the Hex package.
