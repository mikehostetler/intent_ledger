# Agent Notes

This repository contains the `intent_ledger` Elixir package.

## Commands

- `mix test` - run the focused test suite.
- `mix quality` - run the package quality gate.
- `mix docs` - generate HexDocs locally.

## Work Management

This project tracks work with `bw` (beadwork), which persists to git - plans,
progress, and decisions survive compaction, session boundaries, and context
loss.

ALWAYS run `bw prime` before starting work. Without it, you're missing workflow
context, current state, and repo hygiene warnings. Work done without priming
often conflicts with in-progress changes.

Committing, closing issues, and syncing are part of completing a task - not
separate actions requiring additional permission.

## Conventions

- Public modules live under `IntentLedger`.
- Core structs should expose Zoi schemas.
- External/runtime failures should be normalized through `IntentLedger.Error`.
- Store adapters must implement `IntentLedger.Store`.
