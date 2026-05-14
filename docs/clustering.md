# Clustering

Intent Ledger no longer owns shard workers, claim leases, or recovery loops.
Multi-node queue concurrency belongs to `bedrock_job_queue` and Bedrock.

Applications should run the same configured Intents module on each BEAM node
that should process work:

```elixir
children = [
  MyApp.BedrockCluster,
  {MyApp.Intents, concurrency: 20, batch_size: 10}
]
```

## Responsibilities

Intent Ledger owns:

- Intent state;
- lifecycle signals;
- replay;
- outbox entries;
- projection helpers;
- the application-facing `MyApp.Intents` API.

`bedrock_job_queue` owns:

- queue visibility;
- scheduling;
- leases;
- retry and backoff;
- worker concurrency;
- dead-letter mechanics.

Bedrock owns the durable transaction substrate.

## Operational Notes

Keep these values consistent across nodes:

- the configured Intents module;
- the Bedrock repo/cluster;
- handler topic mappings;
- queue IDs used by producers;
- `bedrock_job_queue` consumer options that affect throughput.

The next clustering milestone is a set of opt-in tests that start multiple
consumers against the same Bedrock-backed queue and verify that Intent lifecycle
state remains consistent across retries, restarts, and handler crashes.
