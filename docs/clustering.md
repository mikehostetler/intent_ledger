# Clustering And Multi-Node Testing

Intent Ledger does not form a BEAM cluster by itself. The host application owns
node discovery, node connectivity, durable store startup, and release topology.
Intent Ledger coordinates claim ownership through the configured store.

Use `IntentLedger.Store.Bedrock` for clustered durable deployments. The Memory
adapter is process-local, and the Ecto/Postgres adapter is scoped to local
durable and single-node use.

## Production Topology

For a clustered deployment, each participating BEAM node should start:

- the same named ledger, for example `MyApp.IntentLedger`;
- the same queue names and shard counts;
- compatible lease, poll, recovery, and dispatcher intervals;
- the same Bedrock repo module and Bedrock cluster configuration;
- application workers only after the ledger is available on that node.

Start dependencies in this order:

1. BEAM node and distribution.
2. Bedrock cluster, object storage, logs, materializers, and repo.
3. `IntentLedger` instance that uses the Bedrock repo.
4. Workers that submit, claim, complete, fail, or release work.
5. Optional signal handlers and application projection processes.

The host release should use its platform, static configuration, DNS, Kubernetes
service discovery, or a clustering library to connect nodes. Intent Ledger only
requires that all nodes can reach the durable store and that workers use the
same ledger configuration.

## Coordination Model

Cross-node coordination happens through store commits:

- queue shard workers acquire durable shard leases before claiming due work;
- only a current shard owner should claim from that queue shard;
- claim commits write claim fences containing owner, claim ID, token hash, and
  lease expiry;
- heartbeat, complete, fail, and release commits must satisfy the claim fence;
- stale shard leases can be expired and taken over by another node;
- local wakeups are best-effort only. Periodic shard polling and recovery are
  the cross-node progress mechanisms.

This model gives at-least-once work execution with fenced ledger state. It does
not make external side effects exactly-once. See
[Reliability Semantics](reliability.md) for worker and ambiguity rules.

## Clock And Lease Requirements

Visibility, claim leases, shard leases, and recovery are time-based. Nodes that
share a durable store must keep wall clocks synchronized.

Tune these values together:

- `:lease_ms` - claim lease duration and base shard lease duration;
- `:lease_renew_ms` - how often shard owners renew ownership;
- `:lease_retry_ms` - retry delay after failed shard acquisition;
- `:poll_interval_ms` - due-work polling interval;
- `:recovery_interval_ms` - expired-claim and stale-shard recovery interval.

Short intervals reduce failover time but increase transaction pressure. Long
intervals reduce pressure but increase the window before another node can make
progress after a worker or node dies.

## Supervision Example

Each node should use the same logical ledger name and queue configuration:

```elixir
children = [
  MyApp.BedrockCluster,
  MyApp.BedrockRepo,
  {IntentLedger,
   name: MyApp.IntentLedger,
   queues: [default: [shards: 16], emails: [shards: 8]],
   lease_ms: 30_000,
   lease_renew_ms: 10_000,
   lease_retry_ms: 2_000,
   poll_interval_ms: 1_000,
   recovery_interval_ms: 5_000,
   store: {IntentLedger.Store.Bedrock, repo: MyApp.BedrockRepo}}
]
```

Use a stable atom for `:name`. It is both the local process name and the logical
ledger identifier used in durable store keys.

## Multi-Node Test Harness

The repository includes a local multi-node harness for integration tests. It
uses:

- peer Erlang nodes started by the test process;
- shared code paths and an Erlang cookie for node connectivity;
- temporary local filesystem object storage;
- a temporary Bedrock cluster descriptor;
- a cross-node Store V1 helper that issues commits from different peer nodes.

The harness does not require an external Bedrock service. It also does not boot
full application releases; tests call Store V1 operations on peer nodes so they
can isolate durable coordination behavior.

## Test Tags And Commands

Multi-node tests are tagged so CI can split slow integration coverage from fast
unit tests:

- `:integration` - local distributed-node tests;
- `:bedrock` - tests that use a Bedrock-backed store;
- `:multi_node` - tests that start peer Erlang nodes;
- `:bedrock_cluster` - Bedrock cluster setup helper tests;
- `:bedrock_multi_node` - Bedrock cross-node scenario matrix.

Useful commands:

```sh
mix test
mix test.integration
mix test.bedrock
mix test.multi_node
mix test --exclude flaky --only bedrock_multi_node
```

`mix test` runs the complete non-flaky suite. The narrower aliases are useful
for CI shards and focused local debugging.

## Covered Scenarios

The existing multi-node tests cover:

- peer node startup, code loading, cookie configuration, and node connectivity;
- Bedrock cluster descriptor creation and object-storage access from different
  nodes;
- cross-node submit, claim, complete, fail, release, recover, replay, and
  outbox operations;
- claim races where only one node can claim an available intent;
- stale owner rejection after release or replacement;
- claim owner death followed by recovery and survivor claiming;
- shard owner death followed by lease takeover;
- command replay across nodes;
- lifecycle stream replay consistency across nodes;
- durable outbox read, ack, and replay behavior across nodes.

These tests intentionally focus on coordination boundaries: stream
preconditions, claim fences, shard leases, command replay rows, queue listings,
and outbox acknowledgement.

## Adding A Multi-Node Test

Use this pattern for a new scenario:

1. Mark the test `async: false`.
2. Add the `:integration`, `:bedrock`, `:multi_node`, and either
   `:bedrock_cluster` or `:bedrock_multi_node` tags.
3. Start a three-node test Bedrock cluster.
4. Start the cross-node Store V1 helper against that cluster.
5. Issue operations from different peers.
6. Assert both the direct operation result and durable replay/inspection state.
7. Let the helper cleanup run through ExUnit `on_exit` callbacks.

Keep command IDs, intent IDs, claim IDs, owner IDs, and timestamps explicit.
Deterministic data makes cross-node races and recovery assertions much easier
to debug.

## Failure Injection

Current tests use local peer shutdown to model node death. Useful failure
patterns include:

- stop the claim owner, advance `now` beyond `lease_until`, then recover from a
  surviving peer;
- stop the shard owner, advance `now`, then take over the shard lease from
  another peer;
- run concurrent claims from multiple peers and assert exactly one winner;
- replay the same command ID from another peer and assert deterministic replay;
- ack an outbox entry from one peer and assert another peer no longer reads it
  for the same consumer.

When adding failure tests, assert conflicts precisely. For claim races, losing
nodes commonly see `:stream_version` or `:intent_status` conflicts. For stale
owners, expect `:claim_fence`. For stale shard owners, expect `:shard_lease`.

## Debugging Local Multi-Node Runs

Most local failures come from environment setup rather than ledger semantics.
Check:

- distributed Erlang can bind to `127.0.0.1`;
- peer node names are unique for the test run;
- the Erlang cookie matches across peers;
- test code paths include the compiled project and test support modules;
- temporary cluster paths were cleaned before startup;
- Bedrock services became available before the test timeout;
- previous crashed peers were stopped before retrying a focused test.

The harness uses long node names and temporary directories by default. If a
focused run fails early, rerun with the specific test file first, then run the
tagged alias to catch ordering or cleanup assumptions.

## CI Guidance

Multi-node tests are integration tests. Run them separately from the fastest
unit shard when CI latency matters:

- fast shard: `mix test --exclude integration --exclude bedrock --exclude multi_node`;
- Bedrock adapter shard: `mix test.bedrock`;
- multi-node shard: `mix test.multi_node`;
- scenario shard: `mix test --exclude flaky --only bedrock_multi_node`.

Keep all shards on the same Elixir/OTP and dependency lock to avoid testing
different protocol or serialization behavior in different jobs.
