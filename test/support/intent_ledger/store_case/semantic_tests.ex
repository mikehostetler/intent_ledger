defmodule IntentLedger.StoreCase.SemanticTests do
  @moduledoc """
  Shared Store V1 conformance tests for replay, fencing, listings, and outbox behavior.
  """

  defmacro __using__(_opts) do
    quote do
      alias IntentLedger.Store.{Commit, CommitRequest, Conflict, Listing, Outbox, Precondition, Write}

      describe "semantic store conformance" do
        test "handles command idempotency replay and command conflicts", context do
          result = %{intent_id: "int_idempotent", status: :submitted}

          request =
            CommitRequest.new(
              command_id: "cmd_idempotent",
              operation: :submit,
              command: %{key: "job:idempotent"},
              preconditions: [Precondition.command_absent("cmd_idempotent")],
              writes: [Write.put_idempotency("cmd_idempotent", result)]
            )

          assert {:ok, %Commit{} = commit} = commit(context, request)
          assert commit.result == result
          refute commit.replayed

          replay =
            CommitRequest.new(
              command_id: "cmd_idempotent",
              operation: :submit,
              command: %{key: "job:idempotent"},
              preconditions: [Precondition.command_replay("cmd_idempotent")]
            )

          assert {:ok, %Commit{} = replayed} = commit(context, replay)
          assert replayed.replayed
          assert replayed.replay_of == "cmd_idempotent"
          assert replayed.result == result

          conflicting_replay =
            CommitRequest.new(
              command_id: "cmd_idempotent",
              operation: :complete,
              command: %{claim_id: "clm_idempotent"},
              preconditions: [Precondition.command_replay("cmd_idempotent")]
            )

          assert {:error, %Conflict{} = conflict} = commit(context, conflicting_replay)
          assert conflict.type == :command_conflict
        end

        test "rejects stale claim fences after token mismatch and release", context do
          now = ~U[2026-01-01 00:00:00Z]
          lease_until = ~U[2026-01-01 00:01:00Z]

          seed =
            CommitRequest.new(
              command_id: "cmd_claim_seed",
              operation: :submit,
              writes: [
                Write.new(:put_state,
                  key: "int_claim",
                  value: %{
                    intent_id: "int_claim",
                    queue: "default",
                    shard: 0,
                    status: :available,
                    visible_at: now,
                    priority: 0
                  }
                )
              ]
            )

          assert {:ok, %Commit{}} = commit(context, seed)

          claim =
            CommitRequest.new(
              command_id: "cmd_claim",
              operation: :claim,
              preconditions: [Precondition.intent_status("int_claim", [:available])],
              writes: [
                Write.new(:put_state,
                  key: "int_claim",
                  value: %{
                    intent_id: "int_claim",
                    queue: "default",
                    shard: 0,
                    status: :claimed,
                    claim_id: "clm_1",
                    claim_token_hash: "hash_1",
                    lease_until: lease_until
                  }
                ),
                Write.put_claim("clm_1", %{
                  intent_id: "int_claim",
                  owner_id: "worker-1",
                  token_hash: "hash_1",
                  lease_until: lease_until
                })
              ]
            )

          assert {:ok, %Commit{}} = commit(context, claim)

          stale =
            CommitRequest.new(
              command_id: "cmd_claim_stale",
              operation: :heartbeat,
              preconditions: [Precondition.claim_fence("clm_1", "bad_hash", metadata: %{now: now})],
              writes: [Write.put_claim("clm_1", %{token_hash: "bad_hash"})]
            )

          assert {:error, %Conflict{type: :claim_fence}} = commit(context, stale)

          release =
            CommitRequest.new(
              command_id: "cmd_claim_release",
              operation: :release,
              preconditions: [Precondition.claim_fence("clm_1", "hash_1", metadata: %{now: now})],
              writes: [Write.delete_claim("clm_1")]
            )

          assert {:ok, %Commit{}} = commit(context, release)

          after_release =
            CommitRequest.new(
              command_id: "cmd_claim_after_release",
              operation: :complete,
              preconditions: [Precondition.claim_fence("clm_1", "hash_1", metadata: %{now: now})],
              writes: [Write.append_signal("intent:int_claim", %{id: "sig_complete"})]
            )

          assert {:error, %Conflict{type: :claim_fence}} = commit(context, after_release)
        end

        test "fences shard lease acquire renew release expiry and takeover", context do
          now = ~U[2026-01-01 00:00:00Z]
          lease_until = ~U[2026-01-01 00:00:30Z]
          renewed_until = ~U[2026-01-01 00:01:00Z]
          expired_at = ~U[2026-01-01 00:01:01Z]

          acquire =
            CommitRequest.new(
              command_id: "cmd_shard_acquire",
              operation: :shard_acquire,
              preconditions: [Precondition.shard_available(:default, 0, now)],
              writes: [Write.put_shard_lease(:default, 0, %{owner_id: "node-a", lease_until: lease_until})]
            )

          assert {:ok, %Commit{}} = commit(context, acquire)

          duplicate_acquire =
            CommitRequest.new(
              command_id: "cmd_shard_duplicate",
              operation: :shard_acquire,
              preconditions: [Precondition.shard_available(:default, 0, now)],
              writes: [Write.put_shard_lease(:default, 0, %{owner_id: "node-b", lease_until: lease_until})]
            )

          assert {:error, %Conflict{type: :shard_lease}} = commit(context, duplicate_acquire)

          renew =
            CommitRequest.new(
              command_id: "cmd_shard_renew",
              operation: :shard_renew,
              preconditions: [Precondition.shard_lease(:default, 0, "node-a", metadata: %{now: now})],
              writes: [Write.put_shard_lease(:default, 0, %{owner_id: "node-a", lease_until: renewed_until})]
            )

          assert {:ok, %Commit{}} = commit(context, renew)

          early_takeover =
            CommitRequest.new(
              command_id: "cmd_shard_early_takeover",
              operation: :shard_takeover,
              preconditions: [Precondition.shard_expired(:default, 0, now)],
              writes: [Write.put_shard_lease(:default, 0, %{owner_id: "node-b", lease_until: renewed_until})]
            )

          assert {:error, %Conflict{type: :shard_lease}} = commit(context, early_takeover)

          takeover =
            CommitRequest.new(
              command_id: "cmd_shard_takeover",
              operation: :shard_takeover,
              preconditions: [Precondition.shard_expired(:default, 0, expired_at)],
              writes: [Write.put_shard_lease(:default, 0, %{owner_id: "node-b", lease_until: ~U[2026-01-01 00:02:00Z]})]
            )

          assert {:ok, %Commit{}} = commit(context, takeover)
        end

        test "lists due intents and expired claims with deterministic ordering", context do
          now = ~U[2026-01-01 00:00:00Z]
          future = ~U[2026-01-01 00:01:00Z]
          expired = ~U[2025-12-31 23:59:59Z]

          seed =
            CommitRequest.new(
              command_id: "cmd_listing_seed",
              operation: :seed,
              writes: [
                Write.new(:put_state,
                  key: "int_low",
                  value: %{
                    intent_id: "int_low",
                    queue: "default",
                    shard: 0,
                    status: :available,
                    visible_at: now,
                    priority: 1
                  }
                ),
                Write.new(:put_state,
                  key: "int_high",
                  value: %{
                    intent_id: "int_high",
                    queue: "default",
                    shard: 0,
                    status: :available,
                    visible_at: now,
                    priority: 10
                  }
                ),
                Write.new(:put_state,
                  key: "int_future",
                  value: %{
                    intent_id: "int_future",
                    queue: "default",
                    shard: 0,
                    status: :available,
                    visible_at: future,
                    priority: 20
                  }
                ),
                Write.new(:put_state,
                  key: "int_other_shard",
                  value: %{
                    intent_id: "int_other_shard",
                    queue: "default",
                    shard: 1,
                    status: :available,
                    visible_at: now,
                    priority: 30
                  }
                ),
                Write.new(:put_state,
                  key: "int_expired",
                  value: %{intent_id: "int_expired", queue: "default", shard: 0, status: :claimed, lease_until: expired}
                )
              ]
            )

          assert {:ok, %Commit{}} = commit(context, seed)

          assert {:ok, due} = listing(context, Listing.due_intents(:default, 0, now))
          assert Enum.map(due, & &1.intent_id) == ["int_high", "int_low"]

          assert {:ok, expired_claims} = listing(context, Listing.expired_claims(:default, nil, now))
          assert Enum.map(expired_claims, & &1.intent_id) == ["int_expired"]
        end

        test "reads acks and replays durable outbox entries", context do
          now = ~U[2026-01-01 00:00:00Z]
          signal = %{id: "sig_outbox", type: "intent_ledger.intent.completed"}

          seed =
            CommitRequest.new(
              command_id: "cmd_outbox_seed",
              operation: :complete,
              writes: [Write.put_outbox("out_1", %{stream: "intent:int_outbox", signal: signal, inserted_at: now})]
            )

          assert {:ok, %Commit{}} = commit(context, seed)

          assert {:ok, [entry]} = outbox(context, Outbox.read("dispatcher"))
          assert entry.key == "out_1"
          assert entry.sequence == 1
          assert entry.signal == signal

          assert {:ok, acked} = outbox(context, Outbox.ack("out_1", "dispatcher", metadata: %{acked_at: now}))
          assert acked.key == "out_1"
          assert acked.consumer == "dispatcher"

          assert {:ok, []} = outbox(context, Outbox.read("dispatcher"))

          assert {:ok, [replayed]} = outbox(context, Outbox.replay(cursor: 0, limit: 10))
          assert replayed.key == "out_1"
          assert replayed.acked_at == now

          assert {:error, %Conflict{type: :outbox}} =
                   outbox(context, Outbox.ack("out_1", "dispatcher", metadata: %{acked_at: now}))
        end
      end
    end
  end
end
