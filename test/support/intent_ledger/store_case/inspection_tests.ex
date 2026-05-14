defmodule IntentLedger.StoreCase.InspectionTests do
  @moduledoc """
  Shared Store V1 inspection tests for Memory and Bedrock adapters.
  """

  defmacro __using__(_opts) do
    quote do
      alias IntentLedger.{Inspection, Intent, IntentState}
      alias IntentLedger.Store.{Commit, CommitRequest, Outbox, Write}

      describe "inspection store conformance" do
        test "inspects queues shards claims retries ambiguity outbox and projection lag", context do
          now = ~U[2026-01-01 00:00:00Z]
          expired = ~U[2025-12-31 23:59:59Z]
          lease_until = ~U[2026-01-01 00:01:00Z]
          signal_time = ~U[2025-12-31 23:59:58Z]
          stream = "ledger:inspection"

          seed =
            CommitRequest.new(
              command_id: "cmd_inspection_seed",
              operation: :inspection_seed,
              writes:
                inspection_intent_writes(now) ++
                  [
                    Write.put_claim("clm_inspection", %{
                      intent_id: "int_claimed",
                      owner_id: "worker-1",
                      token_hash: "secret-token-hash",
                      lease_until: expired
                    }),
                    Write.put_shard_lease(:default, 0, %{
                      queue: "default",
                      shard: 0,
                      owner_id: "node-a",
                      lease_until: lease_until
                    }),
                    Write.put_outbox("out_1", %{
                      key: "out_1",
                      sequence: 1,
                      stream: "intent:int_available",
                      signal: inspection_signal("sig_out_1", signal_time)
                    }),
                    Write.put_outbox("out_2", %{
                      key: "out_2",
                      sequence: 2,
                      stream: "intent:int_retry",
                      signal: inspection_signal("sig_out_2", signal_time)
                    }),
                    Write.append_signal(stream, inspection_signal("sig_projection_1", now), metadata: %{version: 1}),
                    Write.append_signal(stream, inspection_signal("sig_projection_2", now), metadata: %{version: 2})
                  ]
            )

          assert {:ok, %Commit{}} = commit(context, seed)
          assert {:ok, _acked} = outbox(context, Outbox.ack("out_1", "dispatcher", metadata: %{acked_at: now}))

          assert {:ok, [queue]} =
                   read(context, Inspection.queues(queue_config: %{"default" => %{shards: 2}}, at: now))

          assert queue.queue == "default"
          assert queue.shards == 2
          assert queue.depth == 2
          assert queue.available == 1
          assert queue.retry_scheduled == 1
          assert queue.claimed == 1
          assert queue.expired_claims == 1
          assert queue.ambiguous == 1
          assert queue.total_open == 4

          assert {:ok, shards} =
                   read(context, Inspection.shards(queue_config: %{"default" => %{shards: 2}}, at: now))

          shard_0 = Enum.find(shards, &(&1.queue == "default" and &1.shard == 0))
          shard_1 = Enum.find(shards, &(&1.queue == "default" and &1.shard == 1))
          assert shard_0.status == :owned
          assert shard_0.owner_id == "node-a"
          assert shard_0.depth == 1
          assert shard_0.expired_claims == 1
          assert shard_1.status == :unowned
          assert shard_1.depth == 1

          assert {:ok, [claim]} = read(context, Inspection.claims(at: now))
          assert claim.claim_id == "clm_inspection"
          assert claim.intent_id == "int_claimed"
          assert claim.owner_id == "worker-1"
          assert claim.expired? == true
          refute Map.has_key?(claim, :token_hash)
          refute Map.has_key?(claim, :token)

          assert {:ok, [retry]} = read(context, Inspection.retries(at: now))
          assert retry.intent_id == "int_retry"
          assert retry.due? == true
          assert retry.retry_at == now

          assert {:ok, [ambiguous]} = read(context, Inspection.ambiguous(at: now))
          assert ambiguous.intent_id == "int_ambiguous"
          assert ambiguous.error_class == :manual_review

          assert {:ok, outbox_lag} = read(context, Inspection.outbox_lag(consumer: "dispatcher", at: now))
          assert outbox_lag.cursor == 1
          assert outbox_lag.max_sequence == 2
          assert outbox_lag.lag == 1
          assert outbox_lag.unacked == 1
          assert outbox_lag.oldest_unacked_sequence == 2
          assert outbox_lag.oldest_unacked_age_ms >= 2_000

          assert {:ok, projection_lag} =
                   read(context, Inspection.projection_lag(:inspection_projection, stream: stream, cursor: 1))

          assert projection_lag.projection == "inspection_projection"
          assert projection_lag.stream == stream
          assert projection_lag.cursor == 1
          assert projection_lag.stream_version == 2
          assert projection_lag.lag == 1
        end
      end

      defp inspection_intent_writes(now) do
        [
          {"int_available", "job:available", 0, :available, now, nil, nil},
          {"int_retry", "job:retry", 1, :retry_scheduled, now, nil, nil},
          {"int_claimed", "job:claimed", 0, :claimed, now, "clm_inspection", ~U[2025-12-31 23:59:59Z]},
          {"int_ambiguous", "job:ambiguous", 0, :ambiguous, now, nil, nil}
        ]
        |> Enum.flat_map(fn {intent_id, key, shard, status, visible_at, claim_id, lease_until} ->
          intent = inspection_intent(intent_id, key, shard, visible_at)
          state = inspection_state(intent_id, shard, status, visible_at, claim_id, lease_until, now)

          [
            Write.new(:put_intent, key: intent_id, value: intent),
            Write.new(:put_state, key: intent_id, value: state)
          ]
        end)
      end

      defp inspection_intent(intent_id, key, shard, visible_at) do
        {:ok, intent} =
          Intent.new(%{
            id: intent_id,
            key: key,
            kind: "job.run",
            queue: "default",
            shard: shard,
            visible_at: visible_at
          })

        intent
      end

      defp inspection_state(intent_id, shard, status, visible_at, claim_id, lease_until, now) do
        %IntentState{
          intent_id: intent_id,
          queue: "default",
          shard: shard,
          status: status,
          visible_at: visible_at,
          priority: 1,
          attempt: if(status == :available, do: 0, else: 1),
          max_attempts: 3,
          claim_id: claim_id,
          claim_token_hash: if(claim_id, do: "secret-token-hash"),
          lease_until: lease_until,
          updated_at: now,
          error: if(status == :ambiguous, do: :manual_review)
        }
      end

      defp inspection_signal(id, time) do
        %{
          id: id,
          type: "intent_ledger.intent.available",
          subject: "inspection",
          time: time
        }
      end
    end
  end
end
