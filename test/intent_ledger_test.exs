defmodule IntentLedgerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias IntentLedger.{Claim, Claimed, Error, Intent, IntentState, Names, Record, ShardState, Store, Time}
  alias IntentLedger.Store.Outbox

  @pre_release_namespace Enum.join(["Jido", "IntentLedger"], ".")
  @public_namespace_files [
    "README.md",
    "CHANGELOG.md",
    "CONTRIBUTING.md",
    "usage-rules.md",
    "mix.exs"
  ]

  defmodule TestLifecycle do
    @behaviour IntentLedger.Lifecycle

    @impl true
    def before_submit(intent, _context) do
      {:ok, %{intent | metadata: Map.put(intent.metadata, :hooked, true)}}
    end

    @impl true
    def after_transition(signal, _context) do
      if pid = Process.whereis(:intent_ledger_lifecycle_test) do
        send(pid, {:lifecycle_signal, signal.type})
      end

      :ok
    end
  end

  defmodule FailingAfterTransitionLifecycle do
    @behaviour IntentLedger.Lifecycle

    @impl true
    def after_transition(signal, _context) do
      if pid = Process.whereis(:intent_ledger_failing_lifecycle_test) do
        send(pid, {:failing_lifecycle_signal, signal.type})
      end

      {:error, :boom}
    end
  end

  setup do
    name = Module.concat(__MODULE__, "Ledger#{System.unique_integer([:positive])}")

    start_supervised!(
      {IntentLedger, name: name, queues: [default: [shards: 2]], lease_ms: 1_000, store: IntentLedger.Store.Memory}
    )

    %{ledger: name}
  end

  test "submits an intent, assigns a shard, and records history", %{ledger: ledger} do
    {:ok, %Record{} = record} =
      IntentLedger.submit(ledger, %{
        key: "invoice:1",
        kind: "invoice.send",
        payload: %{invoice_id: 1},
        idempotency_key: "invoice:1:send"
      })

    assert record.state.status == :available
    assert record.intent.queue == "default"
    assert record.intent.shard in [0, 1]

    {:ok, fetched} = IntentLedger.get(ledger, record.intent.id)
    assert fetched.intent.id == record.intent.id
    intent_id = record.intent.id

    assert {:error, {:idempotency_conflict, ^intent_id}} =
             IntentLedger.submit(ledger, %{
               key: "invoice:1",
               kind: "invoice.send",
               idempotency_key: "invoice:1:send"
             })

    {:ok, history} = IntentLedger.history(ledger, record.intent.id)

    assert Enum.map(history, & &1.type) == [
             "intent_ledger.intent.submitted",
             "intent_ledger.intent.available"
           ]
  end

  test "writes lifecycle signals to durable outbox entries", %{ledger: ledger} do
    {:ok, record} =
      IntentLedger.submit(ledger, %{
        key: "outbox:1",
        kind: "outbox.test"
      })

    {:ok, claimed} = IntentLedger.claim(ledger, :default, "outbox-worker")
    assert claimed.intent.id == record.intent.id

    assert {:ok, _completed} =
             IntentLedger.complete(ledger, claimed.claim.id, claimed.claim.token, %{ok: true})

    {:ok, history} = IntentLedger.history(ledger, record.intent.id)

    assert {:ok, entries} =
             IntentLedger.Store.Memory.outbox(
               Names.store(ledger),
               ledger,
               Outbox.read("dispatcher", cursor: 0, limit: 10),
               []
             )

    assert Enum.map(entries, & &1.signal.id) == Enum.map(history, & &1.id)
    assert Enum.all?(entries, &(&1.stream == "intent:" <> record.intent.id))
    assert Enum.all?(entries, &is_integer(&1.sequence))
  end

  test "replays intent queue ledger and outbox windows", %{ledger: ledger} do
    {:ok, record} =
      IntentLedger.submit(ledger, %{
        key: "replay:1",
        kind: "replay.test",
        shard: 0
      })

    {:ok, claimed} = IntentLedger.claim(ledger, :default, "replay-worker")

    assert {:ok, _completed} =
             IntentLedger.complete(ledger, claimed.claim.id, claimed.claim.token, %{ok: true})

    assert {:ok, intent_window} = IntentLedger.replay_intent(ledger, record.intent.id, cursor: 1, limit: 2)
    assert Enum.map(intent_window, & &1.type) == ["intent_ledger.intent.available", "intent_ledger.intent.claimed"]

    assert {:ok, queue_signals} = IntentLedger.replay_queue(ledger, :default, 0, limit: 10)

    assert Enum.map(queue_signals, & &1.type) == [
             "intent_ledger.intent.submitted",
             "intent_ledger.intent.available",
             "intent_ledger.intent.claimed",
             "intent_ledger.intent.completed"
           ]

    assert {:ok, ledger_window} = IntentLedger.replay_ledger(ledger, cursor: 2, limit: 2)
    assert Enum.map(ledger_window, & &1.type) == ["intent_ledger.intent.claimed", "intent_ledger.intent.completed"]

    assert {:ok, outbox_entries} = IntentLedger.replay_outbox(ledger, cursor: 0, limit: 2)

    assert Enum.map(outbox_entries, & &1.signal.type) == [
             "intent_ledger.intent.submitted",
             "intent_ledger.intent.available"
           ]
  end

  test "claims and completes an intent with token protection", %{ledger: ledger} do
    {:ok, record} =
      IntentLedger.submit(ledger, %{
        key: "job:1",
        kind: "job.run",
        payload: %{work: true}
      })

    {:ok, %Claimed{} = claimed} = IntentLedger.claim(ledger, :default, "worker-1")
    assert claimed.intent.id == record.intent.id
    assert claimed.state.status == :claimed
    assert claimed.claim.token =~ "tok_"

    assert {:error, :stale_claim} =
             IntentLedger.complete(ledger, claimed.claim.id, "bad-token", :ok)

    {:ok, completed} =
      IntentLedger.complete(ledger, claimed.claim.id, claimed.claim.token, %{ok: true})

    assert completed.state.status == :completed
    assert completed.state.result == %{ok: true}
    assert :empty = IntentLedger.claim(ledger, :default, "worker-2")

    {:ok, history} = IntentLedger.history(ledger, record.intent.id)

    assert Enum.map(history, & &1.type) == [
             "intent_ledger.intent.submitted",
             "intent_ledger.intent.available",
             "intent_ledger.intent.claimed",
             "intent_ledger.intent.completed"
           ]
  end

  test "failure schedules retry until attempts are exhausted", %{ledger: ledger} do
    now = ~U[2026-01-01 00:00:00Z]
    retry_at = Time.add_ms(now, 100)

    {:ok, record} =
      IntentLedger.submit(
        ledger,
        %{
          key: "job:retry",
          kind: "job.run",
          max_attempts: 2
        },
        now: now
      )

    {:ok, claimed} = IntentLedger.claim(ledger, :default, "worker-1", now: now, lease_ms: 1_000)

    {:ok, retry_record} =
      IntentLedger.fail(
        ledger,
        claimed.claim.id,
        claimed.claim.token,
        %{reason: :temporary},
        now: now,
        retry_at: retry_at
      )

    assert retry_record.intent.id == record.intent.id
    assert retry_record.state.status == :retry_scheduled
    assert :empty = IntentLedger.claim(ledger, :default, "worker-2", now: now)

    {:ok, claimed_again} =
      IntentLedger.claim(ledger, :default, "worker-2", now: retry_at, lease_ms: 1_000)

    assert claimed_again.claim.attempt == 2

    {:ok, failed_record} =
      IntentLedger.fail(
        ledger,
        claimed_again.claim.id,
        claimed_again.claim.token,
        %{reason: :permanent},
        now: retry_at
      )

    assert failed_record.state.status == :failed
  end

  test "recovers expired claims back to the queue", %{ledger: ledger} do
    now = ~U[2026-01-01 00:00:00Z]
    expired_at = Time.add_ms(now, 2)

    {:ok, record} =
      IntentLedger.submit(
        ledger,
        %{
          key: "job:expired",
          kind: "job.run",
          max_attempts: 3
        },
        now: now
      )

    {:ok, claimed} = IntentLedger.claim(ledger, :default, "worker-1", now: now, lease_ms: 1)

    {:ok, [recovered]} = IntentLedger.recover(ledger, :default, now: expired_at)
    assert recovered.intent.id == record.intent.id
    assert recovered.state.status == :available

    {:ok, claimed_again} =
      IntentLedger.claim(ledger, :default, "worker-2", now: expired_at, lease_ms: 1_000)

    assert claimed_again.intent.id == claimed.intent.id
    assert claimed_again.claim.attempt == 2
  end

  test "submits batches and claims multiple intents", %{ledger: ledger} do
    {:ok, records} =
      IntentLedger.submit_many(
        ledger,
        [
          %{key: "batch:1", kind: "job.run", priority: 1},
          %{key: "batch:2", kind: "job.run", priority: 2}
        ],
        correlation_id: "corr_batch",
        root_intent_id: "int_root",
        depth: 1,
        actor: "batcher"
      )

    assert length(records) == 2
    assert Enum.all?(records, &(&1.intent.correlation_id == "corr_batch"))
    assert Enum.all?(records, &(&1.intent.root_intent_id == "int_root"))
    assert Enum.all?(records, &(&1.intent.depth == 1))
    assert Enum.all?(records, &(&1.intent.actor == "batcher"))

    {:ok, claimed} = IntentLedger.claim(ledger, :default, "worker-1", limit: 2)
    assert Enum.map(claimed, & &1.intent.key) == ["batch:2", "batch:1"]
  end

  test "heartbeats and releases claims", %{ledger: ledger} do
    now = ~U[2026-01-01 00:00:00Z]
    later = Time.add_ms(now, 5)

    {:ok, _record} =
      IntentLedger.submit(ledger, %{key: "job:heartbeat", kind: "job.run"}, now: now)

    {:ok, claimed} = IntentLedger.claim(ledger, :default, "worker-1", now: now, lease_ms: 10)

    {:ok, heartbeat} =
      IntentLedger.heartbeat(ledger, claimed.claim.id, claimed.claim.token, now: later, lease_ms: 50)

    assert DateTime.compare(heartbeat.lease_until, claimed.claim.lease_until) == :gt

    {:ok, released} = IntentLedger.release(ledger, claimed.claim.id, claimed.claim.token, now: later)
    assert released.state.status == :available

    {:ok, claimed_again} = IntentLedger.claim(ledger, :default, "worker-2", now: later)
    assert claimed_again.claim.attempt == 2
  end

  test "cancels, requeues, and marks intents ambiguous", %{ledger: ledger} do
    {:ok, cancelled} =
      IntentLedger.submit(ledger, %{key: "job:cancel", kind: "job.run"})
      |> then(fn {:ok, record} -> IntentLedger.cancel(ledger, record.intent.id, :no_longer_needed) end)

    assert cancelled.state.status == :cancelled
    assert {:error, {:final_state, :cancelled}} = IntentLedger.requeue(ledger, cancelled.intent.id)

    {:ok, requeued_source} = IntentLedger.submit(ledger, %{key: "job:requeue", kind: "job.run"})
    retry_at = Time.add_ms(requeued_source.intent.visible_at, 1_000)
    {:ok, requeued} = IntentLedger.requeue(ledger, requeued_source.intent.id, retry_at: retry_at)
    assert requeued.state.status == :retry_scheduled

    {:ok, ambiguous_source} = IntentLedger.submit(ledger, %{key: "job:ambiguous", kind: "job.run"})
    {:ok, ambiguous} = IntentLedger.mark_ambiguous(ledger, ambiguous_source.intent.id, :manual_review)
    assert ambiguous.state.status == :ambiguous
  end

  test "moves exhausted manual intents to ambiguous", %{ledger: ledger} do
    {:ok, _record} =
      IntentLedger.submit(ledger, %{
        key: "job:manual",
        kind: "job.run",
        max_attempts: 1,
        ambiguity_policy: :manual
      })

    {:ok, claimed} = IntentLedger.claim(ledger, :default, "worker-1")
    {:ok, ambiguous} = IntentLedger.fail(ledger, claimed.claim.id, claimed.claim.token, :boom)

    assert ambiguous.state.status == :ambiguous
  end

  test "marks exhausted expired claims ambiguous for non-retry policy", %{ledger: ledger} do
    now = ~U[2026-01-01 00:00:00Z]

    {:ok, _record} =
      IntentLedger.submit(
        ledger,
        %{key: "job:expired-manual", kind: "job.run", max_attempts: 1, ambiguity_policy: :manual},
        now: now
      )

    {:ok, _claimed} = IntentLedger.claim(ledger, :default, "worker-1", now: now, lease_ms: 1)
    {:ok, [ambiguous]} = IntentLedger.recover(ledger, :default, now: Time.add_ms(now, 2))

    assert ambiguous.state.status == :ambiguous
  end

  test "validates inputs and time coercion", %{ledger: ledger} do
    assert {:error, {:required, :kind}} = IntentLedger.submit(ledger, %{key: "bad"})

    assert {:error, {:invalid_positive_integer, :max_attempts, 0}} =
             IntentLedger.submit(ledger, %{key: "bad:attempts", kind: "job.run", max_attempts: 0})

    visible_at = "2026-01-01T00:00:00Z"
    {:ok, record} = IntentLedger.submit(ledger, %{key: "job:iso", kind: :run, visible_at: visible_at})

    assert record.intent.kind == "run"
    assert record.intent.visible_at == ~U[2026-01-01 00:00:00Z]

    assert {:error, {:invalid_datetime, "not-a-date", :invalid_format}} =
             IntentLedger.submit(ledger, %{key: "bad:time", kind: "job.run", visible_at: "not-a-date"})
  end

  test "normalizes public API commands before lifecycle commits", %{ledger: ledger} do
    assert {:error, {:invalid_non_negative_integer, :depth, -1}} =
             IntentLedger.submit(ledger, %{key: "job:bad-command", kind: "job.run"}, depth: -1)

    assert :empty = IntentLedger.claim(ledger, :default, "worker-1")

    assert {:ok, record} =
             IntentLedger.submit(
               ledger,
               %{key: "job:lineage", kind: "job.run"},
               command_id: "cmd_lineage",
               correlation_id: "corr_1",
               causation_id: "cmd_parent",
               root_intent_id: "int_root",
               parent_intent_id: "int_parent",
               depth: 2,
               actor: "agent"
             )

    assert record.intent.correlation_id == "corr_1"
    assert record.intent.causation_id == "cmd_parent"
    assert record.intent.root_intent_id == "int_root"
    assert record.intent.parent_intent_id == "int_parent"
    assert record.intent.depth == 2
    assert record.intent.actor == "agent"

    {:ok, history} = IntentLedger.history(ledger, record.intent.id)
    assert Enum.all?(history, &(&1.data.correlation_id == "corr_1"))
    assert Enum.all?(history, &(&1.data.causation_id == "cmd_parent"))
    assert Enum.all?(history, &(&1.data.root_intent_id == "int_root"))
    assert Enum.all?(history, &(&1.data.parent_intent_id == "int_parent"))
    assert Enum.all?(history, &(&1.data.depth == 2))
    assert Enum.all?(history, &(&1.data.actor == "agent"))
  end

  test "rejects intents beyond configured max depth before committing", %{ledger: _ledger} do
    name = Module.concat(__MODULE__, "MaxDepthLedger#{System.unique_integer([:positive])}")

    start_supervised!({IntentLedger, name: name, max_depth: 1, store: IntentLedger.Store.Memory})

    assert {:ok, root} =
             IntentLedger.submit(name, %{
               id: "int_depth_root",
               key: "job:depth-root",
               kind: "job.run",
               depth: 1
             })

    assert root.intent.depth == 1

    assert {:error, {:guardrail_violation, :max_depth, %{depth: 2, max_depth: 1, intent_id: "int_depth_child"}}} =
             IntentLedger.submit(name, %{
               id: "int_depth_child",
               key: "job:depth-child",
               kind: "job.run",
               root_intent_id: root.intent.root_intent_id,
               parent_intent_id: root.intent.id,
               depth: 2
             })

    assert {:error, :not_found} = IntentLedger.get(name, "int_depth_child")
    assert {:error, :not_found} = IntentLedger.history(name, "int_depth_child")
  end

  test "replays duplicate command ids without duplicate lifecycle commits", %{ledger: ledger} do
    {:ok, first} =
      IntentLedger.submit(ledger, %{key: "job:replay", kind: "job.run"}, command_id: "cmd_submit")

    {:ok, replayed} =
      IntentLedger.submit(ledger, %{key: "job:replay-other", kind: "job.run"}, command_id: "cmd_submit")

    assert replayed.intent.id == first.intent.id
    assert replayed.intent.key == "job:replay"

    {:ok, history} = IntentLedger.history(ledger, first.intent.id)
    assert Enum.map(history, & &1.type) == ["intent_ledger.intent.submitted", "intent_ledger.intent.available"]

    {:ok, claimed} = IntentLedger.claim(ledger, :default, "worker-1")
    assert claimed.intent.id == first.intent.id
    assert :empty = IntentLedger.claim(ledger, :default, "worker-2")
  end

  test "command signal entrypoint uses the same replay path as public APIs", %{ledger: ledger} do
    signal =
      IntentLedger.Command.submit(MyApp.IntentLedger, %{key: "job:signal", kind: "job.run"},
        command_id: "cmd_signal",
        correlation_id: "corr_signal",
        causation_id: "cmd_parent",
        root_intent_id: "int_root",
        parent_intent_id: "int_parent",
        depth: 2,
        actor: "agent"
      )

    {:ok, via_signal} = IntentLedger.command(ledger, signal)

    assert via_signal.intent.correlation_id == "corr_signal"
    assert via_signal.intent.causation_id == "cmd_parent"
    assert via_signal.intent.root_intent_id == "int_root"
    assert via_signal.intent.parent_intent_id == "int_parent"
    assert via_signal.intent.depth == 2
    assert via_signal.intent.actor == "agent"

    {:ok, replayed} =
      IntentLedger.submit(ledger, %{key: "job:signal-other", kind: "job.run"}, command_id: "cmd_signal")

    assert replayed == via_signal

    {:ok, history} = IntentLedger.history(ledger, via_signal.intent.id)
    assert Enum.map(history, & &1.type) == ["intent_ledger.intent.submitted", "intent_ledger.intent.available"]
  end

  test "replays duplicate completion command ids after the first commit", %{ledger: ledger} do
    {:ok, record} = IntentLedger.submit(ledger, %{key: "job:complete-replay", kind: "job.run"})
    {:ok, claimed} = IntentLedger.claim(ledger, :default, "worker-1")

    {:ok, completed} =
      IntentLedger.complete(ledger, claimed.claim.id, claimed.claim.token, %{ok: true}, command_id: "cmd_complete")

    {:ok, replayed} =
      IntentLedger.complete(ledger, claimed.claim.id, claimed.claim.token, %{ok: true}, command_id: "cmd_complete")

    assert replayed == completed

    {:ok, history} = IntentLedger.history(ledger, record.intent.id)

    assert Enum.map(history, & &1.type) == [
             "intent_ledger.intent.submitted",
             "intent_ledger.intent.available",
             "intent_ledger.intent.claimed",
             "intent_ledger.intent.completed"
           ]
  end

  test "runs lifecycle hooks", %{ledger: _ledger} do
    Process.register(self(), :intent_ledger_lifecycle_test)

    name = Module.concat(__MODULE__, "HookedLedger#{System.unique_integer([:positive])}")

    start_supervised!({IntentLedger, name: name, lifecycle: TestLifecycle, store: IntentLedger.Store.Memory})

    {:ok, record} = IntentLedger.submit(name, %{key: "job:hooked", kind: "job.run"})

    assert record.intent.metadata.hooked
    assert_receive {:lifecycle_signal, "intent_ledger.intent.submitted"}
  after
    if Process.whereis(:intent_ledger_lifecycle_test) == self() do
      Process.unregister(:intent_ledger_lifecycle_test)
    end
  end

  test "keeps after_transition as best-effort compatibility observation", %{ledger: _ledger} do
    Process.register(self(), :intent_ledger_failing_lifecycle_test)

    name = Module.concat(__MODULE__, "FailingHookLedger#{System.unique_integer([:positive])}")

    start_supervised!(
      {IntentLedger, name: name, lifecycle: FailingAfterTransitionLifecycle, store: IntentLedger.Store.Memory}
    )

    parent = self()

    log =
      capture_log(fn ->
        assert {:ok, record} = IntentLedger.submit(name, %{key: "job:failing-hook", kind: "job.run"})
        send(parent, {:submitted_with_failing_hook, record})
      end)

    assert_receive {:submitted_with_failing_hook, record}
    assert_receive {:failing_lifecycle_signal, "intent_ledger.intent.submitted"}
    assert log =~ "intent ledger lifecycle hook failed"

    assert {:ok, history} = IntentLedger.history(name, record.intent.id)
    assert Enum.map(history, & &1.type) == ["intent_ledger.intent.submitted", "intent_ledger.intent.available"]

    assert {:ok, outbox_entries} =
             IntentLedger.Store.Memory.outbox(Names.store(name), name, Outbox.replay(cursor: 0, limit: 10), [])

    assert Enum.map(outbox_entries, & &1.signal.type) == [
             "intent_ledger.intent.submitted",
             "intent_ledger.intent.available"
           ]
  after
    if Process.whereis(:intent_ledger_failing_lifecycle_test) == self() do
      Process.unregister(:intent_ledger_failing_lifecycle_test)
    end
  end

  test "exposes schemas and error helpers" do
    assert %IntentLedger.Claim{} = struct!(IntentLedger.Claim, [])
    assert %IntentLedger.Claimed{} = struct!(IntentLedger.Claimed, [])
    assert %IntentLedger.Record{} = struct!(IntentLedger.Record, [])
    assert %IntentLedger.ShardState{} = struct!(IntentLedger.ShardState, [])
    assert %Zoi.Types.Struct{} = Claim.schema()
    assert %Zoi.Types.Struct{} = Claimed.schema()
    assert %Zoi.Types.Struct{} = Record.schema()
    assert %Zoi.Types.Struct{} = ShardState.schema()

    assert {:ok, intent} =
             Intent.new(%{
               id: "int_test",
               key: "schema:key",
               kind: "schema.kind",
               visible_at: ~U[2026-01-01 00:00:00Z]
             })

    assert {:ok, %Intent{}} = Zoi.parse(Intent.schema(), intent)

    assert {:ok, %IntentState{}} = Zoi.parse(IntentState.schema(), %IntentState{})
    assert Exception.message(Error.invalid("bad input", field: :key, value: nil)) == "bad input"
    assert Exception.message(Error.runtime("store failed", details: :boom)) == "store failed"
    assert Exception.message(Error.invalid("raw", :details)) == "raw"
    assert %Error.ConflictError{reason: :idempotency_conflict} = Error.conflict(:idempotency_conflict)
    assert %Error.StaleOwnerError{} = Error.stale_owner(claim_id: "clm_1")
    assert %Error.ExpiredLeaseError{} = Error.expired_lease(claim_id: "clm_1")
    assert %Error.FinalStateError{state: :cancelled} = Error.final_state(:cancelled)

    assert %Error.AdapterRuntimeError{adapter: IntentLedger.Store.Memory} =
             Error.adapter_runtime("store failed", adapter: IntentLedger.Store.Memory)

    assert %Error.ConflictError{details: %{intent_id: "int_1"}} =
             Error.from_reason({:idempotency_conflict, "int_1"})

    assert %Error.StaleOwnerError{} = Error.from_reason(:stale_claim)
    assert %Error.ExpiredLeaseError{} = Error.from_reason(:lease_expired)
    assert %Error.FinalStateError{state: :failed} = Error.from_reason({:final_state, :failed})
    assert {IntentLedger.Store.Memory, []} = Store.normalize_spec(nil)
    assert {IntentLedger.Store.Memory, []} = Store.normalize_spec(IntentLedger.Store.Memory)

    assert {IntentLedger.Store.Memory, [foo: :bar]} =
             Store.normalize_spec({IntentLedger.Store.Memory, foo: :bar})

    assert {:ok, %DateTime{}} = Time.normalize(nil, nil)
    assert {:error, {:invalid_datetime, :bad}} = Time.normalize(:bad, nil)
  end

  test "starts and stops manual instances" do
    name = Module.concat(__MODULE__, "ManualLedger#{System.unique_integer([:positive])}")

    assert {:ok, _pid} = IntentLedger.Instance.start_link(name: name)
    assert IntentLedger.Instance.running?(name)
    assert :ok = IntentLedger.Instance.stop(name)
    refute IntentLedger.Instance.running?(name)
    refute IntentLedger.Instance.running?(Module.concat(__MODULE__, :MissingLedger))
    assert :ok = IntentLedger.Instance.stop(Module.concat(__MODULE__, :MissingLedger))
  end

  test "does not expose pre-release compatibility aliases" do
    refute Code.ensure_loaded?(Module.concat([Jido, IntentLedger]))
    refute Code.ensure_loaded?(Module.concat([Jido, IntentLedger, Application]))
    refute Code.ensure_loaded?(Module.concat([Jido, IntentLedger, Store]))
    refute Code.ensure_loaded?(Module.concat([Jido, IntentLedger, Store, Memory]))
  end

  test "public files do not reference the pre-release namespace" do
    scanned_files =
      @public_namespace_files
      |> Enum.concat(release_source_files("lib"))
      |> Enum.concat(release_source_files("test"))
      |> Enum.concat(release_source_files("guides"))

    offenders =
      for file <- scanned_files,
          String.contains?(File.read!(file), @pre_release_namespace),
          do: file

    assert offenders == []
  end

  defp release_source_files(path) do
    if File.dir?(path) do
      path
      |> Path.join("**/*")
      |> Path.wildcard()
      |> Enum.filter(&File.regular?/1)
    else
      []
    end
  end
end
