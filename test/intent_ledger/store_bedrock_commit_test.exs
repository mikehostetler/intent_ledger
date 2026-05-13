defmodule IntentLedger.StoreBedrockCommitTest do
  use ExUnit.Case, async: true

  alias IntentLedger.{Intent, IntentState, Signal}
  alias IntentLedger.Store.{Bedrock, Commit, CommitRequest, Precondition, Write}
  alias IntentLedger.Store.Bedrock.{Keyspace, Value}

  defmodule FakeRepo do
    def transact(fun, opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      Process.put(:ops, [])
      Process.put(:values, Keyword.get(opts, :values, %{}))
      send(test_pid, {:transact, Keyword.delete(opts, :test_pid)})

      result = fun.(__MODULE__)
      send(test_pid, {:ops, Enum.reverse(Process.get(:ops, []))})

      result
    after
      Process.delete(:ops)
      Process.delete(:values)
    end

    def put(key, value), do: record({:put, key, value})
    def clear(key), do: record({:clear, key})

    def get(key) do
      record({:get, key})
      Map.get(Process.get(:values, %{}), key)
    end

    def get_range({start_key, end_key} = range) do
      record({:get_range, range})

      Process.get(:values, %{})
      |> Enum.filter(fn {key, _value} -> key >= start_key and key < end_key end)
      |> Enum.sort_by(fn {key, _value} -> key end)
    end

    defp record(op) do
      Process.put(:ops, [op | Process.get(:ops, [])])
      :ok
    end
  end

  @ledger MyApp.IntentLedger
  @now ~U[2026-01-01 00:00:00Z]

  setup do
    name = :"bedrock_store_#{System.unique_integer([:positive])}"
    store = start_supervised!({Bedrock, name: name, repo: FakeRepo, transaction_opts: [test_pid: self()]})

    %{store: store}
  end

  test "compiles basic Store V1 writes inside a repo transaction", %{store: store} do
    {:ok, intent} = Intent.new(%{id: "int_1", key: "job:1", kind: "job.run", visible_at: @now})
    state = IntentState.new(intent, @now)
    signal = Signal.lifecycle(:intent_available, @ledger, intent.id, %{visible_at: @now})
    result = %{intent_id: intent.id}

    request =
      CommitRequest.new(
        command_id: "cmd_1",
        operation: :submit,
        command: %{key: intent.key},
        writes: [
          Write.new(:put_intent, key: intent.id, value: intent),
          Write.new(:put_state, key: intent.id, value: state),
          Write.append_signal("intent:int_1", signal, metadata: %{version: 1}),
          Write.put_idempotency("cmd_1", result)
        ]
      )

    assert {:ok, %Commit{} = commit} = Bedrock.commit(store, @ledger, request, transaction_opts: [retry_limit: 1])
    assert commit.command_id == "cmd_1"
    assert commit.result == result
    assert commit.signals == [signal]

    assert_receive {:transact, [retry_limit: 1]}
    assert_receive {:ops, ops}

    assert [
             {:put, intent_key, intent_value},
             {:put, state_key, state_value},
             {:put, stream_key, signal_value},
             {:put, command_key, command_value}
           ] = ops

    assert intent_key == Keyspace.intent(@ledger, intent.id)
    assert state_key == Keyspace.state(@ledger, intent.id)
    assert stream_key == Keyspace.stream(@ledger, "intent:int_1", 1)
    assert command_key == Keyspace.command(@ledger, "cmd_1")

    assert {:ok, ^intent} = Value.unpack_intent(intent_value)
    assert {:ok, ^state} = Value.unpack_state(state_value)
    assert {:ok, ^signal} = Value.unpack_signal(signal_value)
    assert {:ok, %{signature: {:submit, %{key: "job:1"}}, result: ^result}} = Value.unpack_command(command_value)
  end

  test "compiles claim, shard lease, and outbox write keys", %{store: store} do
    claim = %{intent_id: "int_1", token_hash: "hash", lease_until: @now}
    lease = %{queue: "default", shard: 0, owner_id: "node-a", lease_until: @now}
    outbox = %{sequence: 1, stream: "intent:int_1", signal: %{id: "sig_1"}, acked_at: nil}

    request =
      CommitRequest.new(
        writes: [
          Write.put_claim("clm_1", claim),
          Write.put_shard_lease(:default, 0, lease),
          Write.put_outbox("out_1", outbox),
          Write.delete_claim("clm_1"),
          Write.delete_shard_lease(:default, 0)
        ]
      )

    assert {:ok, %Commit{}} = Bedrock.commit(store, @ledger, request, [])

    assert_receive {:transact, []}
    assert_receive {:ops, ops}

    assert [
             {:put, claim_key, claim_value},
             {:put, lease_key, lease_value},
             {:put, outbox_key, outbox_value},
             {:clear, clear_claim_key},
             {:clear, clear_lease_key}
           ] = ops

    assert claim_key == Keyspace.claim(@ledger, "clm_1")
    assert lease_key == Keyspace.shard_lease(@ledger, :default, 0)
    assert outbox_key == Keyspace.outbox(@ledger, 1)
    assert clear_claim_key == claim_key
    assert clear_lease_key == lease_key

    assert {:ok, ^claim} = Value.unpack_claim(claim_value)
    assert {:ok, ^lease} = Value.unpack_shard_lease(lease_value)
    assert {:ok, %{key: "out_1", sequence: 1}} = Value.unpack_outbox(outbox_value)
  end

  test "checks command_absent before writing command replay records", %{store: store} do
    result = %{intent_id: "int_1"}

    request =
      CommitRequest.new(
        command_id: "cmd_1",
        operation: :submit,
        command: %{key: "job:1"},
        preconditions: [Precondition.command_absent("cmd_1")],
        writes: [Write.put_idempotency("cmd_1", result)]
      )

    assert {:ok, %Commit{result: ^result}} = Bedrock.commit(store, @ledger, request, [])

    command_key = Keyspace.command(@ledger, "cmd_1")

    assert_receive {:transact, []}
    assert_receive {:ops, [{:get, ^command_key}, {:put, ^command_key, encoded}]}
    assert {:ok, %{signature: {:submit, %{key: "job:1"}}, result: ^result}} = Value.unpack_command(encoded)
  end

  test "returns a command conflict when command_absent finds an existing command", %{store: store} do
    command_key = Keyspace.command(@ledger, "cmd_1")
    existing = Value.pack_command(%{signature: {:submit, %{key: "job:1"}}, result: %{intent_id: "int_1"}})

    request =
      CommitRequest.new(
        command_id: "cmd_1",
        preconditions: [Precondition.command_absent("cmd_1")]
      )

    assert {:error, %IntentLedger.Store.Conflict{type: :command_conflict}} =
             Bedrock.commit(store, @ledger, request, transaction_opts: [values: %{command_key => existing}])

    assert_receive {:transact, [values: %{^command_key => ^existing}]}
    assert_receive {:ops, [{:get, ^command_key}]}
  end

  test "replays an existing command without applying writes", %{store: store} do
    command_key = Keyspace.command(@ledger, "cmd_1")
    result = %{intent_id: "int_1"}
    existing = Value.pack_command(%{signature: {:submit, %{key: "job:1"}}, result: result})

    request =
      CommitRequest.new(
        command_id: "cmd_1",
        operation: :submit,
        command: %{key: "job:1"},
        preconditions: [Precondition.command_replay("cmd_1")],
        writes: [Write.put_idempotency("cmd_1", %{intent_id: "other"})]
      )

    assert {:ok, %Commit{result: ^result, replayed: true, replay_of: "cmd_1", writes: []}} =
             Bedrock.commit(store, @ledger, request, transaction_opts: [values: %{command_key => existing}])

    assert_receive {:transact, [values: %{^command_key => ^existing}]}
    assert_receive {:ops, [{:get, ^command_key}]}
  end

  test "rejects command replay when the stored signature differs", %{store: store} do
    command_key = Keyspace.command(@ledger, "cmd_1")
    existing = Value.pack_command(%{signature: {:submit, %{key: "job:1"}}, result: %{intent_id: "int_1"}})

    request =
      CommitRequest.new(
        command_id: "cmd_1",
        operation: :submit,
        command: %{key: "other"},
        preconditions: [Precondition.command_replay("cmd_1")]
      )

    assert {:error, %IntentLedger.Store.Conflict{type: :command_conflict}} =
             Bedrock.commit(store, @ledger, request, transaction_opts: [values: %{command_key => existing}])
  end

  test "checks stream version and derives append versions", %{store: store} do
    signal = Signal.lifecycle(:intent_available, @ledger, "int_1", %{visible_at: @now})

    request =
      CommitRequest.new(
        preconditions: [Precondition.stream_version("intent:int_1", 0)],
        writes: [Write.append_signal("intent:int_1", signal)]
      )

    assert {:ok, %Commit{signals: [^signal]}} = Bedrock.commit(store, @ledger, request, [])

    stream_range = Keyspace.stream_range(@ledger, "intent:int_1")
    stream_key = Keyspace.stream(@ledger, "intent:int_1", 1)

    assert_receive {:transact, []}
    assert_receive {:ops, [{:get_range, ^stream_range}, {:put, ^stream_key, encoded}]}
    assert {:ok, ^signal} = Value.unpack_signal(encoded)
  end

  test "rejects stale stream version preconditions without appending", %{store: store} do
    stream_key_1 = Keyspace.stream(@ledger, "intent:int_1", 1)
    stream_key_2 = Keyspace.stream(@ledger, "intent:int_1", 2)
    signal_1 = Value.pack_signal(%{id: "sig_1"})
    signal_2 = Value.pack_signal(%{id: "sig_2"})

    request =
      CommitRequest.new(
        preconditions: [Precondition.stream_version("intent:int_1", 1)],
        writes: [Write.append_signal("intent:int_1", %{id: "sig_3"})]
      )

    values = %{stream_key_1 => signal_1, stream_key_2 => signal_2}

    assert {:error, %IntentLedger.Store.Conflict{type: :stream_version, expected: 1, actual: 2}} =
             Bedrock.commit(store, @ledger, request, transaction_opts: [values: values])

    stream_range = Keyspace.stream_range(@ledger, "intent:int_1")

    assert_receive {:transact, [values: ^values]}
    assert_receive {:ops, [{:get_range, ^stream_range}]}
  end

  test "reads ordered lifecycle streams", %{store: store} do
    signal_1 = %{id: "sig_1", type: "intent_ledger.intent.submitted"}
    signal_2 = %{id: "sig_2", type: "intent_ledger.intent.available"}

    values = %{
      Keyspace.stream(@ledger, "intent:int_1", 2) => Value.pack_signal(signal_2),
      Keyspace.stream(@ledger, "intent:int_1", 1) => Value.pack_signal(signal_1),
      Keyspace.stream(@ledger, "intent:int_2", 1) => Value.pack_signal(%{id: "other"})
    }

    assert {:ok, %{stream: "intent:int_1", version: 2, signals: [^signal_1, ^signal_2]}} =
             Bedrock.read(store, @ledger, {:stream, "intent:int_1", []}, transaction_opts: [values: values])
  end

  test "rejects preconditions reserved for later semantic compilers", %{store: store} do
    request = CommitRequest.new(preconditions: [Precondition.intent_status("int_1", [:available])])

    assert {:error,
            %IntentLedger.Error.AdapterRuntimeError{
              details: %{reason: :unsupported_precondition, precondition_type: :intent_status}
            }} =
             Bedrock.commit(store, @ledger, request, [])

    assert_receive {:transact, []}
    assert_receive {:ops, []}
  end

  test "rejects writes that need later Bedrock semantic stories", %{store: store} do
    request = CommitRequest.new(writes: [Write.ack_outbox("out_1")])

    assert {:error,
            %IntentLedger.Error.AdapterRuntimeError{details: %{reason: :unsupported_write, write_type: :ack_outbox}}} =
             Bedrock.commit(store, @ledger, request, [])
  end
end
