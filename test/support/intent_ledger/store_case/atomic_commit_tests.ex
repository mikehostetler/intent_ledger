defmodule IntentLedger.StoreCase.AtomicCommitTests do
  @moduledoc """
  Shared Store V1 conformance tests for atomic commit and rollback behavior.
  """

  defmacro __using__(_opts) do
    quote do
      alias IntentLedger.Store.{Commit, CommitRequest, Conflict, Precondition, Write}

      describe "atomic commit conformance" do
        test "applies a commit only when stream preconditions hold", context do
          stream = "intent:int_atomic_commit"
          signal = %{id: "sig_atomic_1", type: "intent_ledger.intent.submitted"}

          request =
            CommitRequest.new(
              command_id: "cmd_atomic_commit",
              operation: :submit,
              preconditions: [Precondition.stream_version(stream, 0)],
              writes: [
                Write.append_signal(stream, signal),
                Write.put_idempotency("cmd_atomic_commit", %{intent_id: "int_atomic_commit"})
              ]
            )

          assert {:ok, %Commit{} = commit} = commit(context, request)
          assert commit.command_id == "cmd_atomic_commit"
          assert Enum.map(commit.writes, & &1.type) == [:append_signal, :put_idempotency]
          assert commit.signals == [signal]

          stale =
            CommitRequest.new(
              command_id: "cmd_atomic_stale",
              operation: :submit,
              preconditions: [Precondition.stream_version(stream, 0)],
              writes: [Write.append_signal(stream, %{id: "sig_stale"})]
            )

          assert {:error, %Conflict{} = conflict} = commit(context, stale)
          assert conflict.type == :stream_version
          assert conflict.expected == 0
          assert conflict.actual == 1
        end

        test "rolls back all writes when a precondition fails", context do
          stream = "intent:int_atomic_rollback"

          seed =
            CommitRequest.new(
              command_id: "cmd_atomic_seed",
              operation: :submit,
              preconditions: [Precondition.stream_version(stream, 0)],
              writes: [Write.append_signal(stream, %{id: "sig_seed"})]
            )

          assert {:ok, %Commit{}} = commit(context, seed)

          failed =
            CommitRequest.new(
              command_id: "cmd_atomic_failed",
              operation: :complete,
              preconditions: [
                Precondition.command_absent("cmd_atomic_failed"),
                Precondition.stream_version(stream, 0)
              ],
              writes: [
                Write.append_signal(stream, %{id: "sig_should_not_persist"}),
                Write.put_idempotency("cmd_atomic_failed", %{rolled_back?: false})
              ]
            )

          assert {:error, %Conflict{type: :stream_version}} = commit(context, failed)

          recovery =
            CommitRequest.new(
              command_id: "cmd_atomic_recovery",
              operation: :complete,
              preconditions: [
                Precondition.stream_version(stream, 1),
                Precondition.command_absent("cmd_atomic_failed")
              ],
              writes: [Write.append_signal(stream, %{id: "sig_recovery"})]
            )

          assert {:ok, %Commit{} = commit} = commit(context, recovery)
          assert [%{id: "sig_recovery"}] = commit.signals
        end
      end
    end
  end
end
