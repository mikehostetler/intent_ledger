defmodule IntentLedger.JobQueueHook do
  @moduledoc false

  alias Bedrock.JobQueue.Lease
  alias Bedrock.Keyspace
  alias IntentLedger.Runtime

  @doc false
  @spec apply(module(), Keyspace.t(), Lease.t(), term(), term(), term(), module()) :: :ok | {:error, term()}
  def apply(repo, _queue_root, %Lease{} = lease, action, handler_result, queue_result, ledger) do
    Runtime.apply_queue_action(ledger, repo, lease, action, handler_result, queue_result)
  end
end
