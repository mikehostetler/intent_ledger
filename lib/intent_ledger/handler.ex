defmodule IntentLedger.Handler do
  @moduledoc """
  Public execution contract for Intent handlers.

  Handlers are intentionally small: declare a topic, optionally declare Zoi
  schemas, and implement `handle/2`.
  """

  @type result ::
          :ok
          | {:ok, term()}
          | {:error, term()}
          | {:discard, term()}
          | {:snooze, non_neg_integer()}

  @type config :: %{
          required(:topic) => String.t() | nil,
          required(:payload_schema) => Zoi.schema() | nil,
          required(:result_schema) => Zoi.schema() | nil,
          required(:timeout) => pos_integer(),
          required(:metadata) => map()
        }

  @callback handle(payload :: term(), context :: IntentLedger.Context.t()) :: result()

  @doc """
  Defines an Intent handler.
  """
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      @behaviour IntentLedger.Handler
      @behaviour Bedrock.JobQueue.Job

      @intent_handler_config %{
        topic: Keyword.get(opts, :topic),
        payload_schema: Keyword.get(opts, :payload_schema),
        result_schema: Keyword.get(opts, :result_schema),
        timeout: Keyword.get(opts, :timeout, 30_000),
        metadata: Map.new(Keyword.get(opts, :metadata, %{}))
      }

      @doc false
      @spec __intent_handler__() :: IntentLedger.Handler.config()
      def __intent_handler__, do: @intent_handler_config

      @doc false
      @impl Bedrock.JobQueue.Job
      def perform(queue_payload, job_meta),
        do: IntentLedger.Runtime.Execution.perform(__MODULE__, queue_payload, job_meta)

      @doc false
      @impl Bedrock.JobQueue.Job
      def timeout, do: @intent_handler_config.timeout

      defoverridable timeout: 0
    end
  end
end
