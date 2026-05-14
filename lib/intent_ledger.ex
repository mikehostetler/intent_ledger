defmodule IntentLedger do
  @moduledoc """
  Public API generator for Bedrock-backed intent ledgers.

  Applications define a configured module with `use IntentLedger` and then call
  that module directly:

      defmodule MyApp.Intents do
        use IntentLedger,
          otp_app: :my_app,
          repo: MyApp.Bedrock,
          handlers: %{
            "invoice.send" => MyApp.Intents.SendInvoice
          }
      end

      {:ok, intent} = MyApp.Intents.enqueue("invoice.send", %{invoice_id: 123})

  `IntentLedger` owns the durable Intent model and lifecycle history.
  `bedrock_job_queue` owns queue visibility, leasing, scheduling, retry, and
  concurrent execution.
  """

  @type config :: %{
          required(:otp_app) => atom(),
          required(:repo) => module(),
          required(:handlers) => %{String.t() => module()},
          required(:job_queue) => module()
        }

  @doc """
  Defines a configured Intent API module.
  """
  defmacro __using__(opts) do
    otp_app = Keyword.fetch!(opts, :otp_app)
    repo = Keyword.fetch!(opts, :repo)
    handlers = Keyword.get(opts, :handlers, {:%{}, [], []})

    quote location: :keep do
      import Kernel, except: [inspect: 1, inspect: 2]

      @otp_app unquote(otp_app)
      @repo unquote(repo)
      @handlers unquote(handlers)

      defmodule JobQueue do
        use Bedrock.JobQueue,
          otp_app: unquote(otp_app),
          repo: unquote(repo),
          workers: unquote(handlers)
      end

      @doc """
      Returns a child specification for the underlying `bedrock_job_queue`
      consumer tree.
      """
      @spec child_spec(keyword()) :: Supervisor.child_spec()
      def child_spec(opts), do: JobQueue.child_spec(opts)

      @doc """
      Starts the underlying `bedrock_job_queue` consumer tree.
      """
      @spec start_link(keyword()) :: Supervisor.on_start()
      def start_link(opts \\ []), do: JobQueue.start_link(opts)

      @doc """
      Enqueues one Intent.
      """
      @spec enqueue(String.t() | atom(), term(), keyword()) ::
              {:ok, IntentLedger.Intent.t()} | {:error, term()}
      def enqueue(topic, payload, opts \\ []),
        do: IntentLedger.Runtime.enqueue(__MODULE__, topic, payload, opts)

      @doc """
      Enqueues multiple Intents in one Bedrock transaction.
      """
      @spec enqueue_many(Enumerable.t(), keyword()) ::
              {:ok, [IntentLedger.Intent.t()]} | {:error, term()}
      def enqueue_many(entries, opts \\ []),
        do: IntentLedger.Runtime.enqueue_many(__MODULE__, entries, opts)

      @doc """
      Fetches one Intent by ID.
      """
      @spec fetch(String.t()) :: {:ok, IntentLedger.Intent.t()} | {:error, :not_found}
      def fetch(intent_id), do: IntentLedger.Runtime.fetch(__MODULE__, intent_id)

      @doc """
      Returns lifecycle signals for one Intent.
      """
      @spec history(String.t(), keyword()) :: {:ok, [Jido.Signal.t()]} | {:error, term()}
      def history(intent_id, opts \\ []), do: IntentLedger.Runtime.history(__MODULE__, intent_id, opts)

      @doc """
      Replays durable lifecycle signals.
      """
      @spec replay(IntentLedger.Runtime.replay_source(), keyword()) :: {:ok, [map()]} | {:error, term()}
      def replay(source, opts \\ []), do: IntentLedger.Runtime.replay(__MODULE__, source, opts)

      @doc """
      Cancels an Intent at the IntentLedger layer.

      If the queue item is already pending, the bridge worker observes the
      canceled state and completes the queue item without invoking the handler.
      """
      @spec cancel(String.t(), term(), keyword()) :: {:ok, IntentLedger.Intent.t()} | {:error, term()}
      def cancel(intent_id, reason, opts \\ []),
        do: IntentLedger.Runtime.cancel(__MODULE__, intent_id, reason, opts)

      @doc """
      Requeues an Intent by placing another minimal queue item for the same ID.
      """
      @spec requeue(String.t(), keyword()) :: {:ok, IntentLedger.Intent.t()} | {:error, term()}
      def requeue(intent_id, opts \\ []), do: IntentLedger.Runtime.requeue(__MODULE__, intent_id, opts)

      @doc """
      Marks an Intent as ambiguous for manual reconciliation.
      """
      @spec mark_ambiguous(String.t(), term(), keyword()) ::
              {:ok, IntentLedger.Intent.t()} | {:error, term()}
      def mark_ambiguous(intent_id, reason, opts \\ []),
        do: IntentLedger.Runtime.mark_ambiguous(__MODULE__, intent_id, reason, opts)

      @doc """
      Returns operational views.
      """
      @spec inspect(atom(), keyword()) :: {:ok, term()} | {:error, term()}
      def inspect(view, opts \\ []), do: IntentLedger.Runtime.inspect(__MODULE__, view, opts)

      @doc """
      Returns queue statistics.
      """
      @spec stats(keyword()) :: {:ok, map()} | {:error, term()}
      def stats(opts \\ []), do: IntentLedger.Runtime.stats(__MODULE__, opts)

      @doc """
      Returns a lightweight health view.
      """
      @spec health(keyword()) :: {:ok, map()}
      def health(opts \\ []), do: IntentLedger.Runtime.health(__MODULE__, opts)

      @doc false
      @spec __intent_ledger__() :: IntentLedger.config()
      def __intent_ledger__ do
        %{
          otp_app: @otp_app,
          repo: @repo,
          handlers: @handlers,
          job_queue: JobQueue
        }
      end
    end
  end
end
