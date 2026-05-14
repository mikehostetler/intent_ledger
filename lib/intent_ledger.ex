defmodule IntentLedger do
  @moduledoc """
  Public API generator for Bedrock-backed intent ledgers.

  Applications define a configured module with `use IntentLedger` and then call
  that module directly:

      defmodule MyApp.Intents do
        use IntentLedger,
          otp_app: :my_app,
          repo: MyApp.Bedrock,
          intents: %{
            "invoice.send" => [
              handler: MyApp.Intents.SendInvoice,
              queue: "billing"
            ]
          }
      end

      {:ok, intent} = MyApp.Intents.enqueue("invoice.send", %{invoice_id: 123})

  `IntentLedger` owns the durable Intent model and lifecycle history.
  `bedrock_job_queue` owns queue visibility, leasing, scheduling, retry, and
  concurrent execution.
  """

  @type queue_config :: %{required(:id) => String.t(), optional(term()) => term()}
  @type queue_configs :: %{String.t() => queue_config()}
  @type intent_config :: %{
          required(:topic) => String.t(),
          required(:handler) => module(),
          optional(:queue) => String.t(),
          optional(term()) => term()
        }
  @type intent_configs :: %{String.t() => intent_config()}

  @type config :: %{
          required(:otp_app) => atom(),
          required(:repo) => module(),
          required(:intents) => intent_configs(),
          required(:handlers) => %{String.t() => module()},
          required(:queues) => queue_configs(),
          required(:default_queue) => String.t(),
          required(:job_queue) => module()
        }

  @doc """
  Defines a configured Intent API module.
  """
  defmacro __using__(opts) do
    otp_app = Keyword.fetch!(opts, :otp_app)
    repo = Keyword.fetch!(opts, :repo)
    intents = Keyword.fetch!(opts, :intents)
    queues = Keyword.get(opts, :queues)
    default_queue = Keyword.get(opts, :default_queue)

    quote location: :keep do
      import Kernel, except: [inspect: 1, inspect: 2]

      @otp_app unquote(otp_app)
      @repo unquote(repo)
      @intents unquote(intents)
      @queues unquote(queues)
      @default_queue unquote(default_queue)

      defmodule JobQueue do
        use Bedrock.JobQueue,
          otp_app: unquote(otp_app),
          repo: unquote(repo),
          workers: IntentLedger.Config.handlers_from_intents!(unquote(intents))
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
        do: __MODULE__ |> IntentLedger.Runtime.enqueue(topic, payload, opts) |> IntentLedger.Error.normalize_result()

      @doc """
      Enqueues multiple Intents in one Bedrock transaction.
      """
      @spec enqueue_many(Enumerable.t(), keyword()) ::
              {:ok, [IntentLedger.Intent.t()]} | {:error, term()}
      def enqueue_many(entries, opts \\ []),
        do: __MODULE__ |> IntentLedger.Runtime.enqueue_many(entries, opts) |> IntentLedger.Error.normalize_result()

      @doc """
      Fetches one Intent by ID.
      """
      @spec fetch(String.t()) :: {:ok, IntentLedger.Intent.t()} | {:error, term()}
      def fetch(intent_id),
        do: __MODULE__ |> IntentLedger.Runtime.fetch(intent_id) |> IntentLedger.Error.normalize_result()

      @doc """
      Returns lifecycle signals for one Intent.
      """
      @spec history(String.t(), keyword()) :: {:ok, [Jido.Signal.t()]} | {:error, term()}
      def history(intent_id, opts \\ []),
        do: __MODULE__ |> IntentLedger.Runtime.history(intent_id, opts) |> IntentLedger.Error.normalize_result()

      @doc """
      Replays durable lifecycle signals.
      """
      @spec replay(IntentLedger.Runtime.replay_source(), keyword()) :: {:ok, [map()]} | {:error, term()}
      def replay(source, opts \\ []),
        do: __MODULE__ |> IntentLedger.Runtime.replay(source, opts) |> IntentLedger.Error.normalize_result()

      @doc """
      Cancels an Intent at the IntentLedger layer.

      If the queue item is already pending, the bridge worker observes the
      canceled state and completes the queue item without invoking the handler.
      """
      @spec cancel(String.t(), term(), keyword()) :: {:ok, IntentLedger.Intent.t()} | {:error, term()}
      def cancel(intent_id, reason, opts \\ []),
        do: __MODULE__ |> IntentLedger.Runtime.cancel(intent_id, reason, opts) |> IntentLedger.Error.normalize_result()

      @doc """
      Requeues an Intent by placing another minimal queue item for the same ID.
      """
      @spec requeue(String.t(), keyword()) :: {:ok, IntentLedger.Intent.t()} | {:error, term()}
      def requeue(intent_id, opts \\ []),
        do: __MODULE__ |> IntentLedger.Runtime.requeue(intent_id, opts) |> IntentLedger.Error.normalize_result()

      @doc """
      Marks an Intent as ambiguous for manual reconciliation.
      """
      @spec mark_ambiguous(String.t(), term(), keyword()) ::
              {:ok, IntentLedger.Intent.t()} | {:error, term()}
      def mark_ambiguous(intent_id, reason, opts \\ []),
        do:
          __MODULE__
          |> IntentLedger.Runtime.mark_ambiguous(intent_id, reason, opts)
          |> IntentLedger.Error.normalize_result()

      @doc """
      Returns operational views.
      """
      @spec inspect(atom(), keyword()) :: {:ok, term()} | {:error, term()}
      def inspect(view, opts \\ []),
        do: __MODULE__ |> IntentLedger.Runtime.inspect(view, opts) |> IntentLedger.Error.normalize_result()

      @doc """
      Returns queue statistics.
      """
      @spec stats(keyword()) :: {:ok, map()} | {:error, term()}
      def stats(opts \\ []), do: __MODULE__ |> IntentLedger.Runtime.stats(opts) |> IntentLedger.Error.normalize_result()

      @doc """
      Returns a lightweight health view.
      """
      @spec health(keyword()) :: {:ok, map()}
      def health(opts \\ []), do: IntentLedger.Runtime.health(__MODULE__, opts)

      @doc false
      @spec __intent_ledger__() :: IntentLedger.config()
      def __intent_ledger__ do
        intents = IntentLedger.Config.normalize_intents!(@intents)
        handlers = IntentLedger.Config.handlers_from_intents(intents)
        queues = IntentLedger.Config.normalize_queues!(@queues, intents)
        default_queue = IntentLedger.Config.normalize_default_queue!(@default_queue, queues)

        %{
          otp_app: @otp_app,
          repo: @repo,
          intents: intents,
          handlers: handlers,
          queues: queues,
          default_queue: default_queue,
          job_queue: JobQueue
        }
      end
    end
  end
end
