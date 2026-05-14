defmodule IntentLedger.BedrockStore do
  @moduledoc false

  alias Bedrock.Keyspace
  alias IntentLedger.BedrockStore.{Intents, Keyspaces, Outbox, Projections, Streams}
  alias IntentLedger.Intent

  @type stream_source :: Streams.stream_source()
  @type projection_ref :: Projections.projection_ref()
  @type outbox_consumer_ref :: Outbox.consumer_ref()
  @type intent_status ::
          :enqueued
          | :started
          | :completed
          | :failed
          | :retry_scheduled
          | :discarded
          | :canceled
          | :ambiguous

  @doc false
  @spec transact(module(), (module(), Keyspace.t() -> term()), keyword()) :: term()
  def transact(ledger, fun, opts \\ []) when is_atom(ledger) and is_function(fun, 2) do
    repo = repo!(ledger)
    root = root_keyspace(ledger)

    repo.transact(fn -> fun.(repo, root) end, opts)
  end

  @doc false
  @spec create_intent(module(), module(), Keyspace.t(), Intent.t(), keyword()) ::
          {:ok, Intent.t(), :created | :existing}
  defdelegate create_intent(ledger, repo, root, intent, opts \\ []), to: Intents, as: :create

  @doc false
  @spec fetch(module(), String.t()) :: {:ok, Intent.t()} | {:error, :not_found}
  defdelegate fetch(ledger, intent_id), to: Intents

  @doc false
  @spec fetch(module(), Keyspace.t(), String.t()) :: {:ok, Intent.t()} | {:error, :not_found}
  defdelegate fetch(repo, root, intent_id), to: Intents

  @doc false
  @spec put_intent(module(), Keyspace.t(), Intent.t()) :: :ok
  defdelegate put_intent(repo, root, intent), to: Intents, as: :put

  @doc false
  @spec record_lifecycle(module(), Keyspace.t(), module(), Intent.t(), atom(), map()) :: Jido.Signal.t()
  defdelegate record_lifecycle(repo, root, ledger, intent, event, data), to: Streams

  @doc false
  @spec update_intent(module(), String.t(), atom(), map(), (Intent.t(), DateTime.t() -> Intent.t())) ::
          {:ok, Intent.t()} | {:error, term()}
  def update_intent(ledger, intent_id, event, data, update_fun)
      when is_atom(event) and is_map(data) and is_function(update_fun, 2) do
    transact(ledger, fn repo, root ->
      update_intent(repo, root, ledger, intent_id, event, data, update_fun)
    end)
  end

  @doc false
  @spec update_intent(
          module(),
          Keyspace.t(),
          module(),
          String.t(),
          atom(),
          map(),
          (Intent.t(), DateTime.t() -> Intent.t())
        ) ::
          {:ok, Intent.t()} | {:error, term()}
  defdelegate update_intent(repo, root, ledger, intent_id, event, data, update_fun), to: Intents, as: :update

  @doc false
  @spec history(module(), String.t(), keyword()) :: {:ok, [Jido.Signal.t()]} | {:error, term()}
  defdelegate history(ledger, intent_id, opts \\ []), to: Streams

  @doc false
  @spec replay(module(), stream_source(), keyword()) :: {:ok, [Jido.Signal.t()]} | {:error, term()}
  defdelegate replay(ledger, source, opts \\ []), to: Streams

  @doc false
  @spec outbox(module(), keyword()) :: {:ok, [map()]} | {:error, term()}
  defdelegate outbox(ledger, opts \\ []), to: Outbox

  @doc false
  @spec read_outbox(module(), outbox_consumer_ref(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate read_outbox(ledger, consumer, opts \\ []), to: Outbox, as: :read

  @doc false
  @spec outbox_cursor(module(), outbox_consumer_ref(), keyword()) ::
          {:ok, non_neg_integer() | nil} | {:error, term()}
  defdelegate outbox_cursor(ledger, consumer, opts \\ []), to: Outbox, as: :cursor

  @doc false
  @spec ack_outbox(module(), outbox_consumer_ref(), non_neg_integer(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate ack_outbox(ledger, consumer, cursor, opts \\ []), to: Outbox, as: :ack

  @doc false
  @spec intents(module(), keyword()) :: {:ok, [Intent.t()]} | {:error, term()}
  defdelegate intents(ledger, opts \\ []), to: Intents, as: :all

  @doc false
  @spec projections(module(), keyword()) :: {:ok, [map()]} | {:error, term()}
  defdelegate projections(ledger, opts \\ []), to: Projections

  @doc false
  @spec projection_cursor(module(), projection_ref(), keyword()) ::
          {:ok, non_neg_integer() | nil} | {:error, term()}
  defdelegate projection_cursor(ledger, projection, opts \\ []), to: Projections, as: :cursor

  @doc false
  @spec put_projection_cursor(module(), projection_ref(), non_neg_integer(), keyword()) :: :ok | {:error, term()}
  defdelegate put_projection_cursor(ledger, projection, cursor, opts \\ []), to: Projections, as: :put_cursor

  @doc false
  @spec heads(module()) :: {:ok, %{ledger: non_neg_integer(), outbox: non_neg_integer()}} | {:error, term()}
  def heads(ledger) do
    transact(ledger, fn repo, root ->
      {:ok, %{ledger: Streams.head(repo, root, "ledger"), outbox: Outbox.head(repo, root)}}
    end)
  end

  @doc false
  @spec root_keyspace(module()) :: Keyspace.t()
  def root_keyspace(ledger), do: Keyspaces.root(ledger)

  defp repo!(ledger), do: ledger.__intent_ledger__().repo
end
