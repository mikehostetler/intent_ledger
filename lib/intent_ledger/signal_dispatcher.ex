defmodule IntentLedger.SignalDispatcher do
  @moduledoc """
  Per-ledger durable outbox dispatcher.

  The dispatcher owns the supervised polling loop for lifecycle signal outbox
  entries. Registered `IntentLedger.SignalHandler` modules are invoked at least
  once; entries are acknowledged after every handler succeeds and retried with
  backoff when any handler fails.
  """

  use GenServer

  alias IntentLedger.{Names, SignalHandler, Time}
  alias IntentLedger.Store.Outbox

  @default_interval_ms 1_000
  @default_batch_size 100
  @default_consumer "intent_ledger.signal_dispatcher"
  @default_retry_ms 1_000
  @default_max_retry_ms 30_000

  @type option ::
          {:name, atom()}
          | {:store, {module(), GenServer.server()}}
          | {:dispatcher_interval_ms, pos_integer()}
          | {:dispatcher_batch_size, pos_integer()}
          | {:dispatcher_consumer, String.t() | atom()}
          | {:dispatcher_retry_ms, pos_integer()}
          | {:dispatcher_max_retry_ms, pos_integer()}
          | {:signal_handlers, [SignalHandler.spec()]}

  @type t :: %__MODULE__{
          name: atom(),
          store_module: module(),
          store_ref: GenServer.server(),
          interval_ms: pos_integer(),
          batch_size: pos_integer(),
          consumer: String.t(),
          retry_ms: pos_integer(),
          max_retry_ms: pos_integer(),
          handlers: [SignalHandler.normalized()],
          timer_ref: reference() | nil,
          poll_count: non_neg_integer(),
          read_count: non_neg_integer(),
          dispatched_count: non_neg_integer(),
          acked_count: non_neg_integer(),
          failed_count: non_neg_integer(),
          skipped_count: non_neg_integer(),
          retries: %{optional(String.t()) => map()},
          last_entries: [map()],
          last_acked: [map()],
          last_skipped: [map()],
          last_errors: [term()],
          last_error: term()
        }

  defstruct [
    :name,
    :store_module,
    :store_ref,
    :interval_ms,
    :batch_size,
    :consumer,
    :retry_ms,
    :max_retry_ms,
    :timer_ref,
    handlers: [],
    poll_count: 0,
    read_count: 0,
    dispatched_count: 0,
    acked_count: 0,
    failed_count: 0,
    skipped_count: 0,
    retries: %{},
    last_entries: [],
    last_acked: [],
    last_skipped: [],
    last_errors: [],
    last_error: nil
  ]

  @doc false
  @spec child_spec([option()]) :: Supervisor.child_spec()
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5000
    }
  end

  @doc false
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: Names.signal_dispatcher(name))
  end

  @doc false
  @spec state(GenServer.server()) :: t()
  def state(server), do: GenServer.call(server, :state)

  @doc false
  @spec poll_once(GenServer.server()) :: {:ok, [map()]} | {:error, term()}
  def poll_once(server), do: GenServer.call(server, :poll_once)

  @impl true
  def init(opts) do
    {store_module, store_ref} = Keyword.fetch!(opts, :store)

    state = %__MODULE__{
      name: Keyword.fetch!(opts, :name),
      store_module: store_module,
      store_ref: store_ref,
      interval_ms: Keyword.get(opts, :dispatcher_interval_ms, @default_interval_ms),
      batch_size: Keyword.get(opts, :dispatcher_batch_size, @default_batch_size),
      consumer: opts |> Keyword.get(:dispatcher_consumer, @default_consumer) |> to_string(),
      retry_ms: Keyword.get(opts, :dispatcher_retry_ms, @default_retry_ms),
      max_retry_ms: Keyword.get(opts, :dispatcher_max_retry_ms, @default_max_retry_ms),
      handlers: opts |> Keyword.get(:signal_handlers, []) |> SignalHandler.normalize(),
      timer_ref: nil
    }

    {:ok, schedule_poll(state)}
  end

  @impl true
  def handle_call(:state, _from, %__MODULE__{} = state) do
    {:reply, state, state}
  end

  def handle_call(:poll_once, _from, %__MODULE__{} = state) do
    {reply, next_state} = poll_outbox(state)
    {:reply, reply, next_state}
  end

  @impl true
  def handle_info({:dispatch_poll, ref}, %__MODULE__{timer_ref: ref} = state) do
    {_reply, next_state} = poll_outbox(%{state | timer_ref: nil})
    {:noreply, schedule_poll(next_state)}
  end

  def handle_info({:dispatch_poll, _stale_ref}, %__MODULE__{} = state) do
    {:noreply, state}
  end

  defp poll_outbox(%__MODULE__{} = state) do
    request = Outbox.read(state.consumer, limit: state.batch_size)

    case state.store_module.outbox(state.store_ref, state.name, request, []) do
      {:ok, entries} ->
        now = Time.utc_now()
        {next_state, delivered_count, acked, skipped, errors} = process_entries(state, entries, now)

        next_state = %{
          next_state
          | poll_count: state.poll_count + 1,
            read_count: state.read_count + length(entries),
            dispatched_count: state.dispatched_count + delivered_count,
            acked_count: state.acked_count + length(acked),
            failed_count: state.failed_count + length(errors),
            skipped_count: state.skipped_count + length(skipped),
            last_entries: entries,
            last_acked: acked,
            last_skipped: skipped,
            last_errors: errors,
            last_error: nil
        }

        {{:ok, entries}, next_state}

      {:error, reason} ->
        next_state = %{state | poll_count: state.poll_count + 1, last_error: reason}
        {{:error, reason}, next_state}
    end
  end

  defp process_entries(%__MODULE__{handlers: []} = state, _entries, _now), do: {state, 0, [], [], []}

  defp process_entries(%__MODULE__{} = state, entries, now) do
    Enum.reduce(entries, {state, 0, [], [], []}, fn entry, {acc_state, delivered_count, acked, skipped, errors} ->
      cond do
        backoff_active?(acc_state, entry, now) ->
          {acc_state, delivered_count, acked, [entry | skipped], errors}

        true ->
          case dispatch_entry_to_handlers(acc_state, entry) do
            {:ok, handler_count} ->
              case ack_entry(acc_state, entry, now) do
                {:ok, acked_entry, next_state} ->
                  {next_state, delivered_count + handler_count, [acked_entry | acked], skipped, errors}

                {:error, reason, next_state} ->
                  {next_state, delivered_count + handler_count, acked, skipped, [reason | errors]}
              end

            {:error, handler_errors} ->
              next_state = put_retry(acc_state, entry, handler_errors, now)
              {next_state, delivered_count, acked, skipped, handler_errors ++ errors}
          end
      end
    end)
    |> then(fn {state, delivered_count, acked, skipped, errors} ->
      {state, delivered_count, Enum.reverse(acked), Enum.reverse(skipped), Enum.reverse(errors)}
    end)
  end

  defp dispatch_entry_to_handlers(%__MODULE__{} = state, entry) do
    state.handlers
    |> Enum.reduce({0, []}, fn handler, {delivered_count, errors} ->
      case dispatch_entry(state, handler, entry) do
        :ok -> {delivered_count + 1, errors}
        {:error, reason} -> {delivered_count, [reason | errors]}
      end
    end)
    |> case do
      {delivered_count, []} -> {:ok, delivered_count}
      {_delivered_count, errors} -> {:error, Enum.reverse(errors)}
    end
  end

  defp dispatch_entry(%__MODULE__{} = state, %{module: module, opts: opts}, entry) do
    context = %{ledger: state.name, consumer: state.consumer, handler: module, opts: opts}

    case module.handle_signal(entry, context) do
      :ok -> :ok
      {:error, reason} -> {:error, {module, reason}}
      result -> {:error, {module, {:invalid_handler_result, result}}}
    end
  catch
    kind, reason -> {:error, {module, {kind, reason}}}
  end

  defp ack_entry(%__MODULE__{} = state, entry, now) do
    key = entry_key(entry)
    request = Outbox.ack(key, state.consumer, metadata: %{acked_at: now, handler_count: length(state.handlers)})

    case state.store_module.outbox(state.store_ref, state.name, request, []) do
      {:ok, acked_entry} ->
        {:ok, acked_entry, %{state | retries: Map.delete(state.retries, key)}}

      {:error, reason} ->
        {:error, {key, reason}, put_retry(state, entry, [{:ack_failed, reason}], now)}
    end
  end

  defp backoff_active?(%__MODULE__{} = state, entry, now) do
    case Map.get(state.retries, entry_key(entry)) do
      %{next_attempt_at: %DateTime{} = next_attempt_at} -> DateTime.compare(next_attempt_at, now) == :gt
      _missing_or_due -> false
    end
  end

  defp put_retry(%__MODULE__{} = state, entry, errors, now) do
    key = entry_key(entry)
    retry_count = state.retries |> Map.get(key, %{}) |> Map.get(:retry_count, 0) |> Kernel.+(1)
    retry_ms = backoff_ms(state, retry_count)

    retry = %{
      retry_count: retry_count,
      next_attempt_at: Time.add_ms(now, retry_ms),
      last_errors: errors
    }

    %{state | retries: Map.put(state.retries, key, retry)}
  end

  defp backoff_ms(%__MODULE__{} = state, retry_count) do
    multiplier = :math.pow(2, max(retry_count - 1, 0)) |> round()
    min(state.max_retry_ms, state.retry_ms * multiplier)
  end

  defp entry_key(%{key: key}), do: key
  defp entry_key(%{"key" => key}), do: key

  defp schedule_poll(%__MODULE__{} = state) do
    ref = make_ref()
    Process.send_after(self(), {:dispatch_poll, ref}, state.interval_ms)
    %{state | timer_ref: ref}
  end
end
