defmodule IntentLedger.Store.Memory do
  @moduledoc """
  In-memory reference store adapter for tests and local examples.

  This adapter keeps the same semantic commit shape expected from durable
  stores: every lifecycle operation updates state and appends lifecycle signals
  inside one GenServer call. It is process-local, loses state on restart, and is
  not a clustered production backend.
  """

  use GenServer

  @behaviour IntentLedger.Store

  alias IntentLedger.{
    Claim,
    Claimed,
    Intent,
    IntentState,
    OutboxEntry,
    Record,
    Signal,
    Time
  }

  alias IntentLedger.Store.{Commit, CommitRequest, Conflict, Lineage, Listing, Outbox}

  defstruct intents: %{},
            states: %{},
            claims: %{},
            idempotency: %{},
            commands: %{},
            shard_leases: %{},
            outbox: %{},
            next_outbox_sequence: 1,
            streams: %{}

  @type t :: %__MODULE__{
          intents: %{optional(String.t()) => Intent.t()},
          states: %{optional(String.t()) => IntentState.t()},
          claims: map(),
          idempotency: %{optional(String.t()) => String.t()},
          commands: %{optional(String.t()) => map()},
          shard_leases: %{optional(String.t()) => map()},
          outbox: %{optional(String.t()) => map()},
          next_outbox_sequence: pos_integer(),
          streams: %{optional(String.t()) => [Jido.Signal.t()]}
        }

  @doc false
  @impl true
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
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, [], name: name)
  end

  @doc false
  @impl true
  def init(_opts), do: {:ok, %__MODULE__{}}

  @impl true
  def commit(ref, ledger, %CommitRequest{} = request, opts) do
    GenServer.call(ref, {:store_v1_commit, ledger, request, opts})
  end

  @impl true
  def read(ref, ledger, request, opts) do
    GenServer.call(ref, {:store_v1_read, ledger, request, opts})
  end

  @impl true
  def lease(ref, ledger, request, opts) do
    GenServer.call(ref, {:store_v1_lease, ledger, request, opts})
  end

  @impl true
  def listing(ref, ledger, request, opts) do
    GenServer.call(ref, {:store_v1_listing, ledger, request, opts})
  end

  @impl true
  def outbox(ref, ledger, request, opts) do
    GenServer.call(ref, {:store_v1_outbox, ledger, request, opts})
  end

  def submit(ref, ledger, intent, opts) do
    GenServer.call(ref, {:submit, ledger, intent, opts})
  end

  def submit_many(ref, ledger, intents, opts) do
    GenServer.call(ref, {:submit_many, ledger, intents, opts})
  end

  def get(ref, intent_id), do: GenServer.call(ref, {:get, intent_id})

  def history(ref, intent_id), do: GenServer.call(ref, {:history, intent_id})

  def claim(ref, ledger, queue, owner_id, opts) do
    GenServer.call(ref, {:claim, ledger, queue, owner_id, opts})
  end

  def heartbeat(ref, ledger, claim_id, token, opts) do
    GenServer.call(ref, {:heartbeat, ledger, claim_id, token, opts})
  end

  def complete(ref, ledger, claim_id, token, result, opts) do
    GenServer.call(ref, {:complete, ledger, claim_id, token, result, opts})
  end

  def fail(ref, ledger, claim_id, token, error, opts) do
    GenServer.call(ref, {:fail, ledger, claim_id, token, error, opts})
  end

  def release(ref, ledger, claim_id, token, opts) do
    GenServer.call(ref, {:release, ledger, claim_id, token, opts})
  end

  def cancel(ref, ledger, intent_id, reason, opts) do
    GenServer.call(ref, {:cancel, ledger, intent_id, reason, opts})
  end

  def requeue(ref, ledger, intent_id, opts) do
    GenServer.call(ref, {:requeue, ledger, intent_id, opts})
  end

  def mark_ambiguous(ref, ledger, intent_id, reason, opts) do
    GenServer.call(ref, {:mark_ambiguous, ledger, intent_id, reason, opts})
  end

  def recover(ref, ledger, queue, opts) do
    GenServer.call(ref, {:recover, ledger, queue, opts})
  end

  @impl true
  def handle_call(
        {:store_v1_commit, _ledger, %CommitRequest{preconditions: [], writes: []} = request, _opts},
        _from,
        state
      ) do
    commit = Commit.new(command_id: request.command_id, result: nil, writes: [], signals: [])

    {:reply, {:ok, commit}, state}
  end

  def handle_call({:store_v1_commit, _ledger, %CommitRequest{} = request, _opts}, _from, state) do
    if supported_commit?(request) do
      case check_commit_preconditions(state, request) do
        {:replay, entry} ->
          commit =
            Commit.new(
              command_id: request.command_id,
              result: entry.result,
              replayed: true,
              replay_of: request.command_id
            )

          {:reply, {:ok, commit}, state}

        :ok ->
          {next_state, result, signals} = apply_commit_writes(state, request)
          commit = Commit.new(command_id: request.command_id, result: result, writes: request.writes, signals: signals)

          {:reply, {:ok, commit}, next_state}

        {:error, conflict} ->
          {:reply, {:error, conflict}, state}
      end
    else
      {:reply, unsupported_store_v1(:commit, request), state}
    end
  end

  def handle_call({:store_v1_read, _ledger, {:intent, intent_id}, _opts}, _from, state) do
    {:reply, fetch_record(state, intent_id), state}
  end

  def handle_call({:store_v1_read, _ledger, {:history, intent_id}, _opts}, _from, state) do
    if Map.has_key?(state.intents, intent_id) do
      {:reply, {:ok, Map.get(state.streams, intent_stream(intent_id), [])}, state}
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:store_v1_read, _ledger, {:stream, stream, read_opts}, _opts}, _from, state) do
    signals = Map.get(state.streams, stream, [])

    {:reply, {:ok, %{stream: stream, version: length(signals), signals: window(signals, read_opts)}}, state}
  end

  def handle_call({:store_v1_read, _ledger, {:lineage_counts, attrs}, _opts}, _from, state) do
    counts = Lineage.counts(Map.values(state.intents), Map.values(state.states), attrs)
    {:reply, {:ok, counts}, state}
  end

  def handle_call({:store_v1_read, _ledger, request, _opts}, _from, state) do
    {:reply, unsupported_store_v1(:read, request), state}
  end

  def handle_call({:store_v1_lease, _ledger, request, _opts}, _from, state) do
    case apply_shard_lease_request(state, request) do
      {:ok, reply, next_state} -> {:reply, {:ok, reply}, next_state}
      {:error, conflict} -> {:reply, {:error, conflict}, state}
      :unsupported -> {:reply, unsupported_store_v1(:lease, request), state}
    end
  end

  def handle_call({:store_v1_listing, _ledger, request, _opts}, _from, state) do
    case normalize_listing_request(request) do
      {:ok, listing} -> {:reply, {:ok, list_store_v1(state, listing)}, state}
      :unsupported -> {:reply, unsupported_store_v1(:listing, request), state}
    end
  end

  def handle_call({:store_v1_outbox, _ledger, request, _opts}, _from, state) do
    case apply_outbox_request(state, request) do
      {:ok, reply, next_state} -> {:reply, {:ok, reply}, next_state}
      {:error, conflict} -> {:reply, {:error, conflict}, state}
      :unsupported -> {:reply, unsupported_store_v1(:outbox, request), state}
    end
  end

  def handle_call({:submit, ledger, %Intent{} = intent, opts}, _from, state) do
    case commit_submit(state, ledger, intent, opts) do
      {:ok, next_state, record, signals} -> {:reply, {:ok, record, signals}, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:submit_many, ledger, intents, opts}, _from, state) do
    case Enum.reduce_while(intents, {:ok, state, [], []}, fn intent, {:ok, acc_state, records, signals} ->
           case commit_submit(acc_state, ledger, intent, opts) do
             {:ok, next_state, record, new_signals} ->
               {:cont, {:ok, next_state, [record | records], signals ++ new_signals}}

             {:error, reason} ->
               {:halt, {:error, reason}}
           end
         end) do
      {:ok, next_state, records, signals} ->
        {:reply, {:ok, Enum.reverse(records), signals}, next_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get, intent_id}, _from, state) do
    {:reply, fetch_record(state, intent_id), state}
  end

  def handle_call({:history, intent_id}, _from, state) do
    if Map.has_key?(state.intents, intent_id) do
      {:reply, {:ok, Map.get(state.streams, intent_stream(intent_id), [])}, state}
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:claim, ledger, queue, owner_id, opts}, _from, state) do
    now = Keyword.fetch!(opts, :now)
    limit = Keyword.get(opts, :limit, 1)
    lease_ms = Keyword.fetch!(opts, :lease_ms)

    if limit < 1 do
      {:reply, {:error, {:invalid_limit, limit}}, state}
    else
      {next_state, claimed, signals} =
        state
        |> available(queue, now)
        |> Enum.take(limit)
        |> Enum.reduce({state, [], []}, fn {intent, intent_state}, {acc_state, acc_claimed, acc_signals} ->
          {next_state, claimed, new_signals} =
            commit_claim(acc_state, ledger, intent, intent_state, owner_id, now, lease_ms)

          {next_state, [claimed | acc_claimed], acc_signals ++ new_signals}
        end)

      {:reply, {:ok, Enum.reverse(claimed), signals}, next_state}
    end
  end

  def handle_call({:heartbeat, ledger, claim_id, token, opts}, _from, state) do
    now = Keyword.fetch!(opts, :now)
    lease_ms = Keyword.fetch!(opts, :lease_ms)

    with {:ok, intent, intent_state, _claim_info} <- validate_claim(state, claim_id, token, now),
         lease_until = Time.add_ms(now, lease_ms),
         next_intent_state = %{intent_state | lease_until: lease_until, updated_at: now},
         claim = %Claim{
           id: claim_id,
           intent_id: intent.id,
           owner_id: claim_owner(state, claim_id),
           token: token,
           attempt: intent_state.attempt,
           lease_until: lease_until
         },
         signal <-
           signal(ledger, :claim_heartbeat, intent, %{
             claim_id: claim_id,
             lease_until: lease_until
           }),
         next_state <-
           put_state(state, intent, next_intent_state) |> append(ledger, intent, [signal]) do
      {:reply, {:ok, claim, [signal]}, next_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:complete, ledger, claim_id, token, result, opts}, _from, state) do
    now = Keyword.fetch!(opts, :now)

    with {:ok, intent, intent_state, _claim_info} <- validate_claim(state, claim_id, token, now),
         next_intent_state <-
           intent_state
           |> clear_claim(now)
           |> Map.merge(%{status: :completed, completed_at: now, result: result}),
         signal <-
           signal(ledger, :intent_completed, intent, %{claim_id: claim_id, result: result}),
         next_state <-
           state
           |> drop_claim(claim_id)
           |> put_state(intent, next_intent_state)
           |> append(ledger, intent, [signal]) do
      {:reply, {:ok, record(next_state, intent.id), [signal]}, next_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:fail, ledger, claim_id, token, error, opts}, _from, state) do
    now = Keyword.fetch!(opts, :now)

    with {:ok, intent, intent_state, _claim_info} <- validate_claim(state, claim_id, token, now),
         {:ok, next_state, record, signals} <-
           commit_failure(state, ledger, intent, intent_state, claim_id, error, opts) do
      {:reply, {:ok, record, signals}, next_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:release, ledger, claim_id, token, opts}, _from, state) do
    now = Keyword.fetch!(opts, :now)

    with {:ok, intent, intent_state, _claim_info} <- validate_claim(state, claim_id, token, now),
         next_intent_state <-
           intent_state
           |> clear_claim(now)
           |> Map.merge(%{status: :available, visible_at: now}),
         signal <- signal(ledger, :intent_released, intent, %{claim_id: claim_id}),
         next_state <-
           state
           |> drop_claim(claim_id)
           |> put_state(intent, next_intent_state)
           |> append(ledger, intent, [signal]) do
      {:reply, {:ok, record(next_state, intent.id), [signal]}, next_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:cancel, ledger, intent_id, reason, opts}, _from, state) do
    now = Keyword.fetch!(opts, :now)

    with {:ok, %Record{intent: intent, state: intent_state}} <- fetch_record(state, intent_id),
         :ok <- ensure_not_final(intent_state),
         next_intent_state <-
           intent_state
           |> clear_claim(now)
           |> Map.merge(%{status: :cancelled, cancel_reason: reason}),
         signal <- signal(ledger, :intent_cancelled, intent, %{reason: reason}),
         next_state <-
           state
           |> maybe_drop_claim(intent_state.claim_id)
           |> put_state(intent, next_intent_state)
           |> append(ledger, intent, [signal]) do
      {:reply, {:ok, record(next_state, intent.id), [signal]}, next_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:requeue, ledger, intent_id, opts}, _from, state) do
    now = Keyword.fetch!(opts, :now)
    retry_at = Keyword.get(opts, :retry_at, now)

    with {:ok, %Record{intent: intent, state: intent_state}} <- fetch_record(state, intent_id),
         :ok <- ensure_not_final(intent_state),
         next_intent_state <-
           intent_state
           |> clear_claim(now)
           |> Map.merge(%{status: :retry_scheduled, visible_at: retry_at}),
         signal <- signal(ledger, :intent_retry_scheduled, intent, %{retry_at: retry_at}),
         next_state <-
           state
           |> maybe_drop_claim(intent_state.claim_id)
           |> put_state(intent, next_intent_state)
           |> append(ledger, intent, [signal]) do
      {:reply, {:ok, record(next_state, intent.id), [signal]}, next_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:mark_ambiguous, ledger, intent_id, reason, opts}, _from, state) do
    now = Keyword.fetch!(opts, :now)

    with {:ok, %Record{intent: intent, state: intent_state}} <- fetch_record(state, intent_id),
         :ok <- ensure_not_final(intent_state),
         next_intent_state <-
           intent_state
           |> clear_claim(now)
           |> Map.merge(%{status: :ambiguous, error: reason}),
         signal <- signal(ledger, :intent_marked_ambiguous, intent, %{reason: reason}),
         next_state <-
           state
           |> maybe_drop_claim(intent_state.claim_id)
           |> put_state(intent, next_intent_state)
           |> append(ledger, intent, [signal]) do
      {:reply, {:ok, record(next_state, intent.id), [signal]}, next_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:recover, ledger, queue, opts}, _from, state) do
    now = Keyword.fetch!(opts, :now)
    limit = Keyword.get(opts, :limit, 100)

    {next_state, records, signals} =
      state.states
      |> Enum.filter(fn {_id, intent_state} ->
        intent_state.queue == queue and intent_state.status == :claimed and
          DateTime.compare(intent_state.lease_until, now) != :gt
      end)
      |> Enum.take(limit)
      |> Enum.reduce({state, [], []}, fn {intent_id, intent_state}, {acc_state, acc_records, acc_signals} ->
        intent = Map.fetch!(acc_state.intents, intent_id)

        {next_state, record, new_signals} =
          commit_expired_claim(acc_state, ledger, intent, intent_state, now)

        {next_state, [record | acc_records], acc_signals ++ new_signals}
      end)

    {:reply, {:ok, Enum.reverse(records), signals}, next_state}
  end

  defp commit_submit(state, ledger, %Intent{} = intent, opts) do
    now = Keyword.fetch!(opts, :now)

    cond do
      Map.has_key?(state.intents, intent.id) ->
        {:error, {:duplicate_intent_id, intent.id}}

      idempotency_conflict?(state, intent) ->
        {:error, {:idempotency_conflict, Map.fetch!(state.idempotency, intent.idempotency_key)}}

      true ->
        intent_state = IntentState.new(intent, now)
        submitted = signal(ledger, :intent_submitted, intent, intent_data(intent))
        signals = [submitted] ++ maybe_available_signal(ledger, intent, now)

        next_state =
          state
          |> put_intent(intent)
          |> put_state(intent, intent_state)
          |> put_idempotency(intent)
          |> append(ledger, intent, signals)

        {:ok, next_state, record(next_state, intent.id), signals}
    end
  end

  defp commit_claim(state, ledger, intent, intent_state, owner_id, now, lease_ms) do
    lease_until = Time.add_ms(now, lease_ms)
    claim = Claim.new(intent.id, owner_id, intent_state.attempt + 1, lease_until)
    token_hash = Claim.token_hash(claim.token)

    next_intent_state = %{
      intent_state
      | status: :claimed,
        attempt: claim.attempt,
        claim_id: claim.id,
        claim_token_hash: token_hash,
        lease_until: lease_until,
        updated_at: now,
        error: nil
    }

    claim_info = %{
      intent_id: intent.id,
      owner_id: claim.owner_id,
      token_hash: token_hash,
      attempt: claim.attempt
    }

    signal =
      signal(ledger, :intent_claimed, intent, %{
        claim_id: claim.id,
        owner_id: claim.owner_id,
        attempt: claim.attempt,
        lease_until: lease_until
      })

    next_state =
      state
      |> put_state(intent, next_intent_state)
      |> put_claim(claim.id, claim_info)
      |> append(ledger, intent, [signal])

    {next_state, %Claimed{intent: intent, state: next_intent_state, claim: claim}, [signal]}
  end

  defp commit_failure(state, ledger, intent, intent_state, claim_id, error, opts) do
    now = Keyword.fetch!(opts, :now)

    with {:ok, classification} <- classify_failure(opts, %Record{intent: intent, state: intent_state}, error) do
      failed =
        signal(ledger, :intent_failed, intent, %{
          claim_id: claim_id,
          error: error,
          attempt: intent_state.attempt
        })

      {next_intent_state, extra_signal} =
        failure_transition(ledger, intent, intent_state, error, now, opts, classification)

      signals = List.wrap(failed) ++ List.wrap(extra_signal)

      next_state =
        state
        |> drop_claim(claim_id)
        |> put_state(intent, next_intent_state)
        |> append(ledger, intent, signals)

      {:ok, next_state, record(next_state, intent.id), signals}
    end
  end

  defp classify_failure(opts, %Record{} = record, error) do
    case Keyword.get(opts, :classify_failure) do
      nil ->
        {:ok, :default}

      classify when is_function(classify, 2) ->
        case classify.(record, error) do
          classification when classification in [:default, :retry, :fail, :ambiguous] ->
            {:ok, classification}

          {:retry, %DateTime{}} = classification ->
            {:ok, classification}

          {:error, reason} ->
            {:error, reason}

          classification ->
            {:error, {:invalid_failure_classification, classification}}
        end
    end
  end

  defp failure_transition(ledger, intent, intent_state, error, now, opts, :default) do
    if intent_state.attempt < intent_state.max_attempts do
      retry_at = Keyword.get(opts, :retry_at, Time.add_ms(now, Keyword.get(opts, :retry_ms, 0)))
      retry_failure(ledger, intent, intent_state, error, now, retry_at)
    else
      exhausted_failure(ledger, intent, intent_state, error, now, :attempts_exhausted)
    end
  end

  defp failure_transition(ledger, intent, intent_state, error, now, opts, :retry) do
    if intent_state.attempt < intent_state.max_attempts do
      retry_at = Keyword.get(opts, :retry_at, Time.add_ms(now, Keyword.get(opts, :retry_ms, 0)))
      retry_failure(ledger, intent, intent_state, error, now, retry_at)
    else
      exhausted_failure(ledger, intent, intent_state, error, now, :attempts_exhausted)
    end
  end

  defp failure_transition(ledger, intent, intent_state, error, now, _opts, {:retry, retry_at}) do
    if intent_state.attempt < intent_state.max_attempts do
      retry_failure(ledger, intent, intent_state, error, now, retry_at)
    else
      exhausted_failure(ledger, intent, intent_state, error, now, :attempts_exhausted)
    end
  end

  defp failure_transition(_ledger, _intent, intent_state, error, now, _opts, :fail) do
    {%{intent_state | status: :failed, error: error} |> clear_claim(now), []}
  end

  defp failure_transition(ledger, intent, intent_state, error, now, _opts, :ambiguous) do
    ambiguous_failure(ledger, intent, intent_state, error, now, :failure_classified)
  end

  defp retry_failure(ledger, intent, intent_state, error, now, retry_at) do
    {%{
       intent_state
       | status: :retry_scheduled,
         visible_at: retry_at,
         error: error
     }
     |> clear_claim(now),
     signal(ledger, :intent_retry_scheduled, intent, %{
       retry_at: retry_at,
       attempt: intent_state.attempt
     })}
  end

  defp exhausted_failure(ledger, intent, intent_state, error, now, reason) do
    if intent.ambiguity_policy in [:manual, :reconcile] do
      ambiguous_failure(ledger, intent, intent_state, error, now, reason)
    else
      {%{intent_state | status: :failed, error: error} |> clear_claim(now), []}
    end
  end

  defp ambiguous_failure(ledger, intent, intent_state, error, now, reason) do
    {%{intent_state | status: :ambiguous, error: error} |> clear_claim(now),
     signal(ledger, :intent_marked_ambiguous, intent, %{
       reason: reason,
       error: error
     })}
  end

  defp commit_expired_claim(state, ledger, intent, intent_state, now) do
    lease_expired =
      signal(ledger, :claim_lease_expired, intent, %{
        claim_id: intent_state.claim_id,
        lease_until: intent_state.lease_until
      })

    {next_intent_state, extra_signal} =
      if intent.ambiguity_policy == :retry and intent_state.attempt < intent_state.max_attempts do
        {%{clear_claim(intent_state, now) | status: :available, visible_at: now},
         signal(ledger, :intent_available, intent, %{visible_at: now})}
      else
        {%{clear_claim(intent_state, now) | status: :ambiguous},
         signal(ledger, :intent_marked_ambiguous, intent, %{reason: :lease_expired})}
      end

    signals = [lease_expired, extra_signal]

    next_state =
      state
      |> maybe_drop_claim(intent_state.claim_id)
      |> put_state(intent, next_intent_state)
      |> append(ledger, intent, signals)

    {next_state, record(next_state, intent.id), signals}
  end

  defp available(state, queue, now) do
    state.states
    |> Enum.flat_map(fn {intent_id, intent_state} ->
      intent = Map.fetch!(state.intents, intent_id)

      if available?(intent_state, queue, now) do
        [{intent, intent_state}]
      else
        []
      end
    end)
    |> Enum.sort_by(fn {intent, intent_state} ->
      {-intent.priority, DateTime.to_unix(intent_state.visible_at, :microsecond), intent.id}
    end)
  end

  defp available?(%IntentState{} = state, queue, now) do
    state.queue == queue and state.status in [:available, :retry_scheduled] and
      DateTime.compare(state.visible_at, now) != :gt
  end

  defp validate_claim(state, claim_id, token, now) do
    with {:ok, claim_info} <- fetch_claim(state, claim_id),
         {:ok, intent_state} <- fetch_state(state, claim_info.intent_id),
         {:ok, intent} <- fetch_intent(state, claim_info.intent_id),
         :ok <- verify_claim_token(claim_info, token),
         :ok <- verify_claim_state(intent_state, claim_id),
         :ok <- verify_lease(intent_state, now) do
      {:ok, intent, intent_state, claim_info}
    end
  end

  defp verify_claim_token(%{token_hash: token_hash}, token) do
    if Claim.token_hash(token) == token_hash, do: :ok, else: {:error, :stale_claim}
  end

  defp verify_claim_state(%IntentState{status: :claimed, claim_id: claim_id}, claim_id), do: :ok
  defp verify_claim_state(_intent_state, _claim_id), do: {:error, :stale_claim}

  defp verify_lease(%IntentState{lease_until: lease_until}, now) do
    if DateTime.compare(lease_until, now) == :gt, do: :ok, else: {:error, :lease_expired}
  end

  defp ensure_not_final(%IntentState{} = intent_state) do
    if IntentState.final?(intent_state),
      do: {:error, {:final_state, intent_state.status}},
      else: :ok
  end

  defp clear_claim(%IntentState{} = intent_state, now) do
    %{
      intent_state
      | claim_id: nil,
        claim_token_hash: nil,
        lease_until: nil,
        updated_at: now
    }
  end

  defp fetch_record(state, intent_id) do
    with {:ok, _intent} <- fetch_intent(state, intent_id),
         {:ok, _intent_state} <- fetch_state(state, intent_id) do
      {:ok, record(state, intent_id)}
    end
  end

  defp record(state, intent_id) do
    %Record{
      intent: Map.fetch!(state.intents, intent_id),
      state: Map.fetch!(state.states, intent_id)
    }
  end

  defp fetch_intent(state, intent_id) do
    case Map.fetch(state.intents, intent_id) do
      {:ok, intent} -> {:ok, intent}
      :error -> {:error, :not_found}
    end
  end

  defp fetch_state(state, intent_id) do
    case Map.fetch(state.states, intent_id) do
      {:ok, intent_state} -> {:ok, intent_state}
      :error -> {:error, :not_found}
    end
  end

  defp fetch_claim(state, claim_id) do
    case Map.fetch(state.claims, claim_id) do
      {:ok, claim_info} -> {:ok, claim_info}
      :error -> {:error, :stale_claim}
    end
  end

  defp claim_owner(state, claim_id),
    do: state.claims |> Map.fetch!(claim_id) |> Map.fetch!(:owner_id)

  defp put_intent(state, intent),
    do: %{state | intents: Map.put(state.intents, intent.id, intent)}

  defp put_state(state, intent, intent_state),
    do: %{state | states: Map.put(state.states, intent.id, intent_state)}

  defp put_claim(state, claim_id, claim_info),
    do: %{state | claims: Map.put(state.claims, claim_id, claim_info)}

  defp drop_claim(state, claim_id), do: %{state | claims: Map.delete(state.claims, claim_id)}
  defp maybe_drop_claim(state, nil), do: state
  defp maybe_drop_claim(state, claim_id), do: drop_claim(state, claim_id)

  defp put_idempotency(state, %Intent{idempotency_key: nil}), do: state

  defp put_idempotency(state, %Intent{idempotency_key: idempotency_key, id: id}) do
    %{state | idempotency: Map.put(state.idempotency, idempotency_key, id)}
  end

  defp idempotency_conflict?(_state, %Intent{idempotency_key: nil}), do: false

  defp idempotency_conflict?(state, %Intent{idempotency_key: idempotency_key}) do
    Map.has_key?(state.idempotency, idempotency_key)
  end

  defp signal(ledger, event, %Intent{} = intent, data) do
    data =
      data
      |> Map.merge(intent_lineage_data(intent))
      |> normalize_signal_data()

    Signal.lifecycle(event, ledger, "intent:" <> intent.id, data)
  end

  defp maybe_available_signal(ledger, intent, now) do
    if DateTime.compare(intent.visible_at, now) != :gt do
      [signal(ledger, :intent_available, intent, %{visible_at: intent.visible_at})]
    else
      []
    end
  end

  defp intent_data(%Intent{} = intent) do
    %{
      intent_id: intent.id,
      key: intent.key,
      kind: intent.kind,
      queue: intent.queue,
      shard: intent.shard,
      visible_at: intent.visible_at,
      max_attempts: intent.max_attempts,
      idempotency_key: intent.idempotency_key,
      ambiguity_policy: intent.ambiguity_policy
    }
  end

  defp normalize_signal_data(data) when is_map(data) do
    Map.new(data, fn
      {key, %DateTime{} = value} -> {key, DateTime.to_iso8601(value)}
      {key, value} -> {key, value}
    end)
  end

  defp intent_lineage_data(%Intent{} = intent) do
    %{
      correlation_id: intent.correlation_id,
      causation_id: intent.causation_id,
      root_intent_id: intent.root_intent_id,
      parent_intent_id: intent.parent_intent_id,
      depth: intent.depth,
      actor: intent.actor
    }
    |> Map.reject(fn {_field, value} -> is_nil(value) end)
  end

  defp append(state, ledger, %Intent{} = intent, signals) do
    stream = intent_stream(intent.id)

    streams =
      [ledger_stream(ledger), queue_stream(intent), stream]
      |> Enum.reduce(state.streams, fn stream, acc ->
        Map.update(acc, stream, signals, &(&1 ++ signals))
      end)

    signals
    |> Enum.reduce(%{state | streams: streams}, fn signal, acc_state ->
      {next_state, _entry} = put_outbox_entry(acc_state, OutboxEntry.key(signal), OutboxEntry.new(stream, signal))
      next_state
    end)
  end

  defp ledger_stream(ledger), do: "ledger:" <> inspect(ledger)
  defp queue_stream(intent), do: "queue:" <> intent.queue <> ":" <> to_string(intent.shard)
  defp intent_stream(intent_id), do: "intent:" <> intent_id

  defp supported_commit?(%CommitRequest{} = request) do
    Enum.all?(
      request.preconditions,
      &(&1.type in [
          :command_absent,
          :command_replay,
          :stream_version,
          :intent_status,
          :claim_fence,
          :shard_lease,
          :outbox_unacked
        ])
    ) and
      Enum.all?(
        request.writes,
        &(&1.type in [
            :append_signal,
            :put_idempotency,
            :put_state,
            :put_claim,
            :delete_claim,
            :put_shard_lease,
            :delete_shard_lease,
            :put_outbox,
            :ack_outbox
          ])
      )
  end

  defp check_commit_preconditions(state, %CommitRequest{} = request) do
    Enum.reduce_while(request.preconditions, :ok, fn
      %{type: :command_replay, key: command_id}, :ok ->
        case Map.fetch(state.commands, command_id) do
          {:ok, entry} ->
            if entry.signature == command_signature(request) do
              {:halt, {:replay, entry}}
            else
              {:halt, {:error, Conflict.command_conflict(command_id, entry.signature, command_signature(request))}}
            end

          :error ->
            {:halt, {:error, Conflict.new(:command_replay, key: command_id, expected: :present, actual: :absent)}}
        end

      %{type: :command_absent, key: command_id}, :ok ->
        if Map.has_key?(state.commands, command_id) do
          {:halt, {:error, Conflict.command_conflict(command_id, :absent, :present)}}
        else
          {:cont, :ok}
        end

      %{type: :stream_version, stream: stream, expected: expected}, :ok ->
        actual = stream_version(state, stream)

        if actual == expected do
          {:cont, :ok}
        else
          {:halt, {:error, Conflict.stream_version(stream, expected, actual)}}
        end

      %{type: :intent_status, key: intent_id, expected: expected}, :ok ->
        actual = state.states |> Map.get(intent_id, %{}) |> store_field(:status, :missing)

        if actual in expected do
          {:cont, :ok}
        else
          {:halt, {:error, Conflict.intent_status(intent_id, expected, actual)}}
        end

      %{type: :claim_fence, key: claim_id, expected: expected, metadata: metadata}, :ok ->
        case claim_fence_conflict(state, claim_id, expected, metadata) do
          nil -> {:cont, :ok}
          conflict -> {:halt, {:error, conflict}}
        end

      %{type: :shard_lease, key: key, expected: expected, metadata: metadata}, :ok ->
        case shard_lease_conflict(state, key, expected, metadata) do
          nil -> {:cont, :ok}
          conflict -> {:halt, {:error, conflict}}
        end

      %{type: :outbox_unacked, key: entry_id}, :ok ->
        case Map.fetch(state.outbox, entry_id) do
          {:ok, entry} ->
            if is_nil(store_field(entry, :acked_at)) do
              {:cont, :ok}
            else
              {:halt, {:error, Conflict.outbox(entry_id, :unacked, entry)}}
            end

          :error ->
            {:halt, {:error, Conflict.outbox(entry_id, :unacked, :missing)}}
        end
    end)
  end

  defp apply_commit_writes(state, %CommitRequest{} = request) do
    Enum.reduce(request.writes, {state, nil, []}, fn
      %{type: :append_signal, stream: stream, value: signal}, {acc_state, result, signals} ->
        next_state = %{acc_state | streams: Map.update(acc_state.streams, stream, [signal], &(&1 ++ [signal]))}

        {next_state, result, signals ++ [signal]}

      %{type: :put_idempotency, key: command_id, value: result}, {acc_state, _old_result, signals} ->
        entry = %{signature: command_signature(request), result: result}

        {%{acc_state | commands: Map.put(acc_state.commands, command_id, entry)}, result, signals}

      %{type: :put_state, key: key, value: value}, {acc_state, result, signals} ->
        {%{acc_state | states: Map.put(acc_state.states, key, value)}, result, signals}

      %{type: :put_claim, key: key, value: value}, {acc_state, result, signals} ->
        {%{acc_state | claims: Map.put(acc_state.claims, key, value)}, result, signals}

      %{type: :delete_claim, key: key}, {acc_state, result, signals} ->
        {%{acc_state | claims: Map.delete(acc_state.claims, key)}, result, signals}

      %{type: :put_shard_lease, key: key, value: value}, {acc_state, result, signals} ->
        {%{acc_state | shard_leases: Map.put(acc_state.shard_leases, key, value)}, result, signals}

      %{type: :delete_shard_lease, key: key}, {acc_state, result, signals} ->
        {%{acc_state | shard_leases: Map.delete(acc_state.shard_leases, key)}, result, signals}

      %{type: :put_outbox, key: key, value: value}, {acc_state, result, signals} ->
        {next_state, _entry} = put_outbox_entry(acc_state, key, value)
        {next_state, result, signals}

      %{type: :ack_outbox, key: key, metadata: metadata}, {acc_state, result, signals} ->
        {ack_outbox_entry(acc_state, key, metadata), result, signals}
    end)
  end

  defp stream_version(state, stream), do: state.streams |> Map.get(stream, []) |> length()

  defp claim_fence_conflict(state, claim_id, expected, metadata) do
    now = Map.get(metadata, :now)

    with {:ok, claim} <- Map.fetch(state.claims, claim_id),
         true <- store_field(claim, :token_hash) == expected.token_hash,
         true <- claim_state_current?(state, claim_id, claim),
         true <- store_lease_current?(claim, now) do
      nil
    else
      _reason -> Conflict.claim_fence(claim_id, expected, Map.get(state.claims, claim_id, :missing))
    end
  end

  defp claim_state_current?(state, claim_id, claim) do
    intent_id = store_field(claim, :intent_id)
    intent_state = Map.get(state.states, intent_id, %{})

    store_field(intent_state, :status) == :claimed and store_field(intent_state, :claim_id) in [nil, claim_id]
  end

  defp store_lease_current?(_value, nil), do: true

  defp store_lease_current?(value, now) do
    case store_field(value, :lease_until) do
      %DateTime{} = lease_until -> DateTime.compare(lease_until, now) == :gt
      _value -> false
    end
  end

  defp store_field(value, key, default \\ nil)
  defp store_field(value, key, default) when is_map(value), do: Map.get(value, key, default)
  defp store_field(_value, _key, default), do: default

  defp shard_lease_conflict(state, key, expected, metadata) do
    lease = Map.get(state.shard_leases, key)
    now = Map.get(metadata, :now)

    cond do
      Map.has_key?(expected, :available_at) and (is_nil(lease) or store_lease_expired?(lease, expected.available_at)) ->
        nil

      Map.has_key?(expected, :expired_at_or_before) and not is_nil(lease) and
          store_lease_expired?(lease, expected.expired_at_or_before) ->
        nil

      Map.get(expected, :status) == :current and not is_nil(lease) and
        store_field(lease, :owner_id) == expected.owner_id and store_lease_current?(lease, now) ->
        nil

      true ->
        Conflict.new(:shard_lease,
          key: key,
          expected: expected,
          actual: lease || :missing,
          message: "shard lease conflict"
        )
    end
  end

  defp apply_shard_lease_request(state, {:shard, operation, attrs}) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    case {Map.fetch(attrs, :queue), Map.fetch(attrs, :shard)} do
      {{:ok, queue}, {:ok, shard}} ->
        key = shard_key(queue, shard)
        now = Map.get(attrs, :now)

        case operation do
          :acquire ->
            lease = shard_lease_value(attrs)
            expected = %{available_at: now}

            if is_nil(shard_lease_conflict(state, key, expected, %{})) do
              next_state = %{state | shard_leases: Map.put(state.shard_leases, key, lease)}
              {:ok, lease, next_state}
            else
              {:error,
               Conflict.new(:shard_lease,
                 key: key,
                 expected: expected,
                 actual: Map.get(state.shard_leases, key, :missing),
                 message: "shard lease conflict"
               )}
            end

          :renew ->
            with nil <-
                   shard_lease_conflict(
                     state,
                     key,
                     %{owner_id: to_string(Map.fetch!(attrs, :owner_id)), status: :current},
                     %{now: now}
                   ) do
              lease = shard_lease_value(attrs)
              next_state = %{state | shard_leases: Map.put(state.shard_leases, key, lease)}
              {:ok, lease, next_state}
            else
              conflict -> {:error, conflict}
            end

          :release ->
            with nil <-
                   shard_lease_conflict(
                     state,
                     key,
                     %{owner_id: to_string(Map.fetch!(attrs, :owner_id)), status: :current},
                     %{now: now}
                   ) do
              lease = Map.fetch!(state.shard_leases, key)
              next_state = %{state | shard_leases: Map.delete(state.shard_leases, key)}
              {:ok, lease, next_state}
            else
              conflict -> {:error, conflict}
            end

          operation when operation in [:expire, :takeover] ->
            expected = %{expired_at_or_before: now}

            with nil <- shard_lease_conflict(state, key, expected, %{}) do
              case operation do
                :expire ->
                  lease = Map.fetch!(state.shard_leases, key)
                  next_state = %{state | shard_leases: Map.delete(state.shard_leases, key)}
                  {:ok, lease, next_state}

                :takeover ->
                  lease = shard_lease_value(attrs)
                  next_state = %{state | shard_leases: Map.put(state.shard_leases, key, lease)}
                  {:ok, lease, next_state}
              end
            else
              conflict -> {:error, conflict}
            end

          _operation ->
            :unsupported
        end

      _missing ->
        :unsupported
    end
  end

  defp apply_shard_lease_request(_state, _request), do: :unsupported

  defp normalize_listing_request(%Listing{} = listing), do: {:ok, listing}

  defp normalize_listing_request({type, attrs})
       when type in [:due_intents, :expired_claims] and (is_map(attrs) or is_list(attrs)) do
    attrs = normalize_attrs(attrs)

    with {:ok, queue} <- Map.fetch(attrs, :queue),
         {:ok, %DateTime{} = at} <- Map.fetch(attrs, :at) do
      {:ok, Listing.new(type, Map.merge(attrs, %{queue: to_string(queue), at: at}))}
    else
      _error -> :unsupported
    end
  end

  defp normalize_listing_request(_request), do: :unsupported

  defp list_store_v1(state, %Listing{} = listing) do
    state.states
    |> Map.values()
    |> Enum.filter(&listing_match?(&1, listing))
    |> Enum.sort_by(&listing_sort_key(&1, listing))
    |> Enum.take(listing.limit)
  end

  defp listing_match?(state, %Listing{type: :due_intents} = listing) do
    store_field(state, :queue) == listing.queue and shard_match?(state, listing) and
      store_field(state, :status) in [:available, :retry_scheduled] and
      DateTime.compare(store_field(state, :visible_at), listing.at) != :gt
  end

  defp listing_match?(state, %Listing{type: :expired_claims} = listing) do
    store_field(state, :queue) == listing.queue and shard_match?(state, listing) and
      store_field(state, :status) == :claimed and
      DateTime.compare(store_field(state, :lease_until), listing.at) != :gt
  end

  defp shard_match?(_state, %Listing{shard: nil}), do: true
  defp shard_match?(state, %Listing{shard: shard}), do: store_field(state, :shard) == shard

  defp listing_sort_key(state, %Listing{type: :due_intents}) do
    {-store_field(state, :priority, 0), DateTime.to_unix(store_field(state, :visible_at), :microsecond),
     store_field(state, :intent_id)}
  end

  defp listing_sort_key(state, %Listing{type: :expired_claims}) do
    {DateTime.to_unix(store_field(state, :lease_until), :microsecond), store_field(state, :intent_id)}
  end

  defp apply_outbox_request(state, %Outbox{} = request), do: do_apply_outbox_request(state, request)

  defp apply_outbox_request(state, {type, attrs})
       when type in [:insert, :read, :ack, :replay] and (is_map(attrs) or is_list(attrs)) do
    apply_outbox_request(state, Outbox.new(type, attrs))
  end

  defp apply_outbox_request(_state, _request), do: :unsupported

  defp do_apply_outbox_request(state, %Outbox{type: :insert, key: key, value: value}) when not is_nil(key) do
    {next_state, entry} = put_outbox_entry(state, key, value)
    {:ok, entry, next_state}
  end

  defp do_apply_outbox_request(state, %Outbox{type: :read, consumer: consumer} = request) when not is_nil(consumer) do
    entries =
      state.outbox
      |> Map.values()
      |> Enum.filter(&(is_nil(store_field(&1, :acked_at)) and outbox_after_cursor?(&1, request.cursor)))
      |> Enum.sort_by(&store_field(&1, :sequence))
      |> Enum.take(request.limit)

    {:ok, entries, state}
  end

  defp do_apply_outbox_request(state, %Outbox{type: :replay} = request) do
    entries =
      state.outbox
      |> Map.values()
      |> Enum.filter(&outbox_after_cursor?(&1, request.cursor))
      |> Enum.sort_by(&store_field(&1, :sequence))
      |> Enum.take(request.limit)

    {:ok, entries, state}
  end

  defp do_apply_outbox_request(state, %Outbox{type: :ack, key: key, consumer: consumer, metadata: metadata})
       when not is_nil(key) and not is_nil(consumer) do
    case Map.fetch(state.outbox, key) do
      {:ok, entry} ->
        if is_nil(store_field(entry, :acked_at)) do
          acked = acked_outbox_entry(entry, Map.put(metadata, :consumer, consumer))
          {:ok, acked, %{state | outbox: Map.put(state.outbox, key, acked)}}
        else
          {:error, Conflict.outbox(key, :unacked, entry)}
        end

      :error ->
        {:error, Conflict.outbox(key, :unacked, :missing)}
    end
  end

  defp do_apply_outbox_request(_state, _request), do: :unsupported

  defp shard_lease_value(attrs) do
    %{
      queue: to_string(Map.fetch!(attrs, :queue)),
      shard: Map.fetch!(attrs, :shard),
      owner_id: to_string(Map.fetch!(attrs, :owner_id)),
      lease_until: Map.fetch!(attrs, :lease_until)
    }
  end

  defp store_lease_expired?(lease, now), do: DateTime.compare(store_field(lease, :lease_until), now) != :gt
  defp shard_key(queue, shard), do: "shard:" <> to_string(queue) <> ":" <> to_string(shard)

  defp window(values, opts) do
    opts = normalize_attrs(opts)
    cursor = opts |> Map.get(:cursor, 0) |> non_negative_or(0)
    limit = opts |> Map.get(:limit, length(values)) |> positive_or(length(values))

    values
    |> Enum.drop(cursor)
    |> Enum.take(limit)
  end

  defp non_negative_or(value, _default) when is_integer(value) and value >= 0, do: value
  defp non_negative_or(_value, default), do: default

  defp positive_or(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_or(_value, default), do: default

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs

  defp put_outbox_entry(state, key, value) do
    sequence = store_field(value, :sequence, state.next_outbox_sequence)

    entry =
      value
      |> Map.put(:key, key)
      |> Map.put(:sequence, sequence)
      |> Map.put_new(:acked_at, nil)

    next_sequence = max(state.next_outbox_sequence, sequence + 1)
    next_state = %{state | outbox: Map.put(state.outbox, key, entry), next_outbox_sequence: next_sequence}

    {next_state, entry}
  end

  defp ack_outbox_entry(state, key, metadata) do
    outbox = Map.update!(state.outbox, key, &acked_outbox_entry(&1, metadata))
    %{state | outbox: outbox}
  end

  defp acked_outbox_entry(entry, metadata) do
    entry
    |> Map.put(:acked_at, Map.get(metadata, :acked_at))
    |> Map.put(:consumer, Map.get(metadata, :consumer))
  end

  defp outbox_after_cursor?(_entry, nil), do: true
  defp outbox_after_cursor?(entry, cursor) when is_integer(cursor), do: store_field(entry, :sequence) > cursor
  defp outbox_after_cursor?(_entry, _cursor), do: true

  defp command_signature(%CommitRequest{} = request), do: {request.operation, request.command}

  defp unsupported_store_v1(callback, request), do: {:error, {:unsupported_store_v1_request, callback, request}}
end
