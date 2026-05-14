defmodule IntentLedger.BedrockStore do
  @moduledoc false

  alias Bedrock.Encoding.Tuple, as: TupleEncoding
  alias Bedrock.Keyspace
  alias IntentLedger.{Intent, Signal, Time}

  @type stream_source :: :ledger | :outbox | {:intent, String.t()}
  @type projection_ref :: module() | String.t()
  @type intent_status ::
          :enqueued
          | :started
          | :completed
          | :failed
          | :retry_scheduled
          | :discarded
          | :canceled
          | :ambiguous

  @intent_statuses [
    :enqueued,
    :started,
    :completed,
    :failed,
    :retry_scheduled,
    :discarded,
    :canceled,
    :ambiguous
  ]
  @intent_status_by_name Map.new(@intent_statuses, fn status -> {Atom.to_string(status), status} end)

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
  def create_intent(ledger, repo, root, %Intent{} = intent, opts \\ []) do
    keys = key_index_keyspace(root)
    intents = intent_keyspace(root)

    case existing_by_key(repo, keys, intents, intent.key) do
      {:ok, %Intent{} = existing} ->
        {:ok, existing, :existing}

      :error ->
        now = Keyword.get(opts, :now, Time.utc_now())
        intent = %{intent | created_at: intent.created_at || now, updated_at: now}

        repo.put(intents, intent.id, encode(intent))
        put_status_index(repo, root, intent)

        if intent.key do
          repo.put(keys, intent.key, intent.id)
        end

        append_lifecycle(repo, root, ledger, intent, :enqueued, %{
          key: intent.key,
          topic: intent.topic,
          queue: intent.queue,
          scheduled_at: intent.scheduled_at,
          max_attempts: intent.max_attempts,
          priority: intent.priority
        })

        {:ok, intent, :created}
    end
  end

  @doc false
  @spec fetch(module(), String.t()) :: {:ok, Intent.t()} | {:error, :not_found}
  def fetch(ledger, intent_id) do
    transact(ledger, fn repo, root -> fetch(repo, root, intent_id) end)
  end

  @doc false
  @spec fetch(module(), Keyspace.t(), String.t()) :: {:ok, Intent.t()} | {:error, :not_found}
  def fetch(repo, root, intent_id) when is_binary(intent_id) do
    case repo.get(intent_keyspace(root), intent_id) do
      nil -> {:error, :not_found}
      value -> {:ok, decode(value)}
    end
  end

  @doc false
  @spec put_intent(module(), Keyspace.t(), Intent.t()) :: :ok
  def put_intent(repo, root, %Intent{} = intent) do
    previous =
      case fetch(repo, root, intent.id) do
        {:ok, %Intent{} = existing} -> existing
        {:error, :not_found} -> nil
      end

    repo.put(intent_keyspace(root), intent.id, encode(intent))
    update_status_index(repo, root, previous, intent)
  end

  @doc false
  @spec record_lifecycle(module(), Keyspace.t(), module(), Intent.t(), atom(), map()) :: Jido.Signal.t()
  def record_lifecycle(repo, root, ledger, %Intent{} = intent, event, data) do
    append_lifecycle(repo, root, ledger, intent, event, data)
  end

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
  def update_intent(repo, root, ledger, intent_id, event, data, update_fun)
      when is_atom(event) and is_map(data) and is_function(update_fun, 2) do
    with {:ok, intent} <- fetch(repo, root, intent_id) do
      now = Time.utc_now()
      next = update_fun.(intent, now)
      repo.put(intent_keyspace(root), next.id, encode(next))
      update_status_index(repo, root, intent, next)
      append_lifecycle(repo, root, ledger, next, event, data)
      {:ok, next}
    end
  end

  @doc false
  @spec history(module(), String.t(), keyword()) :: {:ok, [Jido.Signal.t()]} | {:error, term()}
  def history(ledger, intent_id, opts \\ []) do
    replay(ledger, {:intent, intent_id}, opts)
  end

  @doc false
  @spec replay(module(), stream_source(), keyword()) :: {:ok, [Jido.Signal.t()]} | {:error, term()}
  def replay(ledger, source, opts \\ [])

  def replay(ledger, :outbox, opts) do
    with {:ok, entries} <- outbox(ledger, opts) do
      {:ok, Enum.map(entries, & &1.signal)}
    end
  end

  def replay(ledger, source, opts) do
    with {:ok, stream} <- stream_name(source),
         {:ok, cursor} <- non_negative_integer_option(opts, :cursor, 0),
         {:ok, limit} <- positive_integer_option(opts, :limit, 100) do
      transact(ledger, fn repo, root ->
        keyspace = stream_keyspace(root, stream)

        entries =
          keyspace
          |> range_from_cursor(cursor)
          |> repo.get_range(limit: limit)
          |> Stream.map(fn {_key, value} -> decode(value) end)
          |> Enum.to_list()

        {:ok, Enum.map(entries, & &1.signal)}
      end)
    end
  end

  @doc false
  @spec outbox(module(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def outbox(ledger, opts \\ []) do
    with {:ok, cursor} <- non_negative_integer_option(opts, :cursor, 0),
         {:ok, limit} <- positive_integer_option(opts, :limit, 100) do
      transact(ledger, fn repo, root ->
        keyspace = outbox_keyspace(root)

        entries =
          keyspace
          |> range_from_cursor(cursor)
          |> repo.get_range(limit: limit)
          |> Stream.map(fn {_key, value} -> decode(value) end)
          |> Enum.to_list()

        {:ok, entries}
      end)
    end
  end

  @doc false
  @spec intents(module(), keyword()) :: {:ok, [Intent.t()]} | {:error, term()}
  def intents(ledger, opts \\ []) do
    with {:ok, limit} <- positive_integer_option(opts, :limit, 100),
         {:ok, statuses} <- status_filter(Keyword.get(opts, :status)) do
      queue = Keyword.get(opts, :queue)
      topic = Keyword.get(opts, :topic)

      transact(ledger, fn repo, root ->
        intents =
          statuses
          |> intent_sources(repo, root, limit)
          |> Stream.map(&decode/1)
          |> Stream.filter(&matches_filter?(&1, :queue, queue))
          |> Stream.filter(&matches_filter?(&1, :topic, topic))
          |> Enum.take(limit)

        {:ok, intents}
      end)
    end
  end

  @doc false
  @spec projections(module(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def projections(ledger, opts \\ []) do
    with {:ok, limit} <- positive_integer_option(opts, :limit, 100) do
      transact(ledger, fn repo, root ->
        entries =
          root
          |> projection_offset_keyspace()
          |> repo.get_range(limit: limit)
          |> Stream.map(fn {_key, value} -> decode(value) end)
          |> Enum.to_list()

        {:ok, entries}
      end)
    end
  end

  @doc false
  @spec projection_cursor(module(), projection_ref(), keyword()) ::
          {:ok, non_neg_integer() | nil} | {:error, term()}
  def projection_cursor(ledger, projection, _opts \\ []) do
    transact(ledger, fn repo, root ->
      with {:ok, key} <- projection_key(projection) do
        case repo.get(projection_offset_keyspace(root), key) do
          nil -> {:ok, nil}
          value -> {:ok, decode(value).cursor}
        end
      end
    end)
  end

  @doc false
  @spec put_projection_cursor(module(), projection_ref(), non_neg_integer(), keyword()) :: :ok | {:error, term()}
  def put_projection_cursor(ledger, projection, cursor, _opts \\ []) do
    with {:ok, key} <- projection_key(projection),
         :ok <- validate_projection_cursor(cursor) do
      transact(ledger, fn repo, root ->
        repo.put(
          projection_offset_keyspace(root),
          key,
          encode(%{projection: key, cursor: cursor, updated_at: Time.utc_now()})
        )

        :ok
      end)
    end
  end

  @doc false
  @spec root_keyspace(module()) :: Keyspace.t()
  def root_keyspace(ledger), do: Keyspace.new("intent_ledger/#{module_key(ledger)}/")

  defp append_lifecycle(repo, root, ledger, %Intent{} = intent, event, data) do
    signal = Signal.lifecycle(event, ledger, intent, data)

    append_stream(repo, root, "ledger", signal)
    append_stream(repo, root, "intent:#{intent.id}", signal)
    append_outbox(repo, root, signal)

    signal
  end

  defp append_stream(repo, root, stream, signal) do
    versions = stream_version_keyspace(root)
    streams = stream_keyspace(root, stream)
    cursor = next_counter(repo, versions, stream)
    repo.put(streams, cursor, encode(%{stream: stream, cursor: cursor, signal: signal}))
    cursor
  end

  defp append_outbox(repo, root, signal) do
    cursor = next_counter(repo, outbox_version_keyspace(root), "global")
    repo.put(outbox_keyspace(root), cursor, encode(%{cursor: cursor, signal: signal}))
    cursor
  end

  defp update_status_index(repo, root, nil, %Intent{} = next), do: put_status_index(repo, root, next)

  defp update_status_index(repo, root, %Intent{} = previous, %Intent{} = next) do
    if previous.status != next.status do
      clear_status_index(repo, root, previous)
    end

    put_status_index(repo, root, next)
  end

  defp put_status_index(repo, root, %Intent{} = intent) do
    root
    |> status_index_keyspace(intent.status)
    |> repo.put(intent.id, encode(intent))
  end

  defp clear_status_index(repo, root, %Intent{} = intent) do
    root
    |> status_index_keyspace(intent.status)
    |> repo.clear(intent.id)
  end

  defp intent_sources(nil, repo, root, limit) do
    root
    |> intent_keyspace()
    |> repo.get_range(limit: limit)
    |> Stream.map(fn {_key, value} -> value end)
  end

  defp intent_sources(statuses, repo, root, limit) do
    statuses
    |> Stream.flat_map(fn status ->
      root
      |> status_index_keyspace(status)
      |> repo.get_range(limit: limit)
      |> Stream.map(fn {_key, value} -> value end)
    end)
  end

  defp matches_filter?(_intent, _field, nil), do: true
  defp matches_filter?(%Intent{} = intent, field, value), do: Map.fetch!(intent, field) == value

  defp status_filter(nil), do: {:ok, nil}

  defp status_filter(statuses) when is_list(statuses) do
    statuses
    |> Enum.reduce_while({:ok, []}, fn status, {:ok, acc} ->
      case normalize_status(status) do
        {:ok, status} -> {:cont, {:ok, [status | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, statuses} -> {:ok, statuses |> Enum.reverse() |> Enum.uniq()}
      error -> error
    end
  end

  defp status_filter(status) do
    with {:ok, status} <- normalize_status(status), do: {:ok, [status]}
  end

  defp normalize_status(status) when status in @intent_statuses, do: {:ok, status}

  defp normalize_status(status) when is_binary(status) do
    case Map.fetch(@intent_status_by_name, status) do
      {:ok, status} -> {:ok, status}
      :error -> {:error, {:invalid_status, status}}
    end
  end

  defp normalize_status(status), do: {:error, {:invalid_status, status}}

  defp non_negative_integer_option(opts, key, default) do
    value = Keyword.get(opts, key, default)

    if is_integer(value) and value >= 0 do
      {:ok, value}
    else
      {:error, {:invalid_option, key, value}}
    end
  end

  defp positive_integer_option(opts, key, default) do
    value = Keyword.get(opts, key, default)

    if is_integer(value) and value > 0 do
      {:ok, value}
    else
      {:error, {:invalid_option, key, value}}
    end
  end

  defp next_counter(repo, keyspace, key) do
    next =
      case repo.get(keyspace, key) do
        nil -> 1
        value -> decode(value) + 1
      end

    repo.put(keyspace, key, encode(next))
    next
  end

  defp existing_by_key(_repo, _keys, _intents, nil), do: :error

  defp existing_by_key(repo, keys, intents, key) do
    case repo.get(keys, key) do
      nil ->
        :error

      intent_id ->
        case repo.get(intents, intent_id) do
          nil -> :error
          value -> {:ok, decode(value)}
        end
    end
  end

  defp stream_name(:ledger), do: {:ok, "ledger"}
  defp stream_name({:intent, intent_id}) when is_binary(intent_id), do: {:ok, "intent:#{intent_id}"}
  defp stream_name(source), do: {:error, {:unsupported_replay_source, source}}

  defp projection_key(projection) when projection in [nil, true, false], do: {:error, {:invalid_projection, projection}}
  defp projection_key(projection) when is_atom(projection), do: {:ok, "module:#{module_key(projection)}"}

  defp projection_key(projection) when is_binary(projection) and byte_size(projection) > 0,
    do: {:ok, "name:#{projection}"}

  defp projection_key(projection), do: {:error, {:invalid_projection, projection}}

  defp validate_projection_cursor(cursor) when is_integer(cursor) and cursor >= 0, do: :ok
  defp validate_projection_cursor(cursor), do: {:error, {:invalid_projection_cursor, cursor}}

  defp repo!(ledger), do: ledger.__intent_ledger__().repo

  defp module_key(module) do
    module
    |> Module.split()
    |> Enum.join("_")
    |> Macro.underscore()
  end

  defp intent_keyspace(root), do: Keyspace.partition(root, "intents/")
  defp key_index_keyspace(root), do: Keyspace.partition(root, "keys/")
  defp stream_version_keyspace(root), do: Keyspace.partition(root, "stream_versions/")
  defp outbox_version_keyspace(root), do: Keyspace.partition(root, "outbox_versions/")
  defp outbox_keyspace(root), do: Keyspace.partition(root, "outbox/", key_encoding: TupleEncoding)
  defp status_index_keyspace(root, status), do: Keyspace.partition(root, "intent_status/#{status}/")

  defp projection_offset_keyspace(root),
    do: Keyspace.partition(root, "projection_offsets/", key_encoding: TupleEncoding)

  defp stream_keyspace(root, stream), do: Keyspace.partition(root, "streams/#{stream}/", key_encoding: TupleEncoding)

  defp range_from_cursor(keyspace, cursor) when is_integer(cursor) and cursor >= 0 do
    prefix = Keyspace.prefix(keyspace)
    {Keyspace.pack(keyspace, cursor + 1), prefix <> <<0xFF>>}
  end

  defp encode(term), do: :erlang.term_to_binary(term)
  defp decode(binary), do: :erlang.binary_to_term(binary)
end
