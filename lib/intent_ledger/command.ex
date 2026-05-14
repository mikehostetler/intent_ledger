defmodule IntentLedger.Command do
  @moduledoc """
  Normalized command boundary for IntentLedger mutations.

  Direct API calls and `%Jido.Signal{}` command envelopes both normalize into
  this struct before they touch the runtime state machine. Signals stay at the
  boundary; the runtime works with domain commands.
  """

  @type type :: :enqueue | :cancel | :requeue | :mark_ambiguous

  @types [:enqueue, :cancel, :requeue, :mark_ambiguous]

  @signal_types %{
    enqueue: "intent.command.enqueue",
    cancel: "intent.command.cancel",
    requeue: "intent.command.requeue",
    mark_ambiguous: "intent.command.mark_ambiguous"
  }
  @type_by_signal_type Map.new(@signal_types, fn {type, signal_type} -> {signal_type, type} end)

  @schema Zoi.struct(__MODULE__, %{
            type: Zoi.enum(@types),
            topic: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            payload: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            intent_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            reason: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            opts: Zoi.any() |> Zoi.default([]) |> Zoi.optional(),
            signal: Zoi.any() |> Zoi.default(nil) |> Zoi.optional()
          })

  @type t :: unquote(Zoi.type_spec(@schema))
  @enforce_keys Zoi.Struct.enforce_keys(@schema)
  defstruct Zoi.Struct.struct_fields(@schema)

  @doc """
  Returns the Zoi schema for `t:IntentLedger.Command.t/0`.
  """
  @spec schema() :: Zoi.schema()
  def schema, do: @schema

  @doc false
  @spec enqueue(term(), term(), keyword()) :: {:ok, t()}
  def enqueue(topic, payload, opts \\ []) do
    new(:enqueue, topic: topic, payload: payload, opts: opts)
  end

  @doc false
  @spec cancel(term(), term(), keyword()) :: {:ok, t()}
  def cancel(intent_id, reason, opts \\ []) do
    new(:cancel, intent_id: intent_id, reason: reason, opts: opts)
  end

  @doc false
  @spec requeue(term(), keyword()) :: {:ok, t()}
  def requeue(intent_id, opts \\ []) do
    new(:requeue, intent_id: intent_id, opts: opts)
  end

  @doc false
  @spec mark_ambiguous(term(), term(), keyword()) :: {:ok, t()}
  def mark_ambiguous(intent_id, reason, opts \\ []) do
    new(:mark_ambiguous, intent_id: intent_id, reason: reason, opts: opts)
  end

  @doc false
  @spec from_signal(Jido.Signal.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def from_signal(signal, opts \\ [])

  def from_signal(%Jido.Signal{} = signal, opts) do
    with {:ok, type} <- type_from_signal(signal),
         {:ok, data} <- signal_data(signal) do
      command_from_signal(type, signal, data, opts)
    end
  end

  def from_signal(signal, _opts), do: {:error, {:invalid_command_signal, signal}}

  @doc false
  @spec to_signal(module(), type() | atom() | String.t(), map() | keyword(), keyword()) ::
          {:ok, Jido.Signal.t()} | {:error, term()}
  def to_signal(ledger, command, attrs, opts \\ []) when is_atom(ledger) do
    with {:ok, type} <- normalize_type(command),
         data <- attrs |> Map.new() |> normalize_data_keys(),
         {:ok, signal_type} <- signal_type(type) do
      Jido.Signal.new(signal_type, data,
        source: Keyword.get(opts, :source, source_for(ledger)),
        subject: Keyword.get(opts, :subject, subject_for(type, data)),
        datacontenttype: Keyword.get(opts, :datacontenttype, "application/x-erlang-term"),
        dataschema: Keyword.get(opts, :dataschema, "https://hexdocs.pm/intent_ledger/commands/#{type}/v1"),
        extensions: Keyword.get(opts, :extensions, %{})
      )
    end
  end

  @doc false
  @spec signal_type(type()) :: {:ok, String.t()} | {:error, term()}
  def signal_type(type) do
    case Map.fetch(@signal_types, type) do
      {:ok, signal_type} -> {:ok, signal_type}
      :error -> {:error, {:unsupported_command, type}}
    end
  end

  defp new(type, attrs) do
    struct = struct!(__MODULE__, Keyword.put(attrs, :type, type))

    case Zoi.parse(@schema, struct) do
      {:ok, command} -> {:ok, command}
      {:error, errors} -> {:error, {:invalid_command, errors}}
    end
  end

  defp type_from_signal(%Jido.Signal{type: signal_type}) do
    case Map.fetch(@type_by_signal_type, signal_type) do
      {:ok, type} -> {:ok, type}
      :error -> {:error, {:unsupported_command_signal, signal_type}}
    end
  end

  defp command_from_signal(:enqueue, signal, data, opts) do
    with {:ok, topic} <- required(data, :topic),
         {:ok, payload} <- payload(data) do
      command_opts =
        opts
        |> Keyword.merge(command_opts(data))
        |> Keyword.put_new(:key, field(data, :key) || "signal:#{signal.id}")
        |> put_command_signal_metadata(signal)

      with {:ok, command} <- enqueue(topic, payload, command_opts) do
        {:ok, %{command | signal: signal}}
      end
    end
  end

  defp command_from_signal(:cancel, signal, data, opts) do
    with {:ok, intent_id} <- required(data, :intent_id),
         {:ok, reason} <- required(data, :reason) do
      with {:ok, command} <- cancel(intent_id, reason, put_command_signal_metadata(opts, signal)) do
        {:ok, %{command | signal: signal}}
      end
    end
  end

  defp command_from_signal(:requeue, signal, data, opts) do
    with {:ok, intent_id} <- required(data, :intent_id) do
      command_opts =
        opts
        |> Keyword.merge(command_opts(data))
        |> put_command_signal_metadata(signal)

      with {:ok, command} <- requeue(intent_id, command_opts) do
        {:ok, %{command | signal: signal}}
      end
    end
  end

  defp command_from_signal(:mark_ambiguous, signal, data, opts) do
    with {:ok, intent_id} <- required(data, :intent_id),
         {:ok, reason} <- required(data, :reason) do
      with {:ok, command} <- mark_ambiguous(intent_id, reason, put_command_signal_metadata(opts, signal)) do
        {:ok, %{command | signal: signal}}
      end
    end
  end

  defp signal_data(%Jido.Signal{data: data}) when is_map(data), do: {:ok, normalize_data_keys(data)}
  defp signal_data(%Jido.Signal{data: nil}), do: {:ok, %{}}
  defp signal_data(%Jido.Signal{data: data}), do: {:error, {:invalid_command_signal_data, data}}

  defp payload(data) do
    case fetch_field(data, :payload) do
      {:ok, payload} -> {:ok, payload}
      :error -> {:ok, %{}}
    end
  end

  defp required(data, field) do
    case fetch_field(data, field) do
      {:ok, value} when value not in [nil, ""] -> {:ok, value}
      _missing_or_empty -> {:error, {:missing_command_field, field}}
    end
  end

  defp command_opts(data) do
    known = [
      :key,
      :queue,
      :priority,
      :max_attempts,
      :scheduled_at,
      :context,
      :metadata,
      :root_intent_id,
      :parent_intent_id,
      :depth,
      :correlation_id,
      :causation_id,
      :actor,
      :reason
    ]

    known
    |> Enum.reduce([], fn field, acc ->
      case fetch_field(data, field) do
        {:ok, nil} -> acc
        {:ok, value} -> Keyword.put(acc, field, value)
        :error -> acc
      end
    end)
  end

  defp put_command_signal_metadata(opts, %Jido.Signal{} = signal) do
    metadata =
      opts
      |> Keyword.get(:metadata, %{})
      |> normalize_metadata()
      |> Map.put_new(:command_signal_id, signal.id)
      |> Map.put_new(:command_signal_type, signal.type)

    opts
    |> Keyword.put(:metadata, metadata)
    |> Keyword.put_new(:causation_id, signal.id)
    |> Keyword.put(
      :command_metadata,
      %{
        command_signal_id: signal.id,
        command_signal_type: signal.type,
        command_signal_source: signal.source
      }
    )
  end

  defp normalize_metadata(metadata) when is_map(metadata), do: metadata
  defp normalize_metadata(metadata) when is_list(metadata), do: Map.new(metadata)
  defp normalize_metadata(_metadata), do: %{}

  defp fetch_field(data, field) do
    cond do
      Map.has_key?(data, field) -> {:ok, Map.fetch!(data, field)}
      Map.has_key?(data, Atom.to_string(field)) -> {:ok, Map.fetch!(data, Atom.to_string(field))}
      true -> :error
    end
  end

  defp field(data, field) do
    case fetch_field(data, field) do
      {:ok, value} -> value
      :error -> nil
    end
  end

  defp normalize_data_keys(data) do
    Map.new(data, fn
      {key, value} when is_binary(key) -> {normalize_known_key(key), value}
      pair -> pair
    end)
  end

  defp normalize_known_key(key) do
    case key do
      "topic" -> :topic
      "payload" -> :payload
      "intent_id" -> :intent_id
      "reason" -> :reason
      "key" -> :key
      "queue" -> :queue
      "priority" -> :priority
      "max_attempts" -> :max_attempts
      "scheduled_at" -> :scheduled_at
      "context" -> :context
      "metadata" -> :metadata
      "root_intent_id" -> :root_intent_id
      "parent_intent_id" -> :parent_intent_id
      "depth" -> :depth
      "correlation_id" -> :correlation_id
      "causation_id" -> :causation_id
      "actor" -> :actor
      other -> other
    end
  end

  defp normalize_type(type) when type in @types, do: {:ok, type}

  defp normalize_type(type) when is_binary(type) do
    case Map.fetch(@type_by_signal_type, type) do
      {:ok, type} -> {:ok, type}
      :error -> type |> String.trim() |> String.to_existing_atom() |> normalize_type()
    end
  rescue
    ArgumentError -> {:error, {:unsupported_command, type}}
  end

  defp normalize_type(type), do: {:error, {:unsupported_command, type}}

  defp subject_for(:enqueue, data), do: field(data, :key)
  defp subject_for(_type, data), do: field(data, :intent_id)

  defp source_for(ledger) do
    "/intent_ledger/" <> (ledger |> Module.split() |> Enum.join("."))
  end
end
