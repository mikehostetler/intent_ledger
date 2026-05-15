defmodule IntentLedger.Command do
  @moduledoc """
  Normalized command boundary for IntentLedger mutations.

  Direct API calls and `%Jido.Signal{}` command envelopes both normalize into
  this struct before they touch the runtime state machine. Signals stay at the
  boundary; the runtime works with domain commands.
  """

  alias Jido.Signal.ID

  @type type :: :enqueue | :cancel | :requeue | :mark_ambiguous
  @type ingress :: :direct | :signal

  @types [:enqueue, :cancel, :requeue, :mark_ambiguous]
  @ingresses [:direct, :signal]

  @schema Zoi.struct(__MODULE__, %{
            type: Zoi.enum(@types),
            topic: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            payload: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            intent_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            reason: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            opts: Zoi.any() |> Zoi.default([]) |> Zoi.optional(),
            signal: Zoi.any() |> Zoi.default(nil) |> Zoi.optional(),
            command_id: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            ingress: Zoi.enum(@ingresses) |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            source: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional(),
            submitted_at: Zoi.string() |> Zoi.nullable() |> Zoi.default(nil) |> Zoi.optional()
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
    new(:enqueue, topic: topic, payload: payload, opts: put_direct_command_metadata(opts))
  end

  @doc false
  @spec cancel(term(), term(), keyword()) :: {:ok, t()}
  def cancel(intent_id, reason, opts \\ []) do
    new(:cancel, intent_id: intent_id, reason: reason, opts: put_direct_command_metadata(opts))
  end

  @doc false
  @spec requeue(term(), keyword()) :: {:ok, t()}
  def requeue(intent_id, opts \\ []) do
    new(:requeue, intent_id: intent_id, opts: put_direct_command_metadata(opts))
  end

  @doc false
  @spec mark_ambiguous(term(), term(), keyword()) :: {:ok, t()}
  def mark_ambiguous(intent_id, reason, opts \\ []) do
    new(:mark_ambiguous, intent_id: intent_id, reason: reason, opts: put_direct_command_metadata(opts))
  end

  @doc false
  @spec direct_opts(keyword()) :: keyword()
  def direct_opts(opts), do: put_direct_command_metadata(opts)

  @doc false
  @spec build(type(), keyword()) :: {:ok, t()} | {:error, term()}
  def build(type, attrs), do: new(type, attrs)

  @doc false
  @spec from_signal(Jido.Signal.t(), keyword()) :: {:ok, t()} | {:error, term()}
  defdelegate from_signal(signal, opts \\ []), to: IntentLedger.Command.Signal

  @doc false
  @spec to_signal(module(), type() | atom() | String.t(), map() | keyword(), keyword()) ::
          {:ok, Jido.Signal.t()} | {:error, term()}
  defdelegate to_signal(ledger, command, attrs, opts \\ []), to: IntentLedger.Command.Signal

  @doc false
  @spec signal_type(type()) :: {:ok, String.t()} | {:error, term()}
  defdelegate signal_type(type), to: IntentLedger.Command.Signal

  defp new(type, attrs) do
    attrs = Keyword.put(attrs, :type, type)
    attrs = Keyword.merge(attrs, command_fields(Keyword.get(attrs, :opts, [])))
    struct = struct!(__MODULE__, attrs)

    case Zoi.parse(@schema, struct) do
      {:ok, command} -> {:ok, command}
      {:error, errors} -> {:error, {:invalid_command, errors}}
    end
  end

  defp put_direct_command_metadata(opts) do
    metadata =
      opts
      |> Keyword.get(:metadata, %{})
      |> normalize_metadata()

    command_id =
      opts |> Keyword.get(:command_id, metadata_field(metadata, :command_id)) |> default_string(&ID.generate!/0)

    source =
      opts |> Keyword.get(:source, metadata_field(metadata, :command_source)) |> default_string(fn -> "direct" end)

    submitted_at =
      opts
      |> Keyword.get(:submitted_at, metadata_field(metadata, :command_submitted_at))
      |> normalize_submitted_at()

    metadata =
      metadata
      |> Map.put_new(:command_id, command_id)
      |> Map.put_new(:command_ingress, :direct)
      |> Map.put_new(:command_source, source)
      |> Map.put_new(:command_submitted_at, submitted_at)

    opts
    |> Keyword.put(:metadata, metadata)
    |> put_command_metadata(%{
      command_id: command_id,
      command_ingress: :direct,
      command_source: source,
      command_submitted_at: submitted_at
    })
  end

  defp put_command_metadata(opts, command_metadata) do
    existing =
      opts
      |> Keyword.get(:command_metadata, %{})
      |> normalize_metadata()

    Keyword.put(opts, :command_metadata, Map.merge(existing, command_metadata))
  end

  defp normalize_metadata(metadata) when is_map(metadata), do: metadata

  defp normalize_metadata(metadata) when is_list(metadata) do
    Map.new(metadata)
  rescue
    ArgumentError -> %{}
  end

  defp normalize_metadata(_metadata), do: %{}

  defp command_fields(opts) do
    metadata =
      opts
      |> Keyword.get(:command_metadata, %{})
      |> normalize_metadata()

    [
      command_id: metadata_field(metadata, :command_id),
      ingress: metadata_field(metadata, :command_ingress),
      source: metadata_field(metadata, :command_source),
      submitted_at: metadata_field(metadata, :command_submitted_at)
    ]
  end

  defp metadata_field(metadata, field), do: Map.get(metadata, field, Map.get(metadata, Atom.to_string(field)))

  defp default_string(nil, fun), do: fun.()
  defp default_string("", fun), do: fun.()
  defp default_string(value, _fun), do: to_string(value)

  defp normalize_submitted_at(nil), do: DateTime.utc_now() |> DateTime.to_iso8601()
  defp normalize_submitted_at(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp normalize_submitted_at(value) when is_binary(value), do: value
  defp normalize_submitted_at(value), do: to_string(value)
end
