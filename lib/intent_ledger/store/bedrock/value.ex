defmodule IntentLedger.Store.Bedrock.Value do
  @moduledoc """
  Versioned value encoding for `IntentLedger.Store.Bedrock`.

  Bedrock values are opaque binaries. This module wraps every persisted record
  in a small schema-versioned envelope before encoding it with Erlang external
  term format:

      %{schema_version: 1, type: "intent", value: %IntentLedger.Intent{}}

  The envelope lets the adapter reject type mismatches and future incompatible
  value versions before applying Store V1 semantics.
  """

  alias IntentLedger.{Claim, Intent, IntentState}

  @schema_version 1
  @types [:intent, :state, :signal, :claim, :shard_lease, :command, :outbox]
  @type_tags Map.new(@types, &{&1, Atom.to_string(&1)})
  @types_by_tag Map.new(@type_tags, fn {type, tag} -> {tag, type} end)

  @type type :: :intent | :state | :signal | :claim | :shard_lease | :command | :outbox
  @type unpack_error ::
          {:invalid_bedrock_value, term()}
          | {:unsupported_bedrock_value_version, term()}
          | {:unexpected_bedrock_value_type, %{expected: type(), actual: type()}}
  @type unpack_result :: {:ok, {type(), term()}} | {:error, unpack_error()}
  @type typed_unpack_result(value) :: {:ok, value} | {:error, unpack_error()}

  @doc """
  Returns the value schema version embedded in every encoded value.
  """
  @spec schema_version() :: pos_integer()
  def schema_version, do: @schema_version

  @doc """
  Returns the value types supported by this codec.
  """
  @spec types() :: [type()]
  def types, do: @types

  @doc """
  Encodes a typed Bedrock value.
  """
  @spec pack(type(), term()) :: binary()
  def pack(type, value) when type in @types do
    value = normalize_value!(type, value)

    %{schema_version: @schema_version, type: Map.fetch!(@type_tags, type), value: value}
    |> :erlang.term_to_binary([:deterministic])
  end

  @doc """
  Decodes a Bedrock value and returns its type.
  """
  @spec unpack(binary()) :: unpack_result()
  def unpack(encoded) when is_binary(encoded) do
    encoded
    |> :erlang.binary_to_term()
    |> decode_envelope()
  rescue
    ArgumentError -> {:error, {:invalid_bedrock_value, :malformed_binary}}
  end

  @doc """
  Decodes a Bedrock value and verifies that it has the expected type.
  """
  @spec unpack(binary(), type()) :: typed_unpack_result(term())
  def unpack(encoded, expected_type) when expected_type in @types do
    case unpack(encoded) do
      {:ok, {^expected_type, value}} ->
        {:ok, value}

      {:ok, {actual_type, _value}} ->
        {:error, {:unexpected_bedrock_value_type, %{expected: expected_type, actual: actual_type}}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Encodes an immutable intent value.
  """
  @spec pack_intent(Intent.t()) :: binary()
  def pack_intent(%Intent{} = intent), do: pack(:intent, intent)

  @doc """
  Decodes an immutable intent value.
  """
  @spec unpack_intent(binary()) :: typed_unpack_result(Intent.t())
  def unpack_intent(encoded), do: unpack(encoded, :intent)

  @doc """
  Encodes a materialized state value.
  """
  @spec pack_state(IntentState.t()) :: binary()
  def pack_state(%IntentState{} = state), do: pack(:state, state)

  @doc """
  Decodes a materialized state value.
  """
  @spec unpack_state(binary()) :: typed_unpack_result(IntentState.t())
  def unpack_state(encoded), do: unpack(encoded, :state)

  @doc """
  Encodes a lifecycle signal value.
  """
  @spec pack_signal(Jido.Signal.t() | map()) :: binary()
  def pack_signal(signal), do: pack(:signal, signal)

  @doc """
  Decodes a lifecycle signal value.
  """
  @spec unpack_signal(binary()) :: typed_unpack_result(Jido.Signal.t() | map())
  def unpack_signal(encoded), do: unpack(encoded, :signal)

  @doc """
  Encodes a claim fence value.
  """
  @spec pack_claim(Claim.t() | map()) :: binary()
  def pack_claim(claim), do: pack(:claim, claim)

  @doc """
  Decodes a claim fence value.
  """
  @spec unpack_claim(binary()) :: typed_unpack_result(Claim.t() | map())
  def unpack_claim(encoded), do: unpack(encoded, :claim)

  @doc """
  Encodes a shard lease value.
  """
  @spec pack_shard_lease(map()) :: binary()
  def pack_shard_lease(lease), do: pack(:shard_lease, lease)

  @doc """
  Decodes a shard lease value.
  """
  @spec unpack_shard_lease(binary()) :: typed_unpack_result(map())
  def unpack_shard_lease(encoded), do: unpack(encoded, :shard_lease)

  @doc """
  Encodes a command replay value.
  """
  @spec pack_command(map()) :: binary()
  def pack_command(command), do: pack(:command, command)

  @doc """
  Decodes a command replay value.
  """
  @spec unpack_command(binary()) :: typed_unpack_result(map())
  def unpack_command(encoded), do: unpack(encoded, :command)

  @doc """
  Encodes an outbox record value.
  """
  @spec pack_outbox(map()) :: binary()
  def pack_outbox(outbox), do: pack(:outbox, outbox)

  @doc """
  Decodes an outbox record value.
  """
  @spec unpack_outbox(binary()) :: typed_unpack_result(map())
  def unpack_outbox(encoded), do: unpack(encoded, :outbox)

  defp decode_envelope(%{schema_version: @schema_version, type: type_tag, value: value}) do
    case Map.fetch(@types_by_tag, type_tag) do
      {:ok, type} -> {:ok, {type, value}}
      :error -> {:error, {:invalid_bedrock_value, {:unknown_type, type_tag}}}
    end
  end

  defp decode_envelope(%{schema_version: version}) do
    {:error, {:unsupported_bedrock_value_version, version}}
  end

  defp decode_envelope(_term), do: {:error, {:invalid_bedrock_value, :missing_envelope}}

  defp normalize_value!(:intent, %Intent{} = value), do: value
  defp normalize_value!(:state, %IntentState{} = value), do: value
  defp normalize_value!(:signal, %Jido.Signal{} = value), do: value
  defp normalize_value!(:signal, value) when is_map(value), do: value
  defp normalize_value!(:claim, %Claim{} = value), do: value
  defp normalize_value!(:claim, value) when is_map(value), do: value
  defp normalize_value!(type, value) when type in [:shard_lease, :command, :outbox] and is_map(value), do: value

  defp normalize_value!(type, value) do
    raise ArgumentError, "invalid #{type} Bedrock value: #{inspect(value)}"
  end
end
