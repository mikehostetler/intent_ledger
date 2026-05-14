defmodule IntentLedger.Runtime.Execution do
  @moduledoc false

  alias IntentLedger.{BedrockStore, Context, Intent, Telemetry}

  @spec perform(module(), term(), map()) :: IntentLedger.Handler.result()
  @doc false
  def perform(handler, queue_payload, job_meta) do
    start = System.monotonic_time()
    job_meta = normalize_job_meta(job_meta)

    {ledger, intent_id, result} =
      case decode_queue_payload(queue_payload) do
        {:ok, %{ledger: ledger, intent_id: intent_id}} ->
          result =
            case BedrockStore.fetch(ledger, intent_id) do
              {:ok, intent} ->
                if Intent.runnable?(intent) do
                  execute_handler(ledger, handler, intent, job_meta)
                else
                  :ok
                end

              {:error, :not_found} ->
                {:discard, :intent_not_found}
            end

          {ledger, intent_id, result}

        {:error, reason} ->
          {nil, nil, {:discard, reason}}
      end

    Telemetry.emit(:handler, result, start, ledger, handler_metadata(handler, intent_id, job_meta))
    result
  end

  defp execute_handler(ledger, handler, %Intent{} = intent, job_meta) do
    with {:ok, started} <- mark_started(ledger, intent, job_meta),
         {:ok, payload} <- validate_payload(handler, started.payload) do
      context = Context.new(ledger, started, job_meta)

      handler
      |> safe_handle(payload, context)
      |> normalize_handler_result(handler)
    else
      {:error, {:invalid_payload, _errors} = reason} ->
        {:discard, reason}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp normalize_handler_result(:ok, _handler), do: :ok

  defp normalize_handler_result({:ok, result}, handler) do
    case validate_result(handler, result) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:discard, reason}
    end
  end

  defp normalize_handler_result({:discard, reason}, _handler), do: {:discard, reason}

  defp normalize_handler_result({:snooze, delay_ms}, _handler)
       when is_integer(delay_ms) and delay_ms >= 0 do
    {:snooze, delay_ms}
  end

  defp normalize_handler_result({:error, reason}, _handler), do: {:error, reason}

  defp normalize_handler_result(other, _handler) do
    reason = {:invalid_handler_return, other}
    {:discard, reason}
  end

  defp mark_started(ledger, intent, job_meta) do
    BedrockStore.update_intent(
      ledger,
      intent.id,
      :started,
      %{attempt: Map.fetch!(job_meta, :attempt), queue: intent.queue, topic: intent.topic},
      fn intent, now ->
        %{intent | status: :started, attempt: Map.fetch!(job_meta, :attempt), updated_at: now, error: nil}
      end
    )
  end

  defp safe_handle(handler, payload, context) do
    handler.handle(payload, context)
  rescue
    exception -> {:error, {:exception, exception}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp validate_payload(handler, payload) do
    case handler.__intent_handler__().payload_schema do
      nil -> {:ok, payload}
      schema -> parse_schema(schema, payload, :invalid_payload)
    end
  end

  defp validate_result(handler, result) do
    case handler.__intent_handler__().result_schema do
      nil -> {:ok, result}
      schema -> parse_schema(schema, result, :invalid_result)
    end
  end

  defp parse_schema(schema, value, error_tag) do
    case Zoi.parse(schema, value) do
      {:ok, parsed} -> {:ok, parsed}
      {:error, errors} -> {:error, {error_tag, errors}}
    end
  end

  defp decode_queue_payload(%{raw: binary}) when is_binary(binary), do: decode_raw_queue_payload(binary)
  defp decode_queue_payload(%{"raw" => binary}) when is_binary(binary), do: decode_raw_queue_payload(binary)
  defp decode_queue_payload(%{ledger: ledger, intent_id: intent_id}), do: {:ok, %{ledger: ledger, intent_id: intent_id}}

  defp decode_queue_payload(%{"ledger" => ledger, "intent_id" => intent_id}) when is_binary(ledger) do
    {:ok, %{ledger: String.to_existing_atom(ledger), intent_id: intent_id}}
  rescue
    ArgumentError -> {:error, {:unknown_ledger, ledger}}
  end

  defp decode_queue_payload(_payload), do: {:error, :invalid_queue_payload}

  defp decode_raw_queue_payload(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    ArgumentError -> {:error, :invalid_queue_payload}
  end

  defp normalize_job_meta(meta) do
    %{
      topic: Map.get(meta, :topic),
      queue_id: Map.get(meta, :queue_id),
      item_id: Map.get(meta, :item_id),
      attempt: Map.get(meta, :attempt, 1)
    }
  end

  defp handler_metadata(handler, intent_id, job_meta) do
    [
      handler: handler,
      intent_id: intent_id,
      topic: Map.get(job_meta, :topic),
      queue: Map.get(job_meta, :queue_id),
      item_id: Map.get(job_meta, :item_id),
      attempt: Map.get(job_meta, :attempt)
    ]
  end
end
