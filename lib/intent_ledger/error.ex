defmodule IntentLedger.Error do
  @moduledoc """
  Error helpers for Intent Ledger failures.
  """

  use Splode,
    error_classes: [
      invalid: IntentLedger.Error.Invalid,
      runtime: IntentLedger.Error.Runtime
    ],
    unknown_error: IntentLedger.Error.Runtime.UnknownError,
    filter_stacktraces: [IntentLedger, "IntentLedger."]

  defmodule Invalid do
    @moduledoc false
    use Splode.ErrorClass, class: :invalid
  end

  defmodule Runtime do
    @moduledoc false
    use Splode.ErrorClass, class: :runtime

    defmodule UnknownError do
      @moduledoc false

      use Splode.Error, class: :runtime, fields: [:message, :details]

      @impl true
      def exception(opts) do
        opts
        |> Keyword.put_new(:message, "Unknown intent ledger error")
        |> Keyword.put_new(:details, %{})
        |> super()
      end
    end
  end

  defmodule InvalidInputError do
    @moduledoc false

    use Splode.Error, class: :invalid, fields: [:message, :field, :value, :details]

    @impl true
    def exception(opts) do
      opts
      |> Keyword.put_new(:message, "Invalid intent ledger input")
      |> Keyword.put_new(:details, %{})
      |> super()
    end
  end

  defmodule ConflictError do
    @moduledoc false

    use Splode.Error, class: :invalid, fields: [:message, :reason, :resource, :details]

    @impl true
    def exception(opts) do
      opts
      |> Keyword.put_new(:message, "Intent ledger conflict")
      |> Keyword.put_new(:details, %{})
      |> super()
    end
  end

  defmodule RuntimeError do
    @moduledoc false

    use Splode.Error, class: :runtime, fields: [:message, :details]

    @impl true
    def exception(opts) do
      opts
      |> Keyword.put_new(:message, "Intent ledger runtime failure")
      |> Keyword.put_new(:details, %{})
      |> super()
    end
  end

  @doc """
  Creates an invalid input error.
  """
  @spec invalid(String.t(), keyword() | map()) :: Exception.t()
  def invalid(message, details \\ %{}) do
    details = normalize_details(details)

    InvalidInputError.exception(
      message: message,
      field: Map.get(details, :field),
      value: Map.get(details, :value),
      details: details,
      splode: __MODULE__
    )
  end

  @doc """
  Creates a conflict error.
  """
  @spec conflict(term(), keyword() | map()) :: Exception.t()
  def conflict(reason, details \\ %{}) do
    details = normalize_details(details)

    ConflictError.exception(
      reason: reason,
      resource: Map.get(details, :resource),
      details: details,
      splode: __MODULE__
    )
  end

  @doc """
  Creates a runtime error.
  """
  @spec runtime(String.t(), keyword() | map()) :: Exception.t()
  def runtime(message, details \\ %{}) do
    RuntimeError.exception(
      message: message,
      details: normalize_details(details),
      splode: __MODULE__
    )
  end

  @doc """
  Normalizes common tuple reasons into Splode exceptions.
  """
  @spec from_reason(term()) :: Exception.t()
  def from_reason({:unknown_topic, topic}), do: invalid("Unknown Intent topic", field: :topic, value: topic)
  def from_reason({:invalid_entry, entry}), do: invalid("Invalid Intent enqueue entry", value: entry)
  def from_reason({:terminal_intent, status}), do: conflict(:terminal_intent, status: status)
  def from_reason(:not_found), do: invalid("Intent not found")
  def from_reason(reason), do: runtime("Intent ledger failure", reason: reason)

  defp normalize_details(details) when is_map(details), do: details
  defp normalize_details(details) when is_list(details), do: Map.new(details)
  defp normalize_details(details), do: %{reason: details}
end
