defmodule Jido.IntentLedger.Error do
  @moduledoc """
  Error namespace for intent ledger failures.

  The public API currently returns simple tagged tuples for the in-memory spike,
  while this module establishes the Splode boundary expected by durable adapters
  and external integrations.
  """

  use Splode,
    error_classes: [
      invalid: Jido.IntentLedger.Error.Invalid,
      runtime: Jido.IntentLedger.Error.Runtime
    ],
    unknown_error: Jido.IntentLedger.Error.Runtime.UnknownError,
    filter_stacktraces: [Jido.IntentLedger, "Jido.IntentLedger."]

  defmodule Invalid do
    @moduledoc """
    Invalid input or state-transition errors.
    """

    use Splode.ErrorClass, class: :invalid
  end

  defmodule Runtime do
    @moduledoc """
    Runtime failures from stores, hooks, or lifecycle operations.
    """

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
    @moduledoc """
    Error for invalid input or invalid lifecycle transitions.
    """

    use Splode.Error, class: :invalid, fields: [:message, :field, :value, :details]

    @impl true
    def exception(opts) do
      opts
      |> Keyword.put_new(:message, "Invalid intent ledger input")
      |> Keyword.put_new(:details, %{})
      |> super()
    end
  end

  defmodule RuntimeError do
    @moduledoc """
    Error for store, hook, and lifecycle runtime failures.
    """

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

  defp normalize_details(details) when is_list(details), do: Map.new(details)
  defp normalize_details(details) when is_map(details), do: details
  defp normalize_details(details), do: %{details: details}
end
