defmodule IntentLedger.Error do
  @moduledoc """
  Error namespace for intent ledger failures.

  The public API still accepts simple tagged tuples from the in-memory adapter,
  while this module defines the Splode boundary expected by durable adapters and
  external integrations.
  """

  use Splode,
    error_classes: [
      invalid: IntentLedger.Error.Invalid,
      runtime: IntentLedger.Error.Runtime
    ],
    unknown_error: IntentLedger.Error.Runtime.UnknownError,
    filter_stacktraces: [IntentLedger, "IntentLedger."]

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

  defmodule ConflictError do
    @moduledoc """
    Error for optimistic concurrency, idempotency, or durable commit conflicts.
    """

    use Splode.Error, class: :invalid, fields: [:message, :reason, :resource, :details]

    @impl true
    def exception(opts) do
      opts
      |> Keyword.put_new(:message, "Intent ledger conflict")
      |> Keyword.put_new(:details, %{})
      |> super()
    end
  end

  defmodule StaleOwnerError do
    @moduledoc """
    Error for stale claim owners or fencing tokens.
    """

    use Splode.Error, class: :invalid, fields: [:message, :claim_id, :details]

    @impl true
    def exception(opts) do
      opts
      |> Keyword.put_new(:message, "Intent ledger claim owner is stale")
      |> Keyword.put_new(:details, %{})
      |> super()
    end
  end

  defmodule ExpiredLeaseError do
    @moduledoc """
    Error for claim operations attempted after lease expiry.
    """

    use Splode.Error, class: :invalid, fields: [:message, :claim_id, :lease_until, :details]

    @impl true
    def exception(opts) do
      opts
      |> Keyword.put_new(:message, "Intent ledger claim lease expired")
      |> Keyword.put_new(:details, %{})
      |> super()
    end
  end

  defmodule FinalStateError do
    @moduledoc """
    Error for commands rejected because an intent is already terminal.
    """

    use Splode.Error, class: :invalid, fields: [:message, :state, :details]

    @impl true
    def exception(opts) do
      opts
      |> Keyword.put_new(:message, "Intent ledger intent is already in a final state")
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

  defmodule AdapterRuntimeError do
    @moduledoc """
    Error for runtime failures raised by store adapters.
    """

    use Splode.Error, class: :runtime, fields: [:message, :adapter, :details]

    @impl true
    def exception(opts) do
      opts
      |> Keyword.put_new(:message, "Intent ledger adapter runtime failure")
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
  Creates a stale owner/fencing-token error.
  """
  @spec stale_owner(keyword() | map()) :: Exception.t()
  def stale_owner(details \\ %{}) do
    details = normalize_details(details)

    StaleOwnerError.exception(
      claim_id: Map.get(details, :claim_id),
      details: details,
      splode: __MODULE__
    )
  end

  @doc """
  Creates an expired lease error.
  """
  @spec expired_lease(keyword() | map()) :: Exception.t()
  def expired_lease(details \\ %{}) do
    details = normalize_details(details)

    ExpiredLeaseError.exception(
      claim_id: Map.get(details, :claim_id),
      lease_until: Map.get(details, :lease_until),
      details: details,
      splode: __MODULE__
    )
  end

  @doc """
  Creates a final-state rejection error.
  """
  @spec final_state(atom(), keyword() | map()) :: Exception.t()
  def final_state(state, details \\ %{}) do
    details = normalize_details(details)

    FinalStateError.exception(
      state: state,
      details: details,
      splode: __MODULE__
    )
  end

  @doc """
  Creates an adapter runtime error.
  """
  @spec adapter_runtime(String.t(), keyword() | map()) :: Exception.t()
  def adapter_runtime(message, details \\ %{}) do
    details = normalize_details(details)

    AdapterRuntimeError.exception(
      message: message,
      adapter: Map.get(details, :adapter),
      details: details,
      splode: __MODULE__
    )
  end

  @doc """
  Converts common raw store reasons into public error structs.
  """
  @spec from_reason(term()) :: Exception.t()
  def from_reason({:idempotency_conflict, intent_id}) do
    conflict(:idempotency_conflict, intent_id: intent_id, resource: intent_id)
  end

  def from_reason(:stale_claim), do: stale_owner(reason: :stale_claim)
  def from_reason(:lease_expired), do: expired_lease(reason: :lease_expired)
  def from_reason({:final_state, state}), do: final_state(state)
  def from_reason({:runtime, message, details}), do: adapter_runtime(to_string(message), details)
  def from_reason(reason), do: invalid("Invalid intent ledger command", reason: reason)

  defp normalize_details(details) when is_list(details), do: Map.new(details)
  defp normalize_details(details) when is_map(details), do: details
  defp normalize_details(details), do: %{details: details}
end
