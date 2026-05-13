defmodule IntentLedger.StoreCase do
  @moduledoc """
  ExUnit case template for Store V1 adapter conformance tests.

  Adapter suites can pass `:store_module`, `:store_opts`, `:store_ref`, and
  `:ledger` through `use IntentLedger.StoreCase, ...` or via ExUnit tags. When a
  store module is provided without a ref, the case starts the adapter child for
  each test.
  """

  use ExUnit.CaseTemplate

  alias IntentLedger.Store

  using opts do
    quote bind_quoted: [opts: opts] do
      import IntentLedger.StoreCase

      alias IntentLedger.Store
      alias IntentLedger.Store.{Commit, CommitRequest, Conflict, Listing, Outbox, Precondition, Write}

      @store_case_opts opts

      setup context do
        IntentLedger.StoreCase.__setup__(context, @store_case_opts)
      end
    end
  end

  @doc false
  @spec __setup__(map(), keyword()) :: {:ok, keyword()}
  def __setup__(context, opts) do
    store_module = context[:store_module] || Keyword.get(opts, :store_module)
    store_opts = context[:store_opts] || Keyword.get(opts, :store_opts, [])
    ledger = context[:ledger] || Keyword.get(opts, :ledger) || unique_ledger(context)
    store_ref = context[:store_ref] || start_store(store_module, store_opts)

    {:ok, store_module: store_module, store_ref: store_ref, store_opts: store_opts, ledger: ledger}
  end

  @doc """
  Calls `c:IntentLedger.Store.commit/4` for the store configured in the test context.
  """
  @spec commit(map(), Store.CommitRequest.t(), keyword()) :: Store.commit_result()
  def commit(context, %Store.CommitRequest{} = request, opts \\ []) do
    context
    |> adapter!()
    |> call(:commit, [request, opts])
  end

  @doc """
  Calls `c:IntentLedger.Store.read/4` for the store configured in the test context.
  """
  @spec read(map(), Store.read_request(), keyword()) :: Store.result()
  def read(context, request, opts \\ []) do
    context
    |> adapter!()
    |> call(:read, [request, opts])
  end

  @doc """
  Calls `c:IntentLedger.Store.lease/4` for the store configured in the test context.
  """
  @spec lease(map(), Store.lease_request(), keyword()) :: Store.result()
  def lease(context, request, opts \\ []) do
    context
    |> adapter!()
    |> call(:lease, [request, opts])
  end

  @doc """
  Calls `c:IntentLedger.Store.listing/4` for the store configured in the test context.
  """
  @spec listing(map(), Store.listing_request(), keyword()) :: Store.result()
  def listing(context, request, opts \\ []) do
    context
    |> adapter!()
    |> call(:listing, [request, opts])
  end

  @doc """
  Calls `c:IntentLedger.Store.outbox/4` for the store configured in the test context.
  """
  @spec outbox(map(), Store.outbox_request(), keyword()) :: Store.result()
  def outbox(context, request, opts \\ []) do
    context
    |> adapter!()
    |> call(:outbox, [request, opts])
  end

  @doc """
  Builds a deterministic test ledger name for adapter isolation.
  """
  @spec unique_ledger(map()) :: atom()
  def unique_ledger(%{module: module, test: test}) do
    test_name =
      test
      |> Atom.to_string()
      |> String.replace(~r/[^A-Za-z0-9]/, "_")

    Module.concat(module, "Ledger#{test_name}")
  end

  defp adapter!(%{store_module: module, store_ref: ref, ledger: ledger})
       when is_atom(module) and not is_nil(ref) and is_atom(ledger) do
    %{module: module, ref: ref, ledger: ledger}
  end

  defp adapter!(_context) do
    raise ArgumentError,
          "StoreCase requires :store_module and :store_ref in the test context, or a :store_module that can be started"
  end

  defp call(%{module: module, ref: ref, ledger: ledger}, callback, [request, opts]) do
    apply(module, callback, [ref, ledger, request, opts])
  end

  defp start_store(nil, _opts), do: nil
  defp start_store(module, opts), do: ExUnit.Callbacks.start_supervised!({module, opts})
end
