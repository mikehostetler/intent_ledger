defmodule IntentLedger.StoreEctoTest do
  use ExUnit.Case, async: true

  alias IntentLedger.Error.AdapterRuntimeError
  alias IntentLedger.Store
  alias IntentLedger.Store.Ecto, as: EctoStore

  defmodule PostgresRepo do
    def __adapter__, do: Ecto.Adapters.Postgres
  end

  defmodule NotARepo do
  end

  test "reports optional Ecto dependencies as available when loaded" do
    assert EctoStore.available?()
  end

  test "returns normalized adapter errors for missing optional modules" do
    missing = Module.concat(__MODULE__, MissingDependency)

    assert {:error, %AdapterRuntimeError{} = error} = EctoStore.ensure_available([missing])
    assert error.adapter == EctoStore
    assert error.details.reason == :missing_dependency
    assert error.details.missing_modules == [missing]
  end

  test "requires a repo option at startup" do
    name = Module.concat(__MODULE__, "MissingRepo#{System.unique_integer([:positive])}")

    assert {:error, %AdapterRuntimeError{} = error} = EctoStore.start_link(name: name)
    assert error.adapter == EctoStore
    assert error.details.reason == :missing_repo
  end

  test "requires a Postgres Ecto repo module" do
    name = Module.concat(__MODULE__, "InvalidRepo#{System.unique_integer([:positive])}")

    assert {:error, %AdapterRuntimeError{} = error} = EctoStore.start_link(name: name, repo: NotARepo)
    assert error.adapter == EctoStore
    assert error.details.reason == :invalid_repo
  end

  test "starts with a loaded Postgres repo module" do
    name = Module.concat(__MODULE__, "Repo#{System.unique_integer([:positive])}")

    pid = start_supervised!({EctoStore, name: name, repo: PostgresRepo})

    assert Process.whereis(name) == pid
  end

  test "returns normalized not-implemented errors for Store V1 operations" do
    name = Module.concat(__MODULE__, "Store#{System.unique_integer([:positive])}")
    store = start_supervised!({EctoStore, name: name, repo: PostgresRepo})
    request = %Store.CommitRequest{operation: :submit, command_id: "cmd_1"}

    assert {:error, %AdapterRuntimeError{} = error} = EctoStore.commit(store, MyLedger, request, [])
    assert error.adapter == EctoStore
    assert error.details.reason == :not_implemented
    assert error.details.operation == :commit
    assert error.details.request == %{operation: :submit, command_id: "cmd_1"}
  end
end
