defmodule Jido.IntentLedger.Store do
  @moduledoc """
  Persistence contract for intent lifecycle state.

  Stores own atomic lifecycle commits. The bundled `Jido.IntentLedger.Store.Memory`
  adapter is intended for tests, local development, and as the executable
  contract for durable adapters.
  """

  alias Jido.IntentLedger.{Claimed, Intent, Record}

  @type ref :: GenServer.server()
  @type result :: {:ok, term()} | {:error, term()}
  @type commit_result(value) :: {:ok, value, [Jido.Signal.t()]} | {:error, term()}

  @callback child_spec(keyword()) :: Supervisor.child_spec()
  @callback submit(ref(), atom(), Intent.t(), keyword()) :: commit_result(Record.t())
  @callback submit_many(ref(), atom(), [Intent.t()], keyword()) :: commit_result([Record.t()])
  @callback get(ref(), String.t()) :: {:ok, Record.t()} | {:error, :not_found}
  @callback history(ref(), String.t()) :: {:ok, [Jido.Signal.t()]} | {:error, :not_found}
  @callback claim(ref(), atom(), String.t(), String.t(), keyword()) ::
              commit_result([Claimed.t()])
  @callback heartbeat(ref(), atom(), String.t(), String.t(), keyword()) :: commit_result(term())
  @callback complete(ref(), atom(), String.t(), String.t(), term(), keyword()) ::
              commit_result(Record.t())
  @callback fail(ref(), atom(), String.t(), String.t(), term(), keyword()) ::
              commit_result(Record.t())
  @callback release(ref(), atom(), String.t(), String.t(), keyword()) :: commit_result(Record.t())
  @callback cancel(ref(), atom(), String.t(), term(), keyword()) :: commit_result(Record.t())
  @callback requeue(ref(), atom(), String.t(), keyword()) :: commit_result(Record.t())
  @callback mark_ambiguous(ref(), atom(), String.t(), term(), keyword()) ::
              commit_result(Record.t())
  @callback recover(ref(), atom(), String.t(), keyword()) :: commit_result([Record.t()])

  @doc false
  @spec normalize_spec(module() | {module(), keyword()} | nil) :: {module(), keyword()}
  def normalize_spec(nil), do: {Jido.IntentLedger.Store.Memory, []}
  def normalize_spec(module) when is_atom(module), do: {module, []}
  def normalize_spec({module, opts}) when is_atom(module) and is_list(opts), do: {module, opts}
end
