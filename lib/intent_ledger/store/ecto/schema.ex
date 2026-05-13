defmodule IntentLedger.Store.Ecto.Schema do
  @moduledoc """
  Source mapping helpers for Ecto Store V1 row schemas.

  The schema modules use the default migration table names. Query builders can
  call `source/2` to apply runtime table-name overrides while still using the
  typed schema module for row shape.
  """

  alias IntentLedger.Store.Ecto.Migration

  @schemas %{
    intents: IntentLedger.Store.Ecto.Schema.Intent,
    states: IntentLedger.Store.Ecto.Schema.State,
    streams: IntentLedger.Store.Ecto.Schema.Stream,
    signals: IntentLedger.Store.Ecto.Schema.Signal,
    commands: IntentLedger.Store.Ecto.Schema.Command,
    claims: IntentLedger.Store.Ecto.Schema.Claim,
    shard_leases: IntentLedger.Store.Ecto.Schema.ShardLease,
    outbox: IntentLedger.Store.Ecto.Schema.OutboxEntry,
    projection_offsets: IntentLedger.Store.Ecto.Schema.ProjectionOffset
  }

  @type table :: Migration.table()

  @doc """
  Returns the Ecto schema module for a logical Store V1 table.
  """
  @spec module_for(table()) :: module()
  def module_for(table), do: Map.fetch!(@schemas, table)

  @doc """
  Returns an Ecto source tuple for a logical table.

  The first tuple element is the configured physical table name as a string, and
  the second is the schema module for the row shape.
  """
  @spec source(table(), [Migration.option()]) :: {String.t(), module()}
  def source(table, opts \\ []) do
    {table |> Migration.table_name(opts) |> to_string(), module_for(table)}
  end
end

if Code.ensure_loaded?(Ecto.Schema) do
  defmodule IntentLedger.Store.Ecto.Schema.Intent do
    @moduledoc """
    Ecto row schema for immutable intent records.
    """

    use Ecto.Schema

    @primary_key false
    schema "intent_ledger_intents" do
      field(:ledger, :string)
      field(:intent_id, :string)
      field(:intent, :map)
      timestamps(type: :utc_datetime_usec)
    end

    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.State do
    @moduledoc """
    Ecto row schema for materialized intent state.
    """

    use Ecto.Schema

    @primary_key false
    schema "intent_ledger_states" do
      field(:ledger, :string)
      field(:intent_id, :string)
      field(:status, :string)
      field(:queue, :string)
      field(:shard, :integer)
      field(:priority, :integer)
      field(:visible_at, :utc_datetime_usec)
      field(:attempt, :integer)
      field(:claim_id, :string)
      field(:token_hash, :string)
      field(:lease_until, :utc_datetime_usec)
      field(:state, :map)
      timestamps(type: :utc_datetime_usec)
    end

    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.Stream do
    @moduledoc """
    Ecto row schema for stream version counters.
    """

    use Ecto.Schema

    @primary_key false
    schema "intent_ledger_streams" do
      field(:ledger, :string)
      field(:stream, :string)
      field(:version, :integer)
      timestamps(type: :utc_datetime_usec)
    end

    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.Signal do
    @moduledoc """
    Ecto row schema for lifecycle stream signals.
    """

    use Ecto.Schema

    @primary_key false
    schema "intent_ledger_signals" do
      field(:ledger, :string)
      field(:stream, :string)
      field(:version, :integer)
      field(:signal, :map)
      timestamps(type: :utc_datetime_usec)
    end

    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.Command do
    @moduledoc """
    Ecto row schema for command idempotency records.
    """

    use Ecto.Schema

    @primary_key false
    schema "intent_ledger_commands" do
      field(:ledger, :string)
      field(:command_id, :string)
      field(:operation, :string)
      field(:command, :map)
      field(:result, :map)
      timestamps(type: :utc_datetime_usec)
    end

    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.Claim do
    @moduledoc """
    Ecto row schema for claim fencing rows.
    """

    use Ecto.Schema

    @primary_key false
    schema "intent_ledger_claims" do
      field(:ledger, :string)
      field(:claim_id, :string)
      field(:intent_id, :string)
      field(:owner_id, :string)
      field(:token_hash, :string)
      field(:lease_until, :utc_datetime_usec)
      field(:claim, :map)
      timestamps(type: :utc_datetime_usec)
    end

    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.ShardLease do
    @moduledoc """
    Ecto row schema for queue shard leases.
    """

    use Ecto.Schema

    @primary_key false
    schema "intent_ledger_shard_leases" do
      field(:ledger, :string)
      field(:queue, :string)
      field(:shard, :integer)
      field(:owner_id, :string)
      field(:lease_until, :utc_datetime_usec)
      field(:lease, :map)
      timestamps(type: :utc_datetime_usec)
    end

    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.OutboxEntry do
    @moduledoc """
    Ecto row schema for durable outbox entries.
    """

    use Ecto.Schema

    @primary_key false
    schema "intent_ledger_outbox" do
      field(:ledger, :string)
      field(:key, :string)
      field(:sequence, :integer)
      field(:stream, :string)
      field(:signal_id, :string)
      field(:signal_type, :string)
      field(:subject, :string)
      field(:signal, :map)
      field(:entry, :map)
      field(:acked_at, :utc_datetime_usec)
      field(:consumer, :string)
      field(:metadata, :map)
      timestamps(type: :utc_datetime_usec)
    end

    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.ProjectionOffset do
    @moduledoc """
    Ecto row schema for projection offsets.
    """

    use Ecto.Schema

    @primary_key false
    schema "intent_ledger_projection_offsets" do
      field(:ledger, :string)
      field(:name, :string)
      field(:cursor, :integer)
      field(:metadata, :map)
      timestamps(type: :utc_datetime_usec)
    end

    @type t :: %__MODULE__{}
  end
else
  defmodule IntentLedger.Store.Ecto.Schema.Intent do
    @moduledoc false
    defstruct []
    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.State do
    @moduledoc false
    defstruct []
    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.Stream do
    @moduledoc false
    defstruct []
    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.Signal do
    @moduledoc false
    defstruct []
    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.Command do
    @moduledoc false
    defstruct []
    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.Claim do
    @moduledoc false
    defstruct []
    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.ShardLease do
    @moduledoc false
    defstruct []
    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.OutboxEntry do
    @moduledoc false
    defstruct []
    @type t :: %__MODULE__{}
  end

  defmodule IntentLedger.Store.Ecto.Schema.ProjectionOffset do
    @moduledoc false
    defstruct []
    @type t :: %__MODULE__{}
  end
end
