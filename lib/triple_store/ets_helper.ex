defmodule TripleStore.ETSHelper do
  @moduledoc """
  Shared utilities for safe ETS table management.

  Provides atomic table creation to avoid TOCTOU race conditions when
  multiple processes attempt to initialize ETS tables simultaneously.

  ## Usage

      defmodule MyCache do
        alias TripleStore.ETSHelper

        @table :my_cache_table

        def init do
          case ETSHelper.ensure_table(@table, [:set, :public, :named_table]) do
            :created -> initialize_defaults()
            :exists -> :ok
          end
        end
      end

  ## Security Considerations

  These helpers use `:public` access by default for performance, allowing
  any process to read/write. For production deployments with untrusted code,
  consider using `:protected` access with a dedicated GenServer owner.
  """

  @doc """
  Ensures an ETS table exists, creating it atomically if needed.

  Uses try/rescue to handle race conditions where multiple processes
  attempt to create the same table simultaneously. The first process
  succeeds, subsequent attempts receive `:exists`.

  ## Arguments

  - `name` - The atom name for the table
  - `opts` - ETS table options (e.g., `[:set, :public, :named_table]`)

  ## Returns

  - `:created` - Table was created by this call
  - `:exists` - Table already existed

  ## Examples

      case ETSHelper.ensure_table(:my_table, [:set, :public, :named_table]) do
        :created -> :ets.insert(:my_table, {:default_key, default_value})
        :exists -> :ok
      end

  """
  @spec ensure_table(atom(), list()) :: :created | :exists
  def ensure_table(name, opts) when is_atom(name) and is_list(opts) do
    try do
      :ets.new(name, opts)
      :created
    rescue
      ArgumentError -> :exists
    end
  end

  @doc """
  Creates an ETS table if it doesn't exist, ignoring if it does.

  A simpler version of `ensure_table/2` that returns `:ok` regardless
  of whether the table was created or already existed.

  ## Examples

      :ok = ETSHelper.ensure_table!(:my_table, [:set, :public, :named_table])

  """
  @spec ensure_table!(atom(), list()) :: :ok
  def ensure_table!(name, opts) when is_atom(name) and is_list(opts) do
    ensure_table(name, opts)
    :ok
  end

  @doc """
  Clears all objects from a table if it exists.

  Safe to call on tables that may not exist yet.

  ## Returns

  - `:ok` - Always

  ## Examples

      ETSHelper.clear_if_exists(:my_table)

  """
  @spec clear_if_exists(atom()) :: :ok
  def clear_if_exists(name) when is_atom(name) do
    case :ets.whereis(name) do
      :undefined -> :ok
      _table -> :ets.delete_all_objects(name)
    end

    :ok
  end
end
