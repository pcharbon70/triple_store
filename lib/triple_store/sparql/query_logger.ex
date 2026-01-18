defmodule TripleStore.SPARQL.QueryLogger do
  @moduledoc """
  Query logging and audit trail (S11).

  Logs all SPARQL queries for auditing purposes:
  - Query text and hash
  - Execution time
  - User/origin
  - Timestamp
  - Result count
  - Success/failure

  Uses a ring buffer to limit memory usage.
  """

  use GenServer
  require Logger

  @type log_entry :: %{
    id: String.t(),
    query: String.t(),
    query_hash: String.t(),
    user: String.t() | nil,
    origin: String.t() | nil,
    timestamp: integer(),
    duration_ms: non_neg_integer() | nil,
    result_count: non_neg_integer() | nil,
    status: :executing | :success | :error,
    error: String.t() | nil
  }

  @type opts :: [
    {:user, String.t()} |
    {:origin, String.t()} |
    {:timeout, pos_integer()} |
    {:max_results, non_neg_integer()}
  ]

  @default_max_entries 10_000
  @table_name :sparql_query_log

  # Client API

  @doc """
  Start the query logger.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Log a query execution start.
  """
  @spec log_start(String.t(), opts()) :: {:ok, String.t()} | {:error, term()}
  def log_start(query, opts \\ []) do
    GenServer.call(__MODULE__, {:log_start, query, opts})
  end

  @doc """
  Log a query completion.
  """
  @spec log_complete(String.t(), keyword()) :: :ok
  def log_complete(query_id, opts \\ []) do
    GenServer.call(__MODULE__, {:log_complete, query_id, opts})
  end

  @doc """
  Log a query error.
  """
  @spec log_error(String.t(), String.t() | Exception.t()) :: :ok
  def log_error(query_id, error) do
    GenServer.call(__MODULE__, {:log_error, query_id, error})
  end

  @doc """
  Get a log entry by ID.
  """
  @spec get_entry(String.t()) :: log_entry() | nil
  def get_entry(query_id) do
    try do
      case :ets.lookup(@table_name, query_id) do
        [{^query_id, entry}] -> entry
        [] -> nil
      end
    rescue
      ArgumentError -> nil
    end
  end

  @doc """
  List all log entries.
  """
  @spec list_entries(keyword()) :: [log_entry()]
  def list_entries(opts \\ []) do
    limit = Keyword.get(opts, :limit, 100)
    status = Keyword.get(opts, :status)

    entries =
      try do
        :ets.tab2list(@table_name)
      rescue
        ArgumentError -> []
      end
      |> Enum.sort_by(fn {_id, entry} -> entry.timestamp end, :desc)

    entries =
      if status do
        Enum.filter(entries, fn {_id, entry} -> entry.status == status end)
      else
        entries
      end

    entries =
      if limit do
        Enum.take(entries, limit)
      else
        entries
      end

    Enum.map(entries, fn {_id, entry} -> entry end)
  end

  @doc """
  Get statistics about logged queries.
  """
  @spec statistics() :: map()
  def statistics do
    GenServer.call(__MODULE__, :statistics)
  end

  @doc """
  Clear all log entries.
  """
  @spec clear() :: :ok
  def clear do
    GenServer.call(__MODULE__, :clear)
  end

  @doc """
  Export log entries as JSON.
  """
  @spec export_json(keyword()) :: String.t()
  def export_json(opts \\ []) do
    entries = list_entries(opts)

    entries
    |> Jason.encode_to_iodata!()
    |> IO.iodata_to_binary()
  end

  # GenServer Callbacks

  @impl true
  def init(opts) do
    max_entries = Keyword.get(opts, :max_entries, @default_max_entries)

    table =
      :ets.new(@table_name, [
        :named_table,
        :set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])

    {:ok,
     %{
       table: table,
       max_entries: max_entries,
       total_logged: 0,
       by_status: %{executing: 0, success: 0, error: 0}
     }}
  end

  @impl true
  def handle_call({:log_start, query, opts}, _from, state) do
    query_id = generate_id()
    query_hash = hash_query(query)

    entry = %{
      id: query_id,
      query: query,
      query_hash: query_hash,
      user: Keyword.get(opts, :user),
      origin: Keyword.get(opts, :origin),
      timestamp: System.system_time(:millisecond),
      duration_ms: nil,
      result_count: nil,
      status: :executing,
      error: nil
    }

    :ets.insert(@table_name, {query_id, entry})

    new_state =
      state
      |> update_total_logged()
      |> update_status_count(:executing, 1)
      |> maybe_evict()

    {:reply, {:ok, query_id}, new_state}
  end

  def handle_call(:statistics, _from, state) do
    all_entries = :ets.tab2list(@table_name)

    stats = %{
      total_logged: state.total_logged,
      current_entries: length(all_entries),
      max_entries: state.max_entries,
      by_status: state.by_status,
      average_duration_ms: calculate_average_duration(all_entries),
      total_results: Enum.reduce(all_entries, 0, fn {_id, e}, acc ->
        acc + (e.result_count || 0)
      end)
    }

    {:reply, stats, state}
  end

  def handle_call(:clear, _from, state) do
    :ets.delete_all_objects(@table_name)

    new_state = %{state | total_logged: 0, by_status: %{executing: 0, success: 0, error: 0}}

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:log_complete, query_id, opts}, _from, state) do
    case :ets.lookup(@table_name, query_id) do
      [{^query_id, entry}] ->
        duration_ms = Keyword.get(opts, :duration_ms)
        result_count = Keyword.get(opts, :result_count)

        updated_entry = %{
          entry
          | duration_ms: duration_ms,
            result_count: result_count,
            status: :success
        }

        :ets.insert(@table_name, {query_id, updated_entry})

        new_state =
          state
          |> update_status_count(:executing, -1)
          |> update_status_count(:success, 1)

        {:reply, :ok, new_state}

      [] ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:log_error, query_id, error}, _from, state) do
    case :ets.lookup(@table_name, query_id) do
      [{^query_id, entry}] ->
        error_message = error_message(error)

        updated_entry = %{
          entry
          | status: :error,
            error: error_message
        }

        :ets.insert(@table_name, {query_id, updated_entry})

        new_state =
          state
          |> update_status_count(:executing, -1)
          |> update_status_count(:error, 1)

        {:reply, :ok, new_state}

      [] ->
        {:reply, :ok, state}
    end
  end

  # Private Helpers

  defp generate_id do
    binary = :crypto.strong_rand_bytes(16)
    Base.encode16(binary, case: :lower)
  end

  defp hash_query(query) do
    :crypto.hash(:sha256, query)
    |> Base.encode16(case: :lower)
  end

  defp update_total_logged(state) do
    %{state | total_logged: state.total_logged + 1}
  end

  defp update_status_count(state, status, delta) do
    %{state | by_status: Map.update!(state.by_status, status, &(&1 + delta))}
  end

  defp maybe_evict(state) do
    current_count = :ets.info(@table_name, :size)

    if current_count > state.max_entries do
      # Remove oldest entries (by timestamp)
      entries_to_remove = current_count - state.max_entries

      :ets.tab2list(@table_name)
      |> Enum.sort_by(fn {_id, entry} -> entry.timestamp end)
      |> Enum.take(entries_to_remove)
      |> Enum.each(fn {id, _entry} -> :ets.delete(@table_name, id) end)
    end

    state
  end

  defp calculate_average_duration(all_entries) do
    completed =
      Enum.filter(all_entries, fn {_id, e} ->
        e.status == :success and e.duration_ms
      end)

    if length(completed) > 0 do
      total = Enum.reduce(completed, 0, fn {_id, e}, acc -> acc + e.duration_ms end)
      div(total, length(completed))
    else
      nil
    end
  end

  defp error_message(error) when is_binary(error), do: error
  defp error_message(error) when is_exception(error), do: Exception.message(error)
  defp error_message(error), do: inspect(error)
end
