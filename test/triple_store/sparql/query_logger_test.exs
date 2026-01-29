defmodule TripleStore.SPARQL.QueryLoggerTest do
  @moduledoc """
  Tests for query logging and audit trail (S11).
  """

  use ExUnit.Case, async: false

  alias TripleStore.SPARQL.QueryLogger

  setup do
    start_supervised!(QueryLogger)
    :ok
  end

  describe "log_start/2" do
    test "logs a query start and returns an ID" do
      query = "SELECT * WHERE { ?s ?p ?o }"

      assert {:ok, query_id} = QueryLogger.log_start(query)

      assert is_binary(query_id)
      assert String.length(query_id) == 32

      entry = QueryLogger.get_entry(query_id)

      assert entry.id == query_id
      assert entry.query == query
      assert entry.status == :executing
      assert entry.timestamp != nil
    end

    test "includes user and origin in log entry" do
      query = "SELECT * WHERE { ?s ?p ?o }"

      assert {:ok, query_id} =
               QueryLogger.log_start(query, user: "alice", origin: "api")

      entry = QueryLogger.get_entry(query_id)

      assert entry.user == "alice"
      assert entry.origin == "api"
    end

    test "computes query hash" do
      query = "SELECT * WHERE { ?s ?p ?o }"

      assert {:ok, query_id} = QueryLogger.log_start(query)

      entry = QueryLogger.get_entry(query_id)

      assert is_binary(entry.query_hash)
      assert String.length(entry.query_hash) == 64
    end
  end

  describe "log_complete/2" do
    test "marks query as complete with duration and result count" do
      query = "SELECT * WHERE { ?s ?p ?o }"

      assert {:ok, query_id} = QueryLogger.log_start(query)

      QueryLogger.log_complete(query_id, duration_ms: 123, result_count: 456)

      entry = QueryLogger.get_entry(query_id)

      assert entry.status == :success
      assert entry.duration_ms == 123
      assert entry.result_count == 456
    end

    test "handles completion with optional fields" do
      query = "SELECT * WHERE { ?s ?p ?o }"

      assert {:ok, query_id} = QueryLogger.log_start(query)

      QueryLogger.log_complete(query_id, [])

      entry = QueryLogger.get_entry(query_id)

      assert entry.status == :success
    end

    test "does not error for unknown query ID" do
      assert :ok == QueryLogger.log_complete("unknown", duration_ms: 100)
    end
  end

  describe "log_error/2" do
    test "marks query as errored with error message" do
      query = "SELECT * WHERE { ?s ?p ?o }"

      assert {:ok, query_id} = QueryLogger.log_start(query)

      QueryLogger.log_error(query_id, "syntax error")

      entry = QueryLogger.get_entry(query_id)

      assert entry.status == :error
      assert entry.error == "syntax error"
    end

    test "handles exception error" do
      query = "SELECT * WHERE { ?s ?p ?o }"

      assert {:ok, query_id} = QueryLogger.log_start(query)

      exception = RuntimeError.exception("test error")
      QueryLogger.log_error(query_id, exception)

      entry = QueryLogger.get_entry(query_id)

      assert entry.status == :error
      assert entry.error == "test error"
    end

    test "does not error for unknown query ID" do
      assert :ok == QueryLogger.log_error("unknown", "error")
    end
  end

  describe "get_entry/1" do
    test "returns nil for unknown entry" do
      assert nil == QueryLogger.get_entry("unknown")
    end

    test "returns entry with all fields" do
      query = "SELECT * WHERE { ?s ?p ?o }"

      assert {:ok, query_id} =
               QueryLogger.log_start(query, user: "bob", origin: "cli")

      QueryLogger.log_complete(query_id, duration_ms: 50, result_count: 10)

      entry = QueryLogger.get_entry(query_id)

      assert entry.id == query_id
      assert entry.query == query
      assert entry.user == "bob"
      assert entry.origin == "cli"
      assert entry.status == :success
      assert entry.duration_ms == 50
      assert entry.result_count == 10
      assert is_integer(entry.timestamp)
      assert entry.error == nil
    end
  end

  describe "list_entries/1" do
    test "returns all entries sorted by timestamp desc" do
      # Clear any existing entries
      QueryLogger.clear()

      QueryLogger.log_start("SELECT 1")
      QueryLogger.log_start("SELECT 2")
      QueryLogger.log_start("SELECT 3")

      entries = QueryLogger.list_entries()

      assert length(entries) == 3

      # Check sorted by timestamp descending
      timestamps = Enum.map(entries, & &1.timestamp)
      assert timestamps == Enum.sort(timestamps, :desc)
    end

    test "respects limit option" do
      QueryLogger.clear()

      QueryLogger.log_start("SELECT 1")
      QueryLogger.log_start("SELECT 2")
      QueryLogger.log_start("SELECT 3")

      entries = QueryLogger.list_entries(limit: 2)

      assert length(entries) == 2
    end

    test "filters by status" do
      QueryLogger.clear()

      {:ok, id1} = QueryLogger.log_start("SELECT 1")
      {:ok, id2} = QueryLogger.log_start("SELECT 2")
      {:ok, id3} = QueryLogger.log_start("SELECT 3")

      QueryLogger.log_complete(id1, [])
      QueryLogger.log_error(id2, "error")
      # id3 remains executing

      success_entries = QueryLogger.list_entries(status: :success)
      error_entries = QueryLogger.list_entries(status: :error)
      executing_entries = QueryLogger.list_entries(status: :executing)

      assert length(success_entries) == 1
      assert length(error_entries) == 1
      assert length(executing_entries) == 1
    end

    test "combines status and limit options" do
      QueryLogger.clear()

      {:ok, id1} = QueryLogger.log_start("SELECT 1")
      {:ok, id2} = QueryLogger.log_start("SELECT 2")
      {:ok, id3} = QueryLogger.log_start("SELECT 3")

      QueryLogger.log_complete(id1, [])
      QueryLogger.log_complete(id2, [])
      QueryLogger.log_complete(id3, [])

      entries = QueryLogger.list_entries(status: :success, limit: 2)

      assert length(entries) == 2
      assert Enum.all?(entries, fn e -> e.status == :success end)
    end
  end

  describe "statistics/0" do
    test "returns query statistics" do
      QueryLogger.clear()

      {:ok, id1} = QueryLogger.log_start("SELECT 1")
      {:ok, id2} = QueryLogger.log_start("SELECT 2")
      {:ok, id3} = QueryLogger.log_start("SELECT 3")

      QueryLogger.log_complete(id1, duration_ms: 100, result_count: 10)
      QueryLogger.log_complete(id2, duration_ms: 200, result_count: 20)
      QueryLogger.log_error(id3, "error")

      stats = QueryLogger.statistics()

      assert stats.total_logged >= 3
      assert stats.current_entries == 3
      assert stats.by_status.executing == 0
      assert stats.by_status.success == 2
      assert stats.by_status.error == 1
      assert stats.average_duration_ms == 150
      assert stats.total_results == 30
    end

    test "handles empty log" do
      QueryLogger.clear()

      stats = QueryLogger.statistics()

      assert stats.total_logged == 0
      assert stats.current_entries == 0
      assert stats.by_status.executing == 0
      assert stats.by_status.success == 0
      assert stats.by_status.error == 0
      assert stats.average_duration_ms == nil
      assert stats.total_results == 0
    end

    test "includes max_entries in stats" do
      stats = QueryLogger.statistics()

      assert is_integer(stats.max_entries)
      assert stats.max_entries > 0
    end
  end

  describe "clear/0" do
    test "removes all log entries" do
      QueryLogger.log_start("SELECT 1")
      QueryLogger.log_start("SELECT 2")

      assert QueryLogger.statistics().current_entries > 0

      QueryLogger.clear()

      assert QueryLogger.statistics().current_entries == 0
      assert QueryLogger.list_entries() == []
    end

    test "resets statistics" do
      QueryLogger.log_start("SELECT 1")

      assert QueryLogger.statistics().total_logged > 0

      QueryLogger.clear()

      stats = QueryLogger.statistics()

      assert stats.total_logged == 0
      assert stats.by_status.executing == 0
      assert stats.by_status.success == 0
      assert stats.by_status.error == 0
    end
  end

  describe "export_json/1" do
    test "exports entries as JSON" do
      QueryLogger.clear()

      {:ok, id} = QueryLogger.log_start("SELECT * WHERE { ?s ?p ?o }", user: "alice")
      QueryLogger.log_complete(id, duration_ms: 100, result_count: 5)

      json = QueryLogger.export_json()

      assert is_binary(json)

      # Parse and verify structure
      parsed = Jason.decode!(json)

      assert is_list(parsed)
      assert length(parsed) == 1

      [entry] = parsed

      assert entry["query"] == "SELECT * WHERE { ?s ?p ?o }"
      assert entry["user"] == "alice"
      assert entry["status"] == "success"
      assert entry["duration_ms"] == 100
      assert entry["result_count"] == 5
    end

    test "respects options when exporting" do
      QueryLogger.clear()

      {:ok, id1} = QueryLogger.log_start("SELECT 1")
      {:ok, id2} = QueryLogger.log_start("SELECT 2")
      QueryLogger.log_start("SELECT 3")
      QueryLogger.log_start("SELECT 4")

      QueryLogger.log_complete(id1, [])
      QueryLogger.log_complete(id2, [])

      json = QueryLogger.export_json(limit: 2, status: :success)
      parsed = Jason.decode!(json)

      assert length(parsed) == 2
      assert Enum.all?(parsed, fn e -> e["status"] == "success" end)
    end

    test "returns empty array for no entries" do
      QueryLogger.clear()

      json = QueryLogger.export_json()
      parsed = Jason.decode!(json)

      assert parsed == []
    end
  end

  describe "eviction" do
    test "evicts oldest entries when max_entries is reached" do
      # This test verifies eviction works by checking entries stay under max
      stats = QueryLogger.statistics()
      max_entries = stats.max_entries

      # Clear and log more than max_entries
      QueryLogger.clear()

      for i <- 1..(max_entries + 10) do
        QueryLogger.log_start("SELECT #{i}")
      end

      stats = QueryLogger.statistics()

      # Should have evicted to stay at or below max_entries
      assert stats.current_entries <= max_entries
    end
  end
end
