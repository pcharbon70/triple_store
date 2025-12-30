defmodule TripleStore.Test.IntegrationHelpers do
  @moduledoc """
  Shared helper functions for integration tests.

  This module consolidates common utilities used across multiple integration
  test files to reduce code duplication and ensure consistency.

  ## Usage

      defmodule MyTest do
        use ExUnit.Case
        import TripleStore.Test.IntegrationHelpers
      end

  ## Categories

  - **SPARQL Algebra Builders**: `var/1`, `iri/1`, `triple/3`, `literal/1`, etc.
  - **Data Loading**: `add_triple/2`, `add_triple/3`
  - **Result Extraction**: `extract_count/1`, `ast_to_rdf/1`
  - **Database Setup**: `setup_test_db/1`, `cleanup_test_db/1`
  """

  alias TripleStore.Dictionary.Manager
  alias TripleStore.Index

  # ===========================================================================
  # SPARQL Algebra Term Builders
  # ===========================================================================

  @doc "Create a SPARQL variable term"
  def var(name), do: {:variable, name}

  @doc "Create a named node (IRI) term"
  def iri(uri), do: {:named_node, uri}

  @doc "Create a simple literal term"
  def literal(value), do: {:literal, :simple, value}

  @doc "Create a typed literal term"
  def typed_literal(value, type), do: {:literal, :typed, value, type}

  @doc "Create a language-tagged literal term"
  def lang_literal(value, lang), do: {:literal, :lang, value, lang}

  @doc "Create a triple pattern"
  def triple(s, p, o), do: {:triple, s, p, o}

  @doc "Create a blank node term"
  def bnode(id), do: {:blank_node, id}

  # ===========================================================================
  # Data Loading Helpers
  # ===========================================================================

  @doc """
  Add a triple to the database using a query context.

  ## Example

      add_triple(ctx, {iri("http://ex.org/s"), iri("http://ex.org/p"), literal("value")})
  """
  def add_triple(%{db: db, dict_manager: manager}, {s_term, p_term, o_term}) do
    add_triple(db, manager, {s_term, p_term, o_term})
  end

  @doc """
  Add a triple to the database using explicit db and manager.

  ## Example

      add_triple(db, manager, {iri("http://ex.org/s"), iri("http://ex.org/p"), literal("value")})
  """
  def add_triple(db, manager, {s_term, p_term, o_term}) do
    {:ok, s_id} = Manager.get_or_create_id(manager, term_to_rdf(s_term))
    {:ok, p_id} = Manager.get_or_create_id(manager, term_to_rdf(p_term))
    {:ok, o_id} = Manager.get_or_create_id(manager, term_to_rdf(o_term))
    :ok = Index.insert_triple(db, {s_id, p_id, o_id})
  end

  # ===========================================================================
  # Result Extraction Helpers
  # ===========================================================================

  @doc """
  Extract an integer count from a SPARQL query result.

  Handles multiple result formats:
  - `RDF.Literal` structs
  - AST tuple formats `{:literal, :typed, value, datatype}`
  - Plain integers or strings
  """
  def extract_count(result) do
    case result do
      %RDF.Literal{} = lit -> RDF.Literal.value(lit) |> to_string() |> String.to_integer()
      {:literal, :typed, value, _} -> String.to_integer(value)
      {:literal, :simple, value} -> String.to_integer(value)
      value when is_integer(value) -> value
      value when is_binary(value) -> String.to_integer(value)
    end
  end

  @doc """
  Convert AST format back to RDF terms.
  """
  def ast_to_rdf({:named_node, iri}), do: RDF.iri(iri)
  def ast_to_rdf({:blank_node, id}), do: RDF.bnode(id)
  def ast_to_rdf({:literal, :simple, value}), do: RDF.literal(value)
  def ast_to_rdf({:literal, :lang, value, lang}), do: RDF.literal(value, language: lang)
  def ast_to_rdf({:literal, :typed, value, datatype}), do: RDF.literal(value, datatype: datatype)
  def ast_to_rdf(other), do: other

  @doc """
  Extract IRI string from AST or RDF term.

  Use this instead of pattern matching on {:named_node, iri} directly.
  """
  def get_iri({:named_node, iri}), do: iri
  def get_iri(%RDF.IRI{value: iri}), do: iri
  def get_iri(other), do: other

  @doc """
  Extract literal value from AST or RDF term.

  Use this instead of pattern matching on {:literal, ...} directly.
  """
  def get_literal({:literal, :simple, value}), do: value
  def get_literal({:literal, :typed, value, _}), do: value
  def get_literal({:literal, :lang, value, _}), do: value
  def get_literal(%RDF.Literal{} = lit), do: to_string(RDF.Literal.value(lit))
  def get_literal(other), do: other

  @doc """
  Extract IRI values from a list of query results.

  Use this instead of inline pattern matching for result extraction.
  """
  def extract_iris(results, var) do
    results
    |> Enum.map(&get_iri(&1[var]))
    |> MapSet.new()
  end

  # ===========================================================================
  # Database Setup Helpers
  # ===========================================================================

  @doc """
  Set up a test database with dictionary manager.

  Returns `{db, manager}` tuple. Use `cleanup_test_db/2` for teardown.

  ## Example

      {db, manager} = setup_test_db(tmp_dir)
  """
  def setup_test_db(tmp_dir) do
    db_path = Path.join(tmp_dir, "test_db_#{:erlang.unique_integer([:positive])}")
    {:ok, db} = TripleStore.RocksDB.NIF.open(db_path)
    {:ok, manager} = Manager.start_link(db: db)
    {db, manager}
  end

  @doc """
  Clean up a test database.

  Stops the manager and closes the database.
  """
  def cleanup_test_db({_db, manager}) do
    if Process.alive?(manager), do: Manager.stop(manager)
  end

  def cleanup_test_db(db, manager) do
    if Process.alive?(manager), do: Manager.stop(manager)
    # db is automatically closed when the NIF resource is garbage collected
    _ = db
    :ok
  end

  # ===========================================================================
  # Internal Helpers
  # ===========================================================================

  defp term_to_rdf({:named_node, uri}), do: RDF.iri(uri)
  defp term_to_rdf({:blank_node, id}), do: RDF.bnode(id)
  defp term_to_rdf({:literal, :simple, value}), do: RDF.literal(value)

  defp term_to_rdf({:literal, :typed, value, type}) do
    RDF.literal(value, datatype: type)
  end

  defp term_to_rdf({:literal, :lang, value, lang}) do
    RDF.literal(value, language: lang)
  end

  # ===========================================================================
  # Store Lifecycle Helpers (Section 5.7 Review Fixes)
  # ===========================================================================

  # Timing constants for RocksDB operations
  @lock_release_delay_ms 200
  @retry_delay_ms 500
  @max_retries 10

  @doc """
  Create a temporary triple store with a unique path.

  Returns `{store, path}` tuple. Use `cleanup_test_store/2` for cleanup.

  ## Options

  - `:prefix` - Path prefix for the temp store (default: "integration_test")

  ## Example

      {store, path} = create_test_store()
      # ... use store ...
      cleanup_test_store(store, path)
  """
  def create_test_store(opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "integration_test")
    path = Path.join(System.tmp_dir!(), "#{prefix}_#{:rand.uniform(1_000_000)}")
    {:ok, store} = TripleStore.open(path)
    {store, path}
  end

  @doc """
  Clean up a test store and its path.

  Handles errors gracefully, including process exits from closed stores.
  """
  def cleanup_test_store(store, path) do
    try do
      TripleStore.close(store)
    rescue
      e ->
        require Logger
        Logger.debug("Cleanup close failed (expected for already-closed stores): #{inspect(e)}")
        :ok
    catch
      :exit, reason ->
        require Logger

        Logger.debug(
          "Cleanup close exited (expected for already-closed stores): #{inspect(reason)}"
        )

        :ok
    end

    File.rm_rf!(path)
  end

  @doc """
  Clean up a path only (when store is already closed).
  """
  def cleanup_test_path(path) do
    File.rm_rf!(path)
  end

  @doc """
  Open a store with retry logic for RocksDB lock contention.

  Useful after closing a store when the lock may not be immediately released.

  ## Options

  - `:retries` - Maximum retry attempts (default: 10)
  - `:delay_ms` - Delay between retries (default: 500)
  """
  def open_with_retry(path, opts \\ []) do
    retries = Keyword.get(opts, :retries, @max_retries)
    delay_ms = Keyword.get(opts, :delay_ms, @retry_delay_ms)
    do_open_with_retry(path, retries, delay_ms)
  end

  defp do_open_with_retry(path, retries, delay_ms) do
    case TripleStore.open(path) do
      {:ok, store} ->
        {:ok, store}

      {:error, _} when retries > 0 ->
        Process.sleep(delay_ms)
        do_open_with_retry(path, retries - 1, delay_ms)

      error ->
        error
    end
  end

  @doc """
  Wait for RocksDB lock to be released after closing a store.

  Call this after `TripleStore.close/1` before reopening.
  """
  def wait_for_lock_release(delay_ms \\ @lock_release_delay_ms) do
    Process.sleep(delay_ms)
    :erlang.garbage_collect()
  end

  @doc """
  Generate test triples with unique subjects.

  ## Options

  - `:prefix` - Subject URI prefix (default: "http://example.org/item")
  - `:predicate` - Predicate URI (default: "http://example.org/value")

  ## Example

      triples = generate_test_triples(100)
      {:ok, _} = TripleStore.load_graph(store, RDF.Graph.new(triples))
  """
  def generate_test_triples(count, opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "http://example.org/item")
    predicate = Keyword.get(opts, :predicate, "http://example.org/value")

    for i <- 1..count do
      {
        RDF.iri("#{prefix}#{i}"),
        RDF.iri(predicate),
        RDF.literal(i)
      }
    end
  end

  @doc """
  Load test data into a store.

  Returns the number of triples loaded.
  """
  def load_test_data(store, count, opts \\ []) do
    triples = generate_test_triples(count, opts)
    graph = RDF.Graph.new(triples)
    {:ok, loaded} = TripleStore.load_graph(store, graph)
    loaded
  end

  @doc """
  Extract value from RDF literal or AST tuple format.

  Handles multiple result formats consistently.
  """
  def extract_value(%RDF.Literal{} = lit), do: RDF.Literal.value(lit)
  def extract_value({:literal, :simple, value}), do: value
  def extract_value({:literal, :typed, value, _datatype}), do: parse_typed_value(value)
  def extract_value({:literal, :lang, value, _lang}), do: value
  def extract_value(value), do: value

  defp parse_typed_value(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> value
    end
  end

  defp parse_typed_value(value), do: value

  @doc """
  Ensure Prometheus is started for telemetry tests.

  Safe to call multiple times.
  """
  def ensure_prometheus_started do
    case Process.whereis(TripleStore.Prometheus) do
      nil ->
        {:ok, _} = TripleStore.Prometheus.start_link([])
        :ok

      _pid ->
        :ok
    end
  end

  @doc """
  Assert store is in an operational state (healthy or degraded).

  Use this instead of directly asserting on status.
  """
  def assert_store_operational(health) do
    import ExUnit.Assertions

    assert health.status in [:healthy, :degraded],
           "Expected operational status (:healthy or :degraded), got #{inspect(health.status)}"
  end

  @doc """
  Get the triple count from a store.
  """
  def get_triple_count(store) do
    {:ok, stats} = TripleStore.stats(store)
    stats.triple_count
  end
end
