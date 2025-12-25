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
end
