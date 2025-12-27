# credo:disable-for-this-file Credo.Check.Readability.FunctionNames
defmodule TripleStore.Test.ReasonerHelpers do
  @moduledoc """
  Shared test helpers for OWL 2 RL reasoning integration tests.

  This module provides:
  - Namespace constants for RDF, RDFS, OWL vocabularies
  - IRI builder functions for creating test data
  - Query simulation functions using PatternMatcher
  - Materialization helpers

  ## Usage

      use TripleStore.ReasonerTestCase

  Or import directly:

      import TripleStore.Test.ReasonerHelpers

  ## Function Naming Convention

  Helper functions use camelCase suffixes to match OWL/RDF vocabulary terms
  (e.g., `rdfs_subClassOf`, `owl_TransitiveProperty`). This intentionally
  deviates from Elixir's snake_case convention to maintain alignment with
  the W3C specifications.
  """

  alias TripleStore.Reasoner.{SemiNaive, ReasoningProfile, PatternMatcher}

  # ============================================================================
  # Namespace Constants
  # ============================================================================

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"
  @ex "http://example.org/"
  @ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

  # ============================================================================
  # IRI Builders
  # ============================================================================

  @doc "Creates an example namespace IRI: http://example.org/{name}"
  def ex_iri(name), do: {:iri, @ex <> name}

  @doc "Creates a LUBM university benchmark IRI: http://swat.cse.lehigh.edu/onto/univ-bench.owl#\{name\}"
  def ub_iri(name), do: {:iri, @ub <> name}

  # ============================================================================
  # RDF Vocabulary
  # ============================================================================

  @doc "Returns rdf:type IRI"
  def rdf_type, do: {:iri, @rdf <> "type"}

  # ============================================================================
  # RDFS Vocabulary
  # ============================================================================

  @doc "Returns rdfs:subClassOf IRI"
  def rdfs_subClassOf, do: {:iri, @rdfs <> "subClassOf"}

  @doc "Returns rdfs:subPropertyOf IRI"
  def rdfs_subPropertyOf, do: {:iri, @rdfs <> "subPropertyOf"}

  @doc "Returns rdfs:domain IRI"
  def rdfs_domain, do: {:iri, @rdfs <> "domain"}

  @doc "Returns rdfs:range IRI"
  def rdfs_range, do: {:iri, @rdfs <> "range"}

  # ============================================================================
  # OWL Vocabulary
  # ============================================================================

  @doc "Returns owl:TransitiveProperty IRI"
  def owl_TransitiveProperty, do: {:iri, @owl <> "TransitiveProperty"}

  @doc "Returns owl:SymmetricProperty IRI"
  def owl_SymmetricProperty, do: {:iri, @owl <> "SymmetricProperty"}

  @doc "Returns owl:FunctionalProperty IRI"
  def owl_FunctionalProperty, do: {:iri, @owl <> "FunctionalProperty"}

  @doc "Returns owl:InverseFunctionalProperty IRI"
  def owl_InverseFunctionalProperty, do: {:iri, @owl <> "InverseFunctionalProperty"}

  @doc "Returns owl:inverseOf IRI"
  def owl_inverseOf, do: {:iri, @owl <> "inverseOf"}

  @doc "Returns owl:sameAs IRI"
  def owl_sameAs, do: {:iri, @owl <> "sameAs"}

  @doc "Returns owl:Nothing IRI (empty class for inconsistency detection)"
  def owl_Nothing, do: {:iri, @owl <> "Nothing"}

  # ============================================================================
  # Query Helpers
  # ============================================================================

  @doc """
  Simulates a SPARQL-like query by finding all triples matching a pattern.

  Pattern uses {:var, :name} for unbound variables.

  ## Examples

      # Find all types of alice
      query(facts, {ex_iri("alice"), rdf_type(), {:var, :type}})

      # Find all instances of Person
      query(facts, {{:var, :x}, rdf_type(), ex_iri("Person")})
  """
  def query(facts, {s, p, o}) do
    pattern = {:pattern, [s, p, o]}
    PatternMatcher.filter_matching(facts, pattern)
  end

  @doc """
  Finds all types of a subject.

  ## Examples

      select_types(facts, ex_iri("alice"))
      # => [ex_iri("Person"), ex_iri("Student"), ...]
  """
  def select_types(facts, subject) do
    query(facts, {subject, rdf_type(), {:var, :type}})
    |> Enum.map(fn {_, _, type} -> type end)
  end

  @doc """
  Finds all objects for a subject-predicate pair.

  ## Examples

      select_objects(facts, ex_iri("alice"), ex_iri("knows"))
      # => [ex_iri("bob"), ex_iri("charlie"), ...]
  """
  def select_objects(facts, subject, predicate) do
    query(facts, {subject, predicate, {:var, :object}})
    |> Enum.map(fn {_, _, obj} -> obj end)
  end

  @doc """
  Finds all subjects for a predicate-object pair.

  ## Examples

      select_subjects(facts, rdf_type(), ex_iri("Person"))
      # => [ex_iri("alice"), ex_iri("bob"), ...]
  """
  def select_subjects(facts, predicate, object) do
    query(facts, {{:var, :subject}, predicate, object})
    |> Enum.map(fn {subj, _, _} -> subj end)
  end

  @doc """
  Checks if a triple exists in the fact set.

  ## Examples

      has_triple?(facts, {ex_iri("alice"), rdf_type(), ex_iri("Person")})
      # => true
  """
  def has_triple?(facts, triple) do
    MapSet.member?(facts, triple)
  end

  # ============================================================================
  # Materialization Helpers
  # ============================================================================

  @doc """
  Materializes facts using the specified reasoning profile.

  ## Options

  - `:profile` - The reasoning profile to use (default: `:owl2rl`)
    - `:rdfs` - RDFS rules only
    - `:owl2rl` - Full OWL 2 RL rules

  ## Examples

      facts = MapSet.new([...])
      all_facts = materialize(facts)
      all_facts = materialize(facts, :rdfs)
  """
  def materialize(initial_facts, profile \\ :owl2rl) do
    {:ok, rules} = ReasoningProfile.rules_for(profile)
    {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial_facts)
    all_facts
  end

  @doc """
  Materializes facts and returns both facts and statistics.

  ## Examples

      {all_facts, stats} = materialize_with_stats(facts)
      IO.puts("Derived \#{stats.total_derived} facts in \#{stats.iterations} iterations")
  """
  def materialize_with_stats(initial_facts, profile \\ :owl2rl) do
    {:ok, rules} = ReasoningProfile.rules_for(profile)
    {:ok, all_facts, stats} = SemiNaive.materialize_in_memory(rules, initial_facts)
    {all_facts, stats}
  end

  @doc """
  Computes which facts were derived (not in initial set).

  ## Examples

      derived = compute_derived(initial_facts, all_facts)
  """
  def compute_derived(initial_facts, all_facts) do
    MapSet.difference(all_facts, initial_facts)
  end
end
