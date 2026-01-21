defmodule TripleStore.Reasoner.TBoxExtractor do
  @moduledoc """
  Extracts TBox (schema) facts from a graph for use in reasoning.

  TBox (Terminological Box) contains schema information such as:
  - Class hierarchies (rdfs:subClassOf)
  - Property hierarchies (rdfs:subPropertyOf)
  - Property characteristics (TransitiveProperty, SymmetricProperty, etc.)
  - Domain and range declarations
  - Property restrictions

  This module extracts TBox facts from a source graph for use in
  reasoning across multiple graphs that share the same schema.

  ## Usage

      # Extract TBox from graph 0 (default graph)
      {:ok, tbox_facts} = TBoxExtractor.extract_tbox(db, 0)

      # Get TBox fingerprint for change detection
      {:ok, fingerprint} = TBoxExtractor.tbox_fingerprint(db, 0)
  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.IdToString
  alias TripleStore.QuadIndex
  alias TripleStore.Reasoner.Namespaces

  require Logger

  # ============================================================================
  # Constants
  # ============================================================================

  @gspo_cf :gspo

  # TBox predicates - these identify schema triples (as IRIs for comparison)
  @tbox_predicates MapSet.new([
                     # RDFS schema predicates
                     Namespaces.rdf_type(),
                     Namespaces.rdfs_sub_class_of(),
                     Namespaces.rdfs_sub_property_of(),
                     Namespaces.rdfs_domain(),
                     Namespaces.rdfs_range(),
                     # OWL class expressions
                     Namespaces.owl_equivalent_class(),
                     Namespaces.owl_disjoint_with(),
                     # OWL property characteristics
                     Namespaces.owl_transitive_property(),
                     Namespaces.owl_symmetric_property(),
                     Namespaces.owl_reflexive_property(),
                     Namespaces.owl_irreflexive_property(),
                     Namespaces.owl_functional_property(),
                     Namespaces.owl_inverse_functional_property(),
                     Namespaces.owl_asymmetric_property(),
                     # OWL property restrictions
                     Namespaces.owl_inverse_of(),
                     Namespaces.owl_has_value(),
                     Namespaces.owl_some_values_from(),
                     Namespaces.owl_all_values_from(),
                     Namespaces.owl_on_property()
                   ])

  # Cache for predicate ID -> IRI mappings to avoid repeated lookups
  @predicate_cache_table :tbox_predicate_cache

  # ============================================================================
  # Types
  # ============================================================================

  @type db_ref :: NIF.db_ref()
  @type graph_id :: non_neg_integer()
  @type id_quad :: {integer(), integer(), integer(), integer()}
  @type tbox_facts :: MapSet.t(id_quad())

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Extracts TBox (schema) facts from a graph.

  TBox facts are identified by having one of the following predicates:
  - rdf:type (for type declarations)
  - rdfs:subClassOf, rdfs:subPropertyOf
  - rdfs:domain, rdfs:range
  - owl:TransitiveProperty, owl:SymmetricProperty, etc.
  - owl:inverseOf, owl:hasValue, owl:someValuesFrom, owl:allValuesFrom

  ## Parameters

  - `db` - Database reference
  - `graph_id` - Graph ID to extract TBox from

  ## Returns

  - `{:ok, tbox_facts}` - MapSet of TBox quads
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, tbox} = TBoxExtractor.extract_tbox(db, 0)
      # Returns MapSet of schema quads
  """
  @spec extract_tbox(db_ref(), graph_id()) :: {:ok, tbox_facts()} | {:error, term()}
  def extract_tbox(db, graph_id) do
    # Initialize predicate cache for this extraction
    cache_ref = :ets.new(@predicate_cache_table, [:set, :private])

    try do
      # Use fold to iterate over the graph's quads and filter TBox predicates
      graph_prefix = QuadIndex.gspo_prefix(graph_id)

      tbox_quads =
        NIF.fold(db, @gspo_cf, graph_prefix, MapSet.new(), fn
          {key, _value}, acc ->
            case QuadIndex.key_to_quad(:gspo, key) do
              {_g, _s, p, _o} = quad ->
                if tbox_predicate?(db, p, cache_ref) do
                  MapSet.put(acc, quad)
                else
                  acc
                end

              _error ->
                acc
            end
        end)

      {:ok, tbox_quads}
    rescue
      e ->
        Logger.error("Failed to extract TBox from graph #{graph_id}: #{inspect(e)}")
        {:error, {:tbox_extraction_failed, graph_id, e}}
    after
      # Clean up the cache
      :ets.delete(cache_ref)
    end
  end

  @doc """
  Computes a fingerprint of TBox facts in a graph.

  The fingerprint is a hash that can be used for cache invalidation.
  If the fingerprint changes, the TBox has been modified.

  ## Parameters

  - `db` - Database reference
  - `graph_id` - Graph ID to fingerprint

  ## Returns

  - `{:ok, fingerprint}` - SHA-256 hash as hex string
  - `{:error, reason}` - On failure
  """
  @spec tbox_fingerprint(db_ref(), graph_id()) :: {:ok, String.t()} | {:error, term()}
  def tbox_fingerprint(db, graph_id) do
    # Initialize predicate cache for this operation
    cache_ref = :ets.new(@predicate_cache_table, [:set, :private])

    try do
      # Collect all TBox predicate IRIs in the graph
      graph_prefix = QuadIndex.gspo_prefix(graph_id)

      tbox_data =
        NIF.fold(db, @gspo_cf, graph_prefix, [], fn
          {key, _value}, acc ->
            case QuadIndex.key_to_quad(:gspo, key) do
              {_g, _s, p, _o} = quad ->
                if tbox_predicate?(db, p, cache_ref) do
                  [quad | acc]
                else
                  acc
                end

              _error ->
                acc
            end
        end)
        |> Enum.sort()

      # Compute hash of sorted TBox quads
      fingerprint =
        :crypto.hash(:sha256, :erlang.term_to_binary(tbox_data))
        |> Base.encode16(case: :lower)

      {:ok, fingerprint}
    rescue
      e ->
        Logger.error("Failed to compute TBox fingerprint for graph #{graph_id}: #{inspect(e)}")
        {:error, {:fingerprint_failed, graph_id, e}}
    after
      # Clean up the cache
      :ets.delete(cache_ref)
    end
  end

  # Checks if a predicate ID is a TBox (schema) predicate.
  #
  # This function performs a dictionary lookup to get the predicate IRI,
  # then checks if it's in the built-in TBox predicate set. Results are
  # cached in an ETS table to avoid repeated lookups.
  #
  # Parameters:
  # - `db` - Database reference
  # - `predicate_id` - Predicate term ID to check
  # - `cache_ref` - ETS table reference for caching results
  #
  # Returns:
  # - `true` if the predicate is a TBox predicate
  # - `false` otherwise
  @spec tbox_predicate?(db_ref(), integer(), :ets.tid()) :: boolean()
  defp tbox_predicate?(db, predicate_id, cache_ref) do
    case :ets.lookup(cache_ref, predicate_id) do
      [{^predicate_id, result}] ->
        result

      [] ->
        result = do_check_tbox_predicate(db, predicate_id)
        :ets.insert(cache_ref, {predicate_id, result})
        result
    end
  end

  @spec do_check_tbox_predicate(db_ref(), integer()) :: boolean()
  defp do_check_tbox_predicate(db, predicate_id) when is_integer(predicate_id) do
    case IdToString.lookup_term(db, predicate_id) do
      {:ok, %RDF.IRI{value: iri}} ->
        tbox_predicate_by_iri?(iri)

      _ ->
        # Not a URI or not found, can't be a TBox predicate
        false
    end
  end

  @doc """
  Returns the list of built-in TBox predicate IRIs.
  """
  @spec built_in_tbox_predicates() :: MapSet.t(String.t())
  def built_in_tbox_predicates, do: @tbox_predicates

  # ============================================================================
  # Private Functions
  # ============================================================================

  # Check if a predicate is a TBox predicate by IRI
  defp tbox_predicate_by_iri?(predicate_iri) do
    MapSet.member?(@tbox_predicates, predicate_iri)
  end
end
