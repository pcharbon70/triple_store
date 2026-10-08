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

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.StringToId
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

  # ============================================================================
  # Types
  # ============================================================================

  @type db_ref :: ErlangAdapter.db_ref()
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
    predicate_ids = tbox_predicate_ids(db)
    graph_prefix = QuadIndex.gspo_prefix(graph_id)

    tbox_quads =
      ErlangAdapter.fold(db, @gspo_cf, graph_prefix, MapSet.new(), fn entry, acc ->
        maybe_collect_tbox_quad(entry, acc, predicate_ids)
      end)

    {:ok, tbox_quads}
  rescue
    e ->
      Logger.error("Failed to extract TBox from graph #{graph_id}: #{inspect(e)}")
      {:error, {:tbox_extraction_failed, graph_id, e}}
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
    predicate_ids = tbox_predicate_ids(db)
    graph_prefix = QuadIndex.gspo_prefix(graph_id)

    tbox_data =
      ErlangAdapter.fold(db, @gspo_cf, graph_prefix, [], fn entry, acc ->
        maybe_collect_tbox_list(entry, acc, predicate_ids)
      end)
      |> Enum.sort()

    fingerprint =
      :crypto.hash(:sha256, :erlang.term_to_binary(tbox_data))
      |> Base.encode16(case: :lower)

    {:ok, fingerprint}
  rescue
    e ->
      Logger.error("Failed to compute TBox fingerprint for graph #{graph_id}: #{inspect(e)}")
      {:error, {:fingerprint_failed, graph_id, e}}
  end

  defp maybe_collect_tbox_quad({key, _value}, acc, predicate_ids) do
    {s, p, o, g} = QuadIndex.key_to_quad(:gspo, key)
    if MapSet.member?(predicate_ids, p), do: MapSet.put(acc, {g, s, p, o}), else: acc
  end

  defp maybe_collect_tbox_list({key, _value}, acc, predicate_ids) do
    {s, p, o, g} = QuadIndex.key_to_quad(:gspo, key)
    if MapSet.member?(predicate_ids, p), do: [{g, s, p, o} | acc], else: acc
  end

  defp tbox_predicate_ids(db) do
    Enum.reduce(@tbox_predicates, MapSet.new(), fn iri, ids ->
      case StringToId.lookup_id(db, RDF.iri(iri)) do
        {:ok, id} -> MapSet.put(ids, id)
        _ -> ids
      end
    end)
  end

  @doc """
  Returns the list of built-in TBox predicate IRIs.
  """
  def built_in_tbox_predicates, do: @tbox_predicates

  # ============================================================================
  # Private Functions
  # ============================================================================
end
