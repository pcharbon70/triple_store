defmodule TripleStore.Benchmark.LDBC.SNB.BI.ParameterReferences do
  @moduledoc """
  Read-only validation of SNB BI parameters against a mapped quad store.

  Reference rules come from the pinned translation manifest. Entity IDs are
  resolved to stable mapping IRIs; entity names are resolved through the
  mapped `snb:name` predicate. Both paths use dictionary lookups and quad
  indices, so validation never decodes or scans RDF documents.
  """

  alias TripleStore.Benchmark.LDBC.SNB.Mapping
  alias TripleStore.Dictionary.StringToId
  alias TripleStore.QuadOperations

  @rdf_type RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
  @name RDF.iri("https://ldbcouncil.org/snb/ontology/name")
  @ontology "https://ldbcouncil.org/snb/ontology/"
  @entity_kinds %{
    "Person" => :person,
    "Forum" => :forum,
    "Post" => :post,
    "Comment" => :comment,
    "Organisation" => :organisation,
    "Place" => :place,
    "Tag" => :tag,
    "TagClass" => :tag_class
  }

  @type rule ::
          %{required(:kind) => :scalar}
          | %{required(:kind) => :entity_id | :entity_name, required(:entity) => String.t()}

  @doc "Validates one mapped-store reference using read-only dictionary and index lookups."
  @spec validate_rule(rule(), term(), TripleStore.store()) :: :ok | {:error, term()}
  def validate_rule(%{kind: :scalar}, _value, _store), do: :ok

  def validate_rule(%{kind: :entity_id, entity: entity} = rule, value, store) do
    with {:ok, identifier} <- normalize_id(value),
         {:ok, context} <- lookup_context(store, entity, rule, value),
         {:ok, subject_id} <-
           lookup_term(store.db, Mapping.entity_iri(entity, identifier), rule, value) do
      validate_subject(subject_id, context, rule, value)
    end
  end

  def validate_rule(%{kind: :entity_name, entity: entity} = rule, value, store)
      when is_binary(value) do
    with {:ok, context} <- lookup_context(store, entity, rule, value),
         {:ok, value_id} <- lookup_term(store.db, RDF.literal(value), rule, value),
         {:ok, name_id} <- lookup_term(store.db, @name, rule, value) do
      matches =
        store.db
        |> QuadOperations.lookup_quads({:var, :bound, :bound, :var}, %{
          p: name_id,
          o: value_id
        })
        |> Enum.filter(fn {subject_id, _predicate_id, _object_id, graph_id} ->
          QuadOperations.quad_exists?(store.db, {
            subject_id,
            context.type_id,
            context.class_id,
            graph_id
          })
        end)

      validate_matches(matches, context.graph_ids, rule, value)
    end
  end

  def validate_rule(rule, value, _store),
    do: {:error, {:invalid_reference_rule, rule, value}}

  @doc "Returns whether a field is selected by the named validation profile."
  @spec selected?(map(), atom(), String.t()) :: boolean()
  def selected?(_definition, :official, _field_name), do: true

  def selected?(definition, profile, field_name) do
    case get_in(definition, [:reference_profiles, profile, :fields]) do
      fields when is_map(fields) -> field_name in Map.get(fields, definition.number, [])
      _other -> false
    end
  end

  @doc "Validates that a requested reference profile matches the dataset and bundle provenance."
  @spec validate_profile([map()], atom(), map(), map()) :: :ok | {:error, term()}
  def validate_profile([definition | _definitions], profile, manifest, bundle) do
    case get_in(definition, [:reference_profiles, profile]) do
      nil ->
        {:error, {:unknown_reference_profile, profile}}

      profile_metadata ->
        with :ok <- require_provenance(profile_metadata, bundle),
             :ok <- require_manifest_profile(profile_metadata, manifest) do
          :ok
        end
    end
  end

  def validate_profile([], _profile, _manifest, _bundle),
    do: {:error, :empty_reference_definitions}

  defp validate_subject(subject_id, context, rule, value) do
    matches =
      QuadOperations.lookup_quads(
        context.db,
        {:bound, :bound, :bound, :var},
        %{s: subject_id, p: context.type_id, o: context.class_id}
      )

    validate_matches(matches, context.graph_ids, rule, value)
  end

  defp validate_matches([], _expected_graph_ids, rule, value),
    do: {:error, {:unknown_reference, reference_kind(rule), value}}

  defp validate_matches(matches, expected_graph_ids, rule, value) do
    appropriate =
      Enum.filter(matches, fn {_s, _p, _o, graph_id} -> graph_id in expected_graph_ids end)

    cond do
      appropriate == [] ->
        {:error, {:graph_inappropriate_reference, reference_kind(rule), value}}

      appropriate |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> length() > 1 ->
        {:error, {:ambiguous_reference, reference_kind(rule), value}}

      true ->
        :ok
    end
  end

  defp lookup_context(%{db: db}, entity, rule, value) when is_binary(entity) do
    with {:ok, type_id} <- lookup_context_term(db, @rdf_type),
         {:ok, class_id} <- lookup_term(db, RDF.iri(@ontology <> entity), rule, value),
         {:ok, graph_ids} <- expected_graph_ids(db) do
      {:ok, %{db: db, type_id: type_id, class_id: class_id, graph_ids: graph_ids}}
    end
  end

  defp lookup_context(_store, _entity, _rule, _value), do: {:error, :invalid_reference_store}

  defp expected_graph_ids(db) do
    graph_ids =
      [:initial, :updates]
      |> Enum.flat_map(fn component ->
        case StringToId.lookup_id(db, Mapping.graph_iri(:snb_bi, component)) do
          {:ok, id} -> [id]
          :not_found -> []
          {:error, _reason} -> []
        end
      end)

    if graph_ids == [], do: {:error, :snb_bi_graphs_missing}, else: {:ok, graph_ids}
  end

  defp lookup_context_term(db, term) do
    case StringToId.lookup_id(db, term) do
      {:ok, id} -> {:ok, id}
      :not_found -> {:error, :mapping_vocabulary_missing}
      {:error, reason} -> {:error, {:reference_lookup_failed, reason}}
    end
  end

  defp lookup_term(db, term, rule, value) do
    case StringToId.lookup_id(db, term) do
      {:ok, id} -> {:ok, id}
      :not_found -> {:error, {:unknown_reference, reference_kind(rule), value}}
      {:error, reason} -> {:error, {:reference_lookup_failed, reason}}
    end
  end

  defp normalize_id(value) when is_integer(value) and value >= 0, do: {:ok, value}

  defp normalize_id(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} when integer >= 0 -> {:ok, integer}
      _other -> {:error, {:invalid_reference_id, value}}
    end
  end

  defp normalize_id(value), do: {:error, {:invalid_reference_id, value}}

  defp reference_kind(%{entity: entity}), do: Map.fetch!(@entity_kinds, entity)

  defp require_provenance(%{provenance: provenance}, %{provenance: provenance}), do: :ok

  defp require_provenance(%{provenance: expected}, %{provenance: actual}),
    do: {:error, {:reference_profile_provenance, expected, actual}}

  defp require_manifest_profile(%{manifest_profile: :any}, _manifest), do: :ok

  defp require_manifest_profile(%{manifest_profile: expected}, %{profile: expected}), do: :ok

  defp require_manifest_profile(%{manifest_profile: expected}, %{profile: actual}),
    do: {:error, {:reference_profile_manifest, expected, actual}}
end
