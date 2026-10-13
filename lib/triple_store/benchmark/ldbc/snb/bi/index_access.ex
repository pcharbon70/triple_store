defmodule TripleStore.Benchmark.LDBC.SNB.BI.IndexAccess do
  @moduledoc """
  Lazy, bounded access to the mapped SNB quad indices.

  Each call opens a managed quad stream and retains at most one adjacency or
  property lookup in memory. The access layer restricts reads to the SNB BI
  initial and update graphs and checks cancellation, deadlines, and
  intermediate-result limits before returning materialized lookup slices.
  """

  alias TripleStore.Adapter
  alias TripleStore.Benchmark.LDBC.SNB.Mapping
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Executor

  @ontology "https://ldbcouncil.org/snb/ontology/"
  @rdf_subject RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#subject")
  @rdf_predicate RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate")
  @rdf_object RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#object")

  defstruct [:db, :manager, :graphs, :deadline, :cancelled?, :max_visited, :max_intermediate]

  @type t :: %__MODULE__{
          db: pid(),
          manager: pid(),
          graphs: MapSet.t(non_neg_integer()),
          deadline: integer(),
          cancelled?: (-> boolean()),
          max_visited: pos_integer(),
          max_intermediate: pos_integer()
        }

  @doc "Builds an access context from an open quad store."
  @spec new(map(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(%{db: db, dict_manager: manager} = context, opts) when is_pid(db) do
    timeout = Keyword.get(opts, :timeout, Map.get(context, :timeout, 120_000))

    max_intermediate =
      Keyword.get(opts, :max_intermediate, Map.get(context, :max_intermediate, 5_000_000))

    max_visited = Keyword.get(opts, :max_visited, Map.get(context, :max_visited, 1_000_000))
    cancelled? = Keyword.get(opts, :cancelled?, Map.get(context, :cancelled?, fn -> false end))

    if is_integer(timeout) and timeout > 0 and is_integer(max_intermediate) and
         max_intermediate > 0 and is_integer(max_visited) and max_visited > 0 and
         is_function(cancelled?, 0) do
      with :ok <- authorize_graphs(context),
           {:ok, graphs} <- graph_ids(manager) do
        {:ok,
         %__MODULE__{
           db: db,
           manager: manager,
           graphs: graphs,
           deadline: System.monotonic_time(:millisecond) + timeout,
           cancelled?: cancelled?,
           max_visited: max_visited,
           max_intermediate: max_intermediate
         }}
      end
    else
      {:error, :invalid_graph_operation_context}
    end
  end

  def new(_context, _opts), do: {:error, :bi_query_context_required}

  @doc "Checks cancellation and the operation deadline."
  @spec checkpoint(t()) :: :ok | {:error, :cancelled | :timeout}
  def checkpoint(access) do
    cond do
      access.cancelled?.() -> {:error, :cancelled}
      System.monotonic_time(:millisecond) >= access.deadline -> {:error, :timeout}
      true -> :ok
    end
  end

  @doc "Returns the internal ID for one mapped entity without creating it."
  @spec entity(t(), String.t(), term()) ::
          {:ok, non_neg_integer()} | :not_found | {:error, term()}
  def entity(access, type, id),
    do: Manager.lookup_id(access.manager, Mapping.entity_iri(type, id))

  @doc "Returns an entity's external numeric SNB identifier."
  @spec external_id(t(), non_neg_integer()) :: {:ok, integer()} | {:error, term()}
  def external_id(access, id) do
    with {:ok, iri} <- Adapter.to_rdf_iri(access.db, id),
         segment when is_binary(segment) <- iri |> to_string() |> String.split("/") |> List.last(),
         {value, ""} <- segment |> URI.decode() |> Integer.parse() do
      {:ok, value}
    else
      :not_found -> {:error, {:missing_dictionary_term, id}}
      {:error, _} = error -> error
      _ -> {:error, {:invalid_snb_entity_id, id}}
    end
  end

  @doc "Checks whether an internal entity IRI has the requested mapped type."
  @spec entity_type(t(), non_neg_integer(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def entity_type(access, id, type) do
    case Adapter.to_rdf_iri(access.db, id) do
      {:ok, iri} -> {:ok, String.contains?(to_string(iri), "/entity/#{type}/")}
      :not_found -> {:error, {:missing_dictionary_term, id}}
      {:error, reason} -> {:error, {:invalid_mapped_entity, id, reason}}
    end
  end

  @doc "Filters mapped entity IDs by type while propagating dictionary failures."
  @spec entities_of_type(t(), [non_neg_integer()], String.t()) ::
          {:ok, [non_neg_integer()]} | {:error, term()}
  def entities_of_type(access, entities, type) do
    Enum.reduce_while(entities, {:ok, []}, fn entity, {:ok, matches} ->
      case entity_type(access, entity, type) do
        {:ok, true} -> {:cont, {:ok, [entity | matches]}}
        {:ok, false} -> {:cont, {:ok, matches}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> reverse_result()
  end

  @doc "Looks up mapped outgoing relationship or property objects."
  @spec objects(t(), non_neg_integer(), String.t()) ::
          {:ok, [non_neg_integer()]} | {:error, term()}
  def objects(access, subject, predicate) do
    with :ok <- checkpoint(access),
         {:ok, predicate_id} <- predicate_id(access, predicate) do
      collect(
        access,
        QuadOperations.lookup_quads_stream(
          access.db,
          {:bound, :bound, :var, :var},
          %{s: subject, p: predicate_id}
        ),
        fn {_subject, _predicate, object, _graph} -> object end
      )
    else
      :not_found -> {:ok, []}
      {:error, _} = error -> error
    end
  end

  @doc "Looks up mapped incoming relationship subjects."
  @spec subjects(t(), String.t(), non_neg_integer()) ::
          {:ok, [non_neg_integer()]} | {:error, term()}
  def subjects(access, predicate, object) do
    with :ok <- checkpoint(access),
         {:ok, predicate_id} <- predicate_id(access, predicate) do
      collect(
        access,
        QuadOperations.lookup_quads_stream(
          access.db,
          {:var, :bound, :bound, :var},
          %{p: predicate_id, o: object}
        ),
        fn {subject, _predicate, _object, _graph} -> subject end
      )
    else
      :not_found -> {:ok, []}
      {:error, _} = error -> error
    end
  end

  @doc "Returns undirected neighbours for one direct mapped relationship."
  @spec neighbours(t(), non_neg_integer(), String.t()) ::
          {:ok, [non_neg_integer()]} | {:error, term()}
  def neighbours(access, vertex, predicate) do
    with {:ok, outgoing} <- objects(access, vertex, predicate),
         {:ok, incoming} <- subjects(access, predicate, vertex) do
      bounded_unique(access, outgoing ++ incoming)
    end
  end

  @doc "Decodes every literal value for a mapped property."
  @spec property_values(t(), non_neg_integer(), String.t()) :: {:ok, [term()]} | {:error, term()}
  def property_values(access, subject, property) do
    with {:ok, ids} <- objects(access, subject, property) do
      Enum.reduce_while(ids, {:ok, []}, fn id, {:ok, values} ->
        case Adapter.to_rdf_literal(access.db, id) do
          {:ok, literal} -> {:cont, {:ok, [RDF.Literal.value(literal) | values]}}
          :not_found -> {:halt, {:error, {:missing_dictionary_term, id}}}
          {:error, reason} -> {:halt, {:error, {:invalid_mapped_literal, id, reason}}}
        end
      end)
      |> reverse_result()
    end
  end

  @doc "Checks one mapped entity subtype property without hiding lookup failures."
  @spec subtype?(t(), non_neg_integer(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def subtype?(access, entity, subtype) do
    with {:ok, values} <- property_values(access, entity, "type") do
      {:ok, subtype in values}
    end
  end

  @doc "Finds mapped entities by exact name and optional subtype property."
  @spec entities_by_name(t(), String.t(), String.t(), String.t() | nil) ::
          {:ok, [non_neg_integer()]} | {:error, term()}
  def entities_by_name(access, entity_type, name, subtype \\ nil) do
    with {:ok, name_id} <- literal_id(access, name),
         {:ok, candidates} <- subjects(access, "name", name_id),
         {:ok, typed_candidates} <- entities_of_type(access, candidates, entity_type) do
      case filter_subtype(typed_candidates, access, subtype) do
        {:ok, filtered} -> bounded_unique(access, filtered)
        {:error, _} = error -> error
      end
    else
      :not_found -> {:ok, []}
      {:error, _} = error -> error
    end
  end

  @doc "Reads a reified mapped relationship property for one direct edge."
  @spec relationship_property(t(), non_neg_integer(), String.t(), non_neg_integer(), String.t()) ::
          {:ok, [term()]} | {:error, term()}
  def relationship_property(access, from, predicate, to, property) do
    with {:ok, rdf_subject_id} <- term_id(access, @rdf_subject),
         {:ok, rdf_predicate_id} <- term_id(access, @rdf_predicate),
         {:ok, rdf_object_id} <- term_id(access, @rdf_object),
         {:ok, relationship_predicate_id} <- predicate_id(access, predicate),
         {:ok, relationships} <-
           lookup_subjects(access, rdf_subject_id, from),
         {:ok, matching} <-
           matching_relationships(
             access,
             relationships,
             rdf_predicate_id,
             relationship_predicate_id,
             rdf_object_id,
             to
           ) do
      relationship_values(access, matching, property)
    else
      :not_found -> {:ok, []}
      {:error, _} = error -> error
    end
  end

  @doc "Returns remaining traversal time and canonical bounds."
  @spec traversal_options(t(), String.t(), (term() -> term())) :: keyword()
  def traversal_options(access, operation, provider) do
    [
      neighbors: provider,
      timeout: max(access.deadline - System.monotonic_time(:millisecond), 1),
      max_visited: access.max_visited,
      max_intermediate: access.max_intermediate,
      cancelled?: access.cancelled?,
      operation: operation
    ]
  end

  @doc "Rejects a materialized operation slice above the configured bound."
  @spec ensure_bound(t(), list()) :: :ok | {:error, term()}
  def ensure_bound(access, values) when is_list(values) do
    if length(values) > access.max_intermediate do
      {:error, {:intermediate_bound_exceeded, access.max_intermediate}}
    else
      :ok
    end
  end

  defp graph_ids(manager) do
    Enum.reduce_while([:initial, :updates], {:ok, []}, fn component, {:ok, ids} ->
      case Manager.lookup_id(manager, Mapping.graph_iri(:snb_bi, component)) do
        {:ok, id} -> {:cont, {:ok, [id | ids]}}
        :not_found -> {:cont, {:ok, ids}}
        {:error, reason} -> {:halt, {:error, {:graph_identity_lookup_failed, component, reason}}}
      end
    end)
    |> case do
      {:ok, ids} -> {:ok, MapSet.new(ids)}
      {:error, _} = error -> error
    end
  end

  defp authorize_graphs(context) do
    graph_terms =
      Enum.map([:initial, :updates], fn component ->
        {:named_node, Mapping.graph_iri(:snb_bi, component) |> to_string()}
      end)

    Executor.authorize_active_dataset(context, graph_terms)
  end

  defp predicate_id(access, name), do: term_id(access, RDF.iri(@ontology <> name))
  defp term_id(access, term), do: Manager.lookup_id(access.manager, term)
  defp literal_id(access, value), do: Manager.lookup_id(access.manager, RDF.literal(value))

  defp lookup_subjects(access, predicate_id, object) do
    collect(
      access,
      QuadOperations.lookup_quads_stream(
        access.db,
        {:var, :bound, :bound, :var},
        %{p: predicate_id, o: object}
      ),
      fn {subject, _predicate, _object, _graph} -> subject end
    )
  end

  defp matching_relationships(access, relationships, rdf_predicate, predicate, rdf_object, to) do
    Enum.reduce_while(relationships, {:ok, []}, fn relationship, {:ok, matching} ->
      case relationship_matches?(access, relationship, rdf_predicate, predicate, rdf_object, to) do
        {:ok, true} -> {:cont, {:ok, [relationship | matching]}}
        {:ok, false} -> {:cont, {:ok, matching}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> reverse_result()
  end

  defp relationship_matches?(access, relationship, rdf_predicate, predicate, rdf_object, to) do
    with {:ok, true} <- quad?(access, relationship, rdf_predicate, predicate),
         {:ok, true} <- quad?(access, relationship, rdf_object, to) do
      {:ok, true}
    else
      {:ok, false} -> {:ok, false}
      {:error, _} = error -> error
    end
  end

  defp quad?(access, subject, predicate, object) do
    Enum.reduce_while(access.graphs, {:ok, false}, fn graph, {:ok, false} ->
      case QuadOperations.quad_exists?(access.db, {subject, predicate, object, graph}) do
        true -> {:halt, {:ok, true}}
        false -> {:cont, {:ok, false}}
      end
    end)
  end

  defp relationship_values(access, relationships, property) do
    Enum.reduce_while(relationships, {:ok, []}, fn relationship, {:ok, values} ->
      case property_values(access, relationship, property) do
        {:ok, found} -> {:cont, {:ok, values ++ found}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp collect(access, stream, mapper) do
    values =
      stream
      |> Stream.filter(fn {_s, _p, _o, graph} -> MapSet.member?(access.graphs, graph) end)
      |> Stream.map(mapper)
      |> Stream.uniq()
      |> Enum.take(access.max_intermediate + 1)

    if length(values) > access.max_intermediate do
      {:error, {:intermediate_bound_exceeded, access.max_intermediate}}
    else
      {:ok, values}
    end
  end

  defp bounded_unique(access, values) do
    values = Enum.uniq(values)

    if length(values) > access.max_intermediate do
      {:error, {:intermediate_bound_exceeded, access.max_intermediate}}
    else
      {:ok, values}
    end
  end

  defp filter_subtype(candidates, _access, nil), do: {:ok, candidates}

  defp filter_subtype(candidates, access, subtype) do
    Enum.reduce_while(candidates, {:ok, []}, fn candidate, {:ok, matches} ->
      filter_subtype_candidate(
        property_values(access, candidate, "type"),
        candidate,
        matches,
        subtype
      )
    end)
    |> reverse_result()
  end

  defp filter_subtype_candidate({:ok, values}, candidate, matches, subtype) do
    if subtype in values,
      do: {:cont, {:ok, [candidate | matches]}},
      else: {:cont, {:ok, matches}}
  end

  defp filter_subtype_candidate({:error, _} = error, _candidate, _matches, _subtype),
    do: {:halt, error}

  defp reverse_result({:ok, values}), do: {:ok, Enum.reverse(values)}
  defp reverse_result({:error, _} = error), do: error
end
