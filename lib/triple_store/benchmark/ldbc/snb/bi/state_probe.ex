defmodule TripleStore.Benchmark.LDBC.SNB.BI.StateProbe do
  @moduledoc """
  Deterministic post-update state evidence for SNB BI correctness runs.

  A probe reads all four explicit quad indices, normalizes them to canonical
  `{subject, predicate, object, graph}` tuples, and refuses to certify a state
  when the indices disagree. Semantic counts and the state checksum are derived
  from decoded RDF terms, so dictionary allocation order is not part of the
  correctness identity.
  """

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Benchmark.LDBC.SNB.BI.GraphAdapterRegistry
  alias TripleStore.Benchmark.LDBC.SNB.Mapping
  alias TripleStore.QuadIndex

  @schema_version 1
  @indices [:gspo, :gpos, :spog, :posg]
  @rdf_type RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
  @ontology "https://ldbcouncil.org/snb/ontology/"
  @entity_classes MapSet.new(Enum.map(Mapping.entity_types(), &RDF.iri(@ontology <> &1)))
  @relationship_predicates MapSet.new(
                             Enum.map(Mapping.relationship_types(), &RDF.iri(@ontology <> &1))
                           )

  @type snapshot :: %{
          required(:schema_version) => pos_integer(),
          required(:counts) => map(),
          required(:facets) => map(),
          required(:index_counts) => map(),
          required(:indices_consistent?) => boolean(),
          required(:identities) => map(),
          required(:state_checksum) => String.t(),
          required(:observations) => map()
        }

  @doc "Captures deterministic persisted and observer evidence for one open quad store."
  @spec capture(TripleStore.store(), keyword()) :: {:ok, snapshot()} | {:error, term()}
  def capture(%{db: db} = _store, opts \\ []) do
    with {:ok, index_quads} <- scan_indices(db),
         true <- consistent?(index_quads) || {:error, index_divergence(index_quads)},
         canonical <- Map.fetch!(index_quads, :gspo),
         {:ok, rdf_quads} <- Adapter.to_rdf_quads(db, canonical) do
      sorted_rdf = Enum.sort_by(rdf_quads, &quad_key/1)

      {:ok,
       %{
         schema_version: @schema_version,
         counts: counts(sorted_rdf),
         facets: facets(sorted_rdf),
         index_counts: Map.new(index_quads, fn {index, quads} -> {index, length(quads)} end),
         indices_consistent?: true,
         identities: identities(sorted_rdf),
         state_checksum: checksum(sorted_rdf),
         observations: observations(opts)
       }}
    else
      {:error, _} = error -> error
      other -> {:error, {:state_probe_failed, other}}
    end
  rescue
    error -> {:error, {:state_probe_failed, Exception.message(error)}}
  end

  @doc "Loads the versioned expected smoke-state probes."
  @spec expectations(Path.t() | nil) :: {:ok, map()} | {:error, term()}
  def expectations(root \\ nil) do
    path = Path.join(root || default_fixture_root(), "updates/state-probes.exs")

    with true <- File.regular?(path) || {:error, {:state_probe_fixture_missing, path}},
         {fixture, _binding} <- Code.eval_file(path),
         :ok <- validate_expectations(fixture) do
      {:ok, fixture}
    else
      {:error, _} = error -> error
      _other -> {:error, :invalid_state_probe_fixture}
    end
  rescue
    error -> {:error, {:invalid_state_probe_fixture, Exception.message(error)}}
  end

  @doc "Validates counts, checksum, and present or absent mapped quads."
  @spec verify(TripleStore.store(), map(), keyword()) :: :ok | {:error, term()}
  def verify(store, expected, opts \\ []) when is_map(expected) do
    with {:ok, snapshot} <- capture(store, opts),
         :ok <- verify_field(snapshot, expected, :counts),
         :ok <- verify_field(snapshot, expected, :state_checksum),
         :ok <- verify_facets(snapshot.facets, Map.get(expected, :facets, %{})),
         :ok <- verify_identities(snapshot.identities, Map.get(expected, :identities, %{})),
         :ok <- verify_quads(store.db, Map.get(expected, :present, []), true) do
      verify_quads(store.db, Map.get(expected, :absent, []), false)
    end
  end

  @doc "Rejects a changed graph whose observed cache or statistics generation stayed stale."
  @spec verify_transition(snapshot(), snapshot()) :: :ok | {:error, term()}
  def verify_transition(before, after_snapshot)
      when is_map(before) and is_map(after_snapshot) do
    if before.state_checksum == after_snapshot.state_checksum do
      {:error, :post_update_state_unchanged}
    else
      stale =
        [:cache, :statistics]
        |> Enum.filter(fn key ->
          previous = get_in(before, [:observations, key])
          current = get_in(after_snapshot, [:observations, key])
          previous != :unobserved and current != :unobserved and previous == current
        end)

      if stale == [], do: :ok, else: {:error, {:stale_post_update_observations, stale}}
    end
  end

  @doc "Proves that a failed batch preserved persisted state, answers, and observers."
  @spec verify_preserved(snapshot(), snapshot(), map(), map()) :: :ok | {:error, term()}
  def verify_preserved(before, after_snapshot, answers_before, answers_after)
      when is_map(before) and is_map(after_snapshot) and is_map(answers_before) and
             is_map(answers_after) do
    changed =
      []
      |> changed_if(before.state_checksum != after_snapshot.state_checksum, :state)
      |> changed_if(before.counts != after_snapshot.counts, :counts)
      |> changed_if(before.index_counts != after_snapshot.index_counts, :indices)
      |> changed_if(
        get_in(before, [:observations, :cache]) !=
          get_in(after_snapshot, [:observations, :cache]),
        :cache
      )
      |> changed_if(
        get_in(before, [:observations, :statistics]) !=
          get_in(after_snapshot, [:observations, :statistics]),
        :statistics
      )
      |> changed_if(answers_before != answers_after, :answers)
      |> Enum.reverse()

    if changed == [], do: :ok, else: {:error, {:failed_batch_state_changed, changed}}
  end

  @doc "Selects reads whose translated RDF vocabulary intersects mapped mutations."
  @spec affected_reads([map()], [map()]) :: [map()]
  def affected_reads(definitions, mutations) when is_list(definitions) and is_list(mutations) do
    affected = mutations |> affected_terms() |> MapSet.to_list()

    Enum.filter(definitions, fn definition ->
      definition
      |> query_terms()
      |> Enum.any?(&(&1 in affected))
    end)
  end

  defp scan_indices(db) do
    Enum.reduce_while(@indices, {:ok, %{}}, fn index, {:ok, acc} ->
      result =
        db
        |> ErlangAdapter.prefix_stream(index, <<>>, fill_cache: false)
        |> Enum.map(fn {key, _value} -> QuadIndex.key_to_quad(index, key) end)
        |> Enum.sort()

      {:cont, {:ok, Map.put(acc, index, result)}}
    end)
  end

  defp consistent?(indices) do
    case Map.values(indices) do
      [] -> true
      [first | rest] -> Enum.all?(rest, &(&1 == first))
    end
  end

  defp index_divergence(indices) do
    sets = Map.new(indices, fn {index, quads} -> {index, MapSet.new(quads)} end)
    union = sets |> Map.values() |> Enum.reduce(MapSet.new(), &MapSet.union/2)

    details =
      Map.new(sets, fn {index, quads} ->
        {index,
         %{count: MapSet.size(quads), missing: MapSet.size(MapSet.difference(union, quads))}}
      end)

    {:explicit_index_divergence, details}
  end

  defp counts(quads) do
    %{
      statements: length(quads),
      entities:
        Enum.count(quads, fn {_s, p, o, _g} ->
          p == @rdf_type and MapSet.member?(@entity_classes, o)
        end),
      relationships:
        Enum.count(quads, fn {_s, p, _o, _g} -> MapSet.member?(@relationship_predicates, p) end),
      properties: Enum.count(quads, &property_quad?/1),
      graphs: quads |> Enum.map(&elem(&1, 3)) |> MapSet.new() |> MapSet.size()
    }
  end

  defp facets(quads) do
    %{
      entities:
        quads
        |> Enum.filter(fn {_s, p, o, _g} ->
          p == @rdf_type and MapSet.member?(@entity_classes, o)
        end)
        |> Enum.frequencies_by(fn {_s, _p, type, _g} -> iri_local(type) end),
      relationships:
        quads
        |> Enum.filter(fn {_s, p, _o, _g} -> MapSet.member?(@relationship_predicates, p) end)
        |> Enum.frequencies_by(fn {_s, predicate, _o, _g} -> iri_local(predicate) end),
      properties:
        quads
        |> Enum.filter(&property_quad?/1)
        |> Enum.frequencies_by(fn {_s, predicate, _o, _g} -> iri_local(predicate) end),
      graphs: Enum.frequencies_by(quads, fn {_s, _p, _o, graph} -> identity_key(graph) end)
    }
  end

  defp property_quad?({_s, %RDF.IRI{value: @ontology <> _local} = predicate, _o, _g}) do
    predicate != @rdf_type and not MapSet.member?(@relationship_predicates, predicate)
  end

  defp property_quad?(_quad), do: false

  defp identities(quads) do
    entities =
      for {subject, predicate, object, _graph} <- quads,
          predicate == @rdf_type,
          MapSet.member?(@entity_classes, object),
          do: identity_key(subject)

    relationships =
      for {subject, predicate, object, graph} <- quads,
          MapSet.member?(@relationship_predicates, predicate),
          do:
            {identity_key(subject), identity_key(predicate), identity_key(object),
             identity_key(graph)}

    %{entities: Enum.sort(entities), relationships: Enum.sort(relationships)}
  end

  defp observations(opts) do
    %{
      cache: observe(Keyword.get(opts, :cache_observer)),
      statistics: observe(Keyword.get(opts, :statistics_observer))
    }
  end

  defp observe(nil), do: :unobserved
  defp observe(observer) when is_function(observer, 0), do: observer.()

  defp verify_field(snapshot, expected, field) do
    case Map.fetch(expected, field) do
      {:ok, value} ->
        actual = Map.get(snapshot, field)
        if actual == value, do: :ok, else: {:error, {:state_probe_mismatch, field, value, actual}}

      :error ->
        :ok
    end
  end

  defp verify_quads(_db, [], _expected?), do: :ok

  defp verify_quads(db, quads, expected?) do
    actual =
      db
      |> ErlangAdapter.prefix_stream(:gspo, <<>>, fill_cache: false)
      |> Enum.map(fn {key, _value} -> QuadIndex.key_to_quad(:gspo, key) end)

    with {:ok, rdf_quads} <- Adapter.to_rdf_quads(db, actual) do
      available = MapSet.new(rdf_quads, &quad_key/1)

      Enum.reduce_while(quads, :ok, fn quad, :ok ->
        exists? = MapSet.member?(available, quad_key(quad))

        if exists? == expected?,
          do: {:cont, :ok},
          else: {:halt, {:error, {:quad_presence_mismatch, quad, expected?}}}
      end)
    end
  end

  defp verify_identities(_actual, expected) when expected == %{}, do: :ok

  defp verify_identities(actual, expected) do
    present_entities = Map.get(expected, :present_entities, [])
    absent_entities = Map.get(expected, :absent_entities, [])
    present_relationships = Map.get(expected, :present_relationships, [])
    absent_relationships = Map.get(expected, :absent_relationships, [])

    cond do
      not Enum.all?(present_entities, &(&1 in actual.entities)) ->
        {:error, {:identity_probe_mismatch, :present_entities}}

      Enum.any?(absent_entities, &(&1 in actual.entities)) ->
        {:error, {:identity_probe_mismatch, :absent_entities}}

      not Enum.all?(present_relationships, &(&1 in actual.relationships)) ->
        {:error, {:identity_probe_mismatch, :present_relationships}}

      Enum.any?(absent_relationships, &(&1 in actual.relationships)) ->
        {:error, {:identity_probe_mismatch, :absent_relationships}}

      true ->
        :ok
    end
  end

  defp verify_facets(_actual, expected) when expected == %{}, do: :ok

  defp verify_facets(actual, expected) do
    mismatches =
      for {category, expected_counts} <- expected,
          {name, expected_count} <- expected_counts,
          actual_count = actual |> Map.get(category, %{}) |> Map.get(name, 0),
          actual_count != expected_count,
          do: {category, name, expected_count, actual_count}

    case mismatches do
      [] -> :ok
      [mismatch | _rest] -> {:error, {:state_probe_facet_mismatch, mismatch}}
    end
  end

  defp affected_terms(mutations) do
    mutations
    |> Enum.flat_map(&Map.get(&1, :quads, []))
    |> Enum.reduce(MapSet.new(), fn {subject, predicate, object, _graph}, acc ->
      acc
      |> MapSet.put(vocabulary_key(predicate))
      |> maybe_put_type(predicate, object)
      |> maybe_put_entity(subject)
      |> maybe_put_entity(object)
    end)
  end

  defp maybe_put_type(acc, @rdf_type, %RDF.IRI{} = object),
    do: MapSet.put(acc, vocabulary_key(object))

  defp maybe_put_type(acc, _predicate, _object), do: acc

  defp maybe_put_entity(acc, %RDF.IRI{value: "https://ldbcouncil.org/snb/entity/" <> rest}) do
    case String.split(rest, "/", parts: 2) do
      [type, _id] -> MapSet.put(acc, @ontology <> type)
      _other -> acc
    end
  end

  defp maybe_put_entity(acc, _term), do: acc

  defp query_terms(%{translation: %{sparql: sparql}}) when is_binary(sparql) do
    prefixes =
      Regex.scan(~r/PREFIX\s+([A-Za-z][\w-]*):\s*<([^>]+)>/i, sparql, capture: :all_but_first)
      |> Map.new(fn [prefix, iri] -> {prefix, iri} end)

    explicit =
      Regex.scan(~r/<([^>]+)>/, sparql, capture: :all_but_first)
      |> Enum.map(&hd/1)

    compact =
      Regex.scan(~r/\b([A-Za-z][\w-]*):([A-Za-z][\w-]*)\b/, sparql, capture: :all_but_first)
      |> Enum.flat_map(fn [prefix, local] ->
        case Map.fetch(prefixes, prefix) do
          {:ok, base} -> [base <> local]
          :error -> []
        end
      end)

    MapSet.new(explicit ++ compact)
  end

  defp query_terms(%{strategy: {:extension, _id, _owner}} = definition) do
    case GraphAdapterRegistry.fetch(definition) do
      {:ok, entry} -> MapSet.new(entry.affected_terms)
      {:error, _reason} -> MapSet.new()
    end
  end

  defp query_terms(_definition), do: MapSet.new()

  defp checksum(quads) do
    payload = Enum.map(quads, &quad_key/1)

    "sha256:" <>
      Base.encode16(:crypto.hash(:sha256, :erlang.term_to_binary(payload, [:deterministic])),
        case: :lower
      )
  end

  defp quad_key({s, p, o, g}), do: {term_key(s), term_key(p), term_key(o), term_key(g)}
  defp vocabulary_key(%RDF.IRI{value: value}), do: value
  defp vocabulary_key(term), do: term_key(term)
  defp identity_key(%RDF.IRI{value: value}), do: value
  defp identity_key(term), do: inspect(term)
  defp iri_local(%RDF.IRI{value: @ontology <> local}), do: local
  defp term_key(nil), do: ""
  defp term_key(term), do: :erlang.term_to_binary(term, [:deterministic])

  defp changed_if(changed, true, field), do: [field | changed]
  defp changed_if(changed, false, _field), do: changed

  defp default_fixture_root do
    Path.join([
      to_string(:code.priv_dir(:triple_store)),
      "benchmarks",
      "ldbc",
      "fixtures",
      "snb-bi"
    ])
  end

  defp validate_expectations(%{
         schema_version: @schema_version,
         mapping_version: mapping_version,
         checkpoints: checkpoints
       })
       when is_map(checkpoints) do
    required = [:initial, :after_update_batch]

    if mapping_version == Mapping.version() and
         Enum.all?(required, &Map.has_key?(checkpoints, &1)) and
         Enum.all?(checkpoints, fn {_name, probe} -> valid_expected_probe?(probe) end),
       do: :ok,
       else: {:error, :invalid_state_probe_fixture}
  end

  defp validate_expectations(_fixture), do: {:error, :invalid_state_probe_fixture}

  defp valid_expected_probe?(%{counts: counts, state_checksum: "sha256:" <> checksum})
       when is_map(counts) and byte_size(checksum) == 64,
       do: true

  defp valid_expected_probe?(_probe), do: false
end
