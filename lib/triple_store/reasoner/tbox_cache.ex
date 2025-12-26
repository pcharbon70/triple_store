defmodule TripleStore.Reasoner.TBoxCache do
  @moduledoc """
  Caches TBox (schema) inferences for efficient ABox (instance) reasoning.

  The TBox cache computes and stores class and property hierarchies, enabling
  O(1) lookup of superclass/subclass and superproperty/subproperty relationships.
  This avoids repeated traversal during rule application.

  ## Class Hierarchy

  The class hierarchy is computed by following `rdfs:subClassOf` relationships
  transitively. For each class, we precompute:
  - All superclasses (transitive closure of subClassOf)
  - All subclasses (inverse of the superclass relationship)

  ## Storage

  Hierarchies are stored in `:persistent_term` for zero-copy access from all
  processes. The cache uses a versioning scheme to detect staleness.

  ## Usage

      # Compute and cache hierarchies for a fact set
      {:ok, stats} = TBoxCache.compute_class_hierarchy(facts)

      # Query the hierarchy
      superclasses = TBoxCache.superclasses(class_iri)
      subclasses = TBoxCache.subclasses(class_iri)

      # Check if cached
      TBoxCache.cached?(:class_hierarchy)

      # Clear cache
      TBoxCache.clear(:class_hierarchy)

  ## In-Memory API

  For testing, an in-memory API is provided that works directly with fact sets
  without persistent storage:

      {:ok, cache} = TBoxCache.compute_class_hierarchy_in_memory(facts)
      superclasses = TBoxCache.superclasses_from(cache, class_iri)
  """

  alias TripleStore.Reasoner.Namespaces

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "A term in the hierarchy (typically an IRI)"
  @type term_value :: term()

  @typedoc "Map from class to its direct superclasses"
  @type direct_map :: %{term_value() => MapSet.t(term_value())}

  @typedoc "Map from class to all transitive superclasses"
  @type transitive_map :: %{term_value() => MapSet.t(term_value())}

  @typedoc "Class hierarchy cache structure"
  @type class_hierarchy :: %{
          superclass_map: transitive_map(),
          subclass_map: transitive_map(),
          class_count: non_neg_integer(),
          version: String.t()
        }

  @typedoc "Computation statistics"
  @type compute_stats :: %{
          class_count: non_neg_integer(),
          relationship_count: non_neg_integer(),
          computation_time_ms: non_neg_integer()
        }

  # ============================================================================
  # Configuration
  # ============================================================================

  # Maximum iterations for transitive closure to prevent infinite loops
  @max_iterations 1000

  # ============================================================================
  # In-Memory API
  # ============================================================================

  @doc """
  Computes the class hierarchy from a set of facts.

  Extracts all `rdfs:subClassOf` relationships and computes the transitive
  closure to build complete superclass and subclass maps.

  ## Parameters

  - `facts` - A MapSet or list of `{subject, predicate, object}` triples

  ## Returns

  - `{:ok, hierarchy}` - The computed class hierarchy
  - `{:error, :max_iterations_exceeded}` - If transitive closure doesn't converge

  ## Examples

      facts = MapSet.new([
        {{:iri, "Student"}, {:iri, "rdfs:subClassOf"}, {:iri, "Person"}},
        {{:iri, "Person"}, {:iri, "rdfs:subClassOf"}, {:iri, "Agent"}}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)
  """
  @spec compute_class_hierarchy_in_memory(Enumerable.t()) ::
          {:ok, class_hierarchy()} | {:error, :max_iterations_exceeded}
  def compute_class_hierarchy_in_memory(facts) do
    start_time = System.monotonic_time(:millisecond)

    # Extract rdfs:subClassOf relationships
    subclass_of_iri = {:iri, Namespaces.rdfs_subClassOf()}

    direct_superclass_map =
      facts
      |> Enum.filter(fn {_s, p, _o} -> p == subclass_of_iri end)
      |> Enum.reduce(%{}, fn {subclass, _p, superclass}, acc ->
        Map.update(acc, subclass, MapSet.new([superclass]), &MapSet.put(&1, superclass))
      end)

    # Compute transitive closure of superclass relationships
    case compute_transitive_closure(direct_superclass_map) do
      {:ok, superclass_map} ->
        # Compute inverse (subclass map) from superclass map
        subclass_map = invert_hierarchy(superclass_map)

        # Get all classes (both subjects and objects of subClassOf)
        all_classes =
          MapSet.union(
            MapSet.new(Map.keys(superclass_map)),
            MapSet.new(Map.keys(subclass_map))
          )

        duration_ms = System.monotonic_time(:millisecond) - start_time

        hierarchy = %{
          superclass_map: superclass_map,
          subclass_map: subclass_map,
          class_count: MapSet.size(all_classes),
          version: generate_version(),
          stats: %{
            class_count: MapSet.size(all_classes),
            relationship_count: count_relationships(superclass_map),
            computation_time_ms: duration_ms
          }
        }

        {:ok, hierarchy}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Returns all superclasses of a class from a precomputed hierarchy.

  ## Parameters

  - `hierarchy` - The precomputed class hierarchy
  - `class` - The class to look up

  ## Returns

  A MapSet of all superclasses (transitive closure), or empty MapSet if not found.

  ## Examples

      superclasses = TBoxCache.superclasses_from(hierarchy, {:iri, "Student"})
      # => MapSet.new([{:iri, "Person"}, {:iri, "Agent"}])
  """
  @spec superclasses_from(class_hierarchy(), term_value()) :: MapSet.t(term_value())
  def superclasses_from(%{superclass_map: map}, class) do
    Map.get(map, class, MapSet.new())
  end

  @doc """
  Returns all subclasses of a class from a precomputed hierarchy.

  ## Parameters

  - `hierarchy` - The precomputed class hierarchy
  - `class` - The class to look up

  ## Returns

  A MapSet of all subclasses (transitive closure), or empty MapSet if not found.

  ## Examples

      subclasses = TBoxCache.subclasses_from(hierarchy, {:iri, "Person"})
      # => MapSet.new([{:iri, "Student"}, {:iri, "GradStudent"}])
  """
  @spec subclasses_from(class_hierarchy(), term_value()) :: MapSet.t(term_value())
  def subclasses_from(%{subclass_map: map}, class) do
    Map.get(map, class, MapSet.new())
  end

  @doc """
  Checks if one class is a superclass of another (directly or transitively).

  ## Parameters

  - `hierarchy` - The precomputed class hierarchy
  - `subclass` - The potential subclass
  - `superclass` - The potential superclass

  ## Returns

  `true` if superclass is a superclass of subclass.
  """
  @spec is_superclass?(class_hierarchy(), term_value(), term_value()) :: boolean()
  def is_superclass?(hierarchy, subclass, superclass) do
    MapSet.member?(superclasses_from(hierarchy, subclass), superclass)
  end

  @doc """
  Checks if one class is a subclass of another (directly or transitively).

  ## Parameters

  - `hierarchy` - The precomputed class hierarchy
  - `superclass` - The potential superclass
  - `subclass` - The potential subclass

  ## Returns

  `true` if subclass is a subclass of superclass.
  """
  @spec is_subclass?(class_hierarchy(), term_value(), term_value()) :: boolean()
  def is_subclass?(hierarchy, superclass, subclass) do
    MapSet.member?(subclasses_from(hierarchy, superclass), subclass)
  end

  # ============================================================================
  # Persistent Term API
  # ============================================================================

  @doc """
  Computes and stores the class hierarchy in `:persistent_term`.

  ## Parameters

  - `facts` - A MapSet or list of triples
  - `key` - Optional key for the cache (default: :default)

  ## Returns

  - `{:ok, stats}` - Computation statistics
  - `{:error, reason}` - On failure
  """
  @spec compute_and_store_class_hierarchy(Enumerable.t(), atom()) ::
          {:ok, compute_stats()} | {:error, term()}
  def compute_and_store_class_hierarchy(facts, key \\ :default) do
    case compute_class_hierarchy_in_memory(facts) do
      {:ok, hierarchy} ->
        store_hierarchy(:class_hierarchy, key, hierarchy)
        {:ok, hierarchy.stats}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Returns all superclasses of a class from the cached hierarchy.

  Uses the stored `:persistent_term` cache.

  ## Parameters

  - `class` - The class to look up
  - `key` - Optional cache key (default: :default)

  ## Returns

  A MapSet of all superclasses, or empty MapSet if not cached or not found.
  """
  @spec superclasses(term_value(), atom()) :: MapSet.t(term_value())
  def superclasses(class, key \\ :default) do
    case load_hierarchy(:class_hierarchy, key) do
      {:ok, hierarchy} -> superclasses_from(hierarchy, class)
      {:error, :not_found} -> MapSet.new()
    end
  end

  @doc """
  Returns all subclasses of a class from the cached hierarchy.

  Uses the stored `:persistent_term` cache.

  ## Parameters

  - `class` - The class to look up
  - `key` - Optional cache key (default: :default)

  ## Returns

  A MapSet of all subclasses, or empty MapSet if not cached or not found.
  """
  @spec subclasses(term_value(), atom()) :: MapSet.t(term_value())
  def subclasses(class, key \\ :default) do
    case load_hierarchy(:class_hierarchy, key) do
      {:ok, hierarchy} -> subclasses_from(hierarchy, class)
      {:error, :not_found} -> MapSet.new()
    end
  end

  @doc """
  Checks if a hierarchy is cached.

  ## Parameters

  - `type` - The hierarchy type (:class_hierarchy or :property_hierarchy)
  - `key` - Optional cache key (default: :default)
  """
  @spec cached?(atom(), atom()) :: boolean()
  def cached?(type, key \\ :default) do
    case :persistent_term.get({__MODULE__, type, key}, nil) do
      nil -> false
      _ -> true
    end
  end

  @doc """
  Clears a cached hierarchy.

  ## Parameters

  - `type` - The hierarchy type (:class_hierarchy or :property_hierarchy)
  - `key` - Optional cache key (default: :default)
  """
  @spec clear(atom(), atom()) :: :ok
  def clear(type, key \\ :default) do
    unregister_key(type, key)
    :persistent_term.erase({__MODULE__, type, key})
    :ok
  end

  @doc """
  Clears all cached hierarchies.
  """
  @spec clear_all() :: :ok
  def clear_all do
    registry = :persistent_term.get({__MODULE__, :__registry__}, MapSet.new())

    Enum.each(registry, fn {type, key} ->
      :persistent_term.erase({__MODULE__, type, key})
    end)

    :persistent_term.erase({__MODULE__, :__registry__})
    :ok
  end

  @doc """
  Returns all registered cache keys.
  """
  @spec list_cached() :: [{atom(), atom()}]
  def list_cached do
    :persistent_term.get({__MODULE__, :__registry__}, MapSet.new())
    |> MapSet.to_list()
  end

  @doc """
  Returns the version of a cached hierarchy.

  ## Parameters

  - `type` - The hierarchy type
  - `key` - Optional cache key (default: :default)

  ## Returns

  - `{:ok, version}` - The version string
  - `{:error, :not_found}` - If not cached
  """
  @spec version(atom(), atom()) :: {:ok, String.t()} | {:error, :not_found}
  def version(type, key \\ :default) do
    case load_hierarchy(type, key) do
      {:ok, %{version: version}} -> {:ok, version}
      {:error, _} = error -> error
    end
  end

  @doc """
  Returns statistics about a cached hierarchy.

  ## Parameters

  - `type` - The hierarchy type
  - `key` - Optional cache key (default: :default)

  ## Returns

  - `{:ok, stats}` - Statistics map
  - `{:error, :not_found}` - If not cached
  """
  @spec stats(atom(), atom()) :: {:ok, map()} | {:error, :not_found}
  def stats(type, key \\ :default) do
    case load_hierarchy(type, key) do
      {:ok, %{stats: stats}} -> {:ok, stats}
      {:ok, hierarchy} -> {:ok, %{class_count: hierarchy.class_count}}
      {:error, _} = error -> error
    end
  end

  # ============================================================================
  # Private Functions - Transitive Closure
  # ============================================================================

  # Compute transitive closure using iterative fixpoint
  defp compute_transitive_closure(direct_map) do
    compute_closure_loop(direct_map, direct_map, 0)
  end

  defp compute_closure_loop(_direct_map, _current_map, iteration)
       when iteration >= @max_iterations do
    {:error, :max_iterations_exceeded}
  end

  defp compute_closure_loop(direct_map, current_map, iteration) do
    # Extend each class's superclasses with their superclasses
    next_map =
      Map.new(current_map, fn {class, supers} ->
        extended =
          Enum.reduce(supers, supers, fn super, acc ->
            # Add superclasses of this superclass
            super_supers = Map.get(current_map, super, MapSet.new())
            MapSet.union(acc, super_supers)
          end)

        {class, extended}
      end)

    # Also ensure classes that are only superclasses (not subclasses) are in the map
    next_map =
      Enum.reduce(Map.values(direct_map), next_map, fn super_set, acc ->
        Enum.reduce(super_set, acc, fn super, inner_acc ->
          Map.put_new(inner_acc, super, MapSet.new())
        end)
      end)

    # Check for fixpoint
    if maps_equal?(current_map, next_map) do
      {:ok, next_map}
    else
      compute_closure_loop(direct_map, next_map, iteration + 1)
    end
  end

  # Check if two maps with MapSet values are equal
  defp maps_equal?(map1, map2) do
    Map.keys(map1) == Map.keys(map2) and
      Enum.all?(Map.keys(map1), fn key ->
        Map.get(map1, key) == Map.get(map2, key)
      end)
  end

  # Invert a hierarchy: for each (class -> supers), add (super -> class) to inverse
  defp invert_hierarchy(superclass_map) do
    Enum.reduce(superclass_map, %{}, fn {class, supers}, acc ->
      Enum.reduce(supers, acc, fn super, inner_acc ->
        Map.update(inner_acc, super, MapSet.new([class]), &MapSet.put(&1, class))
      end)
    end)
  end

  # Count total relationships in a hierarchy map
  defp count_relationships(map) do
    Enum.reduce(map, 0, fn {_class, supers}, acc ->
      acc + MapSet.size(supers)
    end)
  end

  # ============================================================================
  # Private Functions - Persistent Term Storage
  # ============================================================================

  defp store_hierarchy(type, key, hierarchy) do
    :persistent_term.put({__MODULE__, type, key}, hierarchy)
    register_key(type, key)
    :ok
  end

  defp load_hierarchy(type, key) do
    case :persistent_term.get({__MODULE__, type, key}, nil) do
      nil -> {:error, :not_found}
      hierarchy -> {:ok, hierarchy}
    end
  end

  defp register_key(type, key) do
    registry = :persistent_term.get({__MODULE__, :__registry__}, MapSet.new())
    :persistent_term.put({__MODULE__, :__registry__}, MapSet.put(registry, {type, key}))
  end

  defp unregister_key(type, key) do
    registry = :persistent_term.get({__MODULE__, :__registry__}, MapSet.new())
    :persistent_term.put({__MODULE__, :__registry__}, MapSet.delete(registry, {type, key}))
  end

  defp generate_version do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end
end
