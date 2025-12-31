defmodule TripleStore.Reasoner.TBoxCache do
  # Suppress dialyzer warnings related to MapSet opaque type handling.
  # MapSet is an opaque type and dialyzer is strict about how it's constructed
  # and used. These warnings are false positives due to internal MapSet handling.
  @dialyzer [
    {:nowarn_function, compute_property_hierarchy_in_memory: 1},
    {:nowarn_function, compute_and_store_class_hierarchy: 2},
    {:nowarn_function, compute_and_store_property_hierarchy: 2},
    {:nowarn_function, tbox_triple?: 1},
    {:nowarn_function, categorize_tbox_triples: 1},
    {:nowarn_function, recompute_hierarchies: 2},
    {:nowarn_function, handle_tbox_update: 4},
    {:nowarn_function, store_hierarchy: 3},
    {:nowarn_function, register_key: 2}
  ]

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

  ## Property Hierarchy

  The property hierarchy is computed by following `rdfs:subPropertyOf` relationships
  transitively. Additionally, property characteristics are extracted:
  - Transitive properties (owl:TransitiveProperty)
  - Symmetric properties (owl:SymmetricProperty)
  - Functional properties (owl:FunctionalProperty)
  - Inverse functional properties (owl:InverseFunctionalProperty)
  - Inverse property pairs (owl:inverseOf)

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

  ## Security Considerations

  - **Trusted Input**: This module assumes input triples come from trusted sources.
    No validation is performed on IRI formats or values.
  - **Cache Keys**: Cache keys are atoms and should be controlled by the application.
    Do not derive cache keys from user input to avoid atom table exhaustion.
  - **Memory Usage**: Memory consumption grows linearly with ontology size. For very
    large ontologies (100k+ classes), monitor memory usage during hierarchy computation.
  - **No Input Limits**: There are no built-in limits on fact set size. Applications
    processing untrusted ontologies should implement their own size limits.
  """

  alias TripleStore.Reasoner.Namespaces

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "A term in the hierarchy (typically an IRI)"
  @type term_value :: term()

  @typedoc "Map from class to its direct superclasses"
  @type direct_map :: %{term_value() => MapSet.t()}

  @typedoc "Map from class to all transitive superclasses"
  @type transitive_map :: %{term_value() => MapSet.t()}

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

  @typedoc "Property characteristics"
  @type property_characteristics :: %{
          transitive: MapSet.t(),
          symmetric: MapSet.t(),
          functional: MapSet.t(),
          inverse_functional: MapSet.t(),
          inverse_pairs: %{term_value() => term_value()}
        }

  @typedoc "Property hierarchy cache structure"
  @type property_hierarchy :: %{
          superproperty_map: transitive_map(),
          subproperty_map: transitive_map(),
          characteristics: property_characteristics(),
          property_count: non_neg_integer(),
          version: String.t()
        }

  # ============================================================================
  # Configuration
  # ============================================================================

  # Maximum iterations for transitive closure to prevent infinite loops
  @max_iterations 1000

  # TBox-modifying predicates (computed at compile time for efficiency)
  @tbox_predicates MapSet.new([
                     {:iri, Namespaces.rdfs_sub_class_of()},
                     {:iri, Namespaces.rdfs_sub_property_of()},
                     {:iri, Namespaces.rdf_type()},
                     {:iri, Namespaces.owl_inverse_of()},
                     {:iri, Namespaces.rdfs_domain()},
                     {:iri, Namespaces.rdfs_range()}
                   ])

  # OWL property characteristic types (computed at compile time)
  @property_characteristic_types MapSet.new([
                                   {:iri, Namespaces.owl_transitive_property()},
                                   {:iri, Namespaces.owl_symmetric_property()},
                                   {:iri, Namespaces.owl_functional_property()},
                                   {:iri, Namespaces.owl_inverse_functional_property()}
                                 ])

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
    subclass_of_iri = {:iri, Namespaces.rdfs_sub_class_of()}

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
  @spec superclasses_from(class_hierarchy(), term_value()) :: MapSet.t()
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
  @spec subclasses_from(class_hierarchy(), term_value()) :: MapSet.t()
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
  @spec superclass?(class_hierarchy(), term_value(), term_value()) :: boolean()
  def superclass?(hierarchy, subclass, superclass) do
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
  @spec subclass?(class_hierarchy(), term_value(), term_value()) :: boolean()
  def subclass?(hierarchy, superclass, subclass) do
    MapSet.member?(subclasses_from(hierarchy, superclass), subclass)
  end

  # ============================================================================
  # In-Memory API - Property Hierarchy
  # ============================================================================

  @doc """
  Computes the property hierarchy from a set of facts.

  Extracts all `rdfs:subPropertyOf` relationships and computes the transitive
  closure to build complete superproperty and subproperty maps. Also extracts
  property characteristics (transitive, symmetric, functional, etc.).

  ## Parameters

  - `facts` - A MapSet or list of `{subject, predicate, object}` triples

  ## Returns

  - `{:ok, hierarchy}` - The computed property hierarchy
  - `{:error, :max_iterations_exceeded}` - If transitive closure doesn't converge

  ## Examples

      facts = MapSet.new([
        {{:iri, "hasChild"}, {:iri, "rdfs:subPropertyOf"}, {:iri, "hasDescendant"}},
        {{:iri, "contains"}, {:iri, "rdf:type"}, {:iri, "owl:TransitiveProperty"}}
      ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)
  """
  @spec compute_property_hierarchy_in_memory(Enumerable.t()) ::
          {:ok, property_hierarchy()} | {:error, :max_iterations_exceeded}
  def compute_property_hierarchy_in_memory(facts) do
    start_time = System.monotonic_time(:millisecond)

    # Extract rdfs:subPropertyOf relationships
    subproperty_of_iri = {:iri, Namespaces.rdfs_sub_property_of()}

    direct_superproperty_map =
      facts
      |> Enum.filter(fn {_s, p, _o} -> p == subproperty_of_iri end)
      |> Enum.reduce(%{}, fn {subprop, _p, superprop}, acc ->
        Map.update(acc, subprop, MapSet.new([superprop]), &MapSet.put(&1, superprop))
      end)

    # Compute transitive closure of superproperty relationships
    case compute_transitive_closure(direct_superproperty_map) do
      {:ok, superproperty_map} ->
        # Compute inverse (subproperty map) from superproperty map
        subproperty_map = invert_hierarchy(superproperty_map)

        # Extract property characteristics
        characteristics = extract_property_characteristics(facts)

        # Get all properties (from subPropertyOf and characteristics)
        all_properties =
          MapSet.new()
          |> MapSet.union(MapSet.new(Map.keys(superproperty_map)))
          |> MapSet.union(MapSet.new(Map.keys(subproperty_map)))
          |> MapSet.union(characteristics.transitive)
          |> MapSet.union(characteristics.symmetric)
          |> MapSet.union(characteristics.functional)
          |> MapSet.union(characteristics.inverse_functional)
          |> MapSet.union(MapSet.new(Map.keys(characteristics.inverse_pairs)))
          |> MapSet.union(MapSet.new(Map.values(characteristics.inverse_pairs)))

        duration_ms = System.monotonic_time(:millisecond) - start_time

        hierarchy = %{
          superproperty_map: superproperty_map,
          subproperty_map: subproperty_map,
          characteristics: characteristics,
          property_count: MapSet.size(all_properties),
          version: generate_version(),
          stats: %{
            property_count: MapSet.size(all_properties),
            relationship_count: count_relationships(superproperty_map),
            transitive_count: MapSet.size(characteristics.transitive),
            symmetric_count: MapSet.size(characteristics.symmetric),
            functional_count: MapSet.size(characteristics.functional),
            inverse_functional_count: MapSet.size(characteristics.inverse_functional),
            inverse_pair_count: map_size(characteristics.inverse_pairs),
            computation_time_ms: duration_ms
          }
        }

        {:ok, hierarchy}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Returns all superproperties of a property from a precomputed hierarchy.

  ## Parameters

  - `hierarchy` - The precomputed property hierarchy
  - `property` - The property to look up

  ## Returns

  A MapSet of all superproperties (transitive closure), or empty MapSet if not found.

  ## Examples

      superprops = TBoxCache.superproperties_from(hierarchy, {:iri, "hasChild"})
      # => MapSet.new([{:iri, "hasDescendant"}, {:iri, "hasRelative"}])
  """
  @spec superproperties_from(property_hierarchy(), term_value()) :: MapSet.t()
  def superproperties_from(%{superproperty_map: map}, property) do
    Map.get(map, property, MapSet.new())
  end

  @doc """
  Returns all subproperties of a property from a precomputed hierarchy.

  ## Parameters

  - `hierarchy` - The precomputed property hierarchy
  - `property` - The property to look up

  ## Returns

  A MapSet of all subproperties (transitive closure), or empty MapSet if not found.

  ## Examples

      subprops = TBoxCache.subproperties_from(hierarchy, {:iri, "hasDescendant"})
      # => MapSet.new([{:iri, "hasChild"}, {:iri, "hasGrandchild"}])
  """
  @spec subproperties_from(property_hierarchy(), term_value()) :: MapSet.t()
  def subproperties_from(%{subproperty_map: map}, property) do
    Map.get(map, property, MapSet.new())
  end

  @doc """
  Checks if a property is transitive.

  ## Parameters

  - `hierarchy` - The precomputed property hierarchy
  - `property` - The property to check

  ## Returns

  `true` if the property is declared as owl:TransitiveProperty.
  """
  @spec transitive_property?(property_hierarchy(), term_value()) :: boolean()
  def transitive_property?(%{characteristics: %{transitive: set}}, property) do
    MapSet.member?(set, property)
  end

  @doc """
  Checks if a property is symmetric.

  ## Parameters

  - `hierarchy` - The precomputed property hierarchy
  - `property` - The property to check

  ## Returns

  `true` if the property is declared as owl:SymmetricProperty.
  """
  @spec symmetric_property?(property_hierarchy(), term_value()) :: boolean()
  def symmetric_property?(%{characteristics: %{symmetric: set}}, property) do
    MapSet.member?(set, property)
  end

  @doc """
  Checks if a property is functional.

  ## Parameters

  - `hierarchy` - The precomputed property hierarchy
  - `property` - The property to check

  ## Returns

  `true` if the property is declared as owl:FunctionalProperty.
  """
  @spec functional_property?(property_hierarchy(), term_value()) :: boolean()
  def functional_property?(%{characteristics: %{functional: set}}, property) do
    MapSet.member?(set, property)
  end

  @doc """
  Checks if a property is inverse functional.

  ## Parameters

  - `hierarchy` - The precomputed property hierarchy
  - `property` - The property to check

  ## Returns

  `true` if the property is declared as owl:InverseFunctionalProperty.
  """
  @spec inverse_functional_property?(property_hierarchy(), term_value()) :: boolean()
  def inverse_functional_property?(%{characteristics: %{inverse_functional: set}}, property) do
    MapSet.member?(set, property)
  end

  @doc """
  Returns the inverse of a property if one is declared.

  ## Parameters

  - `hierarchy` - The precomputed property hierarchy
  - `property` - The property to look up

  ## Returns

  The inverse property, or `nil` if no inverse is declared.

  ## Examples

      TBoxCache.inverse_of(hierarchy, {:iri, "hasParent"})
      # => {:iri, "hasChild"}
  """
  @spec inverse_of(property_hierarchy(), term_value()) :: term_value() | nil
  def inverse_of(%{characteristics: %{inverse_pairs: pairs}}, property) do
    Map.get(pairs, property)
  end

  @doc """
  Returns all transitive properties from the hierarchy.
  """
  @spec transitive_properties(property_hierarchy()) :: MapSet.t()
  def transitive_properties(%{characteristics: %{transitive: set}}), do: set

  @doc """
  Returns all symmetric properties from the hierarchy.
  """
  @spec symmetric_properties(property_hierarchy()) :: MapSet.t()
  def symmetric_properties(%{characteristics: %{symmetric: set}}), do: set

  @doc """
  Returns all functional properties from the hierarchy.
  """
  @spec functional_properties(property_hierarchy()) :: MapSet.t()
  def functional_properties(%{characteristics: %{functional: set}}), do: set

  @doc """
  Returns all inverse functional properties from the hierarchy.
  """
  @spec inverse_functional_properties(property_hierarchy()) :: MapSet.t()
  def inverse_functional_properties(%{characteristics: %{inverse_functional: set}}), do: set

  @doc """
  Returns all inverse property pairs as a map.
  """
  @spec inverse_pairs(property_hierarchy()) :: %{term_value() => term_value()}
  def inverse_pairs(%{characteristics: %{inverse_pairs: pairs}}), do: pairs

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
  @dialyzer {:nowarn_function, superclasses: 2}
  @spec superclasses(term_value(), atom()) :: MapSet.t()
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
  @dialyzer {:nowarn_function, subclasses: 2}
  @spec subclasses(term_value(), atom()) :: MapSet.t()
  def subclasses(class, key \\ :default) do
    case load_hierarchy(:class_hierarchy, key) do
      {:ok, hierarchy} -> subclasses_from(hierarchy, class)
      {:error, :not_found} -> MapSet.new()
    end
  end

  @doc """
  Computes and stores the property hierarchy in `:persistent_term`.

  ## Parameters

  - `facts` - A MapSet or list of triples
  - `key` - Optional key for the cache (default: :default)

  ## Returns

  - `{:ok, stats}` - Computation statistics
  - `{:error, reason}` - On failure
  """
  @spec compute_and_store_property_hierarchy(Enumerable.t(), atom()) ::
          {:ok, map()} | {:error, term()}
  def compute_and_store_property_hierarchy(facts, key \\ :default) do
    case compute_property_hierarchy_in_memory(facts) do
      {:ok, hierarchy} ->
        store_hierarchy(:property_hierarchy, key, hierarchy)
        {:ok, hierarchy.stats}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Returns all superproperties of a property from the cached hierarchy.

  Uses the stored `:persistent_term` cache.

  ## Parameters

  - `property` - The property to look up
  - `key` - Optional cache key (default: :default)

  ## Returns

  A MapSet of all superproperties, or empty MapSet if not cached or not found.
  """
  @dialyzer {:nowarn_function, superproperties: 2}
  @spec superproperties(term_value(), atom()) :: MapSet.t()
  def superproperties(property, key \\ :default) do
    case load_hierarchy(:property_hierarchy, key) do
      {:ok, hierarchy} -> superproperties_from(hierarchy, property)
      {:error, :not_found} -> MapSet.new()
    end
  end

  @doc """
  Returns all subproperties of a property from the cached hierarchy.

  Uses the stored `:persistent_term` cache.

  ## Parameters

  - `property` - The property to look up
  - `key` - Optional cache key (default: :default)

  ## Returns

  A MapSet of all subproperties, or empty MapSet if not cached or not found.
  """
  @dialyzer {:nowarn_function, subproperties: 2}
  @spec subproperties(term_value(), atom()) :: MapSet.t()
  def subproperties(property, key \\ :default) do
    case load_hierarchy(:property_hierarchy, key) do
      {:ok, hierarchy} -> subproperties_from(hierarchy, property)
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
  @spec compute_transitive_closure(map()) :: {:ok, map()} | {:error, :max_iterations_exceeded}
  defp compute_transitive_closure(direct_map) do
    compute_closure_loop(direct_map, direct_map, 0)
  end

  @spec compute_closure_loop(map(), map(), non_neg_integer()) ::
          {:ok, map()} | {:error, :max_iterations_exceeded}
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
  # Maps can be directly compared with == since MapSet equality works
  defp maps_equal?(map1, map2), do: map1 == map2

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
    Enum.reduce(map, 0, fn {_term, related}, acc ->
      acc + MapSet.size(related)
    end)
  end

  # ============================================================================
  # Private Functions - Property Characteristics
  # ============================================================================

  # Extract property characteristics from facts in a single pass
  # This is O(n) instead of O(5n) for large ontologies
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp extract_property_characteristics(facts) do
    rdf_type_iri = {:iri, Namespaces.rdf_type()}
    transitive_iri = {:iri, Namespaces.owl_transitive_property()}
    symmetric_iri = {:iri, Namespaces.owl_symmetric_property()}
    functional_iri = {:iri, Namespaces.owl_functional_property()}
    inverse_functional_iri = {:iri, Namespaces.owl_inverse_functional_property()}
    inverse_of_iri = {:iri, Namespaces.owl_inverse_of()}

    initial_acc = %{
      transitive: MapSet.new(),
      symmetric: MapSet.new(),
      functional: MapSet.new(),
      inverse_functional: MapSet.new(),
      inverse_pairs: %{}
    }

    Enum.reduce(facts, initial_acc, fn {s, p, o}, acc ->
      cond do
        # Property characteristic declarations via rdf:type
        p == rdf_type_iri and o == transitive_iri ->
          %{acc | transitive: MapSet.put(acc.transitive, s)}

        p == rdf_type_iri and o == symmetric_iri ->
          %{acc | symmetric: MapSet.put(acc.symmetric, s)}

        p == rdf_type_iri and o == functional_iri ->
          %{acc | functional: MapSet.put(acc.functional, s)}

        p == rdf_type_iri and o == inverse_functional_iri ->
          %{acc | inverse_functional: MapSet.put(acc.inverse_functional, s)}

        # Inverse property pairs (bidirectional)
        p == inverse_of_iri ->
          inverse_pairs = acc.inverse_pairs |> Map.put(s, o) |> Map.put(o, s)
          %{acc | inverse_pairs: inverse_pairs}

        true ->
          acc
      end
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

  # ============================================================================
  # TBox Update Detection
  # ============================================================================

  @doc """
  Returns the set of predicates that indicate TBox (schema) modifications.

  These predicates affect the class hierarchy, property hierarchy, or property
  characteristics. When triples using these predicates are added or removed,
  the TBox caches may need to be invalidated and recomputed.

  ## Returns

  A MapSet of `{:iri, String.t()}` tuples for TBox-modifying predicates.

  ## TBox-Modifying Predicates

  **Class Hierarchy:**
  - `rdfs:subClassOf` - Class subsumption relationships

  **Property Hierarchy:**
  - `rdfs:subPropertyOf` - Property subsumption relationships

  **Property Characteristics:**
  - `rdf:type` with object `owl:TransitiveProperty`
  - `rdf:type` with object `owl:SymmetricProperty`
  - `rdf:type` with object `owl:FunctionalProperty`
  - `rdf:type` with object `owl:InverseFunctionalProperty`
  - `owl:inverseOf` - Inverse property declarations

  **Domain/Range (affects type inference):**
  - `rdfs:domain` - Property domain declarations
  - `rdfs:range` - Property range declarations
  """
  @dialyzer {:nowarn_function, tbox_predicates: 0}
  @spec tbox_predicates() :: MapSet.t()
  def tbox_predicates, do: @tbox_predicates

  @doc """
  Returns the set of OWL class types that indicate property characteristics.

  When a triple has `rdf:type` as predicate and one of these as object,
  it declares a property characteristic that affects reasoning.
  """
  @dialyzer {:nowarn_function, property_characteristic_types: 0}
  @spec property_characteristic_types() :: MapSet.t()
  def property_characteristic_types, do: @property_characteristic_types

  @doc """
  Checks if a triple is a TBox-modifying triple.

  A TBox-modifying triple is one that affects the class hierarchy, property
  hierarchy, or property characteristics. Adding or removing such triples
  requires invalidating and recomputing cached hierarchies.

  ## Parameters

  - `triple` - A `{subject, predicate, object}` tuple

  ## Returns

  `true` if the triple modifies the TBox, `false` otherwise.

  ## Examples

      # Class hierarchy modification
      iex> TBoxCache.tbox_triple?({{:iri, "Student"}, {:iri, "rdfs:subClassOf"}, {:iri, "Person"}})
      true

      # Property characteristic
      iex> TBoxCache.tbox_triple?({{:iri, "knows"}, {:iri, "rdf:type"}, {:iri, "owl:SymmetricProperty"}})
      true

      # Instance data (not TBox)
      iex> TBoxCache.tbox_triple?({{:iri, "alice"}, {:iri, "rdf:type"}, {:iri, "Person"}})
      false
  """
  @spec tbox_triple?({term_value(), term_value(), term_value()}) :: boolean()
  def tbox_triple?({_subject, predicate, object}) do
    rdf_type_iri = {:iri, Namespaces.rdf_type()}

    cond do
      # rdfs:subClassOf or rdfs:subPropertyOf or owl:inverseOf or rdfs:domain or rdfs:range
      predicate != rdf_type_iri and MapSet.member?(@tbox_predicates, predicate) ->
        true

      # rdf:type with a property characteristic type
      predicate == rdf_type_iri and MapSet.member?(@property_characteristic_types, object) ->
        true

      true ->
        false
    end
  end

  @doc """
  Checks if any triples in a collection are TBox-modifying.

  ## Parameters

  - `triples` - An enumerable of `{subject, predicate, object}` tuples

  ## Returns

  `true` if any triple modifies the TBox, `false` if none do.
  """
  @spec contains_tbox_triples?(Enumerable.t()) :: boolean()
  def contains_tbox_triples?(triples) do
    Enum.any?(triples, &tbox_triple?/1)
  end

  @doc """
  Filters TBox-modifying triples from a collection.

  ## Parameters

  - `triples` - An enumerable of `{subject, predicate, object}` tuples

  ## Returns

  A list of triples that are TBox-modifying.
  """
  @spec filter_tbox_triples(Enumerable.t()) :: [{term_value(), term_value(), term_value()}]
  def filter_tbox_triples(triples) do
    Enum.filter(triples, &tbox_triple?/1)
  end

  @doc """
  Categorizes TBox-modifying triples by what they affect.

  ## Parameters

  - `triples` - An enumerable of `{subject, predicate, object}` tuples

  ## Returns

  A map with keys:
  - `:class_hierarchy` - Triples affecting class hierarchy (rdfs:subClassOf)
  - `:property_hierarchy` - Triples affecting property hierarchy (rdfs:subPropertyOf)
  - `:property_characteristics` - Triples declaring property types
  - `:inverse_properties` - Triples declaring inverse relationships
  - `:domain_range` - Triples declaring domain/range constraints
  """
  @spec categorize_tbox_triples(Enumerable.t()) :: %{
          class_hierarchy: list(),
          property_hierarchy: list(),
          property_characteristics: list(),
          inverse_properties: list(),
          domain_range: list()
        }
  def categorize_tbox_triples(triples) do
    subclass_of = {:iri, Namespaces.rdfs_sub_class_of()}
    subprop_of = {:iri, Namespaces.rdfs_sub_property_of()}
    rdf_type = {:iri, Namespaces.rdf_type()}
    inverse_of = {:iri, Namespaces.owl_inverse_of()}
    domain = {:iri, Namespaces.rdfs_domain()}
    range = {:iri, Namespaces.rdfs_range()}
    char_types = property_characteristic_types()

    Enum.reduce(
      triples,
      %{
        class_hierarchy: [],
        property_hierarchy: [],
        property_characteristics: [],
        inverse_properties: [],
        domain_range: []
      },
      fn {_s, p, o} = triple, acc ->
        cond do
          p == subclass_of ->
            %{acc | class_hierarchy: [triple | acc.class_hierarchy]}

          p == subprop_of ->
            %{acc | property_hierarchy: [triple | acc.property_hierarchy]}

          p == rdf_type and MapSet.member?(char_types, o) ->
            %{acc | property_characteristics: [triple | acc.property_characteristics]}

          p == inverse_of ->
            %{acc | inverse_properties: [triple | acc.inverse_properties]}

          p == domain or p == range ->
            %{acc | domain_range: [triple | acc.domain_range]}

          true ->
            acc
        end
      end
    )
  end

  # ============================================================================
  # TBox Cache Invalidation and Recomputation
  # ============================================================================

  @doc """
  Invalidates cached hierarchies based on TBox-modifying triples.

  Analyzes the provided triples and invalidates only the caches that are
  affected by those changes.

  ## Parameters

  - `triples` - Triples that were added or removed
  - `key` - The cache key (default: :default)

  ## Returns

  A map indicating which caches were invalidated:
  - `:class_hierarchy` - `true` if class hierarchy was invalidated
  - `:property_hierarchy` - `true` if property hierarchy was invalidated
  """
  @spec invalidate_affected(Enumerable.t(), atom()) :: %{
          class_hierarchy: boolean(),
          property_hierarchy: boolean()
        }
  def invalidate_affected(triples, key \\ :default) do
    categorized = categorize_tbox_triples(triples)
    {class_affected, property_affected} = determine_affected_caches(categorized)

    if class_affected do
      clear(:class_hierarchy, key)
    end

    if property_affected do
      clear(:property_hierarchy, key)
    end

    %{
      class_hierarchy: class_affected,
      property_hierarchy: property_affected
    }
  end

  @doc """
  Recomputes cached hierarchies from a fact set.

  This is a convenience function that recomputes both class and property
  hierarchies from the current fact set.

  ## Parameters

  - `facts` - The complete fact set (all triples)
  - `key` - The cache key (default: :default)

  ## Returns

  - `{:ok, stats}` - Statistics about the recomputation
  - `{:error, reason}` - If computation fails
  """
  @spec recompute_hierarchies(Enumerable.t(), atom()) ::
          {:ok, %{class: map(), property: map()}} | {:error, term()}
  def recompute_hierarchies(facts, key \\ :default) do
    with {:ok, class_stats} <- compute_and_store_class_hierarchy(facts, key),
         {:ok, property_stats} <- compute_and_store_property_hierarchy(facts, key) do
      {:ok, %{class: class_stats, property: property_stats}}
    end
  end

  @doc """
  Handles TBox updates by invalidating and optionally recomputing caches.

  This is the main entry point for handling TBox modifications. It:
  1. Checks if any triples are TBox-modifying
  2. Invalidates affected caches
  3. Optionally recomputes hierarchies from the updated fact set

  ## Parameters

  - `modified_triples` - Triples that were added or removed
  - `current_facts` - The current complete fact set (after modification)
  - `key` - The cache key (default: :default)
  - `opts` - Options:
    - `:recompute` - Whether to recompute hierarchies (default: true)

  ## Returns

  - `{:ok, result}` with:
    - `:invalidated` - Map of which caches were invalidated
    - `:recomputed` - Map of recomputation stats (if recompute: true)
    - `:tbox_modified` - Whether any TBox triples were found
  - `{:error, reason}` - If recomputation fails
  """
  @spec handle_tbox_update(Enumerable.t(), Enumerable.t(), atom(), keyword()) ::
          {:ok, map()} | {:error, term()}
  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  def handle_tbox_update(modified_triples, current_facts, key \\ :default, opts \\ []) do
    recompute = Keyword.get(opts, :recompute, true)
    tbox_triples = filter_tbox_triples(modified_triples)

    if Enum.empty?(tbox_triples) do
      {:ok,
       %{
         tbox_modified: false,
         invalidated: %{class_hierarchy: false, property_hierarchy: false},
         recomputed: nil
       }}
    else
      invalidated = invalidate_affected(tbox_triples, key)

      if recompute and (invalidated.class_hierarchy or invalidated.property_hierarchy) do
        case recompute_hierarchies(current_facts, key) do
          {:ok, stats} ->
            {:ok,
             %{
               tbox_modified: true,
               invalidated: invalidated,
               recomputed: stats
             }}

          {:error, _} = error ->
            error
        end
      else
        {:ok,
         %{
           tbox_modified: true,
           invalidated: invalidated,
           recomputed: nil
         }}
      end
    end
  end

  @doc """
  Checks if hierarchies need recomputation based on modified triples.

  This is a lightweight check that doesn't actually invalidate or recompute.

  ## Parameters

  - `modified_triples` - Triples that were added or removed

  ## Returns

  A map indicating what would need recomputation:
  - `:class_hierarchy` - `true` if class hierarchy would be affected
  - `:property_hierarchy` - `true` if property hierarchy would be affected
  - `:any` - `true` if any hierarchy would be affected
  """
  @spec needs_recomputation?(Enumerable.t()) :: %{
          class_hierarchy: boolean(),
          property_hierarchy: boolean(),
          any: boolean()
        }
  def needs_recomputation?(modified_triples) do
    categorized = categorize_tbox_triples(modified_triples)
    {class_affected, property_affected} = determine_affected_caches(categorized)

    %{
      class_hierarchy: class_affected,
      property_hierarchy: property_affected,
      any: class_affected or property_affected
    }
  end

  # ============================================================================
  # Private Functions - Affected Cache Detection
  # ============================================================================

  # Determines which caches are affected by categorized TBox triples.
  # Uses != [] pattern instead of length/1 > 0 for O(1) empty check.
  @spec determine_affected_caches(map()) :: {boolean(), boolean()}
  defp determine_affected_caches(categorized) do
    class_affected = categorized.class_hierarchy != []

    property_affected =
      categorized.property_hierarchy != [] or
        categorized.property_characteristics != [] or
        categorized.inverse_properties != []

    {class_affected, property_affected}
  end
end
