defmodule TripleStore.Reasoner.RuleCompiler do
  @moduledoc """
  Compiles OWL 2 RL rules for a specific ontology.

  The rule compiler analyzes an ontology (represented as a set of triples) and:
  1. Identifies which rules are applicable based on the ontology's axioms
  2. Specializes generic rules with ontology-specific constants
  3. Stores compiled rules in `:persistent_term` for zero-copy access

  ## Compilation Process

  ### Rule Applicability

  Rules are only applicable if the ontology contains the necessary schema triples.
  For example:
  - `prp_trp` (transitive property) is only applicable if some property is declared
    as `owl:TransitiveProperty`
  - `cax_sco` (class membership through subclass) is only applicable if there are
    `rdfs:subClassOf` triples

  ### Rule Specialization

  Generic rules can be specialized with specific constants from the ontology:
  - A rule with `?p rdf:type owl:TransitiveProperty` can be specialized for each
    transitive property in the ontology
  - This reduces the search space during rule evaluation
  - Specialization is limited to prevent explosion (configurable via `:max_specializations`)

  ## Usage

      # Compile rules for an ontology context
      {:ok, compiled} = RuleCompiler.compile(ctx, profile: :owl2rl)

      # Get compiled rules
      rules = RuleCompiler.get_rules(compiled)

      # Store in persistent_term for fast access
      RuleCompiler.store(compiled, :my_ontology)

      # Retrieve from persistent_term
      {:ok, compiled} = RuleCompiler.load(:my_ontology)

      # List all stored ontologies
      RuleCompiler.list_stored()

      # Remove when done
      RuleCompiler.remove(:my_ontology)

  ## Storage Keys

  Rules are stored in `:persistent_term` with keys of the form:
  `{TripleStore.Reasoner.RuleCompiler, ontology_key}`

  ## eq-ref Rule Handling

  The `eq_ref` rule generates `x owl:sameAs x` for every resource. During
  materialization, this should be handled specially:
  - Either skip materialization entirely (implicit reflexivity)
  - Or limit to resources that appear in explicit owl:sameAs statements

  ## Security

  All IRIs are validated before being used in queries to prevent SPARQL injection.
  Dynamic rule names use string-based identifiers instead of atoms to prevent
  atom table exhaustion.
  """

  alias TripleStore.Reasoner.{Rule, SchemaInfo, Namespaces, ReasoningProfile}

  require Logger

  # ============================================================================
  # Constants
  # ============================================================================

  @default_max_specializations 1000
  @default_max_properties_per_type 10_000

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "A compiled rule set for an ontology"
  @type compiled :: %{
          rules: [Rule.t()],
          specialized_rules: [Rule.t()],
          profile: :rdfs | :owl2rl | :all,
          schema_info: SchemaInfo.t() | map(),
          compiled_at: DateTime.t(),
          version: String.t()
        }

  @typedoc "Compilation options"
  @type compile_opts :: [
          profile: :rdfs | :owl2rl | :custom | :none,
          rules: [atom()],
          exclude: [atom()],
          specialize: boolean(),
          max_specializations: pos_integer(),
          max_properties: pos_integer()
        ]

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Compiles rules for an ontology.

  Analyzes the ontology schema and returns a compiled rule set containing
  only the applicable rules, optionally specialized with ontology constants.

  ## Options

  - `:profile` - Reasoning profile (`:rdfs`, `:owl2rl`, `:custom`, or `:none`). Default: `:owl2rl`
  - `:rules` - List of rule names for custom profile. Required when profile is `:custom`
  - `:exclude` - List of rule names to exclude from the profile
  - `:specialize` - Whether to create specialized rules. Default: `true`
  - `:max_specializations` - Maximum specialized rules to create. Default: #{@default_max_specializations}
  - `:max_properties` - Maximum properties per type to extract. Default: #{@default_max_properties_per_type}

  ## Returns

  `{:ok, compiled}` where `compiled` contains:
  - `:rules` - Applicable generic rules
  - `:specialized_rules` - Rules specialized with ontology constants
  - `:profile` - The reasoning profile used
  - `:schema_info` - Extracted schema information
  - `:compiled_at` - Compilation timestamp
  - `:version` - Version identifier for cache invalidation

  ## Examples

      {:ok, compiled} = RuleCompiler.compile(ctx, profile: :rdfs)
      length(compiled.rules)  # Number of applicable rules
  """
  @spec compile(map(), compile_opts()) :: {:ok, compiled()} | {:error, term()}
  def compile(ctx, opts \\ []) do
    profile = Keyword.get(opts, :profile, :owl2rl)
    specialize = Keyword.get(opts, :specialize, true)
    max_specs = Keyword.get(opts, :max_specializations, @default_max_specializations)
    max_props = Keyword.get(opts, :max_properties, @default_max_properties_per_type)

    # Build profile options for ReasoningProfile
    profile_opts = [
      rules: Keyword.get(opts, :rules, []),
      exclude: Keyword.get(opts, :exclude, [])
    ]

    with {:ok, schema_info} <- extract_schema_info(ctx, max_properties: max_props),
         {:ok, base_rules} <- ReasoningProfile.rules_for(profile, profile_opts) do
      applicable_rules = filter_applicable_rules(base_rules, schema_info)

      specialized_rules =
        if specialize do
          specialize_rules(applicable_rules, schema_info, max_specs)
        else
          []
        end

      compiled = %{
        rules: applicable_rules,
        specialized_rules: specialized_rules,
        profile: profile,
        schema_info: schema_info,
        compiled_at: DateTime.utc_now(),
        version: generate_version()
      }

      {:ok, compiled}
    end
  end

  @doc """
  Compiles rules without a database context.

  This is useful for testing or when you want to create rules based on
  explicit schema information rather than querying a database.

  ## Examples

      schema_info = SchemaInfo.new(
        has_subclass: true,
        transitive_properties: ["http://example.org/contains"]
      )
      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :owl2rl)
  """
  @spec compile_with_schema(SchemaInfo.t() | map(), compile_opts()) :: {:ok, compiled()} | {:error, term()}
  def compile_with_schema(schema_info, opts \\ []) do
    profile = Keyword.get(opts, :profile, :owl2rl)
    specialize = Keyword.get(opts, :specialize, true)
    max_specs = Keyword.get(opts, :max_specializations, @default_max_specializations)

    # Build profile options for ReasoningProfile
    profile_opts = [
      rules: Keyword.get(opts, :rules, []),
      exclude: Keyword.get(opts, :exclude, [])
    ]

    # Convert map to SchemaInfo if needed
    schema_info = normalize_schema_info(schema_info)

    with {:ok, base_rules} <- ReasoningProfile.rules_for(profile, profile_opts) do
      applicable_rules = filter_applicable_rules(base_rules, schema_info)

      specialized_rules =
        if specialize do
          specialize_rules(applicable_rules, schema_info, max_specs)
        else
          []
        end

      compiled = %{
        rules: applicable_rules,
        specialized_rules: specialized_rules,
        profile: profile,
        schema_info: schema_info,
        compiled_at: DateTime.utc_now(),
        version: generate_version()
      }

      {:ok, compiled}
    end
  end

  @doc """
  Returns all rules from a compiled rule set.

  Returns specialized rules if available, otherwise generic rules.
  """
  @spec get_rules(compiled()) :: [Rule.t()]
  def get_rules(%{specialized_rules: specialized, rules: generic}) do
    if Enum.empty?(specialized), do: generic, else: specialized ++ generic
  end

  @doc """
  Returns only the generic (non-specialized) rules.
  """
  @spec get_generic_rules(compiled()) :: [Rule.t()]
  def get_generic_rules(%{rules: rules}), do: rules

  @doc """
  Returns only the specialized rules.
  """
  @spec get_specialized_rules(compiled()) :: [Rule.t()]
  def get_specialized_rules(%{specialized_rules: rules}), do: rules

  # ============================================================================
  # Persistent Term Storage with Lifecycle Management
  # ============================================================================

  @doc """
  Stores a compiled rule set in `:persistent_term` for fast access.

  The store operation is idempotent - storing again with the same key
  will replace the previous value.

  ## Examples

      :ok = RuleCompiler.store(compiled, :my_ontology)
  """
  @spec store(compiled(), atom()) :: :ok
  def store(compiled, key) when is_atom(key) do
    # Add to registry first
    register_key(key)
    :persistent_term.put({__MODULE__, key}, compiled)
    :ok
  end

  @doc """
  Loads a compiled rule set from `:persistent_term`.

  ## Examples

      {:ok, compiled} = RuleCompiler.load(:my_ontology)
  """
  @spec load(atom()) :: {:ok, compiled()} | {:error, :not_found}
  def load(key) when is_atom(key) do
    case :persistent_term.get({__MODULE__, key}, nil) do
      nil -> {:error, :not_found}
      compiled -> {:ok, compiled}
    end
  end

  @doc """
  Removes a compiled rule set from `:persistent_term`.

  This also removes the key from the registry.
  """
  @spec remove(atom()) :: :ok
  def remove(key) when is_atom(key) do
    unregister_key(key)
    :persistent_term.erase({__MODULE__, key})
    :ok
  end

  @doc """
  Checks if a compiled rule set exists in `:persistent_term`.
  """
  @spec exists?(atom()) :: boolean()
  def exists?(key) when is_atom(key) do
    case :persistent_term.get({__MODULE__, key}, nil) do
      nil -> false
      _ -> true
    end
  end

  @doc """
  Lists all stored ontology keys.
  """
  @spec list_stored() :: [atom()]
  def list_stored do
    case :persistent_term.get({__MODULE__, :__registry__}, nil) do
      nil -> []
      keys -> MapSet.to_list(keys)
    end
  end

  @doc """
  Removes all stored compiled rule sets.

  Use with caution - this clears all cached compilations.
  """
  @spec clear_all() :: :ok
  def clear_all do
    keys = list_stored()

    Enum.each(keys, fn key ->
      :persistent_term.erase({__MODULE__, key})
    end)

    :persistent_term.erase({__MODULE__, :__registry__})
    :ok
  end

  @doc """
  Checks if a stored rule set is stale (version mismatch).

  Returns `true` if the stored version doesn't match the provided version,
  indicating the ontology has changed and rules should be recompiled.
  """
  @spec stale?(atom(), String.t()) :: boolean()
  def stale?(key, expected_version) when is_atom(key) and is_binary(expected_version) do
    case load(key) do
      {:ok, %{version: version}} -> version != expected_version
      {:error, :not_found} -> true
    end
  end

  defp register_key(key) do
    registry = :persistent_term.get({__MODULE__, :__registry__}, MapSet.new())
    :persistent_term.put({__MODULE__, :__registry__}, MapSet.put(registry, key))
  end

  defp unregister_key(key) do
    registry = :persistent_term.get({__MODULE__, :__registry__}, MapSet.new())
    :persistent_term.put({__MODULE__, :__registry__}, MapSet.delete(registry, key))
  end

  # ============================================================================
  # Schema Info Helpers
  # ============================================================================

  @doc """
  Creates an empty schema info structure.

  Useful for testing or when building schema info incrementally.
  """
  @spec empty_schema_info() :: SchemaInfo.t()
  def empty_schema_info do
    SchemaInfo.new()
  end

  defp normalize_schema_info(%SchemaInfo{} = info), do: info

  defp normalize_schema_info(map) when is_map(map) do
    SchemaInfo.from_map(map)
  end

  # ============================================================================
  # Schema Extraction
  # ============================================================================

  @doc """
  Extracts schema information from an ontology context.

  Queries the database to find:
  - Presence of rdfs:subClassOf, rdfs:subPropertyOf, rdfs:domain, rdfs:range
  - Properties declared as owl:TransitiveProperty, owl:SymmetricProperty, etc.
  - owl:inverseOf declarations
  - owl:hasValue, owl:someValuesFrom, owl:allValuesFrom restrictions
  - owl:sameAs usage

  ## Options

  - `:max_properties` - Maximum properties per type to return. Default: #{@default_max_properties_per_type}
  """
  @spec extract_schema_info(map(), keyword()) :: {:ok, SchemaInfo.t()} | {:error, term()}
  def extract_schema_info(ctx, opts \\ []) do
    max_props = Keyword.get(opts, :max_properties, @default_max_properties_per_type)

    schema_info =
      SchemaInfo.new(
        has_subclass: has_predicate?(ctx, Namespaces.rdfs_subClassOf()),
        has_subproperty: has_predicate?(ctx, Namespaces.rdfs_subPropertyOf()),
        has_domain: has_predicate?(ctx, Namespaces.rdfs_domain()),
        has_range: has_predicate?(ctx, Namespaces.rdfs_range()),
        has_sameas: has_predicate?(ctx, Namespaces.owl_sameAs()),
        has_restrictions: has_restrictions?(ctx),
        transitive_properties: get_typed_properties(ctx, Namespaces.owl_TransitiveProperty(), max_props),
        symmetric_properties: get_typed_properties(ctx, Namespaces.owl_SymmetricProperty(), max_props),
        inverse_properties: get_inverse_properties(ctx, max_props),
        functional_properties: get_typed_properties(ctx, Namespaces.owl_FunctionalProperty(), max_props),
        inverse_functional_properties: get_typed_properties(ctx, Namespaces.owl_InverseFunctionalProperty(), max_props),
        max_properties: max_props
      )

    {:ok, schema_info}
  end

  # ============================================================================
  # Rule Filtering
  # ============================================================================

  @doc """
  Filters rules to those applicable given the schema information.
  """
  @spec filter_applicable_rules([Rule.t()], SchemaInfo.t() | map()) :: [Rule.t()]
  def filter_applicable_rules(rules, schema_info) do
    schema = normalize_schema_info(schema_info)
    Enum.filter(rules, &rule_applicable?(&1, schema))
  end

  defp rule_applicable?(rule, schema_info) do
    case rule.name do
      # RDFS rules
      :scm_sco -> get_field(schema_info, :has_subclass)
      :scm_spo -> get_field(schema_info, :has_subproperty)
      :cax_sco -> get_field(schema_info, :has_subclass)
      :prp_spo1 -> get_field(schema_info, :has_subproperty)
      :prp_dom -> get_field(schema_info, :has_domain)
      :prp_rng -> get_field(schema_info, :has_range)

      # Property characteristic rules
      :prp_trp -> not Enum.empty?(get_field(schema_info, :transitive_properties, []))
      :prp_symp -> not Enum.empty?(get_field(schema_info, :symmetric_properties, []))
      :prp_inv1 -> not Enum.empty?(get_field(schema_info, :inverse_properties, []))
      :prp_inv2 -> not Enum.empty?(get_field(schema_info, :inverse_properties, []))
      :prp_fp -> not Enum.empty?(get_field(schema_info, :functional_properties, []))
      :prp_ifp -> not Enum.empty?(get_field(schema_info, :inverse_functional_properties, []))

      # Equality rules
      :eq_ref -> true  # Always potentially applicable
      :eq_sym -> get_field(schema_info, :has_sameas)
      :eq_trans -> get_field(schema_info, :has_sameas)
      :eq_rep_s -> get_field(schema_info, :has_sameas)
      :eq_rep_p -> get_field(schema_info, :has_sameas)
      :eq_rep_o -> get_field(schema_info, :has_sameas)

      # Class restriction rules
      :cls_hv1 -> get_field(schema_info, :has_restrictions)
      :cls_hv2 -> get_field(schema_info, :has_restrictions)
      :cls_svf1 -> get_field(schema_info, :has_restrictions)
      :cls_svf2 -> get_field(schema_info, :has_restrictions)
      :cls_avf -> get_field(schema_info, :has_restrictions)

      # Unknown rule - include by default
      _ -> true
    end
  end

  defp get_field(schema, field, default \\ false)

  defp get_field(%SchemaInfo{} = schema, field, default) do
    Map.get(schema, field, default)
  end

  defp get_field(map, field, default) when is_map(map) do
    Map.get(map, field, default)
  end

  # ============================================================================
  # Rule Specialization
  # ============================================================================

  @doc """
  Specializes rules with ontology-specific constants.

  For example, if the ontology has `ex:contains rdf:type owl:TransitiveProperty`,
  the `prp_trp` rule is specialized to bind `?p` to `ex:contains`.

  Specialization is limited by `max_specializations` to prevent explosion.
  """
  @spec specialize_rules([Rule.t()], SchemaInfo.t() | map(), pos_integer()) :: [Rule.t()]
  def specialize_rules(rules, schema_info, max_specializations \\ @default_max_specializations) do
    schema = normalize_schema_info(schema_info)

    rules
    |> Enum.flat_map(&specialize_rule(&1, schema, max_specializations))
    |> Enum.reject(&is_nil/1)
    |> Enum.take(max_specializations)
  end

  defp specialize_rule(rule, schema_info, max) do
    case rule.name do
      :prp_trp ->
        schema_info.transitive_properties
        |> Enum.take(max)
        |> Enum.map(&specialize_for_property(rule, "p", &1, :transitive))

      :prp_symp ->
        schema_info.symmetric_properties
        |> Enum.take(max)
        |> Enum.map(&specialize_for_property(rule, "p", &1, :symmetric))

      :prp_inv1 ->
        schema_info.inverse_properties
        |> Enum.take(max)
        |> Enum.map(fn {p1, p2} -> specialize_for_inverse(rule, p1, p2, :inv1) end)

      :prp_inv2 ->
        schema_info.inverse_properties
        |> Enum.take(max)
        |> Enum.map(fn {p1, p2} -> specialize_for_inverse(rule, p1, p2, :inv2) end)

      :prp_fp ->
        schema_info.functional_properties
        |> Enum.take(max)
        |> Enum.map(&specialize_for_property(rule, "p", &1, :functional))

      :prp_ifp ->
        schema_info.inverse_functional_properties
        |> Enum.take(max)
        |> Enum.map(&specialize_for_property(rule, "p", &1, :inverse_functional))

      # Rules that don't benefit from specialization
      _ -> []
    end
  end

  defp specialize_for_property(rule, var_name, property_iri, suffix) do
    # Validate IRI before using
    case Namespaces.validate_iri(property_iri) do
      {:ok, _} ->
        # Use string-based rule name to avoid atom exhaustion
        prop_local = Namespaces.extract_local_name(property_iri)
        new_name = String.to_atom("#{rule.name}_#{suffix}_#{sanitize_local_name(prop_local)}")

        # Substitute the property variable with the constant
        binding = %{var_name => {:iri, property_iri}}

        # Remove the type declaration pattern from body (first pattern)
        # and substitute the property in remaining patterns/conditions
        new_body =
          rule.body
          |> Enum.drop(1)  # Drop the rdf:type pattern
          |> Enum.map(&substitute_body_element(&1, binding))

        new_head = Rule.substitute_pattern(rule.head, binding)

        Rule.new(new_name, new_body, new_head,
          description: "#{rule.description} (specialized for #{prop_local})",
          profile: rule.profile
        )

      {:error, _} ->
        Logger.warning("Skipping invalid IRI in rule specialization: #{inspect(property_iri)}")
        nil
    end
  end

  defp specialize_for_inverse(rule, p1_iri, p2_iri, direction) do
    with {:ok, _} <- Namespaces.validate_iri(p1_iri),
         {:ok, _} <- Namespaces.validate_iri(p2_iri) do
      p1_local = Namespaces.extract_local_name(p1_iri)
      p2_local = Namespaces.extract_local_name(p2_iri)

      # Use sanitized names to avoid atom exhaustion with malicious inputs
      new_name = String.to_atom(
        "#{rule.name}_#{direction}_#{sanitize_local_name(p1_local)}_#{sanitize_local_name(p2_local)}"
      )

      binding = %{"p1" => {:iri, p1_iri}, "p2" => {:iri, p2_iri}}

      new_body =
        rule.body
        |> Enum.drop(1)  # Drop the owl:inverseOf pattern
        |> Enum.map(&substitute_body_element(&1, binding))

      new_head = Rule.substitute_pattern(rule.head, binding)

      Rule.new(new_name, new_body, new_head,
        description: "#{rule.description} (specialized for #{p1_local}/#{p2_local})",
        profile: rule.profile
      )
    else
      {:error, _} ->
        Logger.warning("Skipping invalid IRI pair in inverse specialization")
        nil
    end
  end

  # Sanitize local names to prevent atom exhaustion
  # Only allow alphanumeric characters and underscores, limit length
  defp sanitize_local_name(name) do
    name
    |> String.replace(~r/[^a-zA-Z0-9_]/, "_")
    |> String.slice(0, 50)
  end

  # Substitute variables in a body element (pattern or condition)
  defp substitute_body_element({:pattern, _} = pattern, binding) do
    Rule.substitute_pattern(pattern, binding)
  end

  defp substitute_body_element({:not_equal, term1, term2}, binding) do
    {:not_equal, Rule.substitute(term1, binding), Rule.substitute(term2, binding)}
  end

  defp substitute_body_element({:is_iri, term}, binding) do
    {:is_iri, Rule.substitute(term, binding)}
  end

  defp substitute_body_element({:is_blank, term}, binding) do
    {:is_blank, Rule.substitute(term, binding)}
  end

  defp substitute_body_element({:is_literal, term}, binding) do
    {:is_literal, Rule.substitute(term, binding)}
  end

  defp substitute_body_element({:bound, term}, binding) do
    {:bound, Rule.substitute(term, binding)}
  end

  # ============================================================================
  # Database Helpers (with SPARQL injection prevention)
  # ============================================================================

  defp has_predicate?(ctx, predicate_iri) do
    case query_exists?(ctx, nil, predicate_iri, nil) do
      {:ok, result} -> result
      {:error, reason} ->
        Logger.debug("has_predicate? query failed: #{inspect(reason)}")
        false
    end
  end

  defp get_typed_properties(ctx, type_iri, max_count) do
    case query_subjects?(ctx, Namespaces.rdf_type(), type_iri, max_count) do
      {:ok, subjects} -> subjects
      {:error, reason} ->
        Logger.debug("get_typed_properties query failed: #{inspect(reason)}")
        []
    end
  end

  defp get_inverse_properties(ctx, max_count) do
    case query_pairs?(ctx, Namespaces.owl_inverseOf(), max_count) do
      {:ok, pairs} -> pairs
      {:error, reason} ->
        Logger.debug("get_inverse_properties query failed: #{inspect(reason)}")
        []
    end
  end

  defp has_restrictions?(ctx) do
    has_predicate?(ctx, Namespaces.owl_hasValue()) or
    has_predicate?(ctx, Namespaces.owl_someValuesFrom()) or
    has_predicate?(ctx, Namespaces.owl_allValuesFrom()) or
    has_predicate?(ctx, Namespaces.owl_onProperty())
  end

  # Query helpers with SPARQL injection prevention
  defp query_exists?(ctx, subject, predicate, object) do
    with {:ok, pattern} <- build_pattern_safe(subject, predicate, object) do
      sparql = "ASK { #{pattern} }"

      try do
        case TripleStore.SPARQL.Query.query(ctx, sparql) do
          {:ok, true} -> {:ok, true}
          {:ok, false} -> {:ok, false}
          {:ok, _} -> {:ok, true}  # Non-empty result
          {:error, reason} -> {:error, reason}
        end
      rescue
        e in ArgumentError ->
          {:error, {:query_error, e.message}}
        e ->
          Logger.warning("Unexpected error in query_exists?: #{inspect(e)}")
          {:error, {:unexpected_error, e}}
      end
    end
  end

  defp query_subjects?(ctx, predicate, object, max_count) do
    with {:ok, pred_safe} <- validate_iri_for_query(predicate),
         {:ok, obj_safe} <- validate_iri_for_query(object) do
      sparql = "SELECT DISTINCT ?s WHERE { ?s <#{pred_safe}> <#{obj_safe}> } LIMIT #{max_count}"

      try do
        case TripleStore.SPARQL.Query.query(ctx, sparql) do
          {:ok, results} ->
            subjects = Enum.map(results, fn r ->
              case r["s"] do
                {:named_node, iri} -> iri
                %RDF.IRI{value: iri} -> iri
                iri when is_binary(iri) -> iri
                _ -> nil
              end
            end)
            {:ok, Enum.reject(subjects, &is_nil/1)}

          {:error, reason} ->
            {:error, reason}
        end
      rescue
        e in ArgumentError ->
          {:error, {:query_error, e.message}}
        e ->
          Logger.warning("Unexpected error in query_subjects?: #{inspect(e)}")
          {:error, {:unexpected_error, e}}
      end
    end
  end

  defp query_pairs?(ctx, predicate, max_count) do
    with {:ok, pred_safe} <- validate_iri_for_query(predicate) do
      sparql = "SELECT DISTINCT ?s ?o WHERE { ?s <#{pred_safe}> ?o } LIMIT #{max_count}"

      try do
        case TripleStore.SPARQL.Query.query(ctx, sparql) do
          {:ok, results} ->
            pairs = Enum.map(results, fn r ->
              s = case r["s"] do
                {:named_node, iri} -> iri
                %RDF.IRI{value: iri} -> iri
                iri when is_binary(iri) -> iri
                _ -> nil
              end
              o = case r["o"] do
                {:named_node, iri} -> iri
                %RDF.IRI{value: iri} -> iri
                iri when is_binary(iri) -> iri
                _ -> nil
              end
              if s && o, do: {s, o}, else: nil
            end)
            {:ok, Enum.reject(pairs, &is_nil/1)}

          {:error, reason} ->
            {:error, reason}
        end
      rescue
        e in ArgumentError ->
          {:error, {:query_error, e.message}}
        e ->
          Logger.warning("Unexpected error in query_pairs?: #{inspect(e)}")
          {:error, {:unexpected_error, e}}
      end
    end
  end

  defp build_pattern_safe(nil, predicate, nil) do
    with {:ok, pred} <- validate_iri_for_query(predicate) do
      {:ok, "?s <#{pred}> ?o"}
    end
  end

  defp build_pattern_safe(nil, predicate, object) do
    with {:ok, pred} <- validate_iri_for_query(predicate),
         {:ok, obj} <- validate_iri_for_query(object) do
      {:ok, "?s <#{pred}> <#{obj}>"}
    end
  end

  defp build_pattern_safe(subject, predicate, nil) do
    with {:ok, subj} <- validate_iri_for_query(subject),
         {:ok, pred} <- validate_iri_for_query(predicate) do
      {:ok, "<#{subj}> <#{pred}> ?o"}
    end
  end

  defp build_pattern_safe(subject, predicate, object) do
    with {:ok, subj} <- validate_iri_for_query(subject),
         {:ok, pred} <- validate_iri_for_query(predicate),
         {:ok, obj} <- validate_iri_for_query(object) do
      {:ok, "<#{subj}> <#{pred}> <#{obj}>"}
    end
  end

  defp validate_iri_for_query(iri) do
    case Namespaces.validate_iri(iri) do
      {:ok, _} -> {:ok, iri}
      {:error, _} -> {:error, {:invalid_iri, iri}}
    end
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp generate_version do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end
end
