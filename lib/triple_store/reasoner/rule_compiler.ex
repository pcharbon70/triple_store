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

  ## Usage

      # Compile rules for an ontology context
      {:ok, compiled} = RuleCompiler.compile(ctx, profile: :owl2rl)

      # Get compiled rules
      rules = RuleCompiler.get_rules(compiled)

      # Store in persistent_term for fast access
      RuleCompiler.store(compiled, :my_ontology)

      # Retrieve from persistent_term
      {:ok, compiled} = RuleCompiler.load(:my_ontology)

  ## Storage Keys

  Rules are stored in `:persistent_term` with keys of the form:
  `{TripleStore.Reasoner.RuleCompiler, ontology_key}`
  """

  alias TripleStore.Reasoner.{Rule, Rules}

  # ============================================================================
  # Namespace Constants
  # ============================================================================

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "A compiled rule set for an ontology"
  @type compiled :: %{
          rules: [Rule.t()],
          specialized_rules: [Rule.t()],
          profile: :rdfs | :owl2rl | :all,
          schema_info: schema_info(),
          compiled_at: DateTime.t()
        }

  @typedoc "Schema information extracted from the ontology"
  @type schema_info :: %{
          has_subclass: boolean(),
          has_subproperty: boolean(),
          has_domain: boolean(),
          has_range: boolean(),
          transitive_properties: [String.t()],
          symmetric_properties: [String.t()],
          inverse_properties: [{String.t(), String.t()}],
          functional_properties: [String.t()],
          inverse_functional_properties: [String.t()],
          has_sameas: boolean(),
          has_restrictions: boolean()
        }

  @typedoc "Compilation options"
  @type compile_opts :: [
          profile: :rdfs | :owl2rl | :all,
          specialize: boolean()
        ]

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Compiles rules for an ontology.

  Analyzes the ontology schema and returns a compiled rule set containing
  only the applicable rules, optionally specialized with ontology constants.

  ## Options

  - `:profile` - Reasoning profile (`:rdfs`, `:owl2rl`, or `:all`). Default: `:owl2rl`
  - `:specialize` - Whether to create specialized rules. Default: `true`

  ## Returns

  `{:ok, compiled}` where `compiled` contains:
  - `:rules` - Applicable generic rules
  - `:specialized_rules` - Rules specialized with ontology constants
  - `:profile` - The reasoning profile used
  - `:schema_info` - Extracted schema information
  - `:compiled_at` - Compilation timestamp

  ## Examples

      {:ok, compiled} = RuleCompiler.compile(ctx, profile: :rdfs)
      length(compiled.rules)  # Number of applicable rules
  """
  @spec compile(map(), compile_opts()) :: {:ok, compiled()} | {:error, term()}
  def compile(ctx, opts \\ []) do
    profile = Keyword.get(opts, :profile, :owl2rl)
    specialize = Keyword.get(opts, :specialize, true)

    with {:ok, schema_info} <- extract_schema_info(ctx) do
      base_rules = Rules.rules_for_profile(profile)
      applicable_rules = filter_applicable_rules(base_rules, schema_info)

      specialized_rules =
        if specialize do
          specialize_rules(applicable_rules, schema_info)
        else
          []
        end

      compiled = %{
        rules: applicable_rules,
        specialized_rules: specialized_rules,
        profile: profile,
        schema_info: schema_info,
        compiled_at: DateTime.utc_now()
      }

      {:ok, compiled}
    end
  end

  @doc """
  Compiles rules without a database context.

  This is useful for testing or when you want to create rules based on
  explicit schema information rather than querying a database.

  ## Examples

      schema_info = %{
        has_subclass: true,
        transitive_properties: ["http://example.org/contains"],
        ...
      }
      {:ok, compiled} = RuleCompiler.compile_with_schema(schema_info, profile: :owl2rl)
  """
  @spec compile_with_schema(schema_info(), compile_opts()) :: {:ok, compiled()}
  def compile_with_schema(schema_info, opts \\ []) do
    profile = Keyword.get(opts, :profile, :owl2rl)
    specialize = Keyword.get(opts, :specialize, true)

    base_rules = Rules.rules_for_profile(profile)
    applicable_rules = filter_applicable_rules(base_rules, schema_info)

    specialized_rules =
      if specialize do
        specialize_rules(applicable_rules, schema_info)
      else
        []
      end

    compiled = %{
      rules: applicable_rules,
      specialized_rules: specialized_rules,
      profile: profile,
      schema_info: schema_info,
      compiled_at: DateTime.utc_now()
    }

    {:ok, compiled}
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

  @doc """
  Stores a compiled rule set in `:persistent_term` for fast access.

  ## Examples

      :ok = RuleCompiler.store(compiled, :my_ontology)
  """
  @spec store(compiled(), atom()) :: :ok
  def store(compiled, key) when is_atom(key) do
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
  """
  @spec remove(atom()) :: :ok
  def remove(key) when is_atom(key) do
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
  Creates an empty schema info structure.

  Useful for testing or when building schema info incrementally.
  """
  @spec empty_schema_info() :: schema_info()
  def empty_schema_info do
    %{
      has_subclass: false,
      has_subproperty: false,
      has_domain: false,
      has_range: false,
      transitive_properties: [],
      symmetric_properties: [],
      inverse_properties: [],
      functional_properties: [],
      inverse_functional_properties: [],
      has_sameas: false,
      has_restrictions: false
    }
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
  """
  @spec extract_schema_info(map()) :: {:ok, schema_info()} | {:error, term()}
  def extract_schema_info(ctx) do
    # Query for schema predicates
    schema_info = %{
      has_subclass: has_predicate?(ctx, "#{@rdfs}subClassOf"),
      has_subproperty: has_predicate?(ctx, "#{@rdfs}subPropertyOf"),
      has_domain: has_predicate?(ctx, "#{@rdfs}domain"),
      has_range: has_predicate?(ctx, "#{@rdfs}range"),
      transitive_properties: get_typed_properties(ctx, "#{@owl}TransitiveProperty"),
      symmetric_properties: get_typed_properties(ctx, "#{@owl}SymmetricProperty"),
      inverse_properties: get_inverse_properties(ctx),
      functional_properties: get_typed_properties(ctx, "#{@owl}FunctionalProperty"),
      inverse_functional_properties: get_typed_properties(ctx, "#{@owl}InverseFunctionalProperty"),
      has_sameas: has_predicate?(ctx, "#{@owl}sameAs"),
      has_restrictions: has_restrictions?(ctx)
    }

    {:ok, schema_info}
  end

  # ============================================================================
  # Rule Filtering
  # ============================================================================

  @doc """
  Filters rules to those applicable given the schema information.
  """
  @spec filter_applicable_rules([Rule.t()], schema_info()) :: [Rule.t()]
  def filter_applicable_rules(rules, schema_info) do
    Enum.filter(rules, &rule_applicable?(&1, schema_info))
  end

  defp rule_applicable?(rule, schema_info) do
    case rule.name do
      # RDFS rules
      :scm_sco -> schema_info.has_subclass
      :scm_spo -> schema_info.has_subproperty
      :cax_sco -> schema_info.has_subclass
      :prp_spo1 -> schema_info.has_subproperty
      :prp_dom -> schema_info.has_domain
      :prp_rng -> schema_info.has_range

      # Property characteristic rules
      :prp_trp -> length(schema_info.transitive_properties) > 0
      :prp_symp -> length(schema_info.symmetric_properties) > 0
      :prp_inv1 -> length(schema_info.inverse_properties) > 0
      :prp_inv2 -> length(schema_info.inverse_properties) > 0
      :prp_fp -> length(schema_info.functional_properties) > 0
      :prp_ifp -> length(schema_info.inverse_functional_properties) > 0

      # Equality rules
      :eq_ref -> true  # Always potentially applicable
      :eq_sym -> schema_info.has_sameas
      :eq_trans -> schema_info.has_sameas
      :eq_rep_s -> schema_info.has_sameas
      :eq_rep_p -> schema_info.has_sameas
      :eq_rep_o -> schema_info.has_sameas

      # Class restriction rules
      :cls_hv1 -> schema_info.has_restrictions
      :cls_hv2 -> schema_info.has_restrictions
      :cls_svf1 -> schema_info.has_restrictions
      :cls_svf2 -> schema_info.has_restrictions
      :cls_avf -> schema_info.has_restrictions

      # Unknown rule - include by default
      _ -> true
    end
  end

  # ============================================================================
  # Rule Specialization
  # ============================================================================

  @doc """
  Specializes rules with ontology-specific constants.

  For example, if the ontology has `ex:contains rdf:type owl:TransitiveProperty`,
  the `prp_trp` rule is specialized to bind `?p` to `ex:contains`.
  """
  @spec specialize_rules([Rule.t()], schema_info()) :: [Rule.t()]
  def specialize_rules(rules, schema_info) do
    rules
    |> Enum.flat_map(&specialize_rule(&1, schema_info))
    |> Enum.reject(&is_nil/1)
  end

  defp specialize_rule(rule, schema_info) do
    case rule.name do
      :prp_trp ->
        for prop <- schema_info.transitive_properties do
          specialize_for_property(rule, "p", prop, :transitive)
        end

      :prp_symp ->
        for prop <- schema_info.symmetric_properties do
          specialize_for_property(rule, "p", prop, :symmetric)
        end

      :prp_inv1 ->
        for {p1, p2} <- schema_info.inverse_properties do
          specialize_for_inverse(rule, p1, p2, :inv1)
        end

      :prp_inv2 ->
        for {p1, p2} <- schema_info.inverse_properties do
          specialize_for_inverse(rule, p1, p2, :inv2)
        end

      :prp_fp ->
        for prop <- schema_info.functional_properties do
          specialize_for_property(rule, "p", prop, :functional)
        end

      :prp_ifp ->
        for prop <- schema_info.inverse_functional_properties do
          specialize_for_property(rule, "p", prop, :inverse_functional)
        end

      # Rules that don't benefit from specialization
      _ -> []
    end
  end

  defp specialize_for_property(rule, var_name, property_iri, suffix) do
    # Create a specialized rule name
    prop_local = extract_local_name(property_iri)
    new_name = String.to_atom("#{rule.name}_#{suffix}_#{prop_local}")

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
  end

  defp specialize_for_inverse(rule, p1_iri, p2_iri, direction) do
    p1_local = extract_local_name(p1_iri)
    p2_local = extract_local_name(p2_iri)
    new_name = String.to_atom("#{rule.name}_#{direction}_#{p1_local}_#{p2_local}")

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
  end

  defp extract_local_name(iri) do
    cond do
      String.contains?(iri, "#") ->
        iri |> String.split("#") |> List.last()
      String.contains?(iri, "/") ->
        iri |> String.split("/") |> List.last()
      true ->
        iri
    end
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
  # Database Helpers
  # ============================================================================

  defp has_predicate?(ctx, predicate_iri) do
    # Use SPARQL query or direct index lookup
    case query_exists?(ctx, nil, predicate_iri, nil) do
      {:ok, result} -> result
      _ -> false
    end
  end

  defp get_typed_properties(ctx, type_iri) do
    # Query for: ?p rdf:type <type_iri>
    case query_subjects?(ctx, "#{@rdf}type", type_iri) do
      {:ok, subjects} -> subjects
      _ -> []
    end
  end

  defp get_inverse_properties(ctx) do
    # Query for: ?p1 owl:inverseOf ?p2
    case query_pairs?(ctx, "#{@owl}inverseOf") do
      {:ok, pairs} -> pairs
      _ -> []
    end
  end

  defp has_restrictions?(ctx) do
    has_predicate?(ctx, "#{@owl}hasValue") or
    has_predicate?(ctx, "#{@owl}someValuesFrom") or
    has_predicate?(ctx, "#{@owl}allValuesFrom") or
    has_predicate?(ctx, "#{@owl}onProperty")
  end

  # Query helpers that work with the database context
  defp query_exists?(ctx, subject, predicate, object) do
    try do
      pattern = build_pattern(subject, predicate, object)
      sparql = "ASK { #{pattern} }"

      case TripleStore.SPARQL.Query.query(ctx, sparql) do
        {:ok, true} -> {:ok, true}
        {:ok, false} -> {:ok, false}
        {:ok, _} -> {:ok, true}  # Non-empty result
        error -> error
      end
    rescue
      _ -> {:ok, false}
    end
  end

  defp query_subjects?(ctx, predicate, object) do
    try do
      sparql = "SELECT DISTINCT ?s WHERE { ?s <#{predicate}> <#{object}> }"

      case TripleStore.SPARQL.Query.query(ctx, sparql) do
        {:ok, results} ->
          subjects = Enum.map(results, fn r ->
            case r["s"] do
              {:named_node, iri} -> iri
              %RDF.IRI{value: iri} -> iri
              _ -> nil
            end
          end)
          {:ok, Enum.reject(subjects, &is_nil/1)}
        error -> error
      end
    rescue
      _ -> {:ok, []}
    end
  end

  defp query_pairs?(ctx, predicate) do
    try do
      sparql = "SELECT DISTINCT ?s ?o WHERE { ?s <#{predicate}> ?o }"

      case TripleStore.SPARQL.Query.query(ctx, sparql) do
        {:ok, results} ->
          pairs = Enum.map(results, fn r ->
            s = case r["s"] do
              {:named_node, iri} -> iri
              %RDF.IRI{value: iri} -> iri
              _ -> nil
            end
            o = case r["o"] do
              {:named_node, iri} -> iri
              %RDF.IRI{value: iri} -> iri
              _ -> nil
            end
            if s && o, do: {s, o}, else: nil
          end)
          {:ok, Enum.reject(pairs, &is_nil/1)}
        error -> error
      end
    rescue
      _ -> {:ok, []}
    end
  end

  defp build_pattern(nil, predicate, nil), do: "?s <#{predicate}> ?o"
  defp build_pattern(nil, predicate, object), do: "?s <#{predicate}> <#{object}>"
  defp build_pattern(subject, predicate, nil), do: "<#{subject}> <#{predicate}> ?o"
  defp build_pattern(subject, predicate, object), do: "<#{subject}> <#{predicate}> <#{object}>"
end
