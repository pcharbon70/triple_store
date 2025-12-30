defmodule TripleStore.Reasoner.RuleOptimizer do
  @moduledoc """
  Optimizes OWL 2 RL rules for efficient evaluation.

  The optimizer provides three main optimizations:

  1. **Pattern Reordering**: Reorders body patterns by estimated selectivity to
     reduce intermediate result sizes during evaluation.

  2. **Rule Batching**: Groups rules that can be evaluated together because they
     share common body patterns or have compatible structures.

  3. **Dead Rule Detection**: Identifies rules that cannot fire given the current
     schema, allowing them to be skipped during evaluation.

  ## Pattern Selectivity

  Patterns are ranked by selectivity (most selective first):
  - Patterns with bound constants are more selective than all-variable patterns
  - The predicate position is typically most discriminating in RDF data
  - Variables that are already bound by earlier patterns add selectivity

  ## Selectivity Constants

  The optimizer uses configurable selectivity estimates (defined as module attributes):
  - Bound variable: 0.01 (very selective)
  - Literal constant: 0.001 (extremely selective)
  - IRI in predicate position: 0.001 (very selective)
  - IRI in subject/object position: 0.01
  - Unbound subject: 0.1
  - Unbound predicate: 0.01 (predicates rare in RDF)
  - Unbound object: 0.1

  ## Usage

      # Optimize a single rule
      optimized_rule = RuleOptimizer.optimize_rule(rule, schema_info)

      # Optimize a list of rules
      optimized_rules = RuleOptimizer.optimize_rules(rules, schema_info)

      # Group rules into batches
      batches = RuleOptimizer.batch_rules(rules)

      # Find rules that cannot fire
      dead_rules = RuleOptimizer.find_dead_rules(rules, schema_info)
  """

  alias TripleStore.Reasoner.{Rule, Namespaces}

  # ============================================================================
  # Selectivity Constants
  # ============================================================================

  # Selectivity estimates (lower = more selective, filters more)
  @bound_var_selectivity 0.01
  @literal_selectivity 0.001
  @iri_predicate_selectivity 0.001
  @iri_subject_object_selectivity 0.01
  @unbound_subject_selectivity 0.1
  @unbound_predicate_selectivity 0.01
  @unbound_object_selectivity 0.1

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "Schema information for optimization decisions"
  @type schema_info :: %{
          optional(:has_subclass) => boolean(),
          optional(:has_subproperty) => boolean(),
          optional(:has_domain) => boolean(),
          optional(:has_range) => boolean(),
          optional(:transitive_properties) => [String.t()],
          optional(:symmetric_properties) => [String.t()],
          optional(:inverse_properties) => [{String.t(), String.t()}],
          optional(:functional_properties) => [String.t()],
          optional(:inverse_functional_properties) => [String.t()],
          optional(:has_sameas) => boolean(),
          optional(:has_restrictions) => boolean()
        }

  @typedoc "Statistics about the data for better selectivity estimation"
  @type data_stats :: %{
          optional(:predicate_counts) => %{String.t() => non_neg_integer()},
          optional(:class_counts) => %{String.t() => non_neg_integer()},
          optional(:total_triples) => non_neg_integer()
        }

  @typedoc "A batch of rules that can be evaluated together"
  @type rule_batch :: %{
          name: atom(),
          rules: [Rule.t()],
          shared_patterns: [Rule.pattern()],
          batch_type: :same_predicate | :same_head | :independent
        }

  @typedoc "Optimization options"
  @type optimize_opts :: [
          data_stats: data_stats(),
          preserve_conditions: boolean()
        ]

  # ============================================================================
  # Public API - Pattern Reordering
  # ============================================================================

  @doc """
  Optimizes a rule by reordering body patterns for efficient evaluation.

  Patterns are reordered based on selectivity estimates:
  1. Patterns with more bound terms come first
  2. Patterns binding variables used in later patterns come earlier
  3. Conditions are placed after the patterns they depend on

  ## Options

  - `:data_stats` - Optional data statistics for better selectivity estimates
  - `:preserve_conditions` - If true, keeps conditions in original positions (default: false)

  ## Examples

      optimized = RuleOptimizer.optimize_rule(rule)
  """
  @spec optimize_rule(Rule.t(), optimize_opts()) :: Rule.t()
  def optimize_rule(%Rule{} = rule, opts \\ []) do
    data_stats = Keyword.get(opts, :data_stats, %{})
    preserve_conditions = Keyword.get(opts, :preserve_conditions, false)

    patterns = Rule.body_patterns(rule)
    conditions = Rule.body_conditions(rule)

    # Reorder patterns by selectivity
    reordered_patterns = reorder_patterns(patterns, data_stats)

    # Place conditions optimally
    new_body =
      if preserve_conditions do
        reordered_patterns ++ conditions
      else
        interleave_conditions(reordered_patterns, conditions)
      end

    %Rule{rule | body: new_body}
  end

  @doc """
  Optimizes a list of rules.
  """
  @spec optimize_rules([Rule.t()], optimize_opts()) :: [Rule.t()]
  def optimize_rules(rules, opts \\ []) when is_list(rules) do
    Enum.map(rules, &optimize_rule(&1, opts))
  end

  # ============================================================================
  # Public API - Rule Batching
  # ============================================================================

  @doc """
  Groups rules into batches that can be evaluated together efficiently.

  Rules are batched based on:
  - Shared body patterns (can share intermediate results)
  - Same head predicate (can use bulk insert)
  - Same structure (can be vectorized)

  ## Returns

  A list of batch structures, each containing:
  - `:name` - Batch identifier
  - `:rules` - Rules in this batch
  - `:shared_patterns` - Common patterns across rules
  - `:batch_type` - Classification of the batch

  ## Examples

      batches = RuleOptimizer.batch_rules(rules)
      Enum.each(batches, fn b ->
        IO.puts("Batch \#{b.name}: \#{length(b.rules)} rules")
      end)
  """
  @spec batch_rules([Rule.t()]) :: [rule_batch()]
  def batch_rules(rules) when is_list(rules) do
    rules
    |> group_by_head_predicate()
    |> Enum.map(fn {predicate, group_rules} ->
      shared = find_shared_patterns(group_rules)
      batch_type = classify_batch(group_rules, shared)

      %{
        name: batch_name(predicate, batch_type),
        rules: group_rules,
        shared_patterns: shared,
        batch_type: batch_type
      }
    end)
  end

  @doc """
  Returns rules that can share intermediate results due to common patterns.
  """
  @spec find_shareable_rules([Rule.t()]) :: [{Rule.t(), Rule.t(), [Rule.pattern()]}]
  def find_shareable_rules(rules) when is_list(rules) do
    for r1 <- rules,
        r2 <- rules,
        r1.name < r2.name,
        shared = find_common_patterns(r1, r2),
        length(shared) > 0 do
      {r1, r2, shared}
    end
  end

  # ============================================================================
  # Public API - Dead Rule Detection
  # ============================================================================

  @doc """
  Identifies rules that cannot fire given the current schema.

  A rule is "dead" if:
  - It requires a schema feature that doesn't exist
  - It references predicates not in the data
  - It has unsatisfiable conditions

  ## Examples

      dead = RuleOptimizer.find_dead_rules(rules, schema_info)
      active = Enum.reject(rules, &(&1 in dead))
  """
  @spec find_dead_rules([Rule.t()], schema_info()) :: [Rule.t()]
  def find_dead_rules(rules, schema_info) when is_list(rules) do
    Enum.filter(rules, &rule_dead?(&1, schema_info))
  end

  @doc """
  Returns only rules that can potentially fire.
  """
  @spec filter_active_rules([Rule.t()], schema_info()) :: [Rule.t()]
  def filter_active_rules(rules, schema_info) when is_list(rules) do
    Enum.reject(rules, &rule_dead?(&1, schema_info))
  end

  @doc """
  Checks if a specific rule is dead (cannot fire).
  """
  @spec rule_dead?(Rule.t(), schema_info()) :: boolean()
  def rule_dead?(%Rule{} = rule, schema_info) do
    case rule.name do
      # Rules requiring transitive properties
      name when name in [:prp_trp] ->
        empty_list?(schema_info[:transitive_properties])

      # Rules requiring symmetric properties
      name when name in [:prp_symp] ->
        empty_list?(schema_info[:symmetric_properties])

      # Rules requiring inverse properties
      name when name in [:prp_inv1, :prp_inv2] ->
        empty_list?(schema_info[:inverse_properties])

      # Rules requiring functional properties
      name when name in [:prp_fp] ->
        empty_list?(schema_info[:functional_properties])

      # Rules requiring inverse functional properties
      name when name in [:prp_ifp] ->
        empty_list?(schema_info[:inverse_functional_properties])

      # Rules requiring subclass assertions
      name when name in [:scm_sco, :cax_sco] ->
        schema_info[:has_subclass] == false

      # Rules requiring subproperty assertions
      name when name in [:scm_spo, :prp_spo1] ->
        schema_info[:has_subproperty] == false

      # Rules requiring domain/range
      :prp_dom ->
        schema_info[:has_domain] == false

      :prp_rng ->
        schema_info[:has_range] == false

      # Rules requiring sameAs
      name when name in [:eq_sym, :eq_trans, :eq_rep_s, :eq_rep_p, :eq_rep_o] ->
        schema_info[:has_sameas] == false

      # Rules requiring restrictions
      name when name in [:cls_hv1, :cls_hv2, :cls_svf1, :cls_svf2, :cls_avf] ->
        schema_info[:has_restrictions] == false

      # Specialized rules - check if the specialized property still exists
      name ->
        check_specialized_rule_dead(name, schema_info)
    end
  end

  # ============================================================================
  # Pattern Selectivity Estimation
  # ============================================================================

  @doc """
  Estimates the selectivity of a pattern (lower = more selective).

  Selectivity is estimated based on:
  - Number of bound terms (constants vs variables)
  - Position of bound terms (predicate most selective in RDF)
  - Whether variables are already bound from earlier patterns
  """
  @spec estimate_selectivity(Rule.pattern(), MapSet.t(String.t()), data_stats()) :: float()
  def estimate_selectivity({:pattern, [s, p, o]}, bound_vars, data_stats) do
    s_sel = term_selectivity(s, :subject, bound_vars, data_stats)
    p_sel = term_selectivity(p, :predicate, bound_vars, data_stats)
    o_sel = term_selectivity(o, :object, bound_vars, data_stats)

    # Combine selectivities (multiply for independence assumption)
    s_sel * p_sel * o_sel
  end

  # ============================================================================
  # Private - Pattern Reordering
  # ============================================================================

  defp reorder_patterns(patterns, data_stats) do
    reorder_patterns(patterns, MapSet.new(), data_stats, [])
  end

  defp reorder_patterns([], _bound_vars, _data_stats, acc), do: Enum.reverse(acc)

  defp reorder_patterns(remaining, bound_vars, data_stats, acc) do
    # Find the most selective pattern given currently bound variables
    {best, rest} = select_most_selective(remaining, bound_vars, data_stats)

    # Add variables from the selected pattern to bound set
    new_bound = add_pattern_vars(best, bound_vars)

    reorder_patterns(rest, new_bound, data_stats, [best | acc])
  end

  defp select_most_selective(patterns, bound_vars, data_stats) do
    scored =
      patterns
      |> Enum.map(fn p -> {p, estimate_selectivity(p, bound_vars, data_stats)} end)
      |> Enum.sort_by(fn {_p, score} -> score end)

    [{best, _score} | rest_scored] = scored
    rest = Enum.map(rest_scored, fn {p, _s} -> p end)

    {best, rest}
  end

  defp add_pattern_vars({:pattern, terms}, bound_vars) do
    terms
    |> Enum.reduce(bound_vars, fn
      {:var, name}, acc -> MapSet.put(acc, name)
      _, acc -> acc
    end)
  end

  defp term_selectivity(term, position, bound_vars, data_stats) do
    base_selectivity = position_selectivity(position)

    case term do
      {:var, name} ->
        if MapSet.member?(bound_vars, name) do
          # Already bound - very selective
          @bound_var_selectivity
        else
          # Unbound variable - low selectivity
          base_selectivity
        end

      {:iri, iri} ->
        # Constant - check data stats if available
        get_iri_selectivity(iri, position, data_stats)

      {:literal, _, _} ->
        # Literal constant - typically very selective
        @literal_selectivity

      {:literal, _, _, _} ->
        # Typed/lang literal - typically very selective
        @literal_selectivity

      {:blank_node, _} ->
        # Blank nodes are fairly selective
        @iri_subject_object_selectivity

      _ ->
        base_selectivity
    end
  end

  # Predicate position is most selective in typical RDF data
  defp position_selectivity(:subject), do: @unbound_subject_selectivity
  defp position_selectivity(:predicate), do: @unbound_predicate_selectivity
  defp position_selectivity(:object), do: @unbound_object_selectivity

  defp get_iri_selectivity(iri, :predicate, %{predicate_counts: counts, total_triples: total})
       when is_map(counts) and total > 0 do
    count = Map.get(counts, iri, 1)
    count / total
  end

  defp get_iri_selectivity(_iri, :predicate, _data_stats), do: @iri_predicate_selectivity
  defp get_iri_selectivity(_iri, _position, _data_stats), do: @iri_subject_object_selectivity

  # ============================================================================
  # Private - Condition Placement
  # ============================================================================

  defp interleave_conditions(patterns, conditions) do
    # Place each condition as early as possible (after all its variables are bound)
    {result, remaining_conditions} =
      Enum.reduce(patterns, {[], conditions}, fn pattern, {acc, conds} ->
        bound_vars = collect_bound_vars(acc ++ [pattern])

        # Find conditions that can be placed now
        {placeable, still_pending} =
          Enum.split_with(conds, fn cond ->
            condition_vars_bound?(cond, bound_vars)
          end)

        {acc ++ [pattern] ++ placeable, still_pending}
      end)

    # Append any remaining conditions at the end
    result ++ remaining_conditions
  end

  defp collect_bound_vars(elements) do
    Enum.reduce(elements, MapSet.new(), fn
      {:pattern, terms}, acc ->
        Enum.reduce(terms, acc, fn
          {:var, name}, inner_acc -> MapSet.put(inner_acc, name)
          _, inner_acc -> inner_acc
        end)

      _, acc ->
        acc
    end)
  end

  defp condition_vars_bound?(condition, bound_vars) do
    vars = condition_variables(condition)
    Enum.all?(vars, &MapSet.member?(bound_vars, &1))
  end

  defp condition_variables({:not_equal, t1, t2}) do
    term_variables(t1) ++ term_variables(t2)
  end

  defp condition_variables({:is_iri, t}), do: term_variables(t)
  defp condition_variables({:is_blank, t}), do: term_variables(t)
  defp condition_variables({:is_literal, t}), do: term_variables(t)
  defp condition_variables({:bound, t}), do: term_variables(t)

  defp term_variables({:var, name}), do: [name]
  defp term_variables(_), do: []

  # ============================================================================
  # Private - Rule Batching
  # ============================================================================

  defp group_by_head_predicate(rules) do
    Enum.group_by(rules, fn %Rule{head: {:pattern, [_s, p, _o]}} -> p end)
  end

  defp find_shared_patterns(rules) do
    case rules do
      [] ->
        []

      [single] ->
        Rule.body_patterns(single)

      [first | rest] ->
        first_patterns = MapSet.new(Rule.body_patterns(first))

        Enum.reduce(rest, first_patterns, fn rule, acc ->
          rule_patterns = MapSet.new(Rule.body_patterns(rule))
          MapSet.intersection(acc, rule_patterns)
        end)
        |> MapSet.to_list()
    end
  end

  defp find_common_patterns(rule1, rule2) do
    p1 = MapSet.new(Rule.body_patterns(rule1))
    p2 = MapSet.new(Rule.body_patterns(rule2))

    MapSet.intersection(p1, p2) |> MapSet.to_list()
  end

  defp classify_batch(rules, shared_patterns) do
    cond do
      length(shared_patterns) > 0 and length(rules) > 1 ->
        :same_predicate

      same_head_structure?(rules) ->
        :same_head

      true ->
        :independent
    end
  end

  defp same_head_structure?(rules) do
    heads = Enum.map(rules, fn %Rule{head: head} -> head end)

    case heads do
      [] -> true
      [_] -> true
      [first | rest] -> Enum.all?(rest, &patterns_same_structure?(first, &1))
    end
  end

  defp patterns_same_structure?({:pattern, terms1}, {:pattern, terms2}) do
    length(terms1) == length(terms2) and
      Enum.zip(terms1, terms2)
      |> Enum.all?(fn {t1, t2} -> same_term_type?(t1, t2) end)
  end

  defp same_term_type?({:var, _}, {:var, _}), do: true
  defp same_term_type?({:iri, _}, {:iri, _}), do: true
  defp same_term_type?({:literal, _, _}, {:literal, _, _}), do: true
  defp same_term_type?({:literal, _, _, _}, {:literal, _, _, _}), do: true
  defp same_term_type?(_, _), do: false

  defp batch_name(predicate, batch_type) do
    pred_str =
      case predicate do
        {:iri, iri} -> extract_local_name(iri)
        {:var, name} -> "var_#{name}"
        _ -> "unknown"
      end

    String.to_atom("batch_#{batch_type}_#{pred_str}")
  end

  defp extract_local_name(iri) do
    Namespaces.extract_local_name(iri)
  end

  # ============================================================================
  # Private - Dead Rule Detection
  # ============================================================================

  defp empty_list?(nil), do: true
  defp empty_list?([]), do: true
  defp empty_list?(_), do: false

  defp check_specialized_rule_dead(name, schema_info) do
    name_str = Atom.to_string(name)

    cond do
      # Specialized transitive rule
      String.starts_with?(name_str, "prp_trp_transitive_") ->
        prop = extract_specialized_property(name_str, "prp_trp_transitive_")
        not property_in_list?(prop, schema_info[:transitive_properties])

      # Specialized symmetric rule
      String.starts_with?(name_str, "prp_symp_symmetric_") ->
        prop = extract_specialized_property(name_str, "prp_symp_symmetric_")
        not property_in_list?(prop, schema_info[:symmetric_properties])

      # Specialized functional rule
      String.starts_with?(name_str, "prp_fp_functional_") ->
        prop = extract_specialized_property(name_str, "prp_fp_functional_")
        not property_in_list?(prop, schema_info[:functional_properties])

      # Specialized inverse functional rule
      String.starts_with?(name_str, "prp_ifp_inverse_functional_") ->
        prop = extract_specialized_property(name_str, "prp_ifp_inverse_functional_")
        not property_in_list?(prop, schema_info[:inverse_functional_properties])

      # Specialized inverse rule (prp_inv1/prp_inv2)
      String.starts_with?(name_str, "prp_inv") ->
        check_inverse_rule_dead(name_str, schema_info)

      # Unknown rule - assume not dead
      true ->
        false
    end
  end

  defp extract_specialized_property(name_str, prefix) do
    name_str
    |> String.replace_prefix(prefix, "")
    |> String.downcase()
  end

  defp property_in_list?(_prop, nil), do: false
  defp property_in_list?(_prop, []), do: false

  defp property_in_list?(prop_local, properties) do
    Enum.any?(properties, fn full_iri ->
      local = extract_local_name(full_iri) |> String.downcase()
      local == prop_local
    end)
  end

  defp check_inverse_rule_dead(name_str, schema_info) do
    inverse_props = schema_info[:inverse_properties] || []

    if Enum.empty?(inverse_props) do
      true
    else
      # Extract property names from rule name like "prp_inv1_inv1_prop1_prop2"
      # Check if the pair exists
      parts = String.split(name_str, "_")

      case Enum.drop(parts, 3) do
        [p1, p2 | _] ->
          not Enum.any?(inverse_props, fn {full_p1, full_p2} ->
            local_p1 = extract_local_name(full_p1) |> String.downcase()
            local_p2 = extract_local_name(full_p2) |> String.downcase()

            (local_p1 == String.downcase(p1) and local_p2 == String.downcase(p2)) or
              (local_p1 == String.downcase(p2) and local_p2 == String.downcase(p1))
          end)

        _ ->
          false
      end
    end
  end
end
