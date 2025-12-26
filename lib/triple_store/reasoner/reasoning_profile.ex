defmodule TripleStore.Reasoner.ReasoningProfile do
  @moduledoc """
  Defines reasoning profiles for OWL 2 RL inference.

  A reasoning profile determines which inference rules are active during
  materialization. Different profiles provide different levels of inference
  at varying computational costs.

  ## Built-in Profiles

  ### `:rdfs` - RDFS Semantics
  Basic RDFS reasoning including:
  - `rdfs:subClassOf` transitivity (scm-sco)
  - `rdfs:subPropertyOf` transitivity (scm-spo)
  - Class membership through subclass (cax-sco)
  - Property inheritance through subproperty (prp-spo1)
  - Property domain inference (prp-dom)
  - Property range inference (prp-rng)

  This is the fastest profile, suitable for simple ontologies without
  OWL-specific constructs.

  ### `:owl2rl` - OWL 2 RL Full
  Complete OWL 2 RL reasoning including all RDFS rules plus:
  - Transitive properties (prp-trp)
  - Symmetric properties (prp-symp)
  - Inverse properties (prp-inv1, prp-inv2)
  - Functional/inverse functional properties (prp-fp, prp-ifp)
  - owl:sameAs reasoning (eq-sym, eq-trans, eq-rep-*)
  - Class restrictions (cls-hv*, cls-svf*, cls-avf)

  This is the most complete profile but may be slower for large datasets.

  ### `:custom` - User-Selected Rules
  A custom set of rules specified by the user. Allows fine-grained control
  over which inference rules are active.

  ## Usage

      # Use RDFS profile
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Use OWL 2 RL profile
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      # Use custom profile with specific rules
      {:ok, rules} = ReasoningProfile.rules_for(:custom, rules: [:scm_sco, :cax_sco, :prp_trp])

      # Get profile info
      info = ReasoningProfile.info(:rdfs)

  ## Profile Selection Guidelines

  - Use `:rdfs` for datasets with only class/property hierarchies
  - Use `:owl2rl` for datasets with OWL property characteristics or sameAs
  - Use `:custom` when you need specific rules for performance optimization
  """

  alias TripleStore.Reasoner.{Rules, Rule}

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "Built-in reasoning profile names"
  @type profile_name :: :rdfs | :owl2rl | :custom | :none

  @typedoc "Profile options for custom profiles"
  @type profile_opts :: [
          rules: [atom()],
          exclude: [atom()]
        ]

  @typedoc "Profile information"
  @type profile_info :: %{
          name: profile_name(),
          description: String.t(),
          rule_count: non_neg_integer(),
          rule_names: [atom()],
          categories: [atom()]
        }

  # ============================================================================
  # Profile Definitions
  # ============================================================================

  @rdfs_rules [:scm_sco, :scm_spo, :cax_sco, :prp_spo1, :prp_dom, :prp_rng]

  @owl2rl_property_rules [:prp_trp, :prp_symp, :prp_inv1, :prp_inv2, :prp_fp, :prp_ifp]

  @owl2rl_equality_rules [:eq_ref, :eq_sym, :eq_trans, :eq_rep_s, :eq_rep_p, :eq_rep_o]

  @owl2rl_restriction_rules [:cls_hv1, :cls_hv2, :cls_svf1, :cls_svf2, :cls_avf]

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Returns the list of valid profile names.
  """
  @spec profile_names() :: [profile_name()]
  def profile_names, do: [:rdfs, :owl2rl, :custom, :none]

  @doc """
  Validates a profile name.

  ## Examples

      iex> ReasoningProfile.valid_profile?(:rdfs)
      true

      iex> ReasoningProfile.valid_profile?(:unknown)
      false
  """
  @spec valid_profile?(atom()) :: boolean()
  def valid_profile?(name), do: name in profile_names()

  @doc """
  Returns the rules for a given profile.

  ## Parameters

  - `profile` - The profile name (`:rdfs`, `:owl2rl`, `:custom`, or `:none`)
  - `opts` - Options for custom profiles:
    - `:rules` - List of rule names to include (for `:custom`)
    - `:exclude` - List of rule names to exclude (for any profile)

  ## Returns

  - `{:ok, rules}` - List of Rule structs
  - `{:error, reason}` - If profile is invalid or rules not found

  ## Examples

      # RDFS profile
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # OWL 2 RL profile
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      # Custom profile with specific rules
      {:ok, rules} = ReasoningProfile.rules_for(:custom, rules: [:scm_sco, :prp_trp])

      # OWL 2 RL minus equality rules
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl, exclude: [:eq_ref, :eq_sym, :eq_trans])

      # No inference (empty rule set)
      {:ok, []} = ReasoningProfile.rules_for(:none)
  """
  @spec rules_for(profile_name(), profile_opts()) :: {:ok, [Rule.t()]} | {:error, term()}
  def rules_for(profile, opts \\ [])

  def rules_for(:none, _opts), do: {:ok, []}

  def rules_for(:rdfs, opts) do
    rules = Rules.rdfs_rules()
    {:ok, apply_exclusions(rules, opts)}
  end

  def rules_for(:owl2rl, opts) do
    rules = Rules.all_rules()
    {:ok, apply_exclusions(rules, opts)}
  end

  def rules_for(:custom, opts) do
    case Keyword.get(opts, :rules) do
      nil ->
        {:error, {:missing_option, :rules, "custom profile requires :rules option"}}

      rule_names when is_list(rule_names) ->
        rules = get_rules_by_name(rule_names)
        missing = rule_names -- Enum.map(rules, & &1.name)

        if Enum.empty?(missing) do
          {:ok, apply_exclusions(rules, opts)}
        else
          {:error, {:unknown_rules, missing}}
        end

      invalid ->
        {:error, {:invalid_option, :rules, "expected list, got: #{inspect(invalid)}"}}
    end
  end

  def rules_for(profile, _opts) do
    {:error, {:invalid_profile, profile, "expected one of: #{inspect(profile_names())}"}}
  end

  @doc """
  Returns rules for a profile, raising on error.

  ## Examples

      rules = ReasoningProfile.rules_for!(:rdfs)
  """
  @spec rules_for!(profile_name(), profile_opts()) :: [Rule.t()]
  def rules_for!(profile, opts \\ []) do
    case rules_for(profile, opts) do
      {:ok, rules} -> rules
      {:error, reason} -> raise ArgumentError, "Invalid profile: #{inspect(reason)}"
    end
  end

  @doc """
  Returns information about a profile.

  ## Examples

      info = ReasoningProfile.info(:rdfs)
      info.rule_count  # => 6
  """
  @spec info(profile_name()) :: profile_info()
  def info(:none) do
    %{
      name: :none,
      description: "No inference - empty rule set",
      rule_count: 0,
      rule_names: [],
      categories: []
    }
  end

  def info(:rdfs) do
    %{
      name: :rdfs,
      description: "RDFS semantics - subclass/subproperty hierarchies and domain/range",
      rule_count: length(@rdfs_rules),
      rule_names: @rdfs_rules,
      categories: [:schema_hierarchy, :domain_range]
    }
  end

  def info(:owl2rl) do
    all_rules = @rdfs_rules ++ @owl2rl_property_rules ++ @owl2rl_equality_rules ++ @owl2rl_restriction_rules

    %{
      name: :owl2rl,
      description: "OWL 2 RL full - complete OWL 2 RL rule set",
      rule_count: length(all_rules),
      rule_names: all_rules,
      categories: [:schema_hierarchy, :domain_range, :property_characteristics, :equality, :restrictions]
    }
  end

  def info(:custom) do
    %{
      name: :custom,
      description: "Custom profile - user-selected rules",
      rule_count: :variable,
      rule_names: :user_defined,
      categories: [:user_defined]
    }
  end

  @doc """
  Returns a list of all available rule names.
  """
  @spec available_rules() :: [atom()]
  def available_rules do
    Rules.rule_names()
  end

  @doc """
  Returns rule names grouped by category.

  ## Categories

  - `:rdfs` - Basic RDFS rules
  - `:property_characteristics` - Transitive, symmetric, inverse, functional properties
  - `:equality` - owl:sameAs reasoning
  - `:restrictions` - Class restrictions (hasValue, someValuesFrom, allValuesFrom)
  """
  @spec rules_by_category() :: %{atom() => [atom()]}
  def rules_by_category do
    %{
      rdfs: @rdfs_rules,
      property_characteristics: @owl2rl_property_rules,
      equality: @owl2rl_equality_rules,
      restrictions: @owl2rl_restriction_rules
    }
  end

  @doc """
  Returns rules for a specific category.

  ## Examples

      {:ok, rules} = ReasoningProfile.rules_for_category(:property_characteristics)
  """
  @spec rules_for_category(atom()) :: {:ok, [Rule.t()]} | {:error, term()}
  def rules_for_category(category) do
    case Map.get(rules_by_category(), category) do
      nil ->
        {:error, {:unknown_category, category}}

      rule_names ->
        {:ok, get_rules_by_name(rule_names)}
    end
  end

  @doc """
  Creates a custom profile from categories.

  ## Examples

      # RDFS plus property characteristics
      {:ok, rules} = ReasoningProfile.from_categories([:rdfs, :property_characteristics])
  """
  @spec from_categories([atom()], profile_opts()) :: {:ok, [Rule.t()]} | {:error, term()}
  def from_categories(categories, opts \\ []) do
    category_map = rules_by_category()
    unknown = Enum.reject(categories, &Map.has_key?(category_map, &1))

    if Enum.empty?(unknown) do
      rule_names =
        categories
        |> Enum.flat_map(&Map.get(category_map, &1, []))
        |> Enum.uniq()

      rules = get_rules_by_name(rule_names)
      {:ok, apply_exclusions(rules, opts)}
    else
      {:error, {:unknown_categories, unknown}}
    end
  end

  @doc """
  Suggests an appropriate profile based on schema information.

  Analyzes the schema to determine which profile provides sufficient
  inference without unnecessary overhead.

  ## Examples

      suggested = ReasoningProfile.suggest_profile(schema_info)
      # => :rdfs or :owl2rl
  """
  @spec suggest_profile(map()) :: profile_name()
  def suggest_profile(schema_info) do
    has_owl_features =
      has_transitive_properties?(schema_info) or
        has_symmetric_properties?(schema_info) or
        has_inverse_properties?(schema_info) or
        has_functional_properties?(schema_info) or
        has_sameas?(schema_info) or
        has_restrictions?(schema_info)

    if has_owl_features, do: :owl2rl, else: :rdfs
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp get_rules_by_name(names) do
    all_rules = Rules.all_rules()

    Enum.filter(all_rules, fn rule ->
      rule.name in names
    end)
  end

  defp apply_exclusions(rules, opts) do
    case Keyword.get(opts, :exclude) do
      nil -> rules
      exclude_names -> Enum.reject(rules, fn r -> r.name in exclude_names end)
    end
  end

  defp has_transitive_properties?(schema_info) do
    props = Map.get(schema_info, :transitive_properties, [])
    is_list(props) and length(props) > 0
  end

  defp has_symmetric_properties?(schema_info) do
    props = Map.get(schema_info, :symmetric_properties, [])
    is_list(props) and length(props) > 0
  end

  defp has_inverse_properties?(schema_info) do
    props = Map.get(schema_info, :inverse_properties, [])
    is_list(props) and length(props) > 0
  end

  defp has_functional_properties?(schema_info) do
    fp = Map.get(schema_info, :functional_properties, [])
    ifp = Map.get(schema_info, :inverse_functional_properties, [])
    (is_list(fp) and length(fp) > 0) or (is_list(ifp) and length(ifp) > 0)
  end

  defp has_sameas?(schema_info) do
    Map.get(schema_info, :has_sameas, false)
  end

  defp has_restrictions?(schema_info) do
    Map.get(schema_info, :has_restrictions, false)
  end
end
