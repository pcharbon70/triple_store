defmodule TripleStore.Reasoner.Rule do
  @moduledoc """
  Represents a reasoning rule for OWL 2 RL forward-chaining inference.

  A rule consists of:
  - A unique name identifying the rule (e.g., :cax_sco, :prp_trp)
  - A body containing patterns and conditions that must match
  - A head specifying the triple(s) to derive when the body matches

  ## Rule Structure

  Rules follow a Datalog-style representation:

      head :- body_pattern_1, body_pattern_2, ..., condition_1, condition_2, ...

  For example, the subClassOf transitivity rule (scm-sco):

      (?c1 rdfs:subClassOf ?c3) :- (?c1 rdfs:subClassOf ?c2), (?c2 rdfs:subClassOf ?c3)

  Is represented as:

      %Rule{
        name: :scm_sco,
        body: [
          {:pattern, [{:var, "c1"}, {:iri, @rdfs_subClassOf}, {:var, "c2"}]},
          {:pattern, [{:var, "c2"}, {:iri, @rdfs_subClassOf}, {:var, "c3"}]}
        ],
        head: {:pattern, [{:var, "c1"}, {:iri, @rdfs_subClassOf}, {:var, "c3"}]}
      }

  ## Variable Binding

  Variables are shared across patterns via matching names. When evaluating a rule:
  1. Find all bindings that satisfy the first body pattern
  2. For each binding, filter to those that also satisfy subsequent patterns
  3. For surviving bindings, instantiate the head pattern to produce derived triples

  ## Conditions

  Beyond triple patterns, rule bodies can include literal conditions:
  - `{:not_equal, term1, term2}` - Terms must not be equal
  - `{:is_iri, term}` - Term must be an IRI (not blank node or literal)
  - `{:is_blank, term}` - Term must be a blank node
  - `{:is_literal, term}` - Term must be a literal

  These conditions filter bindings without querying the database.
  """

  @enforce_keys [:name, :body, :head]
  defstruct [:name, :body, :head, :description, :profile]

  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"
  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "A variable reference in a pattern"
  @type variable :: {:var, String.t()}

  @typedoc "An IRI constant in a pattern"
  @type iri_term :: {:iri, String.t()}

  @typedoc "A literal constant in a pattern"
  @type literal_term ::
          {:literal, :simple, String.t()}
          | {:literal, :typed, String.t(), String.t()}
          | {:literal, :lang, String.t(), String.t()}

  @typedoc "A term in a rule pattern (variable or constant)"
  @type rule_term :: variable() | iri_term() | literal_term()

  @typedoc "A triple pattern in a rule body or head"
  @type pattern :: {:pattern, [rule_term()]}

  @typedoc "A condition that filters bindings without database access"
  @type condition ::
          {:not_equal, rule_term(), rule_term()}
          | {:is_iri, rule_term()}
          | {:is_blank, rule_term()}
          | {:is_literal, rule_term()}
          | {:bound, rule_term()}

  @typedoc "A body element can be a pattern or condition"
  @type body_element :: pattern() | condition()

  @typedoc "The reasoning profile this rule belongs to"
  @type profile :: :rdfs | :owl2rl | :custom

  @typedoc "A complete reasoning rule"
  @type t :: %__MODULE__{
          name: atom(),
          body: [body_element()],
          head: pattern(),
          description: String.t() | nil,
          profile: profile() | nil
        }

  @typedoc "A variable binding map"
  @type binding :: %{String.t() => iri_term() | literal_term()}

  # ============================================================================
  # Constructor Functions
  # ============================================================================

  @doc """
  Creates a new rule with the given name, body patterns, and head pattern.

  ## Parameters

  - `name` - Unique atom identifying the rule (e.g., :cax_sco, :prp_trp)
  - `body` - List of patterns and conditions that must be satisfied
  - `head` - Pattern to derive when body is satisfied

  ## Options

  - `:description` - Human-readable description of the rule
  - `:profile` - Reasoning profile (:rdfs, :owl2rl, or :custom)

  ## Examples

      iex> Rule.new(:cax_sco,
      ...>   [{:pattern, [{:var, "x"}, {:iri, rdf_type}, {:var, "c1"}]},
      ...>    {:pattern, [{:var, "c1"}, {:iri, rdfs_subClassOf}, {:var, "c2"}]}],
      ...>   {:pattern, [{:var, "x"}, {:iri, rdf_type}, {:var, "c2"}]},
      ...>   description: "Class membership through subclass"
      ...> )
  """
  @spec new(atom(), [body_element()], pattern(), keyword()) :: t()
  def new(name, body, head, opts \\ []) when is_atom(name) and is_list(body) do
    %__MODULE__{
      name: name,
      body: body,
      head: head,
      description: Keyword.get(opts, :description),
      profile: Keyword.get(opts, :profile)
    }
  end

  # ============================================================================
  # Term Constructors
  # ============================================================================

  @doc """
  Creates a variable term.

  ## Examples

      iex> Rule.var("x")
      {:var, "x"}
  """
  @spec var(String.t()) :: variable()
  def var(name) when is_binary(name), do: {:var, name}

  @doc """
  Creates an IRI term.

  ## Examples

      iex> Rule.iri("http://example.org/Person")
      {:iri, "http://example.org/Person"}
  """
  @spec iri(String.t()) :: iri_term()
  def iri(uri) when is_binary(uri), do: {:iri, uri}

  @doc """
  Creates a simple literal term.

  ## Examples

      iex> Rule.literal("hello")
      {:literal, :simple, "hello"}
  """
  @spec literal(String.t()) :: literal_term()
  def literal(value) when is_binary(value), do: {:literal, :simple, value}

  @doc """
  Creates a typed literal term.

  ## Examples

      iex> Rule.literal("42", "http://www.w3.org/2001/XMLSchema#integer")
      {:literal, :typed, "42", "http://www.w3.org/2001/XMLSchema#integer"}
  """
  @spec literal(String.t(), String.t()) :: literal_term()
  def literal(value, datatype) when is_binary(value) and is_binary(datatype) do
    {:literal, :typed, value, datatype}
  end

  @doc """
  Creates a language-tagged literal term.

  ## Examples

      iex> Rule.lang_literal("hello", "en")
      {:literal, :lang, "hello", "en"}
  """
  @spec lang_literal(String.t(), String.t()) :: literal_term()
  def lang_literal(value, lang) when is_binary(value) and is_binary(lang) do
    {:literal, :lang, value, lang}
  end

  # ============================================================================
  # Pattern Constructors
  # ============================================================================

  @doc """
  Creates a triple pattern from subject, predicate, and object terms.

  ## Examples

      iex> Rule.pattern(Rule.var("x"), Rule.iri("http://example.org/knows"), Rule.var("y"))
      {:pattern, [{:var, "x"}, {:iri, "http://example.org/knows"}, {:var, "y"}]}
  """
  @spec pattern(rule_term(), rule_term(), rule_term()) :: pattern()
  def pattern(subject, predicate, object) do
    {:pattern, [subject, predicate, object]}
  end

  # ============================================================================
  # Condition Constructors
  # ============================================================================

  @doc """
  Creates a not-equal condition.

  Used to filter bindings where two terms must be different.

  ## Examples

      iex> Rule.not_equal(Rule.var("x"), Rule.var("y"))
      {:not_equal, {:var, "x"}, {:var, "y"}}
  """
  @spec not_equal(rule_term(), rule_term()) :: condition()
  def not_equal(term1, term2), do: {:not_equal, term1, term2}

  @doc """
  Creates an is-IRI condition.

  Used to ensure a term is an IRI (not a blank node or literal).

  ## Examples

      iex> Rule.is_iri(Rule.var("x"))
      {:is_iri, {:var, "x"}}
  """
  @spec is_iri(rule_term()) :: condition()
  def is_iri(term), do: {:is_iri, term}

  @doc """
  Creates an is-blank-node condition.

  ## Examples

      iex> Rule.is_blank(Rule.var("x"))
      {:is_blank, {:var, "x"}}
  """
  @spec is_blank(rule_term()) :: condition()
  def is_blank(term), do: {:is_blank, term}

  @doc """
  Creates an is-literal condition.

  ## Examples

      iex> Rule.is_literal(Rule.var("x"))
      {:is_literal, {:var, "x"}}
  """
  @spec is_literal(rule_term()) :: condition()
  def is_literal(term), do: {:is_literal, term}

  @doc """
  Creates a bound condition.

  Used to check if a variable is bound in the current solution.

  ## Examples

      iex> Rule.bound(Rule.var("x"))
      {:bound, {:var, "x"}}
  """
  @spec bound(rule_term()) :: condition()
  def bound(term), do: {:bound, term}

  # ============================================================================
  # Commonly Used IRIs
  # ============================================================================

  @doc "Returns the rdf:type IRI"
  @spec rdf_type() :: iri_term()
  def rdf_type, do: {:iri, @rdf <> "type"}

  @doc "Returns the rdfs:subClassOf IRI"
  @spec rdfs_subClassOf() :: iri_term()
  def rdfs_subClassOf, do: {:iri, @rdfs <> "subClassOf"}

  @doc "Returns the rdfs:subPropertyOf IRI"
  @spec rdfs_subPropertyOf() :: iri_term()
  def rdfs_subPropertyOf, do: {:iri, @rdfs <> "subPropertyOf"}

  @doc "Returns the rdfs:domain IRI"
  @spec rdfs_domain() :: iri_term()
  def rdfs_domain, do: {:iri, @rdfs <> "domain"}

  @doc "Returns the rdfs:range IRI"
  @spec rdfs_range() :: iri_term()
  def rdfs_range, do: {:iri, @rdfs <> "range"}

  @doc "Returns the owl:sameAs IRI"
  @spec owl_sameAs() :: iri_term()
  def owl_sameAs, do: {:iri, @owl <> "sameAs"}

  @doc "Returns the owl:TransitiveProperty IRI"
  @spec owl_TransitiveProperty() :: iri_term()
  def owl_TransitiveProperty, do: {:iri, @owl <> "TransitiveProperty"}

  @doc "Returns the owl:SymmetricProperty IRI"
  @spec owl_SymmetricProperty() :: iri_term()
  def owl_SymmetricProperty, do: {:iri, @owl <> "SymmetricProperty"}

  @doc "Returns the owl:inverseOf IRI"
  @spec owl_inverseOf() :: iri_term()
  def owl_inverseOf, do: {:iri, @owl <> "inverseOf"}

  @doc "Returns the owl:FunctionalProperty IRI"
  @spec owl_FunctionalProperty() :: iri_term()
  def owl_FunctionalProperty, do: {:iri, @owl <> "FunctionalProperty"}

  @doc "Returns the owl:InverseFunctionalProperty IRI"
  @spec owl_InverseFunctionalProperty() :: iri_term()
  def owl_InverseFunctionalProperty, do: {:iri, @owl <> "InverseFunctionalProperty"}

  @doc "Returns the owl:hasValue IRI"
  @spec owl_hasValue() :: iri_term()
  def owl_hasValue, do: {:iri, @owl <> "hasValue"}

  @doc "Returns the owl:onProperty IRI"
  @spec owl_onProperty() :: iri_term()
  def owl_onProperty, do: {:iri, @owl <> "onProperty"}

  @doc "Returns the owl:someValuesFrom IRI"
  @spec owl_someValuesFrom() :: iri_term()
  def owl_someValuesFrom, do: {:iri, @owl <> "someValuesFrom"}

  @doc "Returns the owl:allValuesFrom IRI"
  @spec owl_allValuesFrom() :: iri_term()
  def owl_allValuesFrom, do: {:iri, @owl <> "allValuesFrom"}

  # ============================================================================
  # Analysis Functions
  # ============================================================================

  @doc """
  Extracts all variables from a rule (body and head).

  ## Examples

      iex> rule = Rule.new(:test,
      ...>   [{:pattern, [{:var, "x"}, {:iri, "p"}, {:var, "y"}]}],
      ...>   {:pattern, [{:var, "x"}, {:iri, "q"}, {:var, "y"}]}
      ...> )
      iex> Rule.variables(rule)
      MapSet.new(["x", "y"])
  """
  @spec variables(t()) :: MapSet.t(String.t())
  def variables(%__MODULE__{body: body, head: head}) do
    body_vars = Enum.flat_map(body, &extract_vars/1)
    head_vars = extract_vars(head)

    MapSet.new(body_vars ++ head_vars)
  end

  @doc """
  Extracts variables from the body only.
  """
  @spec body_variables(t()) :: MapSet.t(String.t())
  def body_variables(%__MODULE__{body: body}) do
    body
    |> Enum.flat_map(&extract_vars/1)
    |> MapSet.new()
  end

  @doc """
  Extracts variables from the head only.
  """
  @spec head_variables(t()) :: MapSet.t(String.t())
  def head_variables(%__MODULE__{head: head}) do
    head
    |> extract_vars()
    |> MapSet.new()
  end

  @doc """
  Returns the number of patterns in the rule body.
  """
  @spec pattern_count(t()) :: non_neg_integer()
  def pattern_count(%__MODULE__{body: body}) do
    Enum.count(body, fn
      {:pattern, _} -> true
      _ -> false
    end)
  end

  @doc """
  Returns the number of conditions in the rule body.
  """
  @spec condition_count(t()) :: non_neg_integer()
  def condition_count(%__MODULE__{body: body}) do
    Enum.count(body, fn
      {:pattern, _} -> false
      _ -> true
    end)
  end

  @doc """
  Checks if all head variables appear in the body.

  A safe rule has all head variables also in the body, ensuring
  all derived triples are fully grounded.
  """
  @spec safe?(t()) :: boolean()
  def safe?(%__MODULE__{} = rule) do
    head_vars = head_variables(rule)
    body_vars = body_variables(rule)

    MapSet.subset?(head_vars, body_vars)
  end

  @doc """
  Returns patterns from the body only (excluding conditions).
  """
  @spec body_patterns(t()) :: [pattern()]
  def body_patterns(%__MODULE__{body: body}) do
    Enum.filter(body, fn
      {:pattern, _} -> true
      _ -> false
    end)
  end

  @doc """
  Returns conditions from the body only (excluding patterns).
  """
  @spec body_conditions(t()) :: [condition()]
  def body_conditions(%__MODULE__{body: body}) do
    Enum.filter(body, fn
      {:pattern, _} -> false
      _ -> true
    end)
  end

  # ============================================================================
  # Binding Operations
  # ============================================================================

  @doc """
  Applies a binding to a term, substituting variables with their bound values.

  ## Examples

      iex> binding = %{"x" => {:iri, "http://example.org/alice"}}
      iex> Rule.substitute({:var, "x"}, binding)
      {:iri, "http://example.org/alice"}

      iex> Rule.substitute({:iri, "http://example.org/knows"}, %{})
      {:iri, "http://example.org/knows"}
  """
  @spec substitute(rule_term(), binding()) :: rule_term()
  def substitute({:var, name}, binding) do
    Map.get(binding, name, {:var, name})
  end

  def substitute(constant, _binding), do: constant

  @doc """
  Applies a binding to a pattern, substituting all variables.

  ## Examples

      iex> binding = %{"x" => {:iri, "http://example.org/alice"}, "y" => {:iri, "http://example.org/bob"}}
      iex> pattern = {:pattern, [{:var, "x"}, {:iri, "http://example.org/knows"}, {:var, "y"}]}
      iex> Rule.substitute_pattern(pattern, binding)
      {:pattern, [{:iri, "http://example.org/alice"}, {:iri, "http://example.org/knows"}, {:iri, "http://example.org/bob"}]}
  """
  @spec substitute_pattern(pattern(), binding()) :: pattern()
  def substitute_pattern({:pattern, terms}, binding) do
    {:pattern, Enum.map(terms, &substitute(&1, binding))}
  end

  @doc """
  Checks if a pattern is fully ground (no variables).

  ## Examples

      iex> Rule.ground?({:pattern, [{:iri, "s"}, {:iri, "p"}, {:iri, "o"}]})
      true

      iex> Rule.ground?({:pattern, [{:var, "x"}, {:iri, "p"}, {:iri, "o"}]})
      false
  """
  @spec ground?(pattern()) :: boolean()
  def ground?({:pattern, terms}) do
    Enum.all?(terms, fn
      {:var, _} -> false
      _ -> true
    end)
  end

  @doc """
  Evaluates a condition against a binding.

  Returns true if the condition is satisfied, false otherwise.

  ## Examples

      iex> binding = %{"x" => {:iri, "a"}, "y" => {:iri, "b"}}
      iex> Rule.evaluate_condition({:not_equal, {:var, "x"}, {:var, "y"}}, binding)
      true

      iex> binding = %{"x" => {:iri, "a"}, "y" => {:iri, "a"}}
      iex> Rule.evaluate_condition({:not_equal, {:var, "x"}, {:var, "y"}}, binding)
      false
  """
  @spec evaluate_condition(condition(), binding()) :: boolean()
  def evaluate_condition({:not_equal, term1, term2}, binding) do
    substitute(term1, binding) != substitute(term2, binding)
  end

  def evaluate_condition({:is_iri, term}, binding) do
    case substitute(term, binding) do
      {:iri, _} -> true
      _ -> false
    end
  end

  def evaluate_condition({:is_blank, term}, binding) do
    case substitute(term, binding) do
      {:blank_node, _} -> true
      _ -> false
    end
  end

  def evaluate_condition({:is_literal, term}, binding) do
    case substitute(term, binding) do
      {:literal, _, _} -> true
      {:literal, _, _, _} -> true
      _ -> false
    end
  end

  def evaluate_condition({:bound, {:var, name}}, binding) do
    Map.has_key?(binding, name)
  end

  def evaluate_condition({:bound, _}, _binding), do: true

  @doc """
  Evaluates all conditions in a rule body against a binding.

  Returns true if all conditions are satisfied.
  """
  @spec evaluate_conditions(t(), binding()) :: boolean()
  def evaluate_conditions(%__MODULE__{} = rule, binding) do
    rule
    |> body_conditions()
    |> Enum.all?(&evaluate_condition(&1, binding))
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp extract_vars({:pattern, terms}) do
    Enum.flat_map(terms, fn
      {:var, name} -> [name]
      _ -> []
    end)
  end

  defp extract_vars({:not_equal, term1, term2}) do
    extract_term_vars(term1) ++ extract_term_vars(term2)
  end

  defp extract_vars({:is_iri, term}), do: extract_term_vars(term)
  defp extract_vars({:is_blank, term}), do: extract_term_vars(term)
  defp extract_vars({:is_literal, term}), do: extract_term_vars(term)
  defp extract_vars({:bound, term}), do: extract_term_vars(term)

  defp extract_term_vars({:var, name}), do: [name]
  defp extract_term_vars(_), do: []
end
