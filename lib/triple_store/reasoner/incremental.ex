defmodule TripleStore.Reasoner.Incremental do
  @moduledoc """
  Incremental maintenance of materialized inferences.

  This module provides functions for adding and removing facts while
  maintaining the correctness of derived inferences. When facts are added,
  new inferences are computed incrementally. When facts are removed, the
  Backward/Forward algorithm ensures correct retraction without over-deletion.

  ## Incremental Addition

  When new facts are added with `add_in_memory/4`, the system:
  1. Filters out facts that already exist
  2. Uses semi-naive evaluation with the new facts as the initial delta
  3. Returns all facts (existing + new explicit + derived)

  This is more efficient than full rematerialization because only the
  consequences of the new facts are computed.

  ## Two APIs

  This module provides two APIs:

  1. **In-Memory API** (`add_in_memory/4`, `preview_in_memory/3`): Works with
     term-based triples (IRI, literal terms) entirely in memory. Suitable for
     testing and small datasets.

  2. **Database API** (`add_with_reasoning/4`, `preview_additions/3`): Works with
     dictionary-encoded ID triples and integrates with the database storage layer.
     Suitable for production use with persistent storage.

  ## Usage

      # In-memory incremental addition
      existing_facts = MapSet.new([...])
      new_triples = [{alice, rdf_type, person}]
      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, existing_facts, rules)

      # Database-backed incremental addition
      {:ok, stats} = Incremental.add_with_reasoning(db, id_triples, rules)

  ## Performance Considerations

  - Incremental addition is O(|new_derivations|) rather than O(|all_derivations|)
  - For small additions to large stores, this is dramatically faster
  - For bulk loading, full materialization may be more efficient

  ## Future: Incremental Deletion

  Deletion with reasoning (Section 4.3.2-4.3.4) uses the Backward/Forward algorithm:
  1. Backward phase: trace all facts that depended on deleted facts
  2. Forward phase: attempt to re-derive each potentially invalid fact
  3. Delete only facts that cannot be re-derived

  This prevents over-deletion when alternative derivation paths exist.
  """

  alias TripleStore.Reasoner.PatternMatcher
  alias TripleStore.Reasoner.Rule
  alias TripleStore.Reasoner.SemiNaive

  # Database-related imports for the database API
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index
  alias TripleStore.Reasoner.DerivedStore

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "A triple as RDF terms (IRI, literal, blank node)"
  @type term_triple :: {Rule.rule_term(), Rule.rule_term(), Rule.rule_term()}

  @typedoc "A set of term-based triples"
  @type fact_set :: MapSet.t(term_triple())

  @typedoc "Database reference"
  @type db_ref :: NIF.db_ref()

  @typedoc "A triple as dictionary-encoded IDs"
  @type id_triple :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @typedoc "Statistics from incremental addition"
  @type add_stats :: %{
          explicit_added: non_neg_integer(),
          derived_count: non_neg_integer(),
          iterations: non_neg_integer(),
          duration_ms: non_neg_integer()
        }

  @typedoc "Options for incremental addition"
  @type add_opts :: [
          parallel: boolean(),
          max_concurrency: pos_integer(),
          max_iterations: non_neg_integer(),
          max_facts: non_neg_integer(),
          emit_telemetry: boolean()
        ]

  # ============================================================================
  # In-Memory API
  # ============================================================================

  @doc """
  Adds facts to an in-memory fact set and derives consequences using reasoning rules.

  This function performs incremental materialization in memory by:
  1. Filtering out facts that already exist in the existing set
  2. Creating a combined fact set with existing + new facts
  3. Running semi-naive evaluation with the new facts as the initial delta
  4. Returning the complete fact set (existing + new + derived)

  ## Parameters

  - `new_triples` - List of triples to add (as term tuples, e.g., `{iri, predicate, object}`)
  - `existing` - MapSet of existing facts
  - `rules` - List of reasoning rules to apply
  - `opts` - Options (see below)

  ## Options

  - `:parallel` - Enable parallel rule evaluation. Default: `false`
  - `:max_concurrency` - Maximum parallel tasks. Default: `System.schedulers_online()`
  - `:max_iterations` - Maximum iterations before stopping. Default: `1000`
  - `:max_facts` - Maximum total facts before stopping. Default: `10_000_000`
  - `:emit_telemetry` - Emit telemetry events. Default: `true`

  ## Returns

  - `{:ok, all_facts, stats}` - All facts (existing + new + derived) and statistics
  - `{:error, reason}` - On failure

  ## Examples

      existing = MapSet.new([
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ])
      new_triples = [{iri("alice"), rdf_type(), iri("Student")}]
      rules = [Rules.cax_sco()]

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)
      # all_facts now contains alice rdf:type Person (derived)
  """
  @spec add_in_memory([term_triple()], fact_set(), [Rule.t()], add_opts()) ::
          {:ok, fact_set(), add_stats()} | {:error, term()}
  def add_in_memory(new_triples, existing, rules, opts \\ [])

  def add_in_memory([], existing, _rules, _opts) do
    {:ok, existing, %{
      explicit_added: 0,
      derived_count: 0,
      iterations: 0,
      duration_ms: 0
    }}
  end

  def add_in_memory(new_triples, existing, rules, opts) when is_list(new_triples) do
    start_time = System.monotonic_time(:millisecond)

    # Build semi-naive options
    semi_naive_opts = [
      parallel: Keyword.get(opts, :parallel, false),
      max_concurrency: Keyword.get(opts, :max_concurrency, System.schedulers_online()),
      max_iterations: Keyword.get(opts, :max_iterations, 1000),
      max_facts: Keyword.get(opts, :max_facts, 10_000_000),
      emit_telemetry: Keyword.get(opts, :emit_telemetry, true)
    ]

    # Filter out triples that already exist
    novel_triples = filter_existing_terms(new_triples, existing)

    # Combine existing facts with new explicit facts
    combined = Enum.reduce(novel_triples, existing, &MapSet.put(&2, &1))

    # Initial delta is just the new facts
    initial_delta = MapSet.new(novel_triples)

    # Create in-memory lookup and store functions
    {:ok, agent} = Agent.start_link(fn -> combined end)

    lookup_fn = fn pattern ->
      facts = Agent.get(agent, & &1)
      {:ok, match_pattern(pattern, facts)}
    end

    store_fn = fn new_facts ->
      Agent.update(agent, fn current -> MapSet.union(current, new_facts) end)
      :ok
    end

    try do
      case SemiNaive.materialize(lookup_fn, store_fn, rules, initial_delta, semi_naive_opts) do
        {:ok, semi_naive_stats} ->
          all_facts = Agent.get(agent, & &1)
          duration_ms = System.monotonic_time(:millisecond) - start_time

          # Calculate truly new derived facts by comparing with combined
          # (existing + novel explicit facts). This accounts for facts that
          # were already in 'existing' but got re-derived by the rules.
          truly_derived_count = MapSet.size(all_facts) - MapSet.size(combined)

          stats = %{
            explicit_added: length(novel_triples),
            derived_count: truly_derived_count,
            iterations: semi_naive_stats.iterations,
            duration_ms: duration_ms
          }

          {:ok, all_facts, stats}

        {:error, _} = error ->
          error
      end
    after
      Agent.stop(agent)
    end
  end

  @doc """
  Previews what would be derived by adding triples without modifying the existing set.

  This is a dry-run version of `add_in_memory/4` that computes what
  would be derived without modifying the existing fact set.

  ## Parameters

  - `new_triples` - List of triples to check
  - `existing` - MapSet of existing facts
  - `rules` - List of reasoning rules

  ## Returns

  - `{:ok, derived_facts}` - MapSet of facts that would be derived (not including input triples)
  - `{:error, reason}` - On failure

  ## Examples

      existing = MapSet.new([{iri("Student"), rdfs_subClassOf(), iri("Person")}])
      new_triples = [{iri("alice"), rdf_type(), iri("Student")}]
      rules = [Rules.cax_sco()]

      {:ok, derived} = Incremental.preview_in_memory(new_triples, existing, rules)
      # derived contains {iri("alice"), rdf_type(), iri("Person")}
  """
  @spec preview_in_memory([term_triple()], fact_set(), [Rule.t()]) ::
          {:ok, fact_set()} | {:error, term()}
  def preview_in_memory(new_triples, existing, rules) do
    # Filter out triples that already exist
    novel_triples = filter_existing_terms(new_triples, existing)
    novel_set = MapSet.new(novel_triples)

    # Combine existing with new for lookup
    combined = MapSet.union(existing, novel_set)

    # Create an in-memory store for derived facts only
    {:ok, derived_agent} = Agent.start_link(fn -> MapSet.new() end)

    lookup_fn = fn pattern ->
      # Include combined (existing + new) plus any derived so far
      derived = Agent.get(derived_agent, & &1)
      all = MapSet.union(combined, derived)
      {:ok, match_pattern(pattern, all)}
    end

    store_fn = fn new_facts ->
      Agent.update(derived_agent, fn current -> MapSet.union(current, new_facts) end)
      :ok
    end

    try do
      case SemiNaive.materialize(lookup_fn, store_fn, rules, novel_set, emit_telemetry: false) do
        {:ok, _stats} ->
          derived = Agent.get(derived_agent, & &1)
          # Remove the input triples - we only want new derivations
          new_derivations = MapSet.difference(derived, novel_set)
          # Also remove anything that was already in existing
          truly_new = MapSet.difference(new_derivations, existing)
          {:ok, truly_new}

        {:error, _} = error ->
          error
      end
    after
      Agent.stop(derived_agent)
    end
  end

  # ============================================================================
  # Database API
  # ============================================================================

  @doc """
  Adds facts to the database and derives consequences using reasoning rules.

  This function performs incremental materialization with persistent storage by:
  1. Inserting the new explicit facts into the database
  2. Creating a lookup function that queries both explicit and derived facts
  3. Running semi-naive evaluation with the new facts as the initial delta
  4. Storing any new derivations in the derived column family

  ## Parameters

  - `db` - Database reference
  - `triples` - List of `{subject_id, predicate_id, object_id}` tuples to add
  - `rules` - List of reasoning rules to apply
  - `opts` - Options (see `add_opts()`)

  ## Options

  - `:source` - Which facts to query during reasoning (`:both` is typical). Default: `:both`
  - `:parallel` - Enable parallel rule evaluation. Default: `false`
  - `:max_concurrency` - Maximum parallel tasks. Default: `System.schedulers_online()`
  - `:max_iterations` - Maximum iterations before stopping. Default: `1000`
  - `:max_facts` - Maximum total facts before stopping. Default: `10_000_000`
  - `:emit_telemetry` - Emit telemetry events. Default: `true`

  ## Returns

  - `{:ok, stats}` - Addition completed with statistics
  - `{:error, reason}` - On failure

  ## Note

  This function requires the rules to work with dictionary-encoded ID triples.
  For typical OWL 2 RL rules that use IRI terms, use the dictionary to convert
  terms to IDs before calling this function.
  """
  @spec add_with_reasoning(db_ref(), [id_triple()], [Rule.t()], add_opts()) ::
          {:ok, add_stats()} | {:error, term()}
  def add_with_reasoning(db, triples, rules, opts \\ [])

  def add_with_reasoning(_db, [], _rules, _opts) do
    {:ok, %{
      explicit_added: 0,
      derived_count: 0,
      iterations: 0,
      duration_ms: 0
    }}
  end

  def add_with_reasoning(db, triples, rules, opts) when is_list(triples) do
    start_time = System.monotonic_time(:millisecond)

    source = Keyword.get(opts, :source, :both)

    semi_naive_opts = [
      parallel: Keyword.get(opts, :parallel, false),
      max_concurrency: Keyword.get(opts, :max_concurrency, System.schedulers_online()),
      max_iterations: Keyword.get(opts, :max_iterations, 1000),
      max_facts: Keyword.get(opts, :max_facts, 10_000_000),
      emit_telemetry: Keyword.get(opts, :emit_telemetry, true)
    ]

    with {:ok, novel_triples} <- filter_existing_db_triples(db, triples),
         :ok <- insert_explicit_facts(db, novel_triples),
         {:ok, stats} <- run_db_reasoning(db, novel_triples, rules, source, semi_naive_opts) do
      duration_ms = System.monotonic_time(:millisecond) - start_time

      {:ok, %{
        explicit_added: length(novel_triples),
        derived_count: stats.total_derived,
        iterations: stats.iterations,
        duration_ms: duration_ms
      }}
    end
  end

  @doc """
  Checks if adding triples would derive any new facts (database version).

  This is a dry-run version that computes what would be derived without
  actually inserting anything into the database.
  """
  @spec preview_additions(db_ref(), [id_triple()], [Rule.t()]) ::
          {:ok, MapSet.t(id_triple())} | {:error, term()}
  def preview_additions(db, triples, rules) do
    prospective_facts = MapSet.new(triples)

    lookup_fn = fn pattern ->
      case DerivedStore.make_lookup_fn(db, :both).(pattern) do
        {:ok, db_facts} -> {:ok, db_facts}
        error -> error
      end
    end

    {:ok, agent} = Agent.start_link(fn -> MapSet.new() end)

    store_fn = fn new_facts ->
      Agent.update(agent, fn existing -> MapSet.union(existing, new_facts) end)
      :ok
    end

    try do
      case SemiNaive.materialize(lookup_fn, store_fn, rules, prospective_facts, emit_telemetry: false) do
        {:ok, _stats} ->
          all_derived = Agent.get(agent, & &1)
          new_derivations = MapSet.difference(all_derived, prospective_facts)
          {:ok, new_derivations}

        {:error, _} = error ->
          error
      end
    after
      Agent.stop(agent)
    end
  end

  # ============================================================================
  # Private Functions - In-Memory
  # ============================================================================

  defp filter_existing_terms(triples, existing) do
    Enum.reject(triples, fn triple -> MapSet.member?(existing, triple) end)
  end

  defp match_pattern(pattern, facts) do
    PatternMatcher.filter_matching(facts, pattern)
  end

  # ============================================================================
  # Private Functions - Database
  # ============================================================================

  defp filter_existing_db_triples(db, triples) do
    novel =
      Enum.filter(triples, fn triple ->
        case triple_is_novel_db?(db, triple) do
          {:ok, true} -> true
          {:ok, false} -> false
          {:error, _} -> false
        end
      end)

    {:ok, novel}
  end

  defp triple_is_novel_db?(db, {s, p, o}) do
    case Index.triple_exists?(db, {s, p, o}) do
      {:ok, true} ->
        {:ok, false}

      {:ok, false} ->
        case DerivedStore.derived_exists?(db, {s, p, o}) do
          {:ok, exists} -> {:ok, not exists}
          error -> error
        end

      error ->
        error
    end
  end

  defp insert_explicit_facts(_db, []), do: :ok
  defp insert_explicit_facts(db, triples), do: Index.insert_triples(db, triples)

  defp run_db_reasoning(_db, [], _rules, _source, _opts) do
    {:ok, %{
      iterations: 0,
      total_derived: 0,
      derivations_per_iteration: [],
      duration_ms: 0,
      rules_applied: 0
    }}
  end

  defp run_db_reasoning(db, new_facts, rules, source, opts) do
    lookup_fn = DerivedStore.make_lookup_fn(db, source)
    store_fn = DerivedStore.make_store_fn(db)
    initial_delta = MapSet.new(new_facts)

    SemiNaive.materialize(lookup_fn, store_fn, rules, initial_delta, opts)
  end
end
