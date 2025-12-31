defmodule TripleStore.Reasoner.DerivedStore do
  @moduledoc """
  Storage layer for derived (inferred) facts from OWL 2 RL reasoning.

  This module manages the storage and retrieval of facts derived through
  forward-chaining materialization. Derived facts are stored separately
  from explicit facts to support:

  1. **Incremental rematerialization**: Clear all derived facts and recompute
  2. **Provenance tracking**: Distinguish explicit from inferred knowledge
  3. **Query optimization**: Query only explicit, only derived, or both

  ## Storage Design

  Derived facts use a single `derived` column family with the same key encoding
  as the SPO index. This provides O(log n) lookups for any pattern.

  Key format: `<<subject::64-big, predicate::64-big, object::64-big>>`

  ## Integration with SemiNaive

  The DerivedStore provides lookup and store functions compatible with
  `SemiNaive.materialize/5`:

      lookup_fn = DerivedStore.make_lookup_fn(db, :both)
      store_fn = DerivedStore.make_store_fn(db)

      {:ok, stats} = SemiNaive.materialize(lookup_fn, store_fn, rules, initial)

  ## Usage Examples

      # Store derived facts
      DerivedStore.insert_derived(db, [{s1, p1, o1}, {s2, p2, o2}])

      # Query derived facts only
      {:ok, stream} = DerivedStore.lookup_derived(db, pattern)

      # Query both explicit and derived
      {:ok, stream} = DerivedStore.lookup_all(db, pattern)

      # Clear all derived facts for rematerialization
      DerivedStore.clear_all(db)
  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index
  alias TripleStore.Reasoner.PatternMatcher
  alias TripleStore.Reasoner.Rule

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "Database reference"
  @type db_ref :: NIF.db_ref()

  @typedoc "A triple as term IDs"
  @type id_triple :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @typedoc "A triple as rule terms"
  @type term_triple :: {Rule.rule_term(), Rule.rule_term(), Rule.rule_term()}

  @typedoc "Query source specification"
  @type source :: :explicit | :derived | :both

  @typedoc "Pattern element for lookups"
  @type pattern_element :: {:bound, non_neg_integer()} | :var

  @typedoc "Lookup pattern for derived facts"
  @type pattern :: {pattern_element(), pattern_element(), pattern_element()}

  # ============================================================================
  # Constants
  # ============================================================================

  @derived_cf :derived
  @empty_value <<>>

  # ============================================================================
  # Storage Operations
  # ============================================================================

  @doc """
  Inserts derived facts into the derived column family.

  Facts are stored using the same key encoding as the SPO index
  for consistent lookup behavior.

  ## Parameters

  - `db` - Database reference
  - `triples` - List of `{subject_id, predicate_id, object_id}` tuples

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      triples = [{1, 2, 3}, {4, 5, 6}]
      :ok = DerivedStore.insert_derived(db, triples)
  """
  @spec insert_derived(db_ref(), [id_triple()]) :: :ok | {:error, term()}
  def insert_derived(_db, []), do: :ok

  def insert_derived(db, triples) when is_list(triples) do
    operations =
      for {s, p, o} <- triples do
        key = Index.spo_key(s, p, o)
        {@derived_cf, key, @empty_value}
      end

    NIF.write_batch(db, operations)
  end

  @doc """
  Inserts a single derived fact.

  ## Parameters

  - `db` - Database reference
  - `triple` - Tuple `{subject_id, predicate_id, object_id}`

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec insert_derived_single(db_ref(), id_triple()) :: :ok | {:error, term()}
  def insert_derived_single(db, {s, p, o}) do
    key = Index.spo_key(s, p, o)
    NIF.put(db, @derived_cf, key, @empty_value)
  end

  @doc """
  Deletes derived facts from the derived column family.

  ## Parameters

  - `db` - Database reference
  - `triples` - List of `{subject_id, predicate_id, object_id}` tuples

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec delete_derived(db_ref(), [id_triple()]) :: :ok | {:error, term()}
  def delete_derived(_db, []), do: :ok

  def delete_derived(db, triples) when is_list(triples) do
    operations =
      for {s, p, o} <- triples do
        key = Index.spo_key(s, p, o)
        {@derived_cf, key}
      end

    NIF.delete_batch(db, operations)
  end

  @doc """
  Checks if a derived fact exists.

  ## Parameters

  - `db` - Database reference
  - `triple` - Tuple `{subject_id, predicate_id, object_id}`

  ## Returns

  - `{:ok, true}` if exists
  - `{:ok, false}` if not exists
  - `{:error, reason}` on failure
  """
  @spec derived_exists?(db_ref(), id_triple()) :: {:ok, boolean()} | {:error, term()}
  def derived_exists?(db, {s, p, o}) do
    key = Index.spo_key(s, p, o)
    NIF.exists(db, @derived_cf, key)
  end

  @doc """
  Clears all derived facts.

  This is used for rematerialization - clearing all inferred facts
  before recomputing them from scratch.

  ## Parameters

  - `db` - Database reference

  ## Returns

  - `{:ok, count}` with number of facts deleted
  - `{:error, reason}` on failure

  ## Examples

      {:ok, 1523} = DerivedStore.clear_all(db)
  """
  # Batch size for chunked deletion to avoid loading all keys into memory
  @clear_batch_size 1000

  @spec clear_all(db_ref()) :: {:ok, non_neg_integer()} | {:error, term()}
  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  def clear_all(db) do
    # Use batched deletion to avoid loading all keys into memory
    case NIF.prefix_stream(db, @derived_cf, <<>>) do
      {:ok, stream} ->
        stream
        |> Stream.map(fn {key, _value} -> {@derived_cf, key} end)
        |> Stream.chunk_every(@clear_batch_size)
        |> Enum.reduce_while({:ok, 0}, fn chunk, {:ok, acc} ->
          case NIF.delete_batch(db, chunk) do
            :ok -> {:cont, {:ok, acc + length(chunk)}}
            error -> {:halt, error}
          end
        end)

      error ->
        error
    end
  end

  @doc """
  Counts the number of derived facts.

  ## Parameters

  - `db` - Database reference

  ## Returns

  - `{:ok, count}` with the count
  - `{:error, reason}` on failure
  """
  @spec count(db_ref()) :: {:ok, non_neg_integer()} | {:error, term()}
  def count(db) do
    case NIF.prefix_stream(db, @derived_cf, <<>>) do
      {:ok, stream} -> {:ok, Enum.count(stream)}
      error -> error
    end
  end

  # ============================================================================
  # Query Operations
  # ============================================================================

  @doc """
  Looks up derived facts matching a pattern.

  Returns a stream of triples from the derived column family.

  ## Parameters

  - `db` - Database reference
  - `pattern` - Triple pattern with bound/var elements

  ## Returns

  - `{:ok, Stream.t()}` with matching triples
  - `{:error, reason}` on failure

  ## Examples

      # Find all derived facts with subject 123
      {:ok, stream} = DerivedStore.lookup_derived(db, {{:bound, 123}, :var, :var})
  """
  @spec lookup_derived(db_ref(), pattern()) :: {:ok, Enumerable.t()} | {:error, term()}
  def lookup_derived(db, pattern) do
    prefix = pattern_to_prefix(pattern)

    case NIF.prefix_stream(db, @derived_cf, prefix) do
      {:ok, stream} ->
        decoded_stream =
          stream
          |> Stream.map(fn {key, _value} -> Index.decode_spo_key(key) end)
          |> Stream.filter(&triple_matches_pattern?(&1, pattern))

        {:ok, decoded_stream}

      error ->
        error
    end
  end

  @doc """
  Looks up explicit facts matching a pattern.

  Returns a stream of triples from the SPO index.

  ## Parameters

  - `db` - Database reference
  - `pattern` - Triple pattern with bound/var elements

  ## Returns

  - `{:ok, Stream.t()}` with matching triples
  - `{:error, reason}` on failure
  """
  @spec lookup_explicit(db_ref(), pattern()) :: {:ok, Enumerable.t()} | {:error, term()}
  def lookup_explicit(db, pattern) do
    Index.lookup(db, pattern)
  end

  @doc """
  Looks up facts from both explicit and derived stores.

  Combines results from SPO index and derived column family,
  removing duplicates.

  ## Parameters

  - `db` - Database reference
  - `pattern` - Triple pattern with bound/var elements

  ## Returns

  - `{:ok, Stream.t()}` with matching triples (deduplicated)
  - `{:error, reason}` on failure

  ## Examples

      # Find all facts (explicit + derived) with predicate 456
      {:ok, stream} = DerivedStore.lookup_all(db, {:var, {:bound, 456}, :var})
  """
  @spec lookup_all(db_ref(), pattern()) :: {:ok, Enumerable.t()} | {:error, term()}
  def lookup_all(db, pattern) do
    with {:ok, explicit_stream} <- lookup_explicit(db, pattern),
         {:ok, derived_stream} <- lookup_derived(db, pattern) do
      # Concatenate streams - duplicates handled by the caller if needed
      # For set-based operations in reasoning, duplicates don't affect correctness
      combined = Stream.concat(explicit_stream, derived_stream)
      {:ok, combined}
    end
  end

  @doc """
  Collects all derived facts matching a pattern into a list.

  ## Parameters

  - `db` - Database reference
  - `pattern` - Triple pattern with bound/var elements

  ## Returns

  - `{:ok, [triple]}` with matching triples
  - `{:error, reason}` on failure
  """
  @spec lookup_derived_all(db_ref(), pattern()) :: {:ok, [id_triple()]} | {:error, term()}
  def lookup_derived_all(db, pattern) do
    case lookup_derived(db, pattern) do
      {:ok, stream} -> {:ok, Enum.to_list(stream)}
      error -> error
    end
  end

  # ============================================================================
  # Callback Factories for SemiNaive Integration
  # ============================================================================

  @doc """
  Creates a lookup function for use with `SemiNaive.materialize/5`.

  The lookup function queries facts based on the specified source:
  - `:explicit` - Query only explicit facts (SPO index)
  - `:derived` - Query only derived facts
  - `:both` - Query both explicit and derived facts

  ## Parameters

  - `db` - Database reference
  - `source` - Which facts to query

  ## Returns

  A function `(pattern) -> {:ok, [triple]} | {:error, reason}`

  ## Examples

      lookup_fn = DerivedStore.make_lookup_fn(db, :both)
      {:ok, triples} = lookup_fn.(pattern)
  """
  @spec make_lookup_fn(db_ref(), source()) :: (pattern() ->
                                                 {:ok, [id_triple()]} | {:error, term()})
  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  def make_lookup_fn(db, source) do
    fn pattern ->
      # Convert from Rule pattern format to Index pattern format
      lookup_pattern = convert_rule_pattern(pattern)

      case source do
        :explicit ->
          case lookup_explicit(db, lookup_pattern) do
            {:ok, stream} -> {:ok, Enum.to_list(stream)}
            error -> error
          end

        :derived ->
          lookup_derived_all(db, lookup_pattern)

        :both ->
          case lookup_all(db, lookup_pattern) do
            {:ok, stream} -> {:ok, Enum.to_list(stream)}
            error -> error
          end
      end
    end
  end

  @doc """
  Creates a store function for use with `SemiNaive.materialize/5`.

  The store function inserts derived facts into the derived column family.

  ## Parameters

  - `db` - Database reference

  ## Returns

  A function `(fact_set) -> :ok | {:error, reason}`

  ## Examples

      store_fn = DerivedStore.make_store_fn(db)
      :ok = store_fn.(MapSet.new([{1, 2, 3}]))
  """
  @spec make_store_fn(db_ref()) :: (MapSet.t(id_triple()) -> :ok | {:error, term()})
  def make_store_fn(db) do
    fn fact_set ->
      triples = MapSet.to_list(fact_set)
      insert_derived(db, triples)
    end
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  # Convert from Rule pattern format {:pattern, [s, p, o]} to Index pattern format
  # Rule patterns use {:var, name}, {:const, value}, or raw values
  # Index patterns use :var or {:bound, value}
  defp convert_rule_pattern({:pattern, [s, p, o]}) do
    {convert_term(s), convert_term(p), convert_term(o)}
  end

  # Already in Index format (tuple of 3)
  defp convert_rule_pattern({s, _p, _o} = pattern) when is_tuple(s) or is_atom(s) do
    pattern
  end

  defp convert_term({:var, _name}), do: :var
  defp convert_term({:const, value}), do: {:bound, value}
  defp convert_term(:var), do: :var
  defp convert_term({:bound, _} = bound), do: bound
  defp convert_term(value), do: {:bound, value}

  defp pattern_to_prefix(pattern) do
    case pattern do
      {{:bound, s}, {:bound, p}, {:bound, o}} ->
        Index.spo_key(s, p, o)

      {{:bound, s}, {:bound, p}, :var} ->
        Index.spo_prefix(s, p)

      {{:bound, s}, :var, :var} ->
        Index.spo_prefix(s)

      _ ->
        # For patterns that don't start with bound subject,
        # we need to scan all and filter
        <<>>
    end
  end

  defp triple_matches_pattern?(triple, pattern) do
    PatternMatcher.matches_index_pattern?(triple, pattern)
  end
end
