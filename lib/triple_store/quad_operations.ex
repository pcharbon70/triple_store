defmodule TripleStore.QuadOperations do
  @moduledoc """
  Quad insert, delete, and lookup operations for the quad store.

  This module provides CRUD operations for quads (subject, predicate, object, graph)
  across all four quad indices (GSPO, GPOS, SPOG, POSG) using RocksDB WriteBatch
  for atomic multi-index operations.

  ## Quad Indices

  The quad store maintains four indices for efficient pattern matching:

  | Index | Key Ordering | Primary Use Case |
  |-------|-------------|------------------|
  | `gspo` | Graph-Subject-Predicate-Object | Graph-scoped queries |
  | `gpos` | Graph-Predicate-Object-Subject | Graph-predicate queries |
  | `spog` | Subject-Predicate-Object-Graph | Subject-scoped cross-graph queries |
  | `posg` | Predicate-Object-Subject-Graph | Predicate-scoped cross-graph queries |

  ## Default Graph

  The default graph is represented by graph ID `0`, which is reserved and
  never allocated by the dictionary for named graphs.

  ## Atomic Operations

  All insert and delete operations use WriteBatch to ensure atomicity across
  all four indices. Either all indices are updated or none are.

  ## Usage

  ```elixir
  # Insert a single quad
  QuadOperations.insert_quad(db, {1, 2, 3, 0})

  # Check if a quad exists
  QuadOperations.quad_exists?(db, {1, 2, 3, 0})
  # => true

  # Delete a quad
  QuadOperations.delete_quad(db, {1, 2, 3, 0})

  # Pattern-based lookup
  QuadOperations.lookup_quads(db, {:bound, :bound, :var, :bound}, %{s: 1, g: 0})
  # => Stream of {s, p, o, g} tuples
  ```
  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.QuadIndex

  # ===========================================================================
  # Constants
  # ===========================================================================

  @empty_value <<>>

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "64-bit term ID from the dictionary"
  @type term_id :: non_neg_integer()

  @typedoc "A quad as a tuple of four term IDs: {subject, predicate, object, graph}"
  @type quad :: {term_id(), term_id(), term_id(), term_id()}

  @typedoc "Quad pattern: {s_pat, p_pat, o_pat, g_pat} where each is :bound or :var"
  @type quad_pattern :: {:bound | :var, :bound | :var, :bound | :var, :bound | :var}

  # ===========================================================================
  # Guards
  # ===========================================================================

  @spec valid_quad?(term_id(), term_id(), term_id(), term_id()) :: boolean()
  defguardp valid_quad?(s, p, o, g)
            when is_integer(s) and s >= 0 and is_integer(p) and p >= 0 and
                   is_integer(o) and o >= 0 and is_integer(g) and g >= 0

  # ===========================================================================
  # Quad Insert Operations
  # ===========================================================================

  @doc """
  Inserts a single quad into all four indices atomically.

  The quad is written to GSPO, GPOS, SPOG, and POSG indices using a single
  atomic WriteBatch operation. If the quad already exists, this is a no-op
  (idempotent operation).

  **Note**: This function always uses `sync: true` for immediate durability.
  For bulk loading operations where performance is more important than
  per-operation durability, use `insert_quads/3` with `sync: false` instead.

  ## Arguments

  - `db` - RocksDB database reference
  - `quad` - Tuple `{subject_id, predicate_id, object_id, graph_id}` of term IDs

  ## Returns

  - `{:ok, :inserted}` on success
  - `{:error, reason}` on failure

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> QuadOperations.insert_quad(db, {1, 2, 3, 0})
      {:ok, :inserted}

  """
  @spec insert_quad(NIF.db_ref(), quad()) :: {:ok, :inserted} | {:error, term()}
  def insert_quad(db, {subject, predicate, object, graph})
      when valid_quad?(subject, predicate, object, graph) do
    operations = build_insert_operations(subject, predicate, object, graph)

    case NIF.write_batch(db, operations, true) do
      :ok -> {:ok, :inserted}
      {:error, _} = error -> error
    end
  end

  @doc """
  Inserts multiple quads into all four indices atomically.

  All quads are written to GSPO, GPOS, SPOG, and POSG indices using a single
  atomic WriteBatch operation. Either all quads are inserted or none are.
  Duplicate quads are handled idempotently.

  ## Arguments

  - `db` - RocksDB database reference
  - `quads` - List of `{subject_id, predicate_id, object_id, graph_id}` tuples
  - `opts` - Keyword list of options:
    - `:sync` - When `true` (default), forces an fsync after the write.
      When `false`, the write is buffered in the OS. Use `false` for
      bulk loading to improve performance. WAL still provides durability.

  ## Returns

  - `{:ok, count}` where count is the number of quads processed on success
  - `{:error, reason}` on failure

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> quads = [{1, 2, 3, 0}, {4, 5, 6, 0}, {7, 8, 9, 1}]
      iex> QuadOperations.insert_quads(db, quads)
      {:ok, 3}

      # For bulk loading, disable sync for better performance
      iex> QuadOperations.insert_quads(db, quads, sync: false)
      {:ok, 3}

  """
  @spec insert_quads(NIF.db_ref(), [quad()], keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def insert_quads(_db, [], _opts), do: {:ok, 0}

  def insert_quads(db, quads, opts) when is_list(quads) do
    sync = Keyword.get(opts, :sync, true)

    operations =
      for {subject, predicate, object, graph} <- quads,
          op <- build_insert_operations(subject, predicate, object, graph) do
        op
      end

    case NIF.write_batch(db, operations, sync) do
      :ok -> {:ok, length(quads)}
      {:error, _} = error -> error
    end
  end

  # ===========================================================================
  # Quad Delete Operations
  # ===========================================================================

  @doc """
  Deletes a single quad from all four indices atomically.

  The quad is removed from GSPO, GPOS, SPOG, and POSG indices using a single
  atomic WriteBatch operation. If the quad does not exist, this is a no-op
  (idempotent operation).

  ## Arguments

  - `db` - RocksDB database reference
  - `quad` - Tuple `{subject_id, predicate_id, object_id, graph_id}` of term IDs

  ## Returns

  - `{:ok, :deleted}` on success (quad was found and deleted)
  - `{:ok, :not_found}` if the quad does not exist
  - `{:error, reason}` on database error

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> QuadOperations.delete_quad(db, {1, 2, 3, 0})
      {:ok, :deleted}

  """
  @spec delete_quad(NIF.db_ref(), quad()) :: {:ok, :deleted} | {:ok, :not_found} | {:error, term()}
  def delete_quad(db, {subject, predicate, object, graph})
      when valid_quad?(subject, predicate, object, graph) do
    keys = build_delete_keys(subject, predicate, object, graph)

    # Check if quad exists before deleting
    exists = quad_exists_fast?(db, subject, predicate, object, graph)

    if exists do
      operations = for {cf, key} <- keys, do: {cf, key}
      case NIF.delete_batch(db, operations, true) do
        :ok -> {:ok, :deleted}
        {:error, _} = error -> error
      end
    else
      {:ok, :not_found}
    end
  end

  @doc """
  Deletes multiple quads from all four indices atomically.

  All quads are removed from GSPO, GPOS, SPOG, and POSG indices using batch
  delete operations. Quads that don't exist are ignored (idempotent).

  ## Arguments

  - `db` - RocksDB database reference
  - `quads` - List of `{subject_id, predicate_id, object_id, graph_id}` tuples
  - `opts` - Keyword list of options:
    - `:sync` - When `true` (default), forces an fsync after the write.
      When `false`, the write is buffered in the OS.

  ## Returns

  - `{:ok, deleted_count}` where deleted_count is the number of quads actually deleted
  - `{:error, reason}` on failure

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> quads = [{1, 2, 3, 0}, {4, 5, 6, 0}]
      iex> QuadOperations.delete_quads(db, quads)
      {:ok, 2}

  """
  @spec delete_quads(NIF.db_ref(), [quad()], keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def delete_quads(_db, [], _opts), do: {:ok, 0}

  def delete_quads(db, quads, opts) when is_list(quads) do
    sync = Keyword.get(opts, :sync, true)

    # Build delete operations and count which ones exist
    {operations, count} =
      Enum.reduce(quads, {[], 0}, fn {subject, predicate, object, graph}, {ops, acc} ->
        keys = build_delete_keys(subject, predicate, object, graph)

        if quad_exists_fast?(db, subject, predicate, object, graph) do
          new_ops =
            for {cf, key} <- keys do
              {cf, key}
            end

          {ops ++ new_ops, acc + 1}
        else
          {ops, acc}
        end
      end)

    if operations == [] do
      {:ok, 0}
    else
      case NIF.delete_batch(db, operations, sync) do
        :ok -> {:ok, count}
        {:error, _} = error -> error
      end
    end
  end

  # ===========================================================================
  # Quad Existence Check
  # ===========================================================================

  @doc """
  Checks if a quad exists in the database.

  Uses the GSPO index with the full 32-byte key for efficient point lookup.
  Returns `true` if the quad exists, `false` otherwise.

  ## Arguments

  - `db` - RocksDB database reference
  - `quad` - Tuple `{subject_id, predicate_id, object_id, graph_id}` of term IDs

  ## Returns

  - `true` if the quad exists
  - `false` if the quad does not exist
  - `{:error, reason}` on database error

  ## Examples

      iex> QuadOperations.quad_exists?(db, {1, 2, 3, 0})
      true

      iex> QuadOperations.quad_exists?(db, {999, 888, 777, 0})
      false

  """
  @spec quad_exists?(NIF.db_ref(), quad()) :: boolean() | {:error, term()}
  def quad_exists?(db, {subject, predicate, object, graph})
      when valid_quad?(subject, predicate, object, graph) do
    quad_exists_fast?(db, subject, predicate, object, graph)
  end

  # ===========================================================================
  # Quad Lookup
  # ===========================================================================

  @doc """
  Looks up quads matching a pattern.

  Returns a list of quads matching the given pattern. Uses the optimal
  index based on which positions are bound.

  ## Arguments

  - `db` - RocksDB database reference
  - `pattern` - Quad pattern `{s_pat, p_pat, o_pat, g_pat}` where each is
    `:bound` or `:var`
  - `values` - Map of bound term IDs `%{s: id, p: id, o: id, g: id}`

  ## Returns

  - List of `{subject, predicate, object, graph}` tuples

  ## Examples

      # Get all quads in default graph
      QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})

      # Get all quads with subject=1 in any graph
      QuadOperations.lookup_quads(db, {:bound, :var, :var, :var}, %{s: 1})

      # Get all quads matching subject=1, predicate=2 in graph 0
      QuadOperations.lookup_quads(db, {:bound, :bound, :var, :bound}, %{s: 1, p: 2, g: 0})

  """
  @spec lookup_quads(NIF.db_ref(), quad_pattern(), %{s: term_id(), p: term_id(), o: term_id(), g: term_id()}) ::
          [quad()]
  def lookup_quads(db, pattern, values) do
    selection = QuadIndex.build_quad_prefix(pattern, values)

    column_family = selection.index

    prefix = selection.prefix
    prefix_len = byte_size(prefix)

    perform_prefix_scan(db, column_family, prefix, prefix_len, selection.index, pattern, values)
  end

  # Performs prefix scan and returns results as a list
  defp perform_prefix_scan(db, cf, prefix, prefix_len, index, pattern, values) do
    try do
      NIF.fold_keys(db, cf, prefix, [], fn key, acc ->
        # Check if key is within prefix bounds
        if binary_part(key, 0, min(prefix_len, byte_size(key))) == prefix do
          quad = decode_key_to_quad(key, index)

          if apply_post_filter(quad, pattern, values) do
            [quad | acc]
          else
            acc
          end
        else
          # Beyond prefix, stop iteration
          throw(:halt)
        end
      end)
      |> Enum.reverse()
    catch
      :halt -> []
      {:error, _} -> []
    end
  end

  # ===========================================================================
  # Private Helper Functions
  # ===========================================================================

  # Builds insert operations for all four indices
  defp build_insert_operations(s, p, o, g) do
    keys = QuadIndex.encode_quad_keys(s, p, o, g)

    [
      {:gspo, Map.get(keys, :gspo), @empty_value},
      {:gpos, Map.get(keys, :gpos), @empty_value},
      {:spog, Map.get(keys, :spog), @empty_value},
      {:posg, Map.get(keys, :posg), @empty_value}
    ]
  end

  # Builds delete keys for all four indices
  defp build_delete_keys(s, p, o, g) do
    keys = QuadIndex.encode_quad_keys(s, p, o, g)

    [
      {:gspo, Map.get(keys, :gspo)},
      {:gpos, Map.get(keys, :gpos)},
      {:spog, Map.get(keys, :spog)},
      {:posg, Map.get(keys, :posg)}
    ]
  end

  # Fast existence check using GSPO index point lookup
  defp quad_exists_fast?(db, s, p, o, g) do
    key = QuadIndex.gspo_key(g, s, p, o)

    case NIF.get(db, :gspo, key) do
      {:ok, _value} -> true
      :not_found -> false
      {:error, _} -> false
    end
  end

  # Decodes a key from a specific index back to a canonical quad
  defp decode_key_to_quad(key, :gspo) do
    {g, s, p, o} = QuadIndex.decode_gspo_key(key)
    {s, p, o, g}
  end

  defp decode_key_to_quad(key, :gpos) do
    {g, p, o, s} = QuadIndex.decode_gpos_key(key)
    {s, p, o, g}
  end

  defp decode_key_to_quad(key, :spog) do
    {s, p, o, g} = QuadIndex.decode_spog_key(key)
    {s, p, o, g}
  end

  defp decode_key_to_quad(key, :posg) do
    {p, o, s, g} = QuadIndex.decode_posg_key(key)
    {s, p, o, g}
  end

  # Applies post-filtering for patterns that require it
  defp apply_post_filter(quad, pattern, values) do
    QuadIndex.quad_matches_pattern?(quad, pattern, values)
  end
end
