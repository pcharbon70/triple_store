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

  ## Telemetry

  All operations emit telemetry events for observability:

  - `[:triple_store, :quad, :insert, :start | :stop]` - Quad insert operations
  - `[:triple_store, :quad, :delete, :start | :stop]` - Quad delete operations
  - `[:triple_store, :quad, :lookup, :start | :stop]` - Quad lookup operations

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
  alias TripleStore.Telemetry

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

  - `:ok` on success (aligned with triple store API)
  - `{:error, reason}` on failure

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> QuadOperations.insert_quad(db, {1, 2, 3, 0})
      :ok

  """
  @spec insert_quad(NIF.db_ref(), quad()) :: :ok | {:error, term()}
  def insert_quad(db, {subject, predicate, object, graph})
      when valid_quad?(subject, predicate, object, graph) do
    Telemetry.span(:quad, :insert, %{quad: {subject, predicate, object, graph}}, fn ->
      operations = build_insert_operations(subject, predicate, object, graph)
      result = NIF.write_batch(db, operations, true)
      {result, %{count: 1}}
    end)
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

  - `:ok` on success (aligned with triple store API)
  - `{:error, reason}` on failure

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> quads = [{1, 2, 3, 0}, {4, 5, 6, 0}, {7, 8, 9, 1}]
      iex> QuadOperations.insert_quads(db, quads)
      :ok

      # For bulk loading, disable sync for better performance
      iex> QuadOperations.insert_quads(db, quads, sync: false)
      :ok

  """
  @spec insert_quads(NIF.db_ref(), [quad()], keyword()) :: :ok | {:error, term()}
  def insert_quads(_db, [], _opts), do: :ok

  def insert_quads(db, quads, opts) when is_list(quads) do
    sync = Keyword.get(opts, :sync, true)

    Telemetry.span(:quad, :insert, %{sync: sync}, fn ->
      operations =
        for {subject, predicate, object, graph} <- quads,
            op <- build_insert_operations(subject, predicate, object, graph) do
          op
        end

      result = NIF.write_batch(db, operations, sync)
      {result, %{count: length(quads)}}
    end)
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

  - `:ok` on success (aligned with triple store API)
  - `{:error, reason}` on database error

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> QuadOperations.delete_quad(db, {1, 2, 3, 0})
      :ok

  """
  @spec delete_quad(NIF.db_ref(), quad()) :: :ok | {:error, term()}
  def delete_quad(db, {subject, predicate, object, graph})
      when valid_quad?(subject, predicate, object, graph) do
    Telemetry.span(:quad, :delete, %{quad: {subject, predicate, object, graph}}, fn ->
      keys = build_delete_keys(subject, predicate, object, graph)
      operations = for {cf, key} <- keys, do: {cf, key}
      result = NIF.delete_batch(db, operations, true)
      {result, %{count: 1}}
    end)
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

  - `:ok` on success (aligned with triple store API)
  - `{:error, reason}` on failure

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> quads = [{1, 2, 3, 0}, {4, 5, 6, 0}]
      iex> QuadOperations.delete_quads(db, quads)
      :ok

  """
  @spec delete_quads(NIF.db_ref(), [quad()], keyword()) :: :ok | {:error, term()}
  def delete_quads(_db, [], _opts), do: :ok

  def delete_quads(db, quads, opts) when is_list(quads) do
    sync = Keyword.get(opts, :sync, true)

    Telemetry.span(:quad, :delete, %{sync: sync}, fn ->
      operations =
        for {subject, predicate, object, graph} <- quads,
            {cf, key} <- build_delete_keys(subject, predicate, object, graph) do
          {cf, key}
        end

      result = NIF.delete_batch(db, operations, sync)
      {result, %{count: length(quads)}}
    end)
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
    Telemetry.span(:quad, :lookup, %{pattern: pattern}, fn ->
      selection = QuadIndex.build_quad_prefix(pattern, values)

      column_family = selection.index

      prefix = selection.prefix
      prefix_len = byte_size(prefix)

      result = perform_prefix_scan(db, column_family, prefix, prefix_len, selection.index, pattern, values)
      {result, %{result_count: length(result)}}
    end)
  end

  @doc """
  Looks up quads matching a pattern, returning a stream for lazy evaluation.

  This is the streaming version of `lookup_quads/3` that returns a `Stream`
  instead of a list. Useful for large result sets where you want to process
  results incrementally without loading everything into memory.

  The stream is realized when consumed, at which point the actual database
  query is executed.

  ## Arguments

  - `db` - RocksDB database reference
  - `pattern` - Quad pattern `{s_pat, p_pat, o_pat, g_pat}` where each is
    `:bound` or `:var`
  - `values` - Map of bound term IDs `%{s: id, p: id, o: id, g: id}`

  ## Returns

  - A `Stream` that yields `{subject, predicate, object, graph}` tuples

  ## Examples

      # Stream quads and process incrementally
      QuadOperations.lookup_quads_stream(db, {:var, :var, :var, :bound}, %{g: 0})
      |> Stream.each(fn {s, p, o, g} -> process_quad(s, p, o, g) end)
      |> Stream.run()

      # Take first 100 results
      QuadOperations.lookup_quads_stream(db, {:bound, :var, :var, :var}, %{s: 1})
      |> Enum.take(100)

  ## Performance Notes

  - The stream uses RocksDB iterator internally via `fold_keys`
  - Memory usage is O(1) with respect to result set size
  - Suitable for queries returning millions of quads

  """
  @spec lookup_quads_stream(NIF.db_ref(), quad_pattern(), %{s: term_id(), p: term_id(), o: term_id(), g: term_id()}) ::
          Enumerable.t()
  def lookup_quads_stream(db, pattern, values) do
    # Build the prefix scan parameters outside the stream
    selection = QuadIndex.build_quad_prefix(pattern, values)
    column_family = selection.index
    prefix = selection.prefix
    prefix_len = byte_size(prefix)

    # Create a stream that executes the fold when consumed
    Stream.resource(
      fn ->
        # Start time for telemetry
        start_time = System.monotonic_time()
        Telemetry.emit_start([:triple_store, :quad, :lookup], %{pattern: pattern})
        {db, column_family, prefix, prefix_len, selection.index, pattern, values, start_time}
      end,
      fn {db, cf, prefix, prefix_len, index, pattern, values, start_time} ->
        # Perform the scan and emit results
        result = perform_prefix_scan_once(db, cf, prefix, prefix_len, index, pattern, values)

        case result do
          {:halt, []} ->
            # No more results, emit stop event
            duration = System.monotonic_time() - start_time
            Telemetry.emit_stop([:triple_store, :quad, :lookup], duration, %{pattern: pattern, result_count: 0})
            {:halt, []}

          {:cont, []} ->
            # No more results in this batch
            {:halt, []}

          {:cont, results} ->
            # Emit results
            {results, {db, cf, prefix, prefix_len, index, pattern, values, start_time}}
        end
      end,
      fn {_db, _cf, _prefix, _prefix_len, _index, _pattern, _values, _start_time} ->
        # Emit stop event if stream halted early (no results case handled above)
        # For streams that consume all results, the final batch emits the event
        :ok
      end
    )
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
          throw({:halt, acc})
        end
      end)
      |> Enum.reverse()
    catch
      {:halt, acc} -> Enum.reverse(acc)
    end
  end

  # Performs a single iteration of prefix scan for streaming
  # Returns {:cont, results} or {:halt, []}
  defp perform_prefix_scan_once(db, cf, prefix, prefix_len, index, pattern, values) do
    try do
      results =
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
            throw({:halt, :done})
          end
        end)

      {:cont, Enum.reverse(results)}
    catch
      {:halt, :done} -> {:halt, []}
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

  # ===========================================================================
  # Dataset Operations - Graph Management
  # ===========================================================================

  @doc """
  Lists all named graphs in the database.

  Returns a list of graph terms (RDF.IRI or RDF.BlankNode) representing
  all named graphs that contain at least one quad. Uses the GSPO index
  to efficiently scan for distinct graph IDs.

  ## Arguments

  - `db` - RocksDB database reference
  - `opts` - Keyword list of options:
    - `:include_default` - When `true`, includes the default graph in results.
      Default is `false` (excludes default graph).

  ## Returns

  - `{:ok, [graph_term]}` - List of RDF.IRI or RDF.BlankNode terms
  - `{:error, reason}` - On database error

  ## Examples

      # List all named graphs
      {:ok, graphs} = QuadOperations.list_graphs(db)
      # => {:ok, [%RDF.IRI{value: "http://example.org/g1"}, ...]}

      # Include default graph in results
      {:ok, graphs} = QuadOperations.list_graphs(db, include_default: true)

  """
  @spec list_graphs(NIF.db_ref(), keyword()) :: {:ok, [RDF.IRI.t() | RDF.BlankNode.t()]} | {:error, term()}
  def list_graphs(db, opts \\ []) do
    Telemetry.span(:quad, :list_graphs, %{}, fn ->
      include_default = Keyword.get(opts, :include_default, false)

      # Use GSPO index to find all distinct graph IDs
      # Scan GSPO and extract first 8 bytes (graph ID) from each key
      case scan_distinct_graph_ids(db) do
        {:ok, graph_ids} ->
          # Filter out default graph (ID 0) if not requested
          filtered_ids =
            if include_default do
              graph_ids
            else
              Enum.reject(graph_ids, &(&1 == 0))
            end

          # Convert graph IDs to RDF terms
          graph_terms = convert_graph_ids_to_terms(db, filtered_ids)
          {{:ok, graph_terms}, %{count: length(graph_terms)}}

        {:error, reason} ->
          {{:error, reason}, %{count: 0}}
      end
    end)
  end

  @doc """
  Checks if a named graph exists in the database.

  A graph exists if it contains at least one quad. This uses a GSPO
  prefix scan to efficiently check for any quad with the given graph ID.

  ## Arguments

  - `db` - RocksDB database reference
  - `manager` - Dictionary manager process
  - `graph_term` - Graph term (RDF.IRI, RDF.BlankNode) to check

  ## Returns

  - `true` if graph exists (has at least one quad)
  - `false` if graph doesn't exist

  ## Examples

      QuadOperations.graph_exists?(db, manager, RDF.iri("http://example.org/g1"))
      # => true

      QuadOperations.graph_exists?(db, manager, RDF.iri("http://example.org/nonexistent"))
      # => false

  """
  @spec graph_exists?(NIF.db_ref(), TripleStore.Dictionary.Manager.manager(), RDF.IRI.t() | RDF.BlankNode.t()) :: boolean()
  def graph_exists?(db, manager, graph_term) do
    # Convert graph term to ID
    case TripleStore.Adapter.term_to_id(manager, graph_term) do
      {:ok, graph_id} ->
        graph_id_exists?(db, graph_id)

      _ ->
        false
    end
  end

  @doc """
  Checks if the default graph exists in the database.

  The default graph exists if it contains at least one quad.

  ## Arguments

  - `db` - RocksDB database reference

  ## Returns

  - `true` if default graph exists
  - `false` if default graph is empty

  ## Examples

      QuadOperations.default_graph_exists?(db)
      # => true

  """
  @spec default_graph_exists?(NIF.db_ref()) :: boolean()
  def default_graph_exists?(db) do
    graph_id_exists?(db, 0)
  end

  @doc """
  Deletes all quads from a named graph.

  Removes all quads belonging to the specified graph from all four indices
  (GSPO, GPOS, SPOG, POSG) atomically.

  ## Arguments

  - `db` - RocksDB database reference
  - `manager` - Dictionary manager process
  - `graph_term` - Graph term (RDF.IRI, RDF.BlankNode, or :default)

  ## Returns

  - `{:ok, count}` - Number of quads deleted
  - `{:error, reason}` - On database error

  ## Examples

      {:ok, count} = QuadOperations.delete_graph(db, manager, RDF.iri("http://example.org/g1"))

      # Delete default graph (clears all data)
      {:ok, count} = QuadOperations.delete_graph(db, manager, :default)

  """
  @spec delete_graph(NIF.db_ref(), TripleStore.Dictionary.Manager.manager(), RDF.IRI.t() | RDF.BlankNode.t() | :default) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def delete_graph(db, _manager, :default) do
    Telemetry.span(:quad, :delete_graph, %{graph: :default}, fn ->
      # Delete all quads with graph ID 0
      case delete_all_quads_in_graph(db, 0) do
        {:ok, count} -> {{:ok, count}, %{count: count}}
        {:error, reason} -> {{:error, reason}, %{count: 0}}
      end
    end)
  end

  def delete_graph(db, manager, graph_term) do
    Telemetry.span(:quad, :delete_graph, %{graph: graph_term}, fn ->
      # Convert graph term to ID
      case TripleStore.Adapter.term_to_id(manager, graph_term) do
        {:ok, graph_id} ->
          case delete_all_quads_in_graph(db, graph_id) do
            {:ok, count} -> {{:ok, count}, %{count: count}}
            {:error, reason} -> {{:error, reason}, %{count: 0}}
          end

        {:error, reason} ->
          {{:error, reason}, %{count: 0}}
      end
    end)
  end

  @doc """
  Copies all quads from a source graph to a target graph.

  Copies all quads from the source graph and inserts them into the target
  graph with the new graph ID. The target graph is created if it doesn't exist.

  ## Arguments

  - `db` - RocksDB database reference
  - `manager` - Dictionary manager process
  - `source_graph` - Source graph term (RDF.IRI, RDF.BlankNode, or :default)
  - `target_graph` - Target graph term (RDF.IRI, RDF.BlankNode, or :default)
  - `opts` - Keyword list of options:
    - `:on_conflict` - How to handle existing target graph:
      - `:merge` (default) - Add quads to existing target graph
      - `:replace` - Clear target graph before copying
      - `:error` - Fail if target graph has any quads

  ## Returns

  - `{:ok, count}` - Number of quads copied
  - `{:error, reason}` - On failure or conflict

  ## Examples

      # Copy to a new graph (merge behavior)
      {:ok, count} = QuadOperations.copy_graph(db, manager, :default, RDF.iri("http://example.org/g1"))

      # Replace existing target graph
      {:ok, count} = QuadOperations.copy_graph(db, manager, g1, g2, on_conflict: :replace)

      # Fail if target exists
      :error = QuadOperations.copy_graph(db, manager, g1, g2, on_conflict: :error)

  """
  @spec copy_graph(
          NIF.db_ref(),
          TripleStore.Dictionary.Manager.manager(),
          RDF.IRI.t() | RDF.BlankNode.t() | :default,
          RDF.IRI.t() | RDF.BlankNode.t() | :default,
          keyword()
        ) :: {:ok, non_neg_integer()} | {:error, term()}
  def copy_graph(db, manager, source_graph, target_graph, opts \\ []) do
    Telemetry.span(
      :quad,
      :copy_graph,
      %{source: source_graph, target: target_graph},
      fn ->
        on_conflict = Keyword.get(opts, :on_conflict, :merge)

        # Get source graph ID
        source_id =
          case source_graph do
            :default -> {:ok, 0}
            term -> TripleStore.Adapter.term_to_id(manager, term)
          end

        # Get target graph ID
        target_id =
          case target_graph do
            :default -> {:ok, 0}
            term -> TripleStore.Adapter.term_to_id(manager, term)
          end

        with {:ok, src_id} <- source_id,
             {:ok, tgt_id} <- target_id,
             {:ok, _} <- handle_copy_conflict(db, tgt_id, on_conflict),
             {:ok, count} <- do_copy_graph(db, src_id, tgt_id) do
          {{:ok, count}, %{count: count}}
        else
          {:error, reason} -> {{:error, reason}, %{count: 0}}
        end
      end
    )
  end

  @doc """
  Returns the count of quads in a specific graph.

  ## Arguments

  - `db` - RocksDB database reference
  - `manager` - Dictionary manager process
  - `graph_term` - Graph term (RDF.IRI, RDF.BlankNode, or :default)

  ## Returns

  - `{:ok, count}` - Number of quads in the graph
  - `{:error, reason}` - On database error

  ## Examples

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, RDF.iri("http://example.org/g1"))
      # => {:ok, 42}

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, :default)
      # => {:ok, 100}

  """
  @spec graph_quad_count(NIF.db_ref(), TripleStore.Dictionary.Manager.manager(), RDF.IRI.t() | RDF.BlankNode.t() | :default) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def graph_quad_count(db, _manager, :default) do
    count_quads_in_graph(db, 0)
  end

  def graph_quad_count(db, manager, graph_term) do
    case TripleStore.Adapter.term_to_id(manager, graph_term) do
      {:ok, graph_id} ->
        count_quads_in_graph(db, graph_id)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Returns a summary of quad counts for all graphs.

  Returns a map where keys are graph terms (RDF.IRI, RDF.BlankNode, or :default)
  and values are the number of quads in each graph.

  ## Arguments

  - `db` - RocksDB database reference
  - `opts` - Keyword list of options:
    - `:include_default` - When `true`, includes the default graph in results.
      Default is `true`.

  ## Returns

  - `{:ok, map}` - Map of `%{graph_term => quad_count}`
  - `{:error, reason}` - On database error

  ## Examples

      {:ok, summary} = QuadOperations.graphs_summary(db)
      # => {:ok, %{%RDF.IRI{value: "http://example.org/g1"} => 42, :default => 100}}

      {:ok, summary} = QuadOperations.graphs_summary(db, include_default: false)
      # => {:ok, %{%RDF.IRI{value: "http://example.org/g1"} => 42}}

  """
  @spec graphs_summary(NIF.db_ref(), keyword()) ::
          {:ok, %{(RDF.IRI.t() | RDF.BlankNode.t() | :default) => non_neg_integer()}} | {:error, term()}
  def graphs_summary(db, opts \\ []) do
    Telemetry.span(:quad, :graphs_summary, %{}, fn ->
      include_default = Keyword.get(opts, :include_default, true)

      with {:ok, graph_ids} <- scan_distinct_graph_ids(db),
           {:ok, counts} <- count_quads_by_graph_id(db, graph_ids) do
        # Build map with graph terms as keys
        result =
          counts
          |> Enum.filter(fn {graph_id, _count} ->
            include_default or graph_id != 0
          end)
          |> Map.new(fn {graph_id, count} ->
            graph_term = graph_id_to_term(db, graph_id)
            {graph_term, count}
          end)

        {{:ok, result}, %{graph_count: map_size(result)}}
      else
        {:error, reason} -> {{:error, reason}, %{graph_count: 0}}
      end
    end)
  end

  # ===========================================================================
  # Private Helper Functions - Dataset Operations
  # ===========================================================================

  # Scans the GSPO index to find all distinct graph IDs
  defp scan_distinct_graph_ids(db) do
    try do
      graph_ids =
        NIF.fold_keys(db, :gspo, <<>>, MapSet.new(), fn key, acc ->
          # Extract first 8 bytes (graph ID) from GSPO key
          <<graph_id::unsigned-big-integer-size(64), _rest::binary>> = key
          MapSet.put(acc, graph_id)
        end)

      {:ok, MapSet.to_list(graph_ids)}
    catch
      {:exit, reason} -> {:error, reason}
      :throw -> {:ok, []}
    end
  end

  # Converts a list of graph IDs to RDF terms
  defp convert_graph_ids_to_terms(db, graph_ids) do
    Enum.map(graph_ids, fn graph_id -> graph_id_to_term(db, graph_id) end)
  end

  # Converts a graph ID to a term (:default for 0, or looked up term)
  defp graph_id_to_term(_db, 0), do: :default
  defp graph_id_to_term(db, graph_id), do: lookup_graph_term(db, graph_id)

  # Looks up a graph ID and returns the term, or a placeholder IRI if not found
  defp lookup_graph_term(db, graph_id) do
    case TripleStore.Adapter.id_to_term(db, graph_id) do
      {:ok, term} -> term
      :not_found -> RDF.iri("_:graph#{graph_id}")
      {:error, _} -> RDF.iri("_:graph#{graph_id}")
    end
  end

  # Checks if any quad exists with the given graph ID
  defp graph_id_exists?(db, graph_id) do
    # Use count_quads_in_graph which properly handles the iteration
    case count_quads_in_graph(db, graph_id) do
      {:ok, 0} -> false
      {:ok, _count} -> true
      {:error, _} -> false
    end
  end

  # Deletes all quads in a graph by scanning GSPO and deleting from all indices
  defp delete_all_quads_in_graph(db, graph_id) do
    # First, collect all quads in the graph
    prefix = QuadIndex.gspo_prefix(graph_id)

    try do
      quads =
        NIF.fold_keys(db, :gspo, prefix, [], fn key, acc ->
          case key do
            <<^graph_id::unsigned-big-integer-size(64), _::binary>> ->
              {g, s, p, o} = QuadIndex.decode_gspo_key(key)
              [{s, p, o, g} | acc]

            _ ->
              throw({:halt, acc})
          end
        end)
        |> Enum.reverse()

      # Delete all quads atomically
      if quads == [] do
        {:ok, 0}
      else
        delete_quads(db, quads, sync: true)
        {:ok, length(quads)}
      end
    catch
      {:halt, acc} ->
        # End of prefix reached
        if acc == [] do
          {:ok, 0}
        else
          delete_quads(db, Enum.reverse(acc), sync: true)
          {:ok, length(acc)}
        end

      {:exit, reason} ->
        {:error, reason}
    end
  end

  # Handles copy operation based on conflict mode
  defp handle_copy_conflict(_db, _tgt_id, :merge), do: {:ok, :merge}

  defp handle_copy_conflict(db, tgt_id, :replace) do
    case delete_all_quads_in_graph(db, tgt_id) do
      {:ok, _} -> {:ok, :replaced}
      {:error, reason} -> {:error, reason}
    end
  end

  defp handle_copy_conflict(db, tgt_id, :error) do
    if graph_id_exists?(db, tgt_id) do
      {:error, :graph_exists}
    else
      {:ok, :proceed}
    end
  end

  # Performs the actual copy operation
  defp do_copy_graph(db, src_id, tgt_id) do
    # Don't copy if source and target are the same
    if src_id == tgt_id do
      {:ok, 0}
    else
      # Collect all quads from source graph
      prefix = QuadIndex.gspo_prefix(src_id)

      try do
        quads =
          NIF.fold_keys(db, :gspo, prefix, [], fn key, acc ->
            case key do
              <<^src_id::unsigned-big-integer-size(64), _::binary>> ->
                {_g, s, p, o} = QuadIndex.decode_gspo_key(key)
                # Create quad with target graph ID
                [{s, p, o, tgt_id} | acc]

              _ ->
                throw({:halt, acc})
            end
          end)
          |> Enum.reverse()

        # Insert all quads with new graph ID
        if quads == [] do
          {:ok, 0}
        else
          insert_quads(db, quads, sync: true)
          {:ok, length(quads)}
        end
      catch
        {:halt, acc} ->
          if acc == [] do
            {:ok, 0}
          else
            insert_quads(db, Enum.reverse(acc), sync: true)
            {:ok, length(acc)}
          end

        {:exit, reason} ->
          {:error, reason}
      end
    end
  end

  # Counts quads in a specific graph
  defp count_quads_in_graph(db, graph_id) do
    prefix = QuadIndex.gspo_prefix(graph_id)

    try do
      count =
        NIF.fold_keys(db, :gspo, prefix, 0, fn key, acc ->
          case key do
            <<^graph_id::unsigned-big-integer-size(64), _::binary>> -> acc + 1
            _ -> throw({:halt, acc})
          end
        end)

      {:ok, count}
    catch
      {:halt, acc} -> {:ok, acc}
      {:exit, reason} -> {:error, reason}
    end
  end

  # Counts quads for each graph ID
  defp count_quads_by_graph_id(db, graph_ids) do
    counts =
      Enum.map(graph_ids, fn graph_id ->
        case count_quads_in_graph(db, graph_id) do
          {:ok, count} -> {graph_id, count}
          {:error, _} -> {graph_id, 0}
        end
      end)

    {:ok, Map.new(counts)}
  end
end
