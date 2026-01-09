defmodule TripleStore.QuadIndex do
  @moduledoc """
  Quad index layer providing O(log n) access for all quad patterns.

  Maintains four indices over dictionary-encoded quads (s, p, o, g):
  - **GSPO** (Graph-Subject-Predicate-Object): Used for graph-scoped lookups
  - **GPOS** (Graph-Predicate-Object-Subject): Used for graph-predicate lookups
  - **SPOG** (Subject-Predicate-Object-Graph): Used for subject-scoped cross-graph lookups
  - **POSG** (Predicate-Object-Subject-Graph): Used for predicate-scoped cross-graph lookups

  ## Key Encoding

  Each index uses 32-byte keys (4 x 64-bit IDs) in big-endian format
  for correct lexicographic ordering:

      gspo_key = <<graph::64-big, subject::64-big, predicate::64-big, object::64-big>>
      gpos_key = <<graph::64-big, predicate::64-big, object::64-big, subject::64-big>>
      spog_key = <<subject::64-big, predicate::64-big, object::64-big, graph::64-big>>
      posg_key = <<predicate::64-big, object::64-big, subject::64-big, graph::64-big>>

  Big-endian encoding ensures that lexicographic ordering of the binary keys
  matches numeric ordering of the IDs, enabling efficient prefix-based range
  scans for pattern matching.

  ## Default Graph

  The default graph is represented by ID `0`, which is reserved and never
  allocated by the dictionary for named graphs. This allows efficient
  filtering of default vs named graph quads.

  ## Canonical Form

  The canonical representation of a quad is `{subject, predicate, object, graph}`,
  where subject, predicate, and object are term IDs from the dictionary, and graph
  is either `0` (default) or a named graph ID.

  ## Usage

  ```elixir
  # Encode a quad for storage
  key = QuadIndex.gspo_key(graph_id, subject_id, predicate_id, object_id)

  # Decode a key back to IDs
  {g, s, p, o} = QuadIndex.decode_gspo_key(key)

  # Build a prefix for pattern matching
  prefix = QuadIndex.gspo_prefix(graph_id)  # Match all quads in this graph
  prefix = QuadIndex.gspo_prefix(graph_id, subject_id)  # Match G-S pairs
  ```

  ## Quad vs Triple Store

  The quad store differs from the triple store in several key ways:

  | Aspect | Triple Store | Quad Store |
  |--------|--------------|------------|
  | Key size | 24 bytes | 32 bytes |
  | Indices | 3 (SPO, POS, OSP) | 4 (GSPO, GPOS, SPOG, POSG) |
  | Default graph | Implicit | Explicit (ID = 0) |
  | Named graphs | Not supported | Supported |

  """

  import Bitwise, only: [<<<: 2]

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Maximum valid term ID (64-bit unsigned integer)
  @max_term_id (1 <<< 64) - 1

  # Default graph ID (reserved, never allocated by dictionary)
  @default_graph_id 0

  # ===========================================================================
  # Guards
  # ===========================================================================

  # Guard for valid term IDs (0 <= id <= max_term_id)
  defguardp valid_term_id?(id) when is_integer(id) and id >= 0 and id <= @max_term_id

  # Guard for valid quad of term IDs
  defguardp valid_quad?(g, s, p, o)
            when valid_term_id?(g) and valid_term_id?(s) and valid_term_id?(p) and
                   valid_term_id?(o)

  # Guard for valid triple of term IDs (for quad_to_triple)
  defguardp valid_triple?(s, p, o)
            when valid_term_id?(s) and valid_term_id?(p) and valid_term_id?(o)

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "64-bit term ID from the dictionary"
  @type term_id :: non_neg_integer()

  @typedoc "32-byte quad index key (4 x 64-bit big-endian IDs)"
  @type quad_index_key :: <<_::256>>

  @typedoc "A quad as a tuple of four term IDs: {subject, predicate, object, graph}"
  @type quad :: {term_id(), term_id(), term_id(), term_id()}

  @typedoc "A triple as a tuple of three term IDs: {subject, predicate, object}"
  @type triple :: {term_id(), term_id(), term_id()}

  @typedoc "Quad index type"
  @type quad_index :: :gspo | :gpos | :spog | :posg

  @typedoc "Map of all four index keys for a quad"
  @type encoded_keys :: %{
          gspo: quad_index_key(),
          gpos: quad_index_key(),
          spog: quad_index_key(),
          posg: quad_index_key()
        }

  # ===========================================================================
  # GSPO Index Key Encoding
  # ===========================================================================

  @doc """
  Encodes graph, subject, predicate, and object IDs into a GSPO index key.

  The key is 32 bytes: graph (8 bytes), subject (8 bytes), predicate (8 bytes),
  object (8 bytes), all in big-endian format for correct lexicographic ordering.

  ## Arguments

  - `graph` - Graph term ID (0 for default graph)
  - `subject` - Subject term ID
  - `predicate` - Predicate term ID
  - `object` - Object term ID

  ## Returns

  32-byte binary key suitable for the GSPO column family.

  ## Examples

      iex> key = QuadIndex.gspo_key(0, 1, 2, 3)
      iex> byte_size(key)
      32

      iex> {g, s, p, o} = QuadIndex.decode_gspo_key(key)
      iex> {g, s, p, o}
      {0, 1, 2, 3}
  """
  @spec gspo_key(term_id(), term_id(), term_id(), term_id()) :: quad_index_key()
  def gspo_key(graph, subject, predicate, object)
      when valid_quad?(graph, subject, predicate, object) do
    <<graph::64-big, subject::64-big, predicate::64-big, object::64-big>>
  end

  @doc """
  Decodes a GSPO index key back into graph, subject, predicate, and object IDs.

  ## Arguments

  - `key` - 32-byte GSPO index key

  ## Returns

  Tuple `{graph, subject, predicate, object}` with the decoded term IDs.

  ## Examples

      iex> key = QuadIndex.gspo_key(1, 100, 200, 300)
      iex> QuadIndex.decode_gspo_key(key)
      {1, 100, 200, 300}
  """
  @spec decode_gspo_key(quad_index_key()) :: quad()
  def decode_gspo_key(<<graph::64-big, subject::64-big, predicate::64-big, object::64-big>>) do
    {graph, subject, predicate, object}
  end

  @doc """
  Creates a prefix for GSPO index scans matching a graph.

  ## Arguments

  - `graph` - Graph term ID to match

  ## Returns

  8-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = QuadIndex.gspo_prefix(0)
      iex> byte_size(prefix)
      8
  """
  @spec gspo_prefix(term_id()) :: binary()
  def gspo_prefix(graph) when valid_term_id?(graph) do
    <<graph::64-big>>
  end

  @doc """
  Creates a prefix for GSPO index scans matching graph and subject.

  ## Arguments

  - `graph` - Graph term ID to match
  - `subject` - Subject term ID to match

  ## Returns

  16-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = QuadIndex.gspo_prefix(0, 42)
      iex> byte_size(prefix)
      16
  """
  @spec gspo_prefix(term_id(), term_id()) :: binary()
  def gspo_prefix(graph, subject)
      when valid_term_id?(graph) and valid_term_id?(subject) do
    <<graph::64-big, subject::64-big>>
  end

  @doc """
  Creates a prefix for GSPO index scans matching graph, subject, and predicate.

  ## Arguments

  - `graph` - Graph term ID to match
  - `subject` - Subject term ID to match
  - `predicate` - Predicate term ID to match

  ## Returns

  24-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = QuadIndex.gspo_prefix(0, 42, 100)
      iex> byte_size(prefix)
      24
  """
  @spec gspo_prefix(term_id(), term_id(), term_id()) :: binary()
  def gspo_prefix(graph, subject, predicate)
      when valid_term_id?(graph) and valid_term_id?(subject) and valid_term_id?(predicate) do
    <<graph::64-big, subject::64-big, predicate::64-big>>
  end

  # ===========================================================================
  # GPOS Index Key Encoding
  # ===========================================================================

  @doc """
  Encodes graph, predicate, object, and subject IDs into a GPOS index key.

  The key is 32 bytes: graph (8 bytes), predicate (8 bytes), object (8 bytes),
  subject (8 bytes), all in big-endian format for correct lexicographic ordering.

  ## Arguments

  - `graph` - Graph term ID (0 for default graph)
  - `predicate` - Predicate term ID
  - `object` - Object term ID
  - `subject` - Subject term ID

  ## Returns

  32-byte binary key suitable for the GPOS column family.

  ## Examples

      iex> key = QuadIndex.gpos_key(0, 2, 3, 1)
      iex> byte_size(key)
      32

      iex> {g, p, o, s} = QuadIndex.decode_gpos_key(key)
      iex> {g, p, o, s}
      {0, 2, 3, 1}
  """
  @spec gpos_key(term_id(), term_id(), term_id(), term_id()) :: quad_index_key()
  def gpos_key(graph, predicate, object, subject)
      when valid_quad?(graph, predicate, object, subject) do
    <<graph::64-big, predicate::64-big, object::64-big, subject::64-big>>
  end

  @doc """
  Decodes a GPOS index key back into graph, predicate, object, and subject IDs.

  ## Arguments

  - `key` - 32-byte GPOS index key

  ## Returns

  Tuple `{graph, predicate, object, subject}` with the decoded term IDs.

  ## Examples

      iex> key = QuadIndex.gpos_key(1, 200, 300, 100)
      iex> QuadIndex.decode_gpos_key(key)
      {1, 200, 300, 100}
  """
  @spec decode_gpos_key(quad_index_key()) :: {term_id(), term_id(), term_id(), term_id()}
  def decode_gpos_key(<<graph::64-big, predicate::64-big, object::64-big, subject::64-big>>) do
    {graph, predicate, object, subject}
  end

  @doc """
  Creates a prefix for GPOS index scans matching a graph.

  ## Arguments

  - `graph` - Graph term ID to match

  ## Returns

  8-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = QuadIndex.gpos_prefix(0)
      iex> byte_size(prefix)
      8
  """
  @spec gpos_prefix(term_id()) :: binary()
  def gpos_prefix(graph) when valid_term_id?(graph) do
    <<graph::64-big>>
  end

  @doc """
  Creates a prefix for GPOS index scans matching graph and predicate.

  ## Arguments

  - `graph` - Graph term ID to match
  - `predicate` - Predicate term ID to match

  ## Returns

  16-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = QuadIndex.gpos_prefix(0, 42)
      iex> byte_size(prefix)
      16
  """
  @spec gpos_prefix(term_id(), term_id()) :: binary()
  def gpos_prefix(graph, predicate)
      when valid_term_id?(graph) and valid_term_id?(predicate) do
    <<graph::64-big, predicate::64-big>>
  end

  @doc """
  Creates a prefix for GPOS index scans matching graph, predicate, and object.

  ## Arguments

  - `graph` - Graph term ID to match
  - `predicate` - Predicate term ID to match
  - `object` - Object term ID to match

  ## Returns

  24-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = QuadIndex.gpos_prefix(0, 42, 100)
      iex> byte_size(prefix)
      24
  """
  @spec gpos_prefix(term_id(), term_id(), term_id()) :: binary()
  def gpos_prefix(graph, predicate, object)
      when valid_term_id?(graph) and valid_term_id?(predicate) and valid_term_id?(object) do
    <<graph::64-big, predicate::64-big, object::64-big>>
  end

  # ===========================================================================
  # SPOG Index Key Encoding
  # ===========================================================================

  @doc """
  Encodes subject, predicate, object, and graph IDs into an SPOG index key.

  The key is 32 bytes: subject (8 bytes), predicate (8 bytes), object (8 bytes),
  graph (8 bytes), all in big-endian format for correct lexicographic ordering.

  ## Arguments

  - `subject` - Subject term ID
  - `predicate` - Predicate term ID
  - `object` - Object term ID
  - `graph` - Graph term ID (0 for default graph)

  ## Returns

  32-byte binary key suitable for the SPOG column family.

  ## Examples

      iex> key = QuadIndex.spog_key(1, 2, 3, 0)
      iex> byte_size(key)
      32

      iex> {s, p, o, g} = QuadIndex.decode_spog_key(key)
      iex> {s, p, o, g}
      {1, 2, 3, 0}
  """
  @spec spog_key(term_id(), term_id(), term_id(), term_id()) :: quad_index_key()
  def spog_key(subject, predicate, object, graph)
      when valid_quad?(subject, predicate, object, graph) do
    <<subject::64-big, predicate::64-big, object::64-big, graph::64-big>>
  end

  @doc """
  Decodes an SPOG index key back into subject, predicate, object, and graph IDs.

  ## Arguments

  - `key` - 32-byte SPOG index key

  ## Returns

  Tuple `{subject, predicate, object, graph}` with the decoded term IDs.

  ## Examples

      iex> key = QuadIndex.spog_key(100, 200, 300, 1)
      iex> QuadIndex.decode_spog_key(key)
      {100, 200, 300, 1}
  """
  @spec decode_spog_key(quad_index_key()) :: quad()
  def decode_spog_key(<<subject::64-big, predicate::64-big, object::64-big, graph::64-big>>) do
    {subject, predicate, object, graph}
  end

  @doc """
  Creates a prefix for SPOG index scans matching a subject.

  ## Arguments

  - `subject` - Subject term ID to match

  ## Returns

  8-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = QuadIndex.spog_prefix(42)
      iex> byte_size(prefix)
      8
  """
  @spec spog_prefix(term_id()) :: binary()
  def spog_prefix(subject) when valid_term_id?(subject) do
    <<subject::64-big>>
  end

  @doc """
  Creates a prefix for SPOG index scans matching subject and predicate.

  ## Arguments

  - `subject` - Subject term ID to match
  - `predicate` - Predicate term ID to match

  ## Returns

  16-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = QuadIndex.spog_prefix(42, 100)
      iex> byte_size(prefix)
      16
  """
  @spec spog_prefix(term_id(), term_id()) :: binary()
  def spog_prefix(subject, predicate)
      when valid_term_id?(subject) and valid_term_id?(predicate) do
    <<subject::64-big, predicate::64-big>>
  end

  @doc """
  Creates a prefix for SPOG index scans matching subject, predicate, and object.

  ## Arguments

  - `subject` - Subject term ID to match
  - `predicate` - Predicate term ID to match
  - `object` - Object term ID to match

  ## Returns

  24-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = QuadIndex.spog_prefix(42, 100, 200)
      iex> byte_size(prefix)
      24
  """
  @spec spog_prefix(term_id(), term_id(), term_id()) :: binary()
  def spog_prefix(subject, predicate, object)
      when valid_term_id?(subject) and valid_term_id?(predicate) and valid_term_id?(object) do
    <<subject::64-big, predicate::64-big, object::64-big>>
  end

  # ===========================================================================
  # POSG Index Key Encoding
  # ===========================================================================

  @doc """
  Encodes predicate, object, subject, and graph IDs into a POSG index key.

  The key is 32 bytes: predicate (8 bytes), object (8 bytes), subject (8 bytes),
  graph (8 bytes), all in big-endian format for correct lexicographic ordering.

  ## Arguments

  - `predicate` - Predicate term ID
  - `object` - Object term ID
  - `subject` - Subject term ID
  - `graph` - Graph term ID (0 for default graph)

  ## Returns

  32-byte binary key suitable for the POSG column family.

  ## Examples

      iex> key = QuadIndex.posg_key(2, 3, 1, 0)
      iex> byte_size(key)
      32

      iex> {p, o, s, g} = QuadIndex.decode_posg_key(key)
      iex> {p, o, s, g}
      {2, 3, 1, 0}
  """
  @spec posg_key(term_id(), term_id(), term_id(), term_id()) :: quad_index_key()
  def posg_key(predicate, object, subject, graph)
      when valid_quad?(predicate, object, subject, graph) do
    <<predicate::64-big, object::64-big, subject::64-big, graph::64-big>>
  end

  @doc """
  Decodes a POSG index key back into predicate, object, subject, and graph IDs.

  ## Arguments

  - `key` - 32-byte POSG index key

  ## Returns

  Tuple `{predicate, object, subject, graph}` with the decoded term IDs.

  ## Examples

      iex> key = QuadIndex.posg_key(200, 300, 100, 1)
      iex> QuadIndex.decode_posg_key(key)
      {200, 300, 100, 1}
  """
  @spec decode_posg_key(quad_index_key()) :: {term_id(), term_id(), term_id(), term_id()}
  def decode_posg_key(<<predicate::64-big, object::64-big, subject::64-big, graph::64-big>>) do
    {predicate, object, subject, graph}
  end

  @doc """
  Creates a prefix for POSG index scans matching a predicate.

  ## Arguments

  - `predicate` - Predicate term ID to match

  ## Returns

  8-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = QuadIndex.posg_prefix(42)
      iex> byte_size(prefix)
      8
  """
  @spec posg_prefix(term_id()) :: binary()
  def posg_prefix(predicate) when valid_term_id?(predicate) do
    <<predicate::64-big>>
  end

  @doc """
  Creates a prefix for POSG index scans matching predicate and object.

  ## Arguments

  - `predicate` - Predicate term ID to match
  - `object` - Object term ID to match

  ## Returns

  16-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = QuadIndex.posg_prefix(42, 100)
      iex> byte_size(prefix)
      16
  """
  @spec posg_prefix(term_id(), term_id()) :: binary()
  def posg_prefix(predicate, object)
      when valid_term_id?(predicate) and valid_term_id?(object) do
    <<predicate::64-big, object::64-big>>
  end

  @doc """
  Creates a prefix for POSG index scans matching predicate, object, and subject.

  ## Arguments

  - `predicate` - Predicate term ID to match
  - `object` - Object term ID to match
  - `subject` - Subject term ID to match

  ## Returns

  24-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = QuadIndex.posg_prefix(42, 100, 200)
      iex> byte_size(prefix)
      24
  """
  @spec posg_prefix(term_id(), term_id(), term_id()) :: binary()
  def posg_prefix(predicate, object, subject)
      when valid_term_id?(predicate) and valid_term_id?(object) and valid_term_id?(subject) do
    <<predicate::64-big, object::64-big, subject::64-big>>
  end

  # ===========================================================================
  # Utility Functions
  # ===========================================================================

  @doc """
  Encodes a quad for all four indices.

  Returns a map of index names to encoded keys, ready for batch write.
  The value for index entries is always empty (the key contains all info).

  ## Arguments

  - `subject` - Subject term ID
  - `predicate` - Predicate term ID
  - `object` - Object term ID
  - `graph` - Graph term ID (0 for default graph)

  ## Returns

  Map with keys `:gspo`, `:gpos`, `:spog`, `:posg` mapping to 32-byte index keys.

  ## Examples

      iex> keys = QuadIndex.encode_quad_keys(1, 2, 3, 0)
      iex> map_size(keys)
      4
      iex> byte_size(keys.gspo)
      32
  """
  @spec encode_quad_keys(term_id(), term_id(), term_id(), term_id()) :: encoded_keys()
  def encode_quad_keys(subject, predicate, object, graph)
      when valid_quad?(subject, predicate, object, graph) do
    %{
      gspo: gspo_key(graph, subject, predicate, object),
      gpos: gpos_key(graph, predicate, object, subject),
      spog: spog_key(subject, predicate, object, graph),
      posg: posg_key(predicate, object, subject, graph)
    }
  end

  @doc """
  Converts any quad index key back to a canonical `{s, p, o, g}` quad.

  The canonical form is always `{subject, predicate, object, graph}` regardless
  of which index the key came from.

  ## Arguments

  - `index` - Which index the key is from (`:gspo`, `:gpos`, `:spog`, or `:posg`)
  - `key` - 32-byte quad index key

  ## Returns

  Tuple `{subject, predicate, object, graph}` in canonical order.

  ## Examples

      iex> key = QuadIndex.gspo_key(1, 2, 3, 4)
      iex> QuadIndex.key_to_quad(:gspo, key)
      {2, 3, 4, 1}

      iex> key = QuadIndex.gpos_key(1, 3, 4, 2)
      iex> QuadIndex.key_to_quad(:gpos, key)
      {2, 3, 4, 1}
  """
  @spec key_to_quad(quad_index(), quad_index_key()) :: quad()
  def key_to_quad(:gspo, key) do
    {g, s, p, o} = decode_gspo_key(key)
    {s, p, o, g}
  end

  def key_to_quad(:gpos, key) do
    {g, p, o, s} = decode_gpos_key(key)
    {s, p, o, g}
  end

  def key_to_quad(:spog, key), do: decode_spog_key(key)

  def key_to_quad(:posg, key) do
    {p, o, s, g} = decode_posg_key(key)
    {s, p, o, g}
  end

  @doc """
  Extracts the triple portion from a quad.

  This is a compatibility function for working with triple-only code.

  ## Arguments

  - `quad` - A quad `{subject, predicate, object, graph}`

  ## Returns

  A triple `{subject, predicate, object}`.

  ## Examples

      iex> QuadIndex.quad_to_triple({1, 2, 3, 0})
      {1, 2, 3}
  """
  @spec quad_to_triple(quad()) :: triple()
  def quad_to_triple({subject, predicate, object, _graph})
      when valid_triple?(subject, predicate, object) do
    {subject, predicate, object}
  end

  @doc """
  Determines which quad index a 32-byte key belongs to.

  This function inspects a key and attempts to determine which index it
  was encoded for. Note: This is a best-effort detection based on heuristics
  and may not be 100% accurate for all possible ID combinations.

  Since all four indices have the same structure (4 x 64-bit integers),
  we cannot definitively determine the index just from the key structure.
  The caller should track which index a key came from when performing lookups.

  ## Arguments

  - `_key` - 32-byte binary key

  ## Returns

  - `{:error, :unknown_index}` - Index cannot be determined from key alone

  ## Examples

      iex> key = QuadIndex.gspo_key(0, 1, 2, 3)
      iex> QuadIndex.index_for_key(key)
      {:error, :unknown_index}
  """
  @spec index_for_key(quad_index_key()) :: {:error, :unknown_index}
  def index_for_key(<<_::256>>) do
    # Since all four indices have the same structure (4 x 64-bit integers),
    # we cannot definitively determine the index just from the key structure.
    # The caller should track which index a key came from when performing lookups.
    {:error, :unknown_index}
  end

  @doc """
  Returns the default graph ID.

  ## Examples

      iex> QuadIndex.default_graph_id()
      0
  """
  @spec default_graph_id() :: term_id()
  def default_graph_id, do: @default_graph_id

  @doc """
  Checks if a graph ID is the default graph.

  ## Arguments

  - `graph_id` - Graph term ID to check

  ## Returns

  `true` if the ID is the default graph (0), `false` otherwise.

  ## Examples

      iex> QuadIndex.is_default_graph?(0)
      true

      iex> QuadIndex.is_default_graph?(1)
      false
  """
  @spec is_default_graph?(term_id()) :: boolean()
  def is_default_graph?(graph_id) when is_integer(graph_id), do: graph_id == @default_graph_id

  # ===========================================================================
  # Graph ID Resolution Functions (Section 1.3.3)
  # ===========================================================================

  @doc """
  Resolves a graph reference to its term ID.

  Handles both named graphs and the default graph:
  - `:default` → Returns 0 (default graph ID)
  - RDF.IRI.t() → Returns the term ID for the graph IRI (lookup only)
  - RDF.BlankNode.t() → Returns the term ID for the graph blank node (lookup only)

  ## Arguments

  - `graph_ref` - Graph reference: `:default` atom or RDF term (IRI or BlankNode)
  - `db` - Database reference for dictionary lookup (required for named graphs)

  ## Returns

  - `{:ok, graph_id}` - The resolved graph ID
  - `:not_found` - If the named graph is not in the dictionary
  - `{:error, reason}` - If resolution fails

  ## Examples

      # Default graph
      iex> QuadIndex.resolve_graph_id(:default, db)
      {:ok, 0}

      # Named graph (lookup only - does not create new entries)
      # For lookups that might fail, use pattern matching:
      # case QuadIndex.resolve_graph_id(graph_iri, db) do
      #   {:ok, graph_id} -> ...
      #   :not_found -> ... # Graph not in dictionary
      # end
  """
  @spec resolve_graph_id(:default | RDF.IRI.t() | RDF.BlankNode.t(), term()) ::
          {:ok, term_id()} | :not_found | {:error, term()}
  def resolve_graph_id(:default, _db), do: {:ok, @default_graph_id}

  def resolve_graph_id(%RDF.IRI{} = iri, db) do
    TripleStore.Dictionary.StringToId.lookup_id(db, iri)
  end

  def resolve_graph_id(%RDF.BlankNode{} = bnode, db) do
    TripleStore.Dictionary.StringToId.lookup_id(db, bnode)
  end

  def resolve_graph_id(graph_term, _db) do
    {:error, {:invalid_graph_reference, graph_term}}
  end

  @doc """
  Gets or creates a term ID for a named graph.

  This function looks up or creates a dictionary entry for the graph term.
  Graph terms are encoded as regular RDF terms (IRIs or blank nodes).

  ## Note

  This function requires a Dictionary Manager process (GenServer), not a
  raw database reference. For read-only lookups, use `resolve_graph_id/2`
  with a database reference instead.

  ## Arguments

  - `graph_term` - RDF term for the graph (IRI or BlankNode)
  - `manager` - Dictionary Manager process reference

  ## Returns

  - `{:ok, graph_id}` - The term ID for the graph
  - `{:error, reason}` - If the operation fails

  ## Examples

      iex> graph = RDF.iri("http://example.org/mygraph")
      iex> {:ok, graph_id} = QuadIndex.get_or_create_graph_id(graph, manager)
  """
  @spec get_or_create_graph_id(RDF.IRI.t() | RDF.BlankNode.t(), GenServer.server()) ::
          {:ok, term_id()} | {:error, term()}
  def get_or_create_graph_id(%RDF.IRI{} = iri, manager) do
    TripleStore.Dictionary.Manager.get_or_create_id(manager, iri)
  end

  def get_or_create_graph_id(%RDF.BlankNode{} = bnode, manager) do
    TripleStore.Dictionary.Manager.get_or_create_id(manager, bnode)
  end

  def get_or_create_graph_id(graph_term, _manager) do
    {:error, {:invalid_graph_term, graph_term}}
  end

  @doc """
  Converts a graph ID back to its RDF term representation.

  This function looks up the graph ID in the dictionary and returns
  the corresponding RDF term.

  ## Arguments

  - `graph_id` - Graph term ID to look up
  - `db` - Database reference for dictionary lookup

  ## Returns

  - `{:ok, rdf_term}` - The RDF term (IRI or BlankNode)
  - `:not_found` - If the graph ID is not in the dictionary
  - `{:error, reason}` - On database error

  ## Note

  For the default graph (ID 0), this function returns `:not_found`.
  Callers should use `is_default_graph?(graph_id)` to check for the
  default graph before calling this function.

  ## Examples

      iex> # Check for default graph first
      iex> if QuadIndex.is_default_graph?(graph_id) do
      ...>   :default_graph
      ...> else
      ...>   case QuadIndex.id_to_graph_term(graph_id, db) do
      ...>     {:ok, term} -> term
      ...>     :not_found -> :unknown_graph
      ...>   end
      ...> end
  """
  @spec id_to_graph_term(term_id(), term()) ::
          {:ok, RDF.IRI.t() | RDF.BlankNode.t()} | :not_found | {:error, term()}
  def id_to_graph_term(0, _db) do
    # Default graph is a special case - it has no RDF term representation
    # Callers should use is_default_graph?(0) to check for default graph
    :not_found
  end

  def id_to_graph_term(graph_id, db) when is_integer(graph_id) and graph_id > 0 do
    TripleStore.Dictionary.IdToString.lookup_term(db, graph_id)
  end
end
