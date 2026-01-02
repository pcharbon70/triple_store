defmodule TripleStore.Index do
  @moduledoc """
  Triple index layer providing O(log n) access for all triple patterns.

  Maintains three indices over dictionary-encoded triples:
  - **SPO** (Subject-Predicate-Object): Primary index, used for subject-based lookups
  - **POS** (Predicate-Object-Subject): Used for predicate-based lookups
  - **OSP** (Object-Subject-Predicate): Used for object-based lookups

  ## Key Encoding

  Each index uses 24-byte keys (3 x 64-bit IDs) in big-endian format
  for correct lexicographic ordering:

      spo_key = <<subject::64-big, predicate::64-big, object::64-big>>
      pos_key = <<predicate::64-big, object::64-big, subject::64-big>>
      osp_key = <<object::64-big, subject::64-big, predicate::64-big>>

  Big-endian encoding ensures that lexicographic ordering of the binary keys
  matches numeric ordering of the IDs, enabling efficient prefix-based range
  scans for pattern matching.

  ## Pattern Matching

  Given a triple pattern, the optimal index is selected based on which
  components are bound:

  | Pattern | Index | Operation |
  |---------|-------|-----------|
  | SPO, SP?, S?? | SPO | Prefix scan |
  | ?PO, ?P? | POS | Prefix scan |
  | ??O, S?O | OSP | Prefix scan |
  | ??? | SPO | Full scan |

  ## Usage

  ```elixir
  # Encode a triple for storage
  key = Index.spo_key(subject_id, predicate_id, object_id)

  # Decode a key back to IDs
  {subject_id, predicate_id, object_id} = Index.decode_spo_key(key)

  # Build a prefix for pattern matching
  prefix = Index.spo_prefix(subject_id)  # Match all triples with this subject
  prefix = Index.spo_prefix(subject_id, predicate_id)  # Match S-P pairs
  ```
  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary

  import Bitwise, only: [<<<: 2]

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Maximum valid term ID (64-bit unsigned integer)
  @max_term_id (1 <<< 64) - 1

  # Empty binary value for index entries (key contains all information)
  @empty_value <<>>

  # ===========================================================================
  # Guards
  # ===========================================================================

  # Guard for valid term IDs (0 <= id <= max_term_id)
  defguardp valid_term_id?(id) when is_integer(id) and id >= 0 and id <= @max_term_id

  # Guard for valid triple of term IDs
  defguardp valid_triple?(s, p, o)
            when valid_term_id?(s) and valid_term_id?(p) and valid_term_id?(o)

  # ===========================================================================
  # Option Validation Helpers
  # ===========================================================================

  # Validates and extracts the sync option, coercing to boolean if needed.
  # Defaults to true (synchronous writes) if not specified.
  @spec validate_sync_option(keyword()) :: boolean()
  defp validate_sync_option(opts) do
    case Keyword.get(opts, :sync, true) do
      true -> true
      false -> false
      other ->
        require Logger
        Logger.warning("Invalid :sync option value #{inspect(other)}, using default true")
        true
    end
  end

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "64-bit term ID from the dictionary"
  @type term_id :: Dictionary.term_id()

  @typedoc "24-byte index key (3 x 64-bit big-endian IDs)"
  @type index_key :: <<_::192>>

  @typedoc "A triple as a tuple of three term IDs"
  @type triple :: {term_id(), term_id(), term_id()}

  @typedoc "A bound term in a pattern (has a specific ID)"
  @type bound_element :: {:bound, term_id()}

  @typedoc "A variable/unbound term in a pattern"
  @type var_element :: :var

  @typedoc "A pattern element - either bound to a specific ID or a variable"
  @type pattern_element :: bound_element() | var_element()

  @typedoc "A triple pattern for matching"
  @type pattern :: {pattern_element(), pattern_element(), pattern_element()}

  @typedoc "Index selection result with prefix and optional filter requirement"
  @type index_selection :: %{
          index: :spo | :pos | :osp,
          prefix: binary(),
          needs_filter: boolean(),
          filter_position: nil | :predicate
        }

  # ===========================================================================
  # SPO Index Key Encoding
  # ===========================================================================

  @doc """
  Encodes subject, predicate, and object IDs into an SPO index key.

  The key is 24 bytes: subject (8 bytes), predicate (8 bytes), object (8 bytes),
  all in big-endian format for correct lexicographic ordering.

  ## Arguments

  - `subject` - Subject term ID
  - `predicate` - Predicate term ID
  - `object` - Object term ID

  ## Returns

  24-byte binary key suitable for the SPO column family.

  ## Examples

      iex> key = Index.spo_key(1, 2, 3)
      iex> byte_size(key)
      24

      iex> {s, p, o} = Index.decode_spo_key(key)
      iex> {s, p, o}
      {1, 2, 3}
  """
  @spec spo_key(term_id(), term_id(), term_id()) :: index_key()
  def spo_key(subject, predicate, object) when valid_triple?(subject, predicate, object) do
    <<subject::64-big, predicate::64-big, object::64-big>>
  end

  @doc """
  Decodes an SPO index key back into subject, predicate, and object IDs.

  ## Arguments

  - `key` - 24-byte SPO index key

  ## Returns

  Tuple `{subject, predicate, object}` with the decoded term IDs.

  ## Examples

      iex> key = Index.spo_key(100, 200, 300)
      iex> Index.decode_spo_key(key)
      {100, 200, 300}
  """
  @spec decode_spo_key(index_key()) :: triple()
  def decode_spo_key(<<subject::64-big, predicate::64-big, object::64-big>>) do
    {subject, predicate, object}
  end

  @doc """
  Creates a prefix for SPO index scans matching a subject.

  ## Arguments

  - `subject` - Subject term ID to match

  ## Returns

  8-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = Index.spo_prefix(42)
      iex> byte_size(prefix)
      8
  """
  @spec spo_prefix(term_id()) :: binary()
  def spo_prefix(subject) when valid_term_id?(subject) do
    <<subject::64-big>>
  end

  @doc """
  Creates a prefix for SPO index scans matching subject and predicate.

  ## Arguments

  - `subject` - Subject term ID to match
  - `predicate` - Predicate term ID to match

  ## Returns

  16-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = Index.spo_prefix(42, 100)
      iex> byte_size(prefix)
      16
  """
  @spec spo_prefix(term_id(), term_id()) :: binary()
  def spo_prefix(subject, predicate)
      when valid_term_id?(subject) and valid_term_id?(predicate) do
    <<subject::64-big, predicate::64-big>>
  end

  # ===========================================================================
  # POS Index Key Encoding
  # ===========================================================================

  @doc """
  Encodes predicate, object, and subject IDs into a POS index key.

  The key is 24 bytes: predicate (8 bytes), object (8 bytes), subject (8 bytes),
  all in big-endian format for correct lexicographic ordering.

  ## Arguments

  - `predicate` - Predicate term ID
  - `object` - Object term ID
  - `subject` - Subject term ID

  ## Returns

  24-byte binary key suitable for the POS column family.

  ## Examples

      iex> key = Index.pos_key(2, 3, 1)
      iex> byte_size(key)
      24

      iex> {p, o, s} = Index.decode_pos_key(key)
      iex> {p, o, s}
      {2, 3, 1}
  """
  @spec pos_key(term_id(), term_id(), term_id()) :: index_key()
  def pos_key(predicate, object, subject) when valid_triple?(predicate, object, subject) do
    <<predicate::64-big, object::64-big, subject::64-big>>
  end

  @doc """
  Decodes a POS index key back into predicate, object, and subject IDs.

  ## Arguments

  - `key` - 24-byte POS index key

  ## Returns

  Tuple `{predicate, object, subject}` with the decoded term IDs.

  ## Examples

      iex> key = Index.pos_key(200, 300, 100)
      iex> Index.decode_pos_key(key)
      {200, 300, 100}
  """
  @spec decode_pos_key(index_key()) :: {term_id(), term_id(), term_id()}
  def decode_pos_key(<<predicate::64-big, object::64-big, subject::64-big>>) do
    {predicate, object, subject}
  end

  @doc """
  Creates a prefix for POS index scans matching a predicate.

  ## Arguments

  - `predicate` - Predicate term ID to match

  ## Returns

  8-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = Index.pos_prefix(42)
      iex> byte_size(prefix)
      8
  """
  @spec pos_prefix(term_id()) :: binary()
  def pos_prefix(predicate) when valid_term_id?(predicate) do
    <<predicate::64-big>>
  end

  @doc """
  Creates a prefix for POS index scans matching predicate and object.

  ## Arguments

  - `predicate` - Predicate term ID to match
  - `object` - Object term ID to match

  ## Returns

  16-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = Index.pos_prefix(42, 100)
      iex> byte_size(prefix)
      16
  """
  @spec pos_prefix(term_id(), term_id()) :: binary()
  def pos_prefix(predicate, object)
      when valid_term_id?(predicate) and valid_term_id?(object) do
    <<predicate::64-big, object::64-big>>
  end

  # ===========================================================================
  # OSP Index Key Encoding
  # ===========================================================================

  @doc """
  Encodes object, subject, and predicate IDs into an OSP index key.

  The key is 24 bytes: object (8 bytes), subject (8 bytes), predicate (8 bytes),
  all in big-endian format for correct lexicographic ordering.

  ## Arguments

  - `object` - Object term ID
  - `subject` - Subject term ID
  - `predicate` - Predicate term ID

  ## Returns

  24-byte binary key suitable for the OSP column family.

  ## Examples

      iex> key = Index.osp_key(3, 1, 2)
      iex> byte_size(key)
      24

      iex> {o, s, p} = Index.decode_osp_key(key)
      iex> {o, s, p}
      {3, 1, 2}
  """
  @spec osp_key(term_id(), term_id(), term_id()) :: index_key()
  def osp_key(object, subject, predicate) when valid_triple?(object, subject, predicate) do
    <<object::64-big, subject::64-big, predicate::64-big>>
  end

  @doc """
  Decodes an OSP index key back into object, subject, and predicate IDs.

  ## Arguments

  - `key` - 24-byte OSP index key

  ## Returns

  Tuple `{object, subject, predicate}` with the decoded term IDs.

  ## Examples

      iex> key = Index.osp_key(300, 100, 200)
      iex> Index.decode_osp_key(key)
      {300, 100, 200}
  """
  @spec decode_osp_key(index_key()) :: {term_id(), term_id(), term_id()}
  def decode_osp_key(<<object::64-big, subject::64-big, predicate::64-big>>) do
    {object, subject, predicate}
  end

  @doc """
  Creates a prefix for OSP index scans matching an object.

  ## Arguments

  - `object` - Object term ID to match

  ## Returns

  8-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = Index.osp_prefix(42)
      iex> byte_size(prefix)
      8
  """
  @spec osp_prefix(term_id()) :: binary()
  def osp_prefix(object) when valid_term_id?(object) do
    <<object::64-big>>
  end

  @doc """
  Creates a prefix for OSP index scans matching object and subject.

  ## Arguments

  - `object` - Object term ID to match
  - `subject` - Subject term ID to match

  ## Returns

  16-byte prefix for prefix iteration.

  ## Examples

      iex> prefix = Index.osp_prefix(42, 100)
      iex> byte_size(prefix)
      16
  """
  @spec osp_prefix(term_id(), term_id()) :: binary()
  def osp_prefix(object, subject)
      when valid_term_id?(object) and valid_term_id?(subject) do
    <<object::64-big, subject::64-big>>
  end

  # ===========================================================================
  # Utility Functions
  # ===========================================================================

  @doc """
  Encodes a triple for all three indices.

  Returns a list of `{column_family, key}` tuples ready for batch write.
  The value for index entries is always empty (the key contains all info).

  ## Arguments

  - `subject` - Subject term ID
  - `predicate` - Predicate term ID
  - `object` - Object term ID

  ## Returns

  List of `{:spo | :pos | :osp, key}` tuples.

  ## Examples

      iex> keys = Index.encode_triple_keys(1, 2, 3)
      iex> length(keys)
      3
  """
  @spec encode_triple_keys(term_id(), term_id(), term_id()) ::
          [{:spo | :pos | :osp, index_key()}]
  def encode_triple_keys(subject, predicate, object) do
    [
      {:spo, spo_key(subject, predicate, object)},
      {:pos, pos_key(predicate, object, subject)},
      {:osp, osp_key(object, subject, predicate)}
    ]
  end

  @doc """
  Converts any index key back to a canonical `{subject, predicate, object}` triple.

  ## Arguments

  - `index` - Which index the key is from (`:spo`, `:pos`, or `:osp`)
  - `key` - 24-byte index key

  ## Returns

  Tuple `{subject, predicate, object}` in canonical order.

  ## Examples

      iex> key = Index.pos_key(2, 3, 1)
      iex> Index.key_to_triple(:pos, key)
      {1, 2, 3}
  """
  @spec key_to_triple(:spo | :pos | :osp, index_key()) :: triple()
  def key_to_triple(:spo, key), do: decode_spo_key(key)

  def key_to_triple(:pos, key) do
    {predicate, object, subject} = decode_pos_key(key)
    {subject, predicate, object}
  end

  def key_to_triple(:osp, key) do
    {object, subject, predicate} = decode_osp_key(key)
    {subject, predicate, object}
  end

  # ===========================================================================
  # Triple Insert Operations
  # ===========================================================================

  @doc """
  Inserts a single triple into all three indices atomically.

  The triple is written to SPO, POS, and OSP indices using a single atomic
  WriteBatch operation. If the triple already exists, this is a no-op
  (idempotent operation).

  **Note**: This function always uses `sync: true` for immediate durability.
  For bulk loading operations where performance is more important than
  per-operation durability, use `insert_triples/3` with `sync: false` instead.

  ## Arguments

  - `db` - RocksDB database reference
  - `triple` - Tuple `{subject_id, predicate_id, object_id}` of term IDs

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> Index.insert_triple(db, {1, 2, 3})
      :ok

  """
  @spec insert_triple(NIF.db_ref(), triple()) :: :ok | {:error, term()}
  def insert_triple(db, {subject, predicate, object})
      when valid_triple?(subject, predicate, object) do
    operations =
      for {cf, key} <- encode_triple_keys(subject, predicate, object) do
        {cf, key, @empty_value}
      end

    NIF.write_batch(db, operations, true)
  end

  @doc """
  Inserts multiple triples into all three indices atomically.

  All triples are written to SPO, POS, and OSP indices using a single atomic
  WriteBatch operation. Either all triples are inserted or none are.
  Duplicate triples are handled idempotently.

  ## Arguments

  - `db` - RocksDB database reference
  - `triples` - List of `{subject_id, predicate_id, object_id}` tuples
  - `opts` - Keyword list of options:
    - `:sync` - When `true` (default), forces an fsync after the write.
      When `false`, the write is buffered in the OS. Use `false` for
      bulk loading to improve performance. WAL still provides durability.

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> triples = [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]
      iex> Index.insert_triples(db, triples)
      :ok

      # For bulk loading, disable sync for better performance
      iex> Index.insert_triples(db, triples, sync: false)
      :ok

  """
  @spec insert_triples(NIF.db_ref(), [triple()], keyword()) :: :ok | {:error, term()}
  def insert_triples(db, triples, opts \\ [])

  def insert_triples(_db, [], _opts), do: :ok

  def insert_triples(db, triples, opts) when is_list(triples) do
    sync = validate_sync_option(opts)

    operations =
      for {subject, predicate, object} <- triples,
          {cf, key} <- encode_triple_keys(subject, predicate, object) do
        {cf, key, @empty_value}
      end

    NIF.write_batch(db, operations, sync)
  end

  @doc """
  Checks if a triple exists in the database.

  Uses the SPO index for the lookup as it's the primary index.

  ## Arguments

  - `db` - RocksDB database reference
  - `triple` - Tuple `{subject_id, predicate_id, object_id}` of term IDs

  ## Returns

  - `{:ok, true}` if triple exists
  - `{:ok, false}` if triple does not exist
  - `{:error, reason}` on failure

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> Index.insert_triple(db, {1, 2, 3})
      :ok
      iex> Index.triple_exists?(db, {1, 2, 3})
      {:ok, true}
      iex> Index.triple_exists?(db, {9, 9, 9})
      {:ok, false}

  """
  @spec triple_exists?(NIF.db_ref(), triple()) :: {:ok, boolean()} | {:error, term()}
  def triple_exists?(db, {subject, predicate, object})
      when valid_triple?(subject, predicate, object) do
    key = spo_key(subject, predicate, object)
    NIF.exists(db, :spo, key)
  end

  # ===========================================================================
  # Triple Delete Operations
  # ===========================================================================

  @doc """
  Deletes a single triple from all three indices atomically.

  The triple is removed from SPO, POS, and OSP indices using a single atomic
  DeleteBatch operation. If the triple does not exist, this is a no-op
  (idempotent operation).

  **Note**: This function always uses `sync: true` for immediate durability.
  For bulk delete operations where performance is more important than
  per-operation durability, use `delete_triples/3` with `sync: false` instead.

  ## Arguments

  - `db` - RocksDB database reference
  - `triple` - Tuple `{subject_id, predicate_id, object_id}` of term IDs

  ## Returns

  - `:ok` on success (even if triple didn't exist)
  - `{:error, reason}` on failure

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> Index.insert_triple(db, {1, 2, 3})
      :ok
      iex> Index.delete_triple(db, {1, 2, 3})
      :ok
      iex> Index.triple_exists?(db, {1, 2, 3})
      {:ok, false}

  """
  @spec delete_triple(NIF.db_ref(), triple()) :: :ok | {:error, term()}
  def delete_triple(db, {subject, predicate, object})
      when valid_triple?(subject, predicate, object) do
    operations =
      for {cf, key} <- encode_triple_keys(subject, predicate, object) do
        {cf, key}
      end

    NIF.delete_batch(db, operations, true)
  end

  @doc """
  Deletes multiple triples from all three indices atomically.

  All triples are removed from SPO, POS, and OSP indices using a single atomic
  DeleteBatch operation. Either all triples are deleted or none are.
  Non-existent triples are handled idempotently.

  ## Arguments

  - `db` - RocksDB database reference
  - `triples` - List of `{subject_id, predicate_id, object_id}` tuples
  - `opts` - Keyword list of options:
    - `:sync` - When `true` (default), forces an fsync after the write.
      When `false`, the write is buffered in the OS.

  ## Returns

  - `:ok` on success (even if some triples didn't exist)
  - `{:error, reason}` on failure

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> triples = [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]
      iex> Index.insert_triples(db, triples)
      :ok
      iex> Index.delete_triples(db, triples)
      :ok

  """
  @spec delete_triples(NIF.db_ref(), [triple()], keyword()) :: :ok | {:error, term()}
  def delete_triples(db, triples, opts \\ [])

  def delete_triples(_db, [], _opts), do: :ok

  def delete_triples(db, triples, opts) when is_list(triples) do
    sync = validate_sync_option(opts)

    operations =
      for {subject, predicate, object} <- triples,
          {cf, key} <- encode_triple_keys(subject, predicate, object) do
        {cf, key}
      end

    NIF.delete_batch(db, operations, sync)
  end

  # ===========================================================================
  # Pattern Matching
  # ===========================================================================

  @doc """
  Selects the optimal index and builds a prefix for the given triple pattern.

  Given a triple pattern where each position is either bound to a specific ID
  (`{:bound, id}`) or unbound (`:var`), this function selects the most efficient
  index and constructs the appropriate prefix for iteration.

  ## Pattern to Index Mapping

  | Pattern | Index | Prefix | Notes |
  |---------|-------|--------|-------|
  | `{:bound, :bound, :bound}` | SPO | Full key | Exact lookup |
  | `{:bound, :bound, :var}` | SPO | S-P | Subject-predicate prefix |
  | `{:bound, :var, :var}` | SPO | S | Subject prefix |
  | `{:var, :bound, :bound}` | POS | P-O | Predicate-object prefix |
  | `{:var, :bound, :var}` | POS | P | Predicate prefix |
  | `{:var, :var, :bound}` | OSP | O | Object prefix |
  | `{:bound, :var, :bound}` | OSP | O-S | Object-subject prefix, filter by P |
  | `{:var, :var, :var}` | SPO | Empty | Full scan |

  ## Performance Notes

  The **S?O pattern** (`{:bound, :var, :bound}`) is the only pattern that requires
  post-filtering. It uses the OSP index with an O-S prefix, then filters results
  by predicate. This means performance may degrade for graphs where a given
  subject-object pair has many different predicates. For most RDF graphs this is
  rare, but queries like `SELECT ?p WHERE { :entity1 ?p :entity2 }` will scan
  all predicates between two entities.

  For time-critical queries, consider if the pattern can be restructured to avoid
  S?O, or accept that this pattern has O(n) filtering where n is the number of
  predicates between the subject-object pair.

  ## Arguments

  - `pattern` - A tuple of three pattern elements, each being `{:bound, id}` or `:var`

  ## Returns

  A map containing:
  - `:index` - The column family to query (`:spo`, `:pos`, or `:osp`)
  - `:prefix` - The binary prefix for iteration
  - `:needs_filter` - Whether results need post-filtering
  - `:filter_position` - Which position needs filtering (`:predicate` or `nil`)

  ## Examples

      iex> Index.select_index({{:bound, 1}, {:bound, 2}, {:bound, 3}})
      %{index: :spo, prefix: <<...>>, needs_filter: false, filter_position: nil}

      iex> Index.select_index({{:bound, 1}, :var, {:bound, 3}})
      %{index: :osp, prefix: <<...>>, needs_filter: true, filter_position: :predicate}

  """
  @spec select_index(pattern()) :: index_selection()

  # Pattern: SPO - all bound (exact lookup)
  def select_index({{:bound, s}, {:bound, p}, {:bound, o}})
      when valid_triple?(s, p, o) do
    %{
      index: :spo,
      prefix: spo_key(s, p, o),
      needs_filter: false,
      filter_position: nil
    }
  end

  # Pattern: SP? - subject and predicate bound
  def select_index({{:bound, s}, {:bound, p}, :var})
      when valid_term_id?(s) and valid_term_id?(p) do
    %{
      index: :spo,
      prefix: spo_prefix(s, p),
      needs_filter: false,
      filter_position: nil
    }
  end

  # Pattern: S?? - only subject bound
  def select_index({{:bound, s}, :var, :var}) when valid_term_id?(s) do
    %{
      index: :spo,
      prefix: spo_prefix(s),
      needs_filter: false,
      filter_position: nil
    }
  end

  # Pattern: ?PO - predicate and object bound
  def select_index({:var, {:bound, p}, {:bound, o}})
      when valid_term_id?(p) and valid_term_id?(o) do
    %{
      index: :pos,
      prefix: pos_prefix(p, o),
      needs_filter: false,
      filter_position: nil
    }
  end

  # Pattern: ?P? - only predicate bound
  def select_index({:var, {:bound, p}, :var}) when valid_term_id?(p) do
    %{
      index: :pos,
      prefix: pos_prefix(p),
      needs_filter: false,
      filter_position: nil
    }
  end

  # Pattern: ??O - only object bound
  def select_index({:var, :var, {:bound, o}}) when valid_term_id?(o) do
    %{
      index: :osp,
      prefix: osp_prefix(o),
      needs_filter: false,
      filter_position: nil
    }
  end

  # Pattern: S?O - subject and object bound (requires filtering)
  # Uses OSP index with O-S prefix, then filters by predicate
  # Note: This is the only pattern requiring post-filtering. See Performance Notes above.
  def select_index({{:bound, s}, :var, {:bound, o}})
      when valid_term_id?(s) and valid_term_id?(o) do
    %{
      index: :osp,
      prefix: osp_prefix(o, s),
      needs_filter: true,
      filter_position: :predicate
    }
  end

  # Pattern: ??? - nothing bound (full scan)
  def select_index({:var, :var, :var}) do
    %{
      index: :spo,
      prefix: <<>>,
      needs_filter: false,
      filter_position: nil
    }
  end

  @doc """
  Checks if a triple matches the bound values in a pattern.

  This is used for post-filtering results when the pattern requires it
  (specifically for the S?O pattern which uses OSP with O-S prefix).

  ## Arguments

  - `triple` - The triple `{s, p, o}` to check
  - `pattern` - The pattern to match against

  ## Returns

  `true` if the triple matches all bound values in the pattern, `false` otherwise.

  ## Examples

      iex> Index.triple_matches_pattern?({1, 2, 3}, {{:bound, 1}, :var, {:bound, 3}})
      true

      iex> Index.triple_matches_pattern?({1, 2, 3}, {{:bound, 1}, {:bound, 5}, {:bound, 3}})
      false

  """
  @spec triple_matches_pattern?(triple(), pattern()) :: boolean()
  def triple_matches_pattern?({s, p, o}, {s_pat, p_pat, o_pat}) do
    matches_element?(s, s_pat) and matches_element?(p, p_pat) and matches_element?(o, o_pat)
  end

  defp matches_element?(_value, :var), do: true
  defp matches_element?(value, {:bound, expected}), do: value == expected

  @doc """
  Converts a triple pattern to a simplified form for index selection.

  This helper converts patterns with IDs to a form showing just the binding status,
  which can be useful for debugging and documentation.

  ## Examples

      iex> Index.pattern_shape({{:bound, 123}, :var, {:bound, 456}})
      {:bound, :var, :bound}

  """
  @spec pattern_shape(pattern()) :: {atom(), atom(), atom()}
  def pattern_shape({s_pat, p_pat, o_pat}) do
    {element_shape(s_pat), element_shape(p_pat), element_shape(o_pat)}
  end

  defp element_shape(:var), do: :var
  defp element_shape({:bound, _}), do: :bound

  # ===========================================================================
  # Index Lookup
  # ===========================================================================

  @doc """
  Returns a stream of triples matching the given pattern.

  Uses the optimal index based on which pattern positions are bound,
  constructs the appropriate prefix, and iterates over matching entries.
  For the S?O pattern, results are post-filtered by predicate.

  ## Arguments

  - `db` - RocksDB database reference
  - `pattern` - A tuple of three pattern elements, each being `{:bound, id}` or `:var`

  ## Returns

  `{:ok, Stream.t()}` where each element is a triple `{s, p, o}`, or
  `{:error, reason}` on failure.

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> Index.insert_triple(db, {1, 2, 3})
      iex> {:ok, stream} = Index.lookup(db, {{:bound, 1}, :var, :var})
      iex> Enum.to_list(stream)
      [{1, 2, 3}]

  """
  @spec lookup(NIF.db_ref(), pattern()) :: {:ok, Enumerable.t()} | {:error, term()}
  def lookup(db, pattern) do
    %{index: index, prefix: prefix, needs_filter: needs_filter} = select_index(pattern)

    with {:ok, stream} <- NIF.prefix_stream(db, index, prefix) do
      decoded_stream = Stream.map(stream, fn {key, _value} -> key_to_triple(index, key) end)

      final_stream =
        if needs_filter do
          Stream.filter(decoded_stream, &triple_matches_pattern?(&1, pattern))
        else
          decoded_stream
        end

      {:ok, final_stream}
    end
  end

  @doc """
  Returns a list of all triples matching the given pattern.

  This is a convenience function that collects all results from `lookup/2`
  into a list. Use `lookup/2` directly for lazy evaluation on large result sets.

  ## Arguments

  - `db` - RocksDB database reference
  - `pattern` - A tuple of three pattern elements, each being `{:bound, id}` or `:var`

  ## Returns

  `{:ok, [triple()]}` with all matching triples, or `{:error, reason}` on failure.

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> Index.insert_triples(db, [{1, 2, 3}, {1, 2, 4}, {1, 3, 5}])
      iex> Index.lookup_all(db, {{:bound, 1}, {:bound, 2}, :var})
      {:ok, [{1, 2, 3}, {1, 2, 4}]}

  """
  @spec lookup_all(NIF.db_ref(), pattern()) :: {:ok, [triple()]} | {:error, term()}
  def lookup_all(db, pattern) do
    case lookup(db, pattern) do
      {:ok, stream} -> {:ok, Enum.to_list(stream)}
      {:error, _} = error -> error
    end
  end

  @doc """
  Counts the number of triples matching the given pattern.

  ## Arguments

  - `db` - RocksDB database reference
  - `pattern` - A tuple of three pattern elements, each being `{:bound, id}` or `:var`

  ## Returns

  `{:ok, count}` with the number of matching triples, or `{:error, reason}` on failure.

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> Index.insert_triples(db, [{1, 2, 3}, {1, 2, 4}, {1, 3, 5}])
      iex> Index.count(db, {{:bound, 1}, :var, :var})
      {:ok, 3}

  """
  @spec count(NIF.db_ref(), pattern()) :: {:ok, non_neg_integer()} | {:error, term()}
  def count(db, pattern) do
    case lookup(db, pattern) do
      {:ok, stream} -> {:ok, Enum.count(stream)}
      {:error, _} = error -> error
    end
  end
end
