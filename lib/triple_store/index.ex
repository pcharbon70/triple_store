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

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "64-bit term ID from the dictionary"
  @type term_id :: Dictionary.term_id()

  @typedoc "24-byte index key (3 x 64-bit big-endian IDs)"
  @type index_key :: <<_::192>>

  @typedoc "A triple as a tuple of three term IDs"
  @type triple :: {term_id(), term_id(), term_id()}

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
  def spo_key(subject, predicate, object)
      when is_integer(subject) and is_integer(predicate) and is_integer(object) do
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
  def spo_prefix(subject) when is_integer(subject) do
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
      when is_integer(subject) and is_integer(predicate) do
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
  def pos_key(predicate, object, subject)
      when is_integer(predicate) and is_integer(object) and is_integer(subject) do
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
  def pos_prefix(predicate) when is_integer(predicate) do
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
      when is_integer(predicate) and is_integer(object) do
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
  def osp_key(object, subject, predicate)
      when is_integer(object) and is_integer(subject) and is_integer(predicate) do
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
  def osp_prefix(object) when is_integer(object) do
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
      when is_integer(object) and is_integer(subject) do
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
      when is_integer(subject) and is_integer(predicate) and is_integer(object) do
    operations =
      for {cf, key} <- encode_triple_keys(subject, predicate, object) do
        {cf, key, <<>>}
      end

    NIF.write_batch(db, operations)
  end

  @doc """
  Inserts multiple triples into all three indices atomically.

  All triples are written to SPO, POS, and OSP indices using a single atomic
  WriteBatch operation. Either all triples are inserted or none are.
  Duplicate triples are handled idempotently.

  ## Arguments

  - `db` - RocksDB database reference
  - `triples` - List of `{subject_id, predicate_id, object_id}` tuples

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> triples = [{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]
      iex> Index.insert_triples(db, triples)
      :ok

  """
  @spec insert_triples(NIF.db_ref(), [triple()]) :: :ok | {:error, term()}
  def insert_triples(_db, []), do: :ok

  def insert_triples(db, triples) when is_list(triples) do
    operations =
      for {subject, predicate, object} <- triples,
          {cf, key} <- encode_triple_keys(subject, predicate, object) do
        {cf, key, <<>>}
      end

    NIF.write_batch(db, operations)
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
      when is_integer(subject) and is_integer(predicate) and is_integer(object) do
    key = spo_key(subject, predicate, object)
    NIF.exists(db, :spo, key)
  end
end
