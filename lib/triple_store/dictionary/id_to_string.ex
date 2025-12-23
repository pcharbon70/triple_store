defmodule TripleStore.Dictionary.IdToString do
  @moduledoc """
  ID-to-String mapping for RDF terms (Task 1.3.4).

  Provides the reverse mapping from 64-bit IDs to RDF term strings using
  the `id2str` column family in RocksDB.

  ## Term Decoding

  Dictionary-allocated terms (URI, BNode, Literal) are stored in id2str
  with the same encoding format used in str2id:

  - **URI**: `<<1, uri_string::binary>>`
  - **BNode**: `<<2, bnode_id::binary>>`
  - **Literal (plain)**: `<<3, 0, value::binary>>`
  - **Literal (typed)**: `<<3, 1, datatype::binary, 0, value::binary>>`
  - **Literal (lang)**: `<<3, 2, lang::binary, 0, value::binary>>`

  ## Inline-Encoded IDs

  Inline-encoded IDs (integer, decimal, datetime) don't require dictionary
  lookup. The value is extracted directly from the ID bits and converted
  to an RDF.Literal struct.

  ## Usage

  ```elixir
  # Look up a dictionary-allocated term
  {:ok, term} = IdToString.lookup_term(db, uri_id)

  # Batch lookup for result serialization
  {:ok, terms} = IdToString.lookup_terms(db, [id1, id2, id3])
  ```
  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.Batch

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "RDF term (URI, blank node, or literal)"
  @type rdf_term :: RDF.IRI.t() | RDF.BlankNode.t() | RDF.Literal.t()

  @typedoc "Database reference from RocksDB NIF"
  @type db_ref :: reference()

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Looks up the RDF term for an ID.

  For dictionary-allocated IDs (URI, BNode, Literal), performs a lookup
  in the id2str column family. For inline-encoded IDs (integer, decimal,
  datetime), the value is computed directly from the ID bits.

  ## Arguments

  - `db` - Database reference
  - `id` - 64-bit term ID

  ## Returns

  - `{:ok, term}` - RDF term struct
  - `:not_found` - ID not in dictionary (for dictionary-allocated types)
  - `{:error, reason}` - Decoding or database error

  ## Examples

      iex> IdToString.lookup_term(db, uri_id)
      {:ok, %RDF.IRI{value: "http://example.org"}}

      iex> IdToString.lookup_term(db, integer_id)
      {:ok, RDF.literal(42)}
  """
  @spec lookup_term(db_ref(), Dictionary.term_id()) ::
          {:ok, rdf_term()} | :not_found | {:error, term()}
  def lookup_term(db, id) when is_integer(id) and id >= 0 do
    if Dictionary.inline_encoded?(id) do
      decode_inline_term(id)
    else
      lookup_dictionary_term(db, id)
    end
  end

  @doc """
  Looks up multiple terms by their IDs.

  This is a batch version of `lookup_term/2` for efficient result
  serialization. For inline-encoded IDs, values are computed directly.

  ## Arguments

  - `db` - Database reference
  - `ids` - List of term IDs

  ## Returns

  - `{:ok, results}` - List of `{:ok, term}` or `:not_found` for each ID
  - `{:error, reason}` - On database error

  ## Examples

      iex> IdToString.lookup_terms(db, [uri_id, bnode_id, unknown_id])
      {:ok, [{:ok, %RDF.IRI{...}}, {:ok, %RDF.BlankNode{...}}, :not_found]}
  """
  @spec lookup_terms(db_ref(), [Dictionary.term_id()]) ::
          {:ok, [{:ok, rdf_term()} | :not_found]} | {:error, term()}
  def lookup_terms(db, ids) do
    Batch.map_with_early_error(ids, &lookup_term(db, &1))
  end

  @doc """
  Decodes a term binary (from id2str) back to an RDF term struct.

  This function parses the binary format used in both str2id and id2str
  column families and reconstructs the original RDF term.

  ## Arguments

  - `binary` - Encoded term binary

  ## Returns

  - `{:ok, term}` - Decoded RDF term
  - `{:error, :invalid_encoding}` - Binary format not recognized

  ## Examples

      iex> IdToString.decode_term(<<1, "http://example.org">>)
      {:ok, %RDF.IRI{value: "http://example.org"}}

      iex> IdToString.decode_term(<<3, 2, "en", 0, "hello">>)
      {:ok, RDF.literal("hello", language: "en")}
  """
  @spec decode_term(binary()) :: {:ok, rdf_term()} | {:error, :invalid_encoding}
  def decode_term(<<prefix, rest::binary>>) when prefix == 1 do
    # URI (prefix_uri = 1)
    {:ok, RDF.iri(rest)}
  end

  def decode_term(<<prefix, rest::binary>>) when prefix == 2 do
    # BNode (prefix_bnode = 2)
    {:ok, RDF.bnode(rest)}
  end

  def decode_term(<<prefix, subtype, value::binary>>) when prefix == 3 and subtype == 0 do
    # Plain literal (prefix_literal = 3, literal_plain = 0)
    {:ok, RDF.literal(value)}
  end

  def decode_term(<<prefix, subtype, rest::binary>>) when prefix == 3 and subtype == 1 do
    # Typed literal (prefix_literal = 3, literal_typed = 1)
    case split_at_null(rest) do
      {datatype_uri, value} ->
        {:ok, RDF.literal(value, datatype: RDF.iri(datatype_uri))}

      :error ->
        {:error, :invalid_encoding}
    end
  end

  def decode_term(<<prefix, subtype, rest::binary>>) when prefix == 3 and subtype == 2 do
    # Language-tagged literal (prefix_literal = 3, literal_lang = 2)
    case split_at_null(rest) do
      {lang_tag, value} ->
        {:ok, RDF.literal(value, language: lang_tag)}

      :error ->
        {:error, :invalid_encoding}
    end
  end

  def decode_term(_binary) do
    {:error, :invalid_encoding}
  end

  # ===========================================================================
  # Private Functions
  # ===========================================================================

  @spec lookup_dictionary_term(db_ref(), Dictionary.term_id()) ::
          {:ok, rdf_term()} | :not_found | {:error, term()}
  defp lookup_dictionary_term(db, id) do
    id_binary = <<id::64-big>>

    case NIF.get(db, :id2str, id_binary) do
      {:ok, term_binary} ->
        decode_term(term_binary)

      :not_found ->
        :not_found

      {:error, _} = error ->
        error
    end
  end

  @spec decode_inline_term(Dictionary.term_id()) :: {:ok, rdf_term()} | {:error, term()}
  defp decode_inline_term(id) do
    case Dictionary.term_type(id) do
      :integer -> decode_and_wrap(&Dictionary.decode_integer/1, id)
      :decimal -> decode_and_wrap(&Dictionary.decode_decimal/1, id)
      :datetime -> decode_and_wrap(&Dictionary.decode_datetime/1, id)
      _other -> {:error, :unknown_inline_type}
    end
  end

  @spec decode_and_wrap(
          (Dictionary.term_id() -> {:ok, term()} | {:error, term()}),
          Dictionary.term_id()
        ) ::
          {:ok, rdf_term()} | {:error, term()}
  defp decode_and_wrap(decoder, id) do
    case decoder.(id) do
      {:ok, value} -> {:ok, RDF.literal(value)}
      {:error, _} = error -> error
    end
  end

  @spec split_at_null(binary()) :: {String.t(), String.t()} | :error
  defp split_at_null(binary) do
    case :binary.split(binary, <<0>>) do
      [first, second] -> {first, second}
      _ -> :error
    end
  end
end
