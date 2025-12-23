defmodule TripleStore.Dictionary.StringToId do
  @moduledoc """
  String-to-ID mapping for RDF terms (Task 1.3.3).

  Provides the forward mapping from RDF term strings to 64-bit IDs using
  the `str2id` column family in RocksDB.

  ## Term Encoding

  Terms are serialized to binary keys using a type prefix:

  - **URI**: `<<1, uri_string::binary>>`
  - **BNode**: `<<2, bnode_id::binary>>`
  - **Literal (plain)**: `<<3, 0, value::binary>>`
  - **Literal (typed)**: `<<3, 1, datatype::binary, 0, value::binary>>`
  - **Literal (lang)**: `<<3, 2, lang::binary, 0, value::binary>>`

  The type prefix ensures no collisions between different term types, and
  the null byte separator in literals allows unambiguous parsing.

  ## Concurrency Model

  - `lookup_id/2`: Direct NIF call, no serialization needed (read-only)
  - `get_or_create_id/3`: Uses GenServer serialization via SequenceCounter
    to ensure atomic create-if-not-exists semantics

  ## Usage

  ```elixir
  # Look up an existing term
  {:ok, id} = StringToId.lookup_id(db, %RDF.IRI{value: "http://example.org"})

  # Get or create (requires sequence counter)
  {:ok, id} = StringToId.get_or_create_id(db, counter, %RDF.IRI{value: "http://example.org"})
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
  Encodes an RDF term to a binary key for str2id lookup.

  ## Term Encoding Formats

  - **URI**: `<<1, uri_string::binary>>` (angle brackets stripped)
  - **BNode**: `<<2, bnode_id::binary>>`
  - **Plain Literal**: `<<3, 0, value::binary>>`
  - **Typed Literal**: `<<3, 1, datatype::binary, 0, value::binary>>`
  - **Language Literal**: `<<3, 2, lang::binary, 0, value::binary>>`

  ## Arguments

  - `term` - RDF term struct (IRI, BlankNode, or Literal)

  ## Returns

  - `{:ok, binary}` - Encoded binary key
  - `{:error, :unsupported_term}` - Term type not supported

  ## Examples

      iex> {:ok, key} = StringToId.encode_term(%RDF.IRI{value: "http://example.org"})
      iex> key
      <<1, "http://example.org"::binary>>

      iex> {:ok, key} = StringToId.encode_term(RDF.bnode("b1"))
      iex> key
      <<2, "b1"::binary>>
  """
  @spec encode_term(rdf_term()) :: {:ok, binary()} | {:error, atom()}
  def encode_term(%RDF.IRI{value: uri}) do
    # Strip angle brackets if present
    uri_string = strip_angle_brackets(uri)

    case Dictionary.validate_term(uri_string, :uri) do
      :ok ->
        normalized = Dictionary.normalize_unicode(uri_string)
        {:ok, <<Dictionary.prefix_uri(), normalized::binary>>}

      {:error, _} = error ->
        error
    end
  end

  def encode_term(%RDF.BlankNode{value: id}) do
    id_string = to_string(id)

    case Dictionary.validate_term(id_string, :bnode) do
      :ok ->
        {:ok, <<Dictionary.prefix_bnode(), id_string::binary>>}

      {:error, _} = error ->
        error
    end
  end

  def encode_term(%RDF.Literal{literal: %RDF.LangString{value: value, language: lang}}) do
    case Dictionary.validate_term(value, :literal) do
      :ok ->
        normalized_value = Dictionary.normalize_unicode(value)
        lang_tag = String.downcase(lang)

        encoded =
          <<Dictionary.prefix_literal(), Dictionary.literal_lang(), lang_tag::binary, 0,
            normalized_value::binary>>

        {:ok, encoded}

      {:error, _} = error ->
        error
    end
  end

  def encode_term(%RDF.Literal{literal: %datatype_mod{value: value}}) do
    value_string = literal_value_to_string(value)

    case Dictionary.validate_term(value_string, :literal) do
      :ok ->
        normalized_value = Dictionary.normalize_unicode(value_string)
        datatype_uri = datatype_mod.id() |> to_string()

        encoded =
          <<Dictionary.prefix_literal(), Dictionary.literal_typed(), datatype_uri::binary, 0,
            normalized_value::binary>>

        {:ok, encoded}

      {:error, _} = error ->
        error
    end
  end

  def encode_term(%RDF.Literal{literal: value}) when is_binary(value) do
    # Plain literal (xsd:string with no explicit datatype)
    case Dictionary.validate_term(value, :literal) do
      :ok ->
        normalized_value = Dictionary.normalize_unicode(value)

        {:ok,
         <<Dictionary.prefix_literal(), Dictionary.literal_plain(), normalized_value::binary>>}

      {:error, _} = error ->
        error
    end
  end

  def encode_term(_term), do: {:error, :unsupported_term}

  @doc """
  Looks up the ID for a term in the str2id column family.

  This is a read-only operation that goes directly to RocksDB
  without GenServer serialization.

  ## Arguments

  - `db` - Database reference
  - `term` - RDF term to look up

  ## Returns

  - `{:ok, term_id}` - ID found
  - `:not_found` - Term not in dictionary
  - `{:error, reason}` - Encoding or database error

  ## Examples

      iex> StringToId.lookup_id(db, %RDF.IRI{value: "http://example.org"})
      {:ok, 1152921504606846977}

      iex> StringToId.lookup_id(db, %RDF.IRI{value: "http://unknown.org"})
      :not_found
  """
  @spec lookup_id(db_ref(), rdf_term()) ::
          {:ok, Dictionary.term_id()} | :not_found | {:error, term()}
  def lookup_id(db, term) do
    case encode_term(term) do
      {:ok, key} ->
        case NIF.get(db, :str2id, key) do
          {:ok, <<id::64-big>>} ->
            {:ok, id}

          :not_found ->
            :not_found

          {:error, _} = error ->
            error
        end

      {:error, _} = error ->
        error
    end
  end

  # NOTE: get_or_create_id and get_or_create_ids have been moved to
  # TripleStore.Dictionary.Manager which provides proper serialization
  # for concurrent access.

  @doc """
  Looks up IDs for multiple terms.

  Batch version of `lookup_id/2` for efficient bulk operations.

  ## Arguments

  - `db` - Database reference
  - `terms` - List of RDF terms

  ## Returns

  - `{:ok, results}` - List of `{:ok, id}` or `:not_found` for each term
  - `{:error, reason}` - On database error
  """
  @spec lookup_ids(db_ref(), [rdf_term()]) ::
          {:ok, [{:ok, Dictionary.term_id()} | :not_found]} | {:error, term()}
  def lookup_ids(db, terms) do
    Batch.map_with_early_error(terms, &lookup_id(db, &1))
  end

  # ===========================================================================
  # Private Functions
  # ===========================================================================

  @spec strip_angle_brackets(String.t()) :: String.t()
  defp strip_angle_brackets(<<"<", rest::binary>>) do
    # Remove leading < and trailing >
    if String.ends_with?(rest, ">") do
      String.slice(rest, 0..-2//1)
    else
      rest
    end
  end

  defp strip_angle_brackets(uri), do: uri

  @spec literal_value_to_string(term()) :: String.t()
  defp literal_value_to_string(value) when is_binary(value), do: value
  defp literal_value_to_string(value) when is_integer(value), do: Integer.to_string(value)
  defp literal_value_to_string(value) when is_float(value), do: Float.to_string(value)
  defp literal_value_to_string(value) when is_boolean(value), do: to_string(value)
  defp literal_value_to_string(%Decimal{} = value), do: Decimal.to_string(value)
  defp literal_value_to_string(%Date{} = value), do: Date.to_iso8601(value)
  defp literal_value_to_string(%Time{} = value), do: Time.to_iso8601(value)
  defp literal_value_to_string(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp literal_value_to_string(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp literal_value_to_string(value), do: to_string(value)
end
