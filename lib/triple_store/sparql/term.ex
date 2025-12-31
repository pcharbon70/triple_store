defmodule TripleStore.SPARQL.Term do
  @moduledoc """
  Shared utilities for converting between RDF.ex terms and SPARQL AST format.

  This module provides conversion functions used by both the Transaction module
  and the Update module to transform RDF.ex data structures into the internal
  AST representation expected by the UpdateExecutor.

  ## AST Format

  The parser AST format for RDF terms:

  - Named nodes: `{:named_node, iri_string}`
  - Blank nodes: `{:blank_node, id_string}`
  - Simple literals: `{:literal, :simple, value_string}`
  - Typed literals: `{:literal, :typed, value_string, datatype_iri}`
  - Language-tagged literals: `{:literal, :lang, value_string, lang_tag}`

  ## Examples

      iex> Term.to_ast(RDF.iri("http://example.org/foo"))
      {:named_node, "http://example.org/foo"}

      iex> Term.to_ast(RDF.literal("hello"))
      {:literal, :simple, "hello"}

      iex> Term.to_ast(RDF.literal("bonjour", language: "fr"))
      {:literal, :lang, "bonjour", "fr"}

  """

  @typedoc "Parser AST format for RDF terms"
  @type ast_term ::
          {:named_node, String.t()}
          | {:blank_node, String.t()}
          | {:literal, :simple, String.t()}
          | {:literal, :typed, String.t(), String.t()}
          | {:literal, :lang, String.t(), String.t()}

  @doc """
  Converts an RDF.ex term to parser AST format.

  ## Arguments

  - `term` - An RDF.ex term (IRI, BlankNode, Literal, or binary)

  ## Returns

  The term in parser AST format suitable for UpdateExecutor.

  ## Examples

      iex> Term.to_ast(RDF.iri("http://example.org/s"))
      {:named_node, "http://example.org/s"}

      iex> Term.to_ast(RDF.bnode("b0"))
      {:blank_node, "b0"}

      iex> Term.to_ast(RDF.literal(42))
      {:literal, :typed, "42", "http://www.w3.org/2001/XMLSchema#integer"}

  """
  @spec to_ast(term()) :: ast_term() | term()
  def to_ast(%RDF.IRI{value: value}), do: {:named_node, value}
  def to_ast(%RDF.BlankNode{value: value}), do: {:blank_node, to_string(value)}

  def to_ast(%RDF.Literal{literal: %RDF.LangString{value: value, language: lang}}) do
    {:literal, :lang, value, lang}
  end

  def to_ast(%RDF.Literal{literal: literal} = lit) do
    datatype = RDF.Literal.datatype_id(lit)

    if datatype == RDF.XSD.String.id() do
      {:literal, :simple, RDF.Literal.value(lit)}
    else
      # Get the string value - prefer stored value, fall back to lexical form
      value =
        case literal do
          %{value: v} when is_binary(v) -> v
          _ -> RDF.Literal.lexical(lit)
        end

      {:literal, :typed, value, to_string(datatype)}
    end
  end

  def to_ast(term) when is_binary(term) do
    # Assume it's a string literal
    {:literal, :simple, term}
  end

  def to_ast(term) do
    # Pass through - might already be in AST format
    term
  end

  # ===========================================================================
  # Dictionary-Based Term Encoding/Decoding
  # ===========================================================================

  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.IdToString
  alias TripleStore.Dictionary.StringToId

  @doc """
  Encodes an AST term to its dictionary ID.

  Handles inline encoding for numeric types (integer, decimal, datetime)
  and dictionary lookup for other terms.

  ## Arguments

  - `term` - An AST term tuple
  - `dict_manager` - The dictionary manager GenServer

  ## Returns

  - `{:ok, id}` - The term ID
  - `:not_found` - Term not in dictionary
  - `{:error, reason}` - On failure

  ## Examples

      iex> Term.encode({:named_node, "http://example.org/foo"}, dict_manager)
      {:ok, 12345}

  """
  @spec encode(ast_term(), GenServer.server()) :: {:ok, integer()} | :not_found | {:error, term()}
  def encode({:named_node, uri}, dict_manager) do
    rdf_term = RDF.iri(uri)
    lookup_term_id(dict_manager, rdf_term)
  end

  def encode({:blank_node, name}, dict_manager) do
    rdf_term = RDF.bnode(name)
    lookup_term_id(dict_manager, rdf_term)
  end

  def encode({:literal, :simple, value}, dict_manager) do
    rdf_term = RDF.literal(value)
    lookup_term_id(dict_manager, rdf_term)
  end

  def encode({:literal, :lang, value, lang}, dict_manager) do
    rdf_term = RDF.literal(value, language: lang)
    lookup_term_id(dict_manager, rdf_term)
  end

  def encode({:literal, :typed, value, datatype}, dict_manager) do
    # Check for inline-encodable types first
    case try_inline_encode(value, datatype) do
      {:ok, id} ->
        {:ok, id}

      :not_inline ->
        rdf_term = RDF.literal(value, datatype: datatype)
        lookup_term_id(dict_manager, rdf_term)
    end
  end

  def encode(_term, _dict_manager) do
    :not_found
  end

  @doc """
  Decodes a term ID back to an AST term.

  Handles inline-encoded types and dictionary lookup for regular terms.

  ## Arguments

  - `term_id` - The term ID (integer)
  - `dict_manager` - The dictionary manager GenServer

  ## Returns

  - `{:ok, ast_term}` - The decoded AST term
  - `{:error, reason}` - On failure

  ## Examples

      iex> Term.decode(12345, dict_manager)
      {:ok, {:named_node, "http://example.org/foo"}}

  """
  @spec decode(integer(), GenServer.server()) :: {:ok, ast_term()} | {:error, term()}
  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  def decode(term_id, dict_manager) do
    if Dictionary.inline_encoded?(term_id) do
      decode_inline(term_id)
    else
      case GenServer.call(dict_manager, :get_db) do
        {:ok, db} ->
          case IdToString.lookup_term(db, term_id) do
            {:ok, rdf_term} -> {:ok, to_ast(rdf_term)}
            :not_found -> {:error, :term_not_found}
            {:error, _} = error -> error
          end

        {:error, _} = error ->
          error
      end
    end
  end

  @doc """
  Looks up a term ID in the dictionary without inline encoding.

  ## Arguments

  - `dict_manager` - The dictionary manager GenServer
  - `rdf_term` - An RDF.ex term

  ## Returns

  - `{:ok, id}` - The term ID
  - `:not_found` - Term not in dictionary
  - `{:error, reason}` - On failure

  """
  @spec lookup_term_id(GenServer.server(), term()) ::
          {:ok, integer()} | :not_found | {:error, term()}
  def lookup_term_id(dict_manager, rdf_term) do
    case GenServer.call(dict_manager, :get_db) do
      {:ok, db} ->
        case StringToId.lookup_id(db, rdf_term) do
          {:ok, id} -> {:ok, id}
          :not_found -> :not_found
          {:error, _} = error -> error
        end

      {:error, _} = error ->
        error
    end
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  # Try to inline-encode numeric types
  defp try_inline_encode(value, "http://www.w3.org/2001/XMLSchema#integer") do
    case Integer.parse(value) do
      {int_val, ""} ->
        case Dictionary.encode_integer(int_val) do
          {:ok, id} -> {:ok, id}
          {:error, :out_of_range} -> :not_inline
        end

      _ ->
        :not_inline
    end
  end

  defp try_inline_encode(value, "http://www.w3.org/2001/XMLSchema#decimal") do
    case Decimal.parse(value) do
      {decimal, ""} ->
        case Dictionary.encode_decimal(decimal) do
          {:ok, id} -> {:ok, id}
          {:error, :out_of_range} -> :not_inline
        end

      {decimal, _remainder} ->
        case Dictionary.encode_decimal(decimal) do
          {:ok, id} -> {:ok, id}
          {:error, :out_of_range} -> :not_inline
        end

      :error ->
        :not_inline
    end
  end

  defp try_inline_encode(_value, _datatype), do: :not_inline

  # Decode inline-encoded terms
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp decode_inline(term_id) do
    case Dictionary.term_type(term_id) do
      :integer ->
        case Dictionary.decode_integer(term_id) do
          {:ok, value} ->
            {:ok,
             {:literal, :typed, Integer.to_string(value),
              "http://www.w3.org/2001/XMLSchema#integer"}}

          error ->
            error
        end

      :decimal ->
        case Dictionary.decode_decimal(term_id) do
          {:ok, value} ->
            {:ok,
             {:literal, :typed, Decimal.to_string(value),
              "http://www.w3.org/2001/XMLSchema#decimal"}}

          error ->
            error
        end

      :datetime ->
        case Dictionary.decode_datetime(term_id) do
          {:ok, value} ->
            {:ok,
             {:literal, :typed, DateTime.to_iso8601(value),
              "http://www.w3.org/2001/XMLSchema#dateTime"}}

          error ->
            error
        end

      _ ->
        {:error, :unknown_inline_type}
    end
  end
end
