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

  def to_ast(%RDF.Literal{literal: %{value: value, datatype: datatype}}) do
    datatype_iri = to_string(datatype)

    if datatype_iri == "http://www.w3.org/2001/XMLSchema#string" do
      {:literal, :simple, to_string(value)}
    else
      {:literal, :typed, to_string(value), datatype_iri}
    end
  end

  def to_ast(%RDF.Literal{literal: literal}) when is_binary(literal) do
    {:literal, :simple, literal}
  end

  def to_ast(term) when is_binary(term) do
    # Assume it's a string literal
    {:literal, :simple, term}
  end

  def to_ast(term) do
    # Pass through - might already be in AST format
    term
  end
end
