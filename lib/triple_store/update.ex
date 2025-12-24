defmodule TripleStore.Update do
  @moduledoc """
  Public API for SPARQL UPDATE operations.

  This module provides convenient functions for modifying triple store data
  using either SPARQL UPDATE strings or direct triple manipulation.

  ## Usage Patterns

  There are two ways to use these functions:

  ### 1. With Transaction Manager (Recommended)

  When you have a running Transaction manager, pass it directly:

      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)
      {:ok, 1} = Update.update(txn, "INSERT DATA { <s> <p> <o> }")

  ### 2. Direct Context (Stateless)

  For one-off operations without a transaction manager:

      {:ok, 1} = Update.update(%{db: db, dict_manager: manager},
        "INSERT DATA { <s> <p> <o> }")

  ## SPARQL UPDATE Support

  Supports the following SPARQL UPDATE operations:

  - `INSERT DATA { ... }` - Insert ground triples
  - `DELETE DATA { ... }` - Delete ground triples
  - `DELETE WHERE { ... }` - Delete triples matching pattern
  - `INSERT { ... } WHERE { ... }` - Insert templated triples
  - `DELETE { ... } INSERT { ... } WHERE { ... }` - Combined modify
  - `CLEAR DEFAULT` / `CLEAR ALL` - Clear all triples

  ## Examples

      # SPARQL UPDATE
      {:ok, 1} = Update.update(ctx, "INSERT DATA { <s> <p> <o> }")
      {:ok, 2} = Update.update(ctx, "DELETE DATA { <s1> <p> <o> . <s2> <p> <o> }")

      # Direct insert with RDF.ex terms
      {:ok, 1} = Update.insert(ctx, [
        {RDF.iri("http://example.org/s"),
         RDF.iri("http://example.org/p"),
         RDF.literal("value")}
      ])

      # Direct delete
      {:ok, 1} = Update.delete(ctx, [
        {RDF.iri("http://example.org/s"),
         RDF.iri("http://example.org/p"),
         RDF.literal("value")}
      ])

  """

  alias TripleStore.SPARQL.Parser
  alias TripleStore.SPARQL.UpdateExecutor
  alias TripleStore.Transaction

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Execution context - either a Transaction manager or a context map"
  @type context :: Transaction.manager() | %{db: reference(), dict_manager: GenServer.server()}

  @typedoc "RDF triple as {subject, predicate, object}"
  @type triple :: {term(), term(), term()}

  @typedoc "Update result"
  @type update_result :: {:ok, non_neg_integer()} | {:error, term()}

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Executes a SPARQL UPDATE operation.

  ## Arguments

  - `context` - Transaction manager or context map with `:db` and `:dict_manager`
  - `sparql` - SPARQL UPDATE string

  ## Options

  - `:timeout` - Operation timeout in milliseconds (default: 5 minutes)

  ## Returns

  - `{:ok, count}` - Number of triples affected
  - `{:error, {:parse_error, msg}}` - SPARQL parse error
  - `{:error, reason}` - Execution error

  ## Examples

      {:ok, 1} = Update.update(ctx, \"\"\"
        INSERT DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
        }
      \"\"\")

      {:ok, 0} = Update.update(ctx, \"\"\"
        DELETE DATA {
          <http://example.org/nonexistent> <http://example.org/p> <http://example.org/o> .
        }
      \"\"\")

  """
  @spec update(context(), String.t(), keyword()) :: update_result()
  def update(context, sparql, opts \\ [])

  def update(txn, sparql, opts) when is_pid(txn) or is_atom(txn) do
    Transaction.update(txn, sparql, opts)
  end

  def update(%{db: _db, dict_manager: _manager} = ctx, sparql, _opts) do
    case Parser.parse_update(sparql) do
      {:ok, ast} ->
        UpdateExecutor.execute(ctx, ast)

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Executes a pre-parsed SPARQL UPDATE AST.

  Useful when you've already parsed the update and want to execute it.

  ## Arguments

  - `context` - Transaction manager or context map
  - `ast` - Parsed UPDATE AST from `Parser.parse_update/1`

  ## Options

  - `:timeout` - Operation timeout (default: 5 minutes)

  ## Returns

  - `{:ok, count}` - Number of triples affected
  - `{:error, reason}` - Execution error

  """
  @spec execute(context(), term(), keyword()) :: update_result()
  def execute(context, ast, opts \\ [])

  def execute(txn, ast, opts) when is_pid(txn) or is_atom(txn) do
    Transaction.execute_update(txn, ast, opts)
  end

  def execute(%{db: _db, dict_manager: _manager} = ctx, ast, _opts) do
    UpdateExecutor.execute(ctx, ast)
  end

  @doc """
  Inserts triples directly without parsing SPARQL.

  ## Arguments

  - `context` - Transaction manager or context map
  - `triples` - List of `{subject, predicate, object}` RDF terms

  ## Options

  - `:timeout` - Operation timeout (default: 5 minutes)

  ## Returns

  - `{:ok, count}` - Number of triples inserted
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, 1} = Update.insert(ctx, [
        {RDF.iri("http://example.org/alice"),
         RDF.iri("http://example.org/name"),
         RDF.literal("Alice")}
      ])

      # Multiple triples
      {:ok, 3} = Update.insert(ctx, [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"), RDF.literal("v1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"), RDF.literal("v2")},
        {RDF.iri("http://example.org/s3"), RDF.iri("http://example.org/p"), RDF.literal("v3")}
      ])

  """
  @spec insert(context(), [triple()], keyword()) :: update_result()
  def insert(context, triples, opts \\ [])

  def insert(txn, triples, opts) when is_pid(txn) or is_atom(txn) do
    Transaction.insert(txn, triples, opts)
  end

  def insert(%{db: _db, dict_manager: _manager} = ctx, triples, _opts) do
    quads = convert_triples_to_quads(triples)
    UpdateExecutor.execute_insert_data(ctx, quads)
  end

  @doc """
  Deletes triples directly without parsing SPARQL.

  ## Arguments

  - `context` - Transaction manager or context map
  - `triples` - List of `{subject, predicate, object}` RDF terms

  ## Options

  - `:timeout` - Operation timeout (default: 5 minutes)

  ## Returns

  - `{:ok, count}` - Number of triples deleted
  - `{:error, reason}` - On failure

  ## Notes

  - Delete is idempotent - deleting non-existent triples returns `{:ok, 0}`
  - Only exact matches are deleted

  ## Examples

      {:ok, 1} = Update.delete(ctx, [
        {RDF.iri("http://example.org/alice"),
         RDF.iri("http://example.org/name"),
         RDF.literal("Alice")}
      ])

  """
  @spec delete(context(), [triple()], keyword()) :: update_result()
  def delete(context, triples, opts \\ [])

  def delete(txn, triples, opts) when is_pid(txn) or is_atom(txn) do
    Transaction.delete(txn, triples, opts)
  end

  def delete(%{db: _db, dict_manager: _manager} = ctx, triples, _opts) do
    quads = convert_triples_to_quads(triples)
    UpdateExecutor.execute_delete_data(ctx, quads)
  end

  @doc """
  Clears all triples from the store.

  ## Arguments

  - `context` - Transaction manager or context map

  ## Options

  - `:timeout` - Operation timeout (default: 5 minutes)

  ## Returns

  - `{:ok, count}` - Number of triples cleared
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, 1000} = Update.clear(ctx)

  """
  @spec clear(context(), keyword()) :: update_result()
  def clear(context, opts \\ [])

  def clear(txn, opts) when is_pid(txn) or is_atom(txn) do
    Transaction.update(txn, "CLEAR DEFAULT", opts)
  end

  def clear(%{db: _db, dict_manager: _manager} = ctx, _opts) do
    UpdateExecutor.execute_clear(ctx, scope: :default)
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  # Convert RDF.ex triples to parser AST format for UpdateExecutor
  defp convert_triples_to_quads(triples) do
    Enum.map(triples, fn {s, p, o} ->
      {:triple, term_to_ast(s), term_to_ast(p), term_to_ast(o)}
    end)
  end

  # Convert RDF.ex terms to parser AST format
  defp term_to_ast(%RDF.IRI{value: value}), do: {:named_node, value}
  defp term_to_ast(%RDF.BlankNode{value: value}), do: {:blank_node, to_string(value)}

  defp term_to_ast(%RDF.Literal{literal: %RDF.LangString{value: value, language: lang}}) do
    {:literal, :lang, value, lang}
  end

  defp term_to_ast(%RDF.Literal{literal: %{value: value, datatype: datatype}}) do
    datatype_iri = to_string(datatype)

    cond do
      datatype_iri == "http://www.w3.org/2001/XMLSchema#string" ->
        {:literal, :simple, to_string(value)}

      true ->
        {:literal, :typed, to_string(value), datatype_iri}
    end
  end

  defp term_to_ast(%RDF.Literal{literal: literal}) when is_binary(literal) do
    {:literal, :simple, literal}
  end

  defp term_to_ast(term) when is_binary(term) do
    # Assume it's a string literal
    {:literal, :simple, term}
  end

  defp term_to_ast(term) do
    # Pass through - might already be in AST format
    term
  end
end
