defmodule TripleStore.Adapter do
  @moduledoc """
  Adapter layer for RDF.ex integration.

  Provides conversion between RDF.ex term types (RDF.IRI, RDF.BlankNode,
  RDF.Literal) and the internal dictionary-encoded representation used
  by the triple store.

  ## Term Conversion

  Terms are converted to 64-bit integer IDs for efficient storage and
  comparison. The conversion uses the Dictionary layer which handles:

  - **URI/BNode/Literal**: Dictionary-allocated IDs with persistent storage
  - **xsd:integer/decimal/dateTime**: Inline-encoded IDs (no storage needed)

  ## Usage

  ```elixir
  # Convert RDF term to internal ID
  {:ok, id} = Adapter.term_to_id(manager, RDF.iri("http://example.org/s"))

  # Convert internal ID back to RDF term
  {:ok, term} = Adapter.id_to_term(db, id)

  # Batch conversion for efficiency
  {:ok, ids} = Adapter.terms_to_ids(manager, [s, p, o])
  {:ok, terms} = Adapter.ids_to_terms(db, [s_id, p_id, o_id])
  ```

  ## Inline Encoding

  Numeric literals within encodable range are stored directly in the ID
  without dictionary lookup:

  - **xsd:integer**: Values in [-2^59, 2^59)
  - **xsd:decimal**: ~14-15 significant digits
  - **xsd:dateTime**: Millisecond precision since Unix epoch

  For these types, `term_to_id/2` returns the inline-encoded ID without
  any database interaction, and `id_to_term/2` decodes without lookup.
  """

  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.IdToString
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Dictionary.ShardedManager
  alias TripleStore.Dictionary.StringToId

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "RDF term (IRI, blank node, or literal)"
  @type rdf_term :: RDF.IRI.t() | RDF.BlankNode.t() | RDF.Literal.t()

  @typedoc "RDF triple as 3-tuple of RDF terms"
  @type rdf_triple :: {RDF.IRI.t() | RDF.BlankNode.t(), RDF.IRI.t(), rdf_term()}

  @typedoc "Internal triple as 3-tuple of term IDs"
  @type internal_triple :: {term_id(), term_id(), term_id()}

  @typedoc "RDF quad as 4-tuple of RDF terms"
  @type rdf_quad ::
          {RDF.IRI.t() | RDF.BlankNode.t(), RDF.IRI.t(), rdf_term(),
           RDF.IRI.t() | RDF.BlankNode.t() | nil}

  @typedoc "Internal quad as 4-tuple of term IDs"
  @type internal_quad :: {term_id(), term_id(), term_id(), term_id()}

  @typedoc "64-bit term ID from dictionary encoding"
  @type term_id :: Dictionary.term_id()

  @typedoc "Dictionary manager process"
  @type manager :: Manager.manager() | ShardedManager.t()

  @typedoc "Database reference"
  @type db_ref :: reference()

  @batch_timeout :timer.minutes(5)
  @manager_batch_size 30_000
  @sharded_batch_size 100_000

  # ===========================================================================
  # Term to ID Conversion
  # ===========================================================================

  @doc """
  Converts an RDF.IRI to an internal term ID.

  Uses the dictionary manager to get or create an ID for the IRI.

  ## Arguments

  - `manager` - Dictionary manager process
  - `iri` - RDF.IRI struct

  ## Returns

  - `{:ok, term_id}` - The dictionary-encoded ID
  - `{:error, reason}` - On validation or allocation failure

  ## Examples

      iex> iri = RDF.iri("http://example.org/subject")
      iex> {:ok, id} = Adapter.from_rdf_iri(manager, iri)
      iex> is_integer(id)
      true
  """
  @spec from_rdf_iri(manager(), RDF.IRI.t()) :: {:ok, term_id()} | {:error, term()}
  def from_rdf_iri(manager, %RDF.IRI{} = iri) do
    get_or_create_id(manager, iri)
  end

  @doc """
  Converts an RDF.BlankNode to an internal term ID.

  Uses the dictionary manager to get or create an ID for the blank node.

  ## Arguments

  - `manager` - Dictionary manager process
  - `bnode` - RDF.BlankNode struct

  ## Returns

  - `{:ok, term_id}` - The dictionary-encoded ID
  - `{:error, reason}` - On validation or allocation failure

  ## Examples

      iex> bnode = RDF.bnode("b1")
      iex> {:ok, id} = Adapter.from_rdf_bnode(manager, bnode)
      iex> is_integer(id)
      true
  """
  @spec from_rdf_bnode(manager(), RDF.BlankNode.t()) :: {:ok, term_id()} | {:error, term()}
  def from_rdf_bnode(manager, %RDF.BlankNode{} = bnode) do
    get_or_create_id(manager, bnode)
  end

  @doc """
  Converts an RDF.Literal to an internal term ID.

  For inline-encodable literals (xsd:integer, xsd:decimal, xsd:dateTime
  within range), returns an inline-encoded ID without dictionary storage.
  Other literals use the dictionary manager for ID allocation.

  ## Arguments

  - `manager` - Dictionary manager process
  - `literal` - RDF.Literal struct

  ## Returns

  - `{:ok, term_id}` - The term ID (inline or dictionary-encoded)
  - `{:error, reason}` - On validation or allocation failure

  ## Examples

      iex> # Inline-encoded integer
      iex> {:ok, id} = Adapter.from_rdf_literal(manager, RDF.literal(42))
      iex> Dictionary.inline_encoded?(id)
      true

      iex> # Dictionary-allocated string literal
      iex> {:ok, id} = Adapter.from_rdf_literal(manager, RDF.literal("hello"))
      iex> Dictionary.inline_encoded?(id)
      false
  """
  @spec from_rdf_literal(manager(), RDF.Literal.t()) :: {:ok, term_id()} | {:error, term()}
  def from_rdf_literal(manager, %RDF.Literal{} = literal) do
    if Dictionary.inline_encodable?(literal) do
      encode_inline_literal(literal)
    else
      get_or_create_id(manager, literal)
    end
  end

  @doc """
  Converts any RDF term to an internal term ID.

  Dispatches to the appropriate conversion function based on term type.

  ## Arguments

  - `manager` - Dictionary manager process
  - `term` - Any RDF term (IRI, BlankNode, or Literal)

  ## Returns

  - `{:ok, term_id}` - The term ID
  - `{:error, :unsupported_term}` - For unsupported term types
  - `{:error, reason}` - On validation or allocation failure

  ## Examples

      iex> {:ok, id} = Adapter.term_to_id(manager, RDF.iri("http://example.org"))
      iex> is_integer(id)
      true
  """
  @spec term_to_id(manager(), rdf_term()) :: {:ok, term_id()} | {:error, term()}
  def term_to_id(manager, %RDF.IRI{} = iri), do: from_rdf_iri(manager, iri)
  def term_to_id(manager, %RDF.BlankNode{} = bnode), do: from_rdf_bnode(manager, bnode)
  def term_to_id(manager, %RDF.Literal{} = literal), do: from_rdf_literal(manager, literal)
  def term_to_id(_manager, _term), do: {:error, :unsupported_term}

  @doc """
  Converts multiple RDF terms to internal term IDs.

  Processes terms in order, returning early on first error.

  ## Arguments

  - `manager` - Dictionary manager process
  - `terms` - List of RDF terms

  ## Returns

  - `{:ok, [term_id]}` - List of term IDs in same order
  - `{:error, reason}` - On first validation or allocation failure

  ## Examples

      iex> terms = [RDF.iri("http://ex.org/s"), RDF.iri("http://ex.org/p"), RDF.literal("o")]
      iex> {:ok, [s_id, p_id, o_id]} = Adapter.terms_to_ids(manager, terms)
  """
  @spec terms_to_ids(manager(), [rdf_term()]) :: {:ok, [term_id()]} | {:error, term()}
  def terms_to_ids(_manager, []), do: {:ok, []}

  def terms_to_ids(manager, terms) when is_list(terms) do
    with {:ok, resolved_ids, dictionary_terms} <- split_inline_terms(terms),
         {:ok, dictionary_ids} <- resolve_dictionary_terms(manager, dictionary_terms) do
      resolved_ids
      |> merge_dictionary_ids(dictionary_terms, dictionary_ids)
      |> build_ordered_ids(length(terms))
    end
  end

  # ===========================================================================
  # ID to Term Conversion
  # ===========================================================================

  @doc """
  Converts an internal term ID to an RDF.IRI.

  Looks up the ID in the dictionary and returns the corresponding IRI.
  This function validates that the ID actually corresponds to an IRI.

  ## Arguments

  - `db` - Database reference
  - `id` - Term ID (must be a URI type)

  ## Returns

  - `{:ok, RDF.IRI.t()}` - The decoded IRI
  - `:not_found` - ID not in dictionary
  - `{:error, :type_mismatch}` - ID is not a URI type
  - `{:error, reason}` - On database error

  ## Examples

      iex> {:ok, iri} = Adapter.to_rdf_iri(db, uri_id)
      iex> iri
      %RDF.IRI{value: "http://example.org/subject"}
  """
  @spec to_rdf_iri(db_ref(), term_id()) :: {:ok, RDF.IRI.t()} | :not_found | {:error, term()}
  def to_rdf_iri(db, id) when is_integer(id) and id >= 0 do
    {type, _value} = Dictionary.decode_id(id)

    if type == :uri do
      case IdToString.lookup_term(db, id) do
        {:ok, %RDF.IRI{} = iri} -> {:ok, iri}
        :not_found -> :not_found
        {:error, _} = error -> error
      end
    else
      {:error, :type_mismatch}
    end
  end

  @doc """
  Converts an internal term ID to an RDF.BlankNode.

  Looks up the ID in the dictionary and returns the corresponding blank node.
  This function validates that the ID actually corresponds to a blank node.

  ## Arguments

  - `db` - Database reference
  - `id` - Term ID (must be a BNode type)

  ## Returns

  - `{:ok, RDF.BlankNode.t()}` - The decoded blank node
  - `:not_found` - ID not in dictionary
  - `{:error, :type_mismatch}` - ID is not a BNode type
  - `{:error, reason}` - On database error

  ## Examples

      iex> {:ok, bnode} = Adapter.to_rdf_bnode(db, bnode_id)
      iex> bnode
      %RDF.BlankNode{value: "b1"}
  """
  @spec to_rdf_bnode(db_ref(), term_id()) ::
          {:ok, RDF.BlankNode.t()} | :not_found | {:error, term()}
  def to_rdf_bnode(db, id) when is_integer(id) and id >= 0 do
    {type, _value} = Dictionary.decode_id(id)

    if type == :bnode do
      case IdToString.lookup_term(db, id) do
        {:ok, %RDF.BlankNode{} = bnode} -> {:ok, bnode}
        :not_found -> :not_found
        {:error, _} = error -> error
      end
    else
      {:error, :type_mismatch}
    end
  end

  @doc """
  Converts an internal term ID to an RDF.Literal.

  For inline-encoded IDs (xsd:integer, xsd:decimal, xsd:dateTime), decodes
  the value directly from the ID bits. For dictionary-allocated literals,
  looks up the ID in the dictionary.

  This function validates that the ID actually corresponds to a literal.

  ## Arguments

  - `db` - Database reference
  - `id` - Term ID (must be a Literal type)

  ## Returns

  - `{:ok, RDF.Literal.t()}` - The decoded literal
  - `:not_found` - ID not in dictionary (for dictionary-allocated)
  - `{:error, :type_mismatch}` - ID is not a Literal type
  - `{:error, reason}` - On database error

  ## Examples

      iex> # Inline-encoded integer
      iex> {:ok, lit} = Adapter.to_rdf_literal(db, integer_id)
      iex> RDF.Literal.value(lit)
      42

      iex> # Dictionary-allocated string
      iex> {:ok, lit} = Adapter.to_rdf_literal(db, string_id)
      iex> RDF.Literal.value(lit)
      "hello"
  """
  @spec to_rdf_literal(db_ref(), term_id()) ::
          {:ok, RDF.Literal.t()} | :not_found | {:error, term()}
  def to_rdf_literal(db, id) when is_integer(id) and id >= 0 do
    {type, _value} = Dictionary.decode_id(id)

    cond do
      type == :literal ->
        # Dictionary-allocated literal
        case IdToString.lookup_term(db, id) do
          {:ok, %RDF.Literal{} = lit} -> {:ok, lit}
          :not_found -> :not_found
          {:error, _} = error -> error
        end

      type in [:integer, :decimal, :datetime] ->
        # Inline-encoded literal
        IdToString.lookup_term(db, id)

      true ->
        {:error, :type_mismatch}
    end
  end

  @doc """
  Converts an internal term ID to the corresponding RDF term.

  Dispatches to the appropriate conversion function based on the ID's type tag.

  ## Arguments

  - `db` - Database reference
  - `id` - Any term ID

  ## Returns

  - `{:ok, rdf_term}` - The decoded RDF term
  - `:not_found` - ID not in dictionary
  - `{:error, reason}` - On database error

  ## Examples

      iex> {:ok, term} = Adapter.id_to_term(db, some_id)
      iex> term
      %RDF.IRI{value: "http://example.org"}
  """
  @spec id_to_term(db_ref(), term_id()) :: {:ok, rdf_term()} | :not_found | {:error, term()}
  def id_to_term(db, id) when is_integer(id) and id >= 0 do
    IdToString.lookup_term(db, id)
  end

  @doc """
  Converts multiple term IDs to RDF terms.

  Processes IDs in order, returning results for all IDs.

  ## Arguments

  - `db` - Database reference
  - `ids` - List of term IDs

  ## Returns

  - `{:ok, results}` - List of results, each being `{:ok, term}` or `:not_found`
  - `{:error, reason}` - On database error

  ## Examples

      iex> {:ok, results} = Adapter.ids_to_terms(db, [id1, id2, id3])
      iex> results
      [{:ok, %RDF.IRI{...}}, {:ok, %RDF.Literal{...}}, :not_found]
  """
  @spec ids_to_terms(db_ref(), [term_id()]) ::
          {:ok, [{:ok, rdf_term()} | :not_found]} | {:error, term()}
  def ids_to_terms(_db, []), do: {:ok, []}

  def ids_to_terms(db, ids) when is_list(ids) do
    IdToString.lookup_terms(db, ids)
  end

  # ===========================================================================
  # Triple Conversion
  # ===========================================================================

  @doc """
  Converts an RDF triple to internal representation.

  Converts each term in the triple `{s, p, o}` to its dictionary-encoded ID.
  The subject must be an IRI or BlankNode, predicate must be an IRI,
  and object can be any RDF term.

  ## Arguments

  - `manager` - Dictionary manager process
  - `triple` - RDF triple as `{subject, predicate, object}` tuple

  ## Returns

  - `{:ok, {s_id, p_id, o_id}}` - Internal triple with term IDs
  - `{:error, reason}` - On validation or allocation failure

  ## Examples

      iex> triple = {RDF.iri("http://ex.org/s"), RDF.iri("http://ex.org/p"), RDF.literal("o")}
      iex> {:ok, {s_id, p_id, o_id}} = Adapter.from_rdf_triple(manager, triple)
      iex> is_integer(s_id) and is_integer(p_id) and is_integer(o_id)
      true
  """
  @spec from_rdf_triple(manager(), rdf_triple()) ::
          {:ok, internal_triple()} | {:error, term()}
  def from_rdf_triple(manager, {subject, predicate, object}) do
    with {:ok, s_id} <- term_to_id(manager, subject),
         {:ok, p_id} <- term_to_id(manager, predicate),
         {:ok, o_id} <- term_to_id(manager, object) do
      {:ok, {s_id, p_id, o_id}}
    end
  end

  @doc """
  Converts an internal triple to RDF representation.

  Looks up each term ID in the dictionary and returns the corresponding
  RDF terms as a triple tuple.

  ## Arguments

  - `db` - Database reference
  - `triple` - Internal triple as `{s_id, p_id, o_id}` tuple

  ## Returns

  - `{:ok, {subject, predicate, object}}` - RDF triple
  - `:not_found` - One or more IDs not found in dictionary
  - `{:error, reason}` - On database error

  ## Examples

      iex> {:ok, {s, p, o}} = Adapter.to_rdf_triple(db, {s_id, p_id, o_id})
      iex> s
      %RDF.IRI{value: "http://ex.org/s"}
  """
  @spec to_rdf_triple(db_ref(), internal_triple()) ::
          {:ok, rdf_triple()} | :not_found | {:error, term()}
  def to_rdf_triple(db, {s_id, p_id, o_id}) do
    with {:ok, s} <- id_to_term(db, s_id),
         {:ok, p} <- id_to_term(db, p_id),
         {:ok, o} <- id_to_term(db, o_id) do
      {:ok, {s, p, o}}
    end
  end

  @doc """
  Converts multiple RDF triples to internal representation.

  Batch conversion for efficiency - processes all terms together.

  ## Arguments

  - `manager` - Dictionary manager process
  - `triples` - List of RDF triples

  ## Returns

  - `{:ok, [internal_triple]}` - List of internal triples
  - `{:error, reason}` - On first validation or allocation failure

  ## Examples

      iex> triples = [triple1, triple2, triple3]
      iex> {:ok, internal_triples} = Adapter.from_rdf_triples(manager, triples)
      iex> length(internal_triples)
      3
  """
  @spec from_rdf_triples(manager(), [rdf_triple()]) ::
          {:ok, [internal_triple()]} | {:error, term()}
  def from_rdf_triples(_manager, []), do: {:ok, []}

  def from_rdf_triples(manager, triples) when is_list(triples) do
    # Collect all terms for batch processing
    all_terms =
      Enum.flat_map(triples, fn {s, p, o} -> [s, p, o] end)

    case terms_to_ids(manager, all_terms) do
      {:ok, all_ids} ->
        # Reassemble into triples
        internal_triples =
          all_ids
          |> Enum.chunk_every(3)
          |> Enum.map(fn [s_id, p_id, o_id] -> {s_id, p_id, o_id} end)

        {:ok, internal_triples}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Converts multiple internal triples to RDF representation.

  Batch conversion for efficiency.

  ## Arguments

  - `db` - Database reference
  - `triples` - List of internal triples

  ## Returns

  - `{:ok, [rdf_triple]}` - List of RDF triples
  - `{:error, reason}` - On database error

  Note: Individual triples with missing IDs will have `:not_found` in their position.
  """
  @spec to_rdf_triples(db_ref(), [internal_triple()]) ::
          {:ok, [rdf_triple() | :not_found]} | {:error, term()}
  def to_rdf_triples(_db, []), do: {:ok, []}

  def to_rdf_triples(db, triples) when is_list(triples) do
    # Collect all IDs for batch lookup
    all_ids =
      Enum.flat_map(triples, fn {s_id, p_id, o_id} -> [s_id, p_id, o_id] end)

    case ids_to_terms(db, all_ids) do
      {:ok, all_results} ->
        # Reassemble into triples
        rdf_triples =
          all_results
          |> Enum.chunk_every(3)
          |> Enum.map(&results_to_triple/1)

        {:ok, rdf_triples}

      {:error, _} = error ->
        error
    end
  end

  defp results_to_triple([{:ok, s}, {:ok, p}, {:ok, o}]), do: {s, p, o}
  defp results_to_triple(_), do: :not_found

  # ===========================================================================
  # Quad Conversion
  # ===========================================================================

  @doc """
  Converts an RDF quad to internal representation.

  Converts each term in the quad `{s, p, o, g}` to its dictionary-encoded ID.
  The subject must be an IRI or BlankNode, predicate must be an IRI,
  object can be any RDF term, and graph can be an IRI, BlankNode, or nil
  (for the default graph).

  When the graph is nil, it is converted to the default graph ID (0).

  ## Arguments

  - `manager` - Dictionary manager process
  - `quad` - RDF quad as `{subject, predicate, object, graph}` tuple

  ## Returns

  - `{:ok, {s_id, p_id, o_id, g_id}}` - Internal quad with term IDs
  - `{:error, reason}` - On validation or allocation failure

  ## Examples

      iex> quad = {RDF.iri("http://ex.org/s"), RDF.iri("http://ex.org/p"),
      ...>         RDF.literal("o"), RDF.iri("http://ex.org/g")}
      iex> {:ok, {s_id, p_id, o_id, g_id}} = Adapter.from_rdf_quad(manager, quad)
      iex> is_integer(s_id) and is_integer(p_id) and is_integer(o_id) and is_integer(g_id)
      true

      iex> # Default graph (nil graph becomes ID 0)
      iex> quad = {RDF.iri("http://ex.org/s"), RDF.iri("http://ex.org/p"),
      ...>         RDF.literal("o"), nil}
      iex> {:ok, {_, _, _, g_id}} = Adapter.from_rdf_quad(manager, quad)
      iex> g_id
      0
  """
  @spec from_rdf_quad(manager(), rdf_quad()) ::
          {:ok, internal_quad()} | {:error, term()}
  def from_rdf_quad(manager, {subject, predicate, object, graph}) do
    with {:ok, s_id} <- term_to_id(manager, subject),
         {:ok, p_id} <- term_to_id(manager, predicate),
         {:ok, o_id} <- term_to_id(manager, object),
         {:ok, g_id} <- graph_to_id(manager, graph) do
      {:ok, {s_id, p_id, o_id, g_id}}
    end
  end

  @doc """
  Converts an internal quad to RDF representation.

  Looks up each term ID in the dictionary and returns the corresponding
  RDF terms as a quad tuple.

  ## Arguments

  - `db` - Database reference
  - `quad` - Internal quad as `{s_id, p_id, o_id, g_id}` tuple

  ## Returns

  - `{:ok, {subject, predicate, object, graph}}` - RDF quad
  - `:not_found` - One or more IDs not found in dictionary
  - `{:error, reason}` - On database error

  ## Examples

      iex> {:ok, {s, p, o, g}} = Adapter.to_rdf_quad(db, {s_id, p_id, o_id, g_id})
      iex> s
      %RDF.IRI{value: "http://ex.org/s"}

      iex> # Default graph (ID 0 becomes nil)
      iex> {:ok, {_, _, _, g}} = Adapter.to_rdf_quad(db, {s_id, p_id, o_id, 0})
      iex> g
      nil
  """
  @spec to_rdf_quad(db_ref(), internal_quad()) ::
          {:ok, rdf_quad()} | :not_found | {:error, term()}
  def to_rdf_quad(db, {s_id, p_id, o_id, g_id}) do
    with {:ok, s} <- id_to_term(db, s_id),
         {:ok, p} <- id_to_term(db, p_id),
         {:ok, o} <- id_to_term(db, o_id),
         {:ok, g} <- id_to_graph(db, g_id) do
      {:ok, {s, p, o, g}}
    end
  end

  @doc """
  Converts multiple RDF quads to internal representation.

  Batch conversion for efficiency - processes all terms together.

  ## Arguments

  - `manager` - Dictionary manager process
  - `quads` - List of RDF quads

  ## Returns

  - `{:ok, [internal_quad]}` - List of internal quads
  - `{:error, reason}` - On first validation or allocation failure

  ## Examples

      iex> quads = [quad1, quad2, quad3]
      iex> {:ok, internal_quads} = Adapter.from_rdf_quads(manager, quads)
      iex> length(internal_quads)
      3
  """
  @spec from_rdf_quads(manager(), [rdf_quad()]) ::
          {:ok, [internal_quad()]} | {:error, term()}
  def from_rdf_quads(_manager, []), do: {:ok, []}

  def from_rdf_quads(manager, quads) when is_list(quads) do
    # Optimize by batching terms while handling nil graphs
    # Collect all terms with their positions for reconstruction
    {term_list, graph_refs, positions} =
      Enum.reduce(quads, {[], [], []}, fn {s, p, o, g}, {terms_acc, graphs_acc, pos_acc} ->
        pos = length(terms_acc)

        {
          terms_acc ++ [s, p, o],
          graphs_acc ++ [g],
          pos_acc ++ [pos, pos + 1, pos + 2]
        }
      end)

    # Batch encode all terms (with chunking to avoid timeout)
    with {:ok, ids} <- terms_to_ids(manager, term_list),
         # Encode graphs (nil becomes 0)
         {:ok, graph_ids} <- encode_graphs(manager, graph_refs) do
      # Reconstruct quads from IDs
      internal_quads =
        positions
        |> Enum.chunk_every(3)
        |> Enum.with_index()
        |> Enum.map(fn {[s_pos, p_pos, o_pos], idx} ->
          s_id = Enum.at(ids, s_pos)
          p_id = Enum.at(ids, p_pos)
          o_id = Enum.at(ids, o_pos)
          g_id = Enum.at(graph_ids, idx)
          {s_id, p_id, o_id, g_id}
        end)

      {:ok, internal_quads}
    end
  end

  @doc """
  Converts multiple internal quads to RDF representation.

  Batch conversion for efficiency.

  ## Arguments

  - `db` - Database reference
  - `quads` - List of internal quads

  ## Returns

  - `{:ok, [rdf_quad]}` - List of RDF quads
  - `{:error, reason}` - On database error

  Note: Individual quads with missing IDs will have `:not_found` in their position.
  """
  @spec to_rdf_quads(db_ref(), [internal_quad()]) ::
          {:ok, [rdf_quad() | :not_found]} | {:error, term()}
  def to_rdf_quads(_db, []), do: {:ok, []}

  def to_rdf_quads(db, quads) when is_list(quads) do
    # Process each quad individually since graph ID 0 (default graph) needs special handling
    Enum.reduce_while(quads, {:ok, []}, fn quad, {:ok, acc} ->
      case to_rdf_quad(db, quad) do
        {:ok, rdf_quad} ->
          {:cont, {:ok, [rdf_quad | acc]}}

        :not_found ->
          {:cont, {:ok, [:not_found | acc]}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, quads_reversed} -> {:ok, Enum.reverse(quads_reversed)}
      error -> error
    end
  end

  # ===========================================================================
  # Graph Conversion
  # ===========================================================================

  @doc """
  Converts an RDF.Graph to a stream of internal triples.

  Efficiently converts all triples in a graph to internal representation
  using batch term conversion for better performance.

  ## Arguments

  - `manager` - Dictionary manager process
  - `graph` - RDF.Graph to convert

  ## Returns

  - `{:ok, [internal_triple]}` - List of internal triples
  - `{:error, reason}` - On validation or allocation failure

  ## Examples

      iex> graph = RDF.Graph.new([triple1, triple2, triple3])
      iex> {:ok, internal_triples} = Adapter.from_rdf_graph(manager, graph)
      iex> length(internal_triples)
      3
  """
  @spec from_rdf_graph(manager(), RDF.Graph.t()) ::
          {:ok, [internal_triple()]} | {:error, term()}
  def from_rdf_graph(manager, %RDF.Graph{} = graph) do
    triples = RDF.Graph.triples(graph)
    from_rdf_triples(manager, triples)
  end

  @doc """
  Converts a list of internal triples to an RDF.Graph.

  Creates a new RDF.Graph containing all the decoded triples.
  Triples with missing term IDs are skipped.

  ## Arguments

  - `db` - Database reference
  - `triples` - List of internal triples
  - `opts` - Optional graph options (name, base_iri, prefixes)

  ## Returns

  - `{:ok, RDF.Graph.t()}` - The constructed graph
  - `{:error, reason}` - On database error

  ## Examples

      iex> {:ok, graph} = Adapter.to_rdf_graph(db, internal_triples)
      iex> RDF.Graph.triple_count(graph)
      3
  """
  @spec to_rdf_graph(db_ref(), [internal_triple()], keyword()) ::
          {:ok, RDF.Graph.t()} | {:error, term()}
  def to_rdf_graph(db, triples, opts \\ [])

  def to_rdf_graph(_db, [], opts) do
    {:ok, RDF.Graph.new(opts)}
  end

  def to_rdf_graph(db, triples, opts) when is_list(triples) do
    case to_rdf_triples(db, triples) do
      {:ok, rdf_triples} ->
        # Filter out :not_found entries
        valid_triples = Enum.filter(rdf_triples, &is_tuple/1)
        {:ok, RDF.Graph.new(valid_triples, opts)}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Converts an RDF.Graph to a stream of internal triples (lazy evaluation).

  Unlike `from_rdf_graph/2`, this returns a Stream that converts triples
  on demand. Useful for very large graphs where you want to process
  incrementally.

  Note: Each triple is converted individually, so this is less efficient
  than `from_rdf_graph/2` for small graphs. Use for large graphs or when
  you need streaming semantics.

  ## Arguments

  - `manager` - Dictionary manager process
  - `graph` - RDF.Graph to convert

  ## Returns

  - Stream of `{:ok, internal_triple}` or `{:error, reason}` tuples

  ## Examples

      iex> graph = RDF.Graph.new([triple1, triple2, triple3])
      iex> stream = Adapter.stream_from_rdf_graph(manager, graph)
      iex> Enum.take(stream, 2)
      [{:ok, {1, 2, 3}}, {:ok, {1, 2, 4}}]
  """
  @spec stream_from_rdf_graph(manager(), RDF.Graph.t()) :: Enumerable.t()
  def stream_from_rdf_graph(manager, %RDF.Graph{} = graph) do
    graph
    |> RDF.Graph.triples()
    |> Stream.map(fn triple -> from_rdf_triple(manager, triple) end)
  end

  # ===========================================================================
  # Lookup-Only Functions (for read-only operations)
  # ===========================================================================

  @doc """
  Looks up the ID for an RDF term without creating if missing.

  This is a read-only operation that checks if a term already exists
  in the dictionary. For inline-encodable literals, returns the inline
  ID without database access.

  ## Arguments

  - `db` - Database reference
  - `term` - RDF term to look up

  ## Returns

  - `{:ok, term_id}` - The existing term ID
  - `:not_found` - Term not in dictionary
  - `{:error, :requires_manager}` - For non-inline literals (see below)
  - `{:error, reason}` - On validation or database error

  ## Literal Handling

  This function has special behavior for literals:

  - **Inline-encodable literals** (xsd:integer, xsd:decimal, xsd:dateTime):
    Returns `{:ok, inline_id}` directly without database lookup.

  - **Non-inline literals** (strings, other datatypes): Returns
    `{:error, :requires_manager}`. These literals require the Dictionary
    Manager for proper lookup. Use `Manager.get_or_create_id/2` instead.

  This design avoids database lookups for inline-encodable literals
  while making the limitation explicit for other literal types.

  ## Examples

      iex> {:ok, id} = Adapter.lookup_term_id(db, RDF.iri("http://example.org"))
      iex> Adapter.lookup_term_id(db, RDF.iri("http://unknown.org"))
      :not_found

      iex> # Inline literals work directly
      iex> {:ok, id} = Adapter.lookup_term_id(db, RDF.integer(42))

      iex> # Non-inline literals require the manager
      iex> Adapter.lookup_term_id(db, ~L"hello")
      {:error, :requires_manager}
  """
  @spec lookup_term_id(db_ref(), rdf_term()) :: {:ok, term_id()} | :not_found | {:error, term()}
  def lookup_term_id(_db, %RDF.Literal{} = literal) do
    if Dictionary.inline_encodable?(literal) do
      encode_inline_literal(literal)
    else
      # For non-inline literals, we'd need the DB but this function signature
      # doesn't support it for literals. We return not_found to indicate
      # the caller should use Manager.get_or_create_id instead.
      # This is a design decision - inline literals don't need lookup.
      {:error, :requires_manager}
    end
  end

  def lookup_term_id(db, term) do
    StringToId.lookup_id(db, term)
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  # Converts an RDF graph term to a graph ID.
  # Handles IRI, BlankNode, and nil (for default graph).
  # Nil graph converts to ID 0 (default graph).
  defp graph_to_id(_manager, nil), do: {:ok, 0}
  defp graph_to_id(manager, %RDF.IRI{} = iri), do: from_rdf_iri(manager, iri)
  defp graph_to_id(manager, %RDF.BlankNode{} = bnode), do: from_rdf_bnode(manager, bnode)

  # Batch encode multiple graphs, with chunking to avoid GenServer timeout
  defp encode_graphs(manager, graphs) do
    {graph_markers, non_nil_graphs} =
      Enum.map_reduce(graphs, [], fn
        nil, acc -> {0, acc}
        graph, acc -> {:graph, [graph | acc]}
      end)

    non_nil_graphs = Enum.reverse(non_nil_graphs)

    with {:ok, encoded_non_nil_graphs} <- terms_to_ids(manager, non_nil_graphs) do
      {graph_ids, []} =
        Enum.map_reduce(graph_markers, encoded_non_nil_graphs, fn
          0, remaining_ids ->
            {0, remaining_ids}

          :graph, [graph_id | rest] ->
            {graph_id, rest}
        end)

      {:ok, graph_ids}
    end
  end

  # Converts a graph ID to an RDF graph term.
  # ID 0 converts to nil (default graph).
  # IDs > 0 convert to IRI or BlankNode.
  defp id_to_graph(_db, 0), do: {:ok, nil}
  defp id_to_graph(db, id) when is_integer(id) and id > 0, do: id_to_term(db, id)

  # Encode an inline-encodable literal to its ID
  defp encode_inline_literal(%RDF.Literal{literal: %RDF.XSD.Integer{value: value}})
       when is_integer(value) do
    Dictionary.encode_integer(value)
  end

  defp encode_inline_literal(%RDF.Literal{literal: %RDF.XSD.Decimal{value: %Decimal{} = value}}) do
    Dictionary.encode_decimal(value)
  end

  defp encode_inline_literal(%RDF.Literal{literal: %RDF.XSD.DateTime{value: %DateTime{} = value}}) do
    Dictionary.encode_datetime(value)
  end

  defp encode_inline_literal(_literal) do
    {:error, :not_inline_encodable}
  end

  defp split_inline_terms(terms) do
    terms
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, {%{}, []}}, fn {term, idx}, {:ok, {resolved_ids, dictionary_terms}} ->
      if Dictionary.inline_encodable?(term) do
        case encode_inline_literal(term) do
          {:ok, id} ->
            {:cont, {:ok, {Map.put(resolved_ids, idx, id), dictionary_terms}}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      else
        {:cont, {:ok, {resolved_ids, [{idx, term} | dictionary_terms]}}}
      end
    end)
    |> case do
      {:ok, {resolved_ids, dictionary_terms}} ->
        {:ok, resolved_ids, Enum.reverse(dictionary_terms)}

      error ->
        error
    end
  end

  defp resolve_dictionary_terms(_manager, []), do: {:ok, []}

  defp resolve_dictionary_terms(manager, dictionary_terms) do
    batch_size = manager_batch_size(manager)

    dictionary_terms
    |> Enum.map(fn {_idx, term} -> term end)
    |> Enum.chunk_every(batch_size)
    |> Enum.reduce_while({:ok, []}, fn chunk, {:ok, acc_ids} ->
      case get_or_create_ids(manager, chunk, @batch_timeout) do
        {:ok, chunk_ids} -> {:cont, {:ok, acc_ids ++ chunk_ids}}
        error -> {:halt, error}
      end
    end)
  end

  defp merge_dictionary_ids(resolved_ids, dictionary_terms, dictionary_ids) do
    Enum.zip(dictionary_terms, dictionary_ids)
    |> Enum.reduce(resolved_ids, fn {{idx, _term}, id}, acc ->
      Map.put(acc, idx, id)
    end)
  end

  defp build_ordered_ids(resolved_ids, term_count) do
    ids =
      for idx <- 0..(term_count - 1) do
        Map.fetch!(resolved_ids, idx)
      end

    {:ok, ids}
  rescue
    KeyError ->
      {:error, :batch_resolution_failed}
  end

  defp get_or_create_id(manager, term) do
    case manager_kind(manager) do
      :sharded -> ShardedManager.get_or_create_id(manager, term)
      :manager -> Manager.get_or_create_id(manager, term)
    end
  end

  defp get_or_create_ids(manager, terms, timeout) do
    case manager_kind(manager) do
      :sharded ->
        ShardedManager.get_or_create_ids(manager, terms, timeout: timeout)

      :manager ->
        GenServer.call(manager, {:get_or_create_ids, terms}, timeout)
    end
  end

  defp manager_batch_size(manager) do
    case manager_kind(manager) do
      :sharded -> @sharded_batch_size
      :manager -> @manager_batch_size
    end
  end

  defp manager_kind(manager) do
    case resolve_manager_pid(manager) do
      pid when is_pid(pid) ->
        case Process.info(pid, :dictionary) do
          {:dictionary, dict} ->
            case Keyword.get(dict, :"$initial_call") do
              {:supervisor, ShardedManager, 1} -> :sharded
              _ -> :manager
            end

          _ ->
            :manager
        end

      _ ->
        :manager
    end
  end

  defp resolve_manager_pid(manager) when is_pid(manager), do: manager
  defp resolve_manager_pid(manager), do: GenServer.whereis(manager)
end
