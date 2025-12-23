defmodule TripleStore.SPARQL.Executor do
  @moduledoc """
  SPARQL query executor using iterator-based lazy evaluation.

  This module executes SPARQL algebra trees against the triple store,
  producing streams of solution bindings. It uses Elixir Streams for
  lazy evaluation, providing natural backpressure and memory-efficient
  processing of large result sets.

  ## Architecture

  The executor follows the iterator model where each algebra node type
  has a corresponding execution strategy:

  - **BGP**: Index nested loop join over triple patterns
  - **Join**: Hash join or nested loop join
  - **LeftJoin**: Left outer join for OPTIONAL
  - **Union**: Stream concatenation
  - **Filter**: Stream filtering with expression evaluation
  - **Project**: Variable projection
  - **Distinct/Reduced**: Deduplication
  - **OrderBy/Slice**: Sorting and pagination

  ## Bindings

  Solution bindings are represented as maps from variable names (strings)
  to RDF terms. The executor produces streams of these binding maps.

  ## Examples

      # Execute a BGP against the database
      {:ok, stream} = Executor.execute_bgp(db, bgp_patterns, %{})
      results = Enum.to_list(stream)

      # Each result is a binding map like:
      # %{"x" => {:named_node, "http://example.org/Alice"}, "name" => {:literal, :simple, "Alice"}}

  """

  alias TripleStore.Index
  alias TripleStore.Dictionary
  alias TripleStore.SPARQL.Optimizer


  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "A solution binding - map from variable names to RDF terms"
  @type binding :: %{String.t() => term()}

  @typedoc "A stream of solution bindings"
  @type binding_stream :: Enumerable.t()

  @typedoc "Database reference"
  @type db :: reference()

  @typedoc "Dictionary manager reference"
  @type dict_manager :: GenServer.server()

  @typedoc "Execution context containing database and dictionary references"
  @type context :: %{
          db: db(),
          dict_manager: dict_manager()
        }

  # ===========================================================================
  # BGP Execution (Task 2.4.1)
  # ===========================================================================

  @doc """
  Executes a Basic Graph Pattern against the database.

  Takes a list of triple patterns and returns a stream of solution bindings
  that satisfy all patterns. Patterns are reordered by selectivity before
  execution using the optimizer's BGP reordering.

  ## Algorithm

  1. If patterns is empty, return a stream with single empty binding
  2. Reorder patterns by selectivity (most selective first)
  3. Execute first pattern against the database
  4. For each subsequent pattern, extend bindings using nested loop join

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `patterns` - List of triple patterns `{:triple, s, p, o}`
  - `initial_binding` - Initial variable bindings (default: empty map)

  ## Returns

  - `{:ok, stream}` - Stream of binding maps
  - `{:error, reason}` - On failure

  ## Examples

      patterns = [
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"}}
      ]
      {:ok, stream} = Executor.execute_bgp(ctx, patterns)
      Enum.to_list(stream)
      # => [%{"s" => {:named_node, "..."}, "name" => {:literal, :simple, "Alice"}}, ...]

  """
  @spec execute_bgp(context(), list(), binding()) :: {:ok, binding_stream()} | {:error, term()}
  def execute_bgp(ctx, patterns, initial_binding \\ %{})

  # Empty pattern returns single empty binding (SPARQL semantics)
  def execute_bgp(_ctx, [], initial_binding) do
    {:ok, Stream.iterate(initial_binding, & &1) |> Stream.take(1)}
  end

  def execute_bgp(ctx, patterns, initial_binding) when is_list(patterns) do
    # Reorder patterns by selectivity
    reordered = Optimizer.reorder_bgp_patterns({:bgp, patterns})
    {:bgp, ordered_patterns} = reordered

    # Start with a stream containing just the initial binding
    initial_stream = Stream.iterate(initial_binding, & &1) |> Stream.take(1)

    # Execute patterns sequentially using nested loop join
    result_stream =
      Enum.reduce(ordered_patterns, initial_stream, fn pattern, stream ->
        {:ok, new_stream} = extend_bindings(ctx, stream, pattern)
        new_stream
      end)

    {:ok, result_stream}
  end

  @doc """
  Executes a single triple pattern against the database with given bindings.

  For each binding in the input stream, substitutes bound variables into
  the pattern and queries the index. Returns a stream of extended bindings.

  ## Arguments

  - `ctx` - Execution context
  - `binding_stream` - Stream of current bindings
  - `pattern` - Triple pattern `{:triple, s, p, o}`

  ## Returns

  - `{:ok, stream}` - Stream of extended binding maps
  - `{:error, reason}` - On failure
  """
  @spec execute_pattern(context(), binding_stream(), tuple()) ::
          {:ok, binding_stream()} | {:error, term()}
  def execute_pattern(ctx, binding_stream, pattern) do
    extend_bindings(ctx, binding_stream, pattern)
  end

  # ===========================================================================
  # Pattern Execution (Internal)
  # ===========================================================================

  # Extends each binding in the stream by matching a pattern
  defp extend_bindings(ctx, binding_stream, {:triple, s, p, o}) do
    result_stream =
      Stream.flat_map(binding_stream, fn binding ->
        case execute_single_pattern(ctx, binding, s, p, o) do
          {:ok, matches} -> matches
          {:error, _} -> []
        end
      end)

    {:ok, result_stream}
  end

  # Execute a single triple pattern with a specific binding
  defp execute_single_pattern(ctx, binding, s, p, o) do
    %{db: db, dict_manager: dict_manager} = ctx

    # Substitute bound variables and encode terms
    with {:ok, s_pattern} <- term_to_index_pattern(s, binding, dict_manager),
         {:ok, p_pattern} <- term_to_index_pattern(p, binding, dict_manager),
         {:ok, o_pattern} <- term_to_index_pattern(o, binding, dict_manager) do
      # Check if any bound term was not found in the dictionary
      # If so, the pattern cannot match anything
      if has_not_found?([s_pattern, p_pattern, o_pattern]) do
        {:ok, empty_stream()}
      else
        # Build index pattern
        index_pattern = {s_pattern, p_pattern, o_pattern}

        # Query the index
        case Index.lookup(db, index_pattern) do
          {:ok, triple_stream} ->
            # Convert matching triples to bindings
            binding_stream =
              Stream.flat_map(triple_stream, fn {s_id, p_id, o_id} ->
                case extend_binding_from_match(binding, s, p, o, s_id, p_id, o_id, dict_manager) do
                  {:ok, new_binding} -> [new_binding]
                  {:error, _} -> []
                end
              end)

            {:ok, binding_stream}

          {:error, _} = error ->
            error
        end
      end
    end
  end

  # Check if any pattern element is a "not found" marker
  defp has_not_found?(patterns) do
    Enum.any?(patterns, fn
      {:bound, :not_found} -> true
      _ -> false
    end)
  end

  # ===========================================================================
  # Term/Pattern Conversion
  # ===========================================================================

  # Convert an algebra term to an index pattern element
  # Returns {:bound, id} for concrete terms or :var for variables
  defp term_to_index_pattern({:variable, name}, binding, dict_manager) do
    case Map.get(binding, name) do
      nil ->
        # Unbound variable
        {:ok, :var}

      term ->
        # Variable is bound - encode the term
        term_to_bound_pattern(term, dict_manager)
    end
  end

  defp term_to_index_pattern(term, _binding, dict_manager) do
    # Concrete term - encode it
    term_to_bound_pattern(term, dict_manager)
  end

  # Convert a concrete term to a bound index pattern
  defp term_to_bound_pattern(term, dict_manager) do
    case encode_term(term, dict_manager) do
      {:ok, id} -> {:ok, {:bound, id}}
      # Term not in dictionary - no matches possible
      :not_found -> {:ok, {:bound, :not_found}}
      {:error, _} = error -> error
    end
  end

  # Encode an RDF term to a dictionary ID (lookup only, don't create)
  defp encode_term({:named_node, uri}, dict_manager) do
    rdf_term = RDF.iri(uri)
    lookup_term_id(dict_manager, rdf_term)
  end

  defp encode_term({:blank_node, name}, dict_manager) do
    rdf_term = RDF.bnode(name)
    lookup_term_id(dict_manager, rdf_term)
  end

  defp encode_term({:literal, :simple, value}, dict_manager) do
    rdf_term = RDF.literal(value)
    lookup_term_id(dict_manager, rdf_term)
  end

  defp encode_term({:literal, :lang, value, lang}, dict_manager) do
    rdf_term = RDF.literal(value, language: lang)
    lookup_term_id(dict_manager, rdf_term)
  end

  defp encode_term({:literal, :typed, value, datatype}, dict_manager) do
    # Check for inline-encodable types first
    case try_inline_encode(value, datatype) do
      {:ok, id} ->
        {:ok, id}

      :not_inline ->
        rdf_term = RDF.literal(value, datatype: datatype)
        lookup_term_id(dict_manager, rdf_term)
    end
  end

  defp encode_term(_term, _dict_manager) do
    {:error, :unsupported_term_type}
  end

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

  # Lookup a term ID from the dictionary (read-only)
  defp lookup_term_id(dict_manager, rdf_term) do
    # Get the database reference from the manager
    case GenServer.call(dict_manager, :get_db) do
      {:ok, db} ->
        case TripleStore.Dictionary.StringToId.lookup_id(db, rdf_term) do
          {:ok, id} -> {:ok, id}
          :not_found -> :not_found
          {:error, _} = error -> error
        end

      {:error, _} = error ->
        error
    end
  end

  # ===========================================================================
  # Binding Extension
  # ===========================================================================

  # Extend a binding with values from a matched triple
  defp extend_binding_from_match(binding, s, p, o, s_id, p_id, o_id, dict_manager) do
    with {:ok, binding1} <- maybe_bind(binding, s, s_id, dict_manager),
         {:ok, binding2} <- maybe_bind(binding1, p, p_id, dict_manager),
         {:ok, binding3} <- maybe_bind(binding2, o, o_id, dict_manager) do
      {:ok, binding3}
    end
  end

  # Bind a variable to a term ID, or verify consistency if already bound
  defp maybe_bind(binding, {:variable, name}, term_id, dict_manager) do
    case Map.get(binding, name) do
      nil ->
        # Variable not bound - decode and bind
        case decode_term(term_id, dict_manager) do
          {:ok, term} -> {:ok, Map.put(binding, name, term)}
          {:error, _} = error -> error
        end

      existing_term ->
        # Variable already bound - verify it matches
        case encode_term(existing_term, dict_manager) do
          {:ok, existing_id} when existing_id == term_id ->
            {:ok, binding}

          {:ok, _different_id} ->
            {:error, :binding_mismatch}

          :not_found ->
            {:error, :binding_mismatch}

          {:error, _} = error ->
            error
        end
    end
  end

  # Non-variable terms don't need binding
  defp maybe_bind(binding, _term, _term_id, _dict_manager) do
    {:ok, binding}
  end

  # Decode a term ID back to an RDF term
  defp decode_term(term_id, dict_manager) do
    # Check for inline-encoded types first
    if Dictionary.inline_encoded?(term_id) do
      decode_inline_term(term_id)
    else
      # Dictionary lookup required
      case GenServer.call(dict_manager, :get_db) do
        {:ok, db} ->
          case TripleStore.Dictionary.IdToString.lookup_term(db, term_id) do
            {:ok, rdf_term} -> {:ok, rdf_term_to_algebra(rdf_term)}
            :not_found -> {:error, :term_not_found}
            {:error, _} = error -> error
          end

        {:error, _} = error ->
          error
      end
    end
  end

  # Decode inline-encoded terms
  defp decode_inline_term(term_id) do
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

  # Convert RDF.ex term to algebra term representation
  defp rdf_term_to_algebra(%RDF.IRI{value: uri}) do
    {:named_node, uri}
  end

  defp rdf_term_to_algebra(%RDF.BlankNode{value: name}) do
    {:blank_node, name}
  end

  defp rdf_term_to_algebra(%RDF.Literal{literal: %{language: lang}} = lit)
       when not is_nil(lang) do
    {:literal, :lang, RDF.Literal.value(lit), lang}
  end

  defp rdf_term_to_algebra(%RDF.Literal{literal: literal} = lit) do
    datatype = RDF.Literal.datatype_id(lit)

    if datatype == RDF.XSD.String.id() do
      {:literal, :simple, RDF.Literal.value(lit)}
    else
      value =
        case literal do
          %{value: v} when is_binary(v) -> v
          _ -> RDF.Literal.lexical(lit)
        end

      {:literal, :typed, value, to_string(datatype)}
    end
  end

  # ===========================================================================
  # Join Execution (Task 2.4.2)
  # ===========================================================================

  # Threshold for switching from nested loop to hash join
  @hash_join_threshold 100

  @doc """
  Executes an inner join between two binding streams.

  Automatically selects between nested loop join and hash join based on
  the estimated size of the inputs. For small inputs (<#{@hash_join_threshold}),
  uses nested loop join. For larger inputs, uses hash join.

  ## Algorithm Selection

  - **Nested Loop Join**: O(n*m) time, O(1) space. Best for small inputs or
    when early termination is likely.
  - **Hash Join**: O(n+m) time, O(n) space. Best for larger inputs where
    the build side fits in memory.

  ## Arguments

  - `left` - Left binding stream (or list)
  - `right` - Right binding stream (or list)
  - `opts` - Options:
    - `:strategy` - Force `:nested_loop` or `:hash` strategy (default: auto-select)

  ## Returns

  Stream of joined bindings where compatible bindings are merged.

  ## Examples

      left = [%{"x" => {:named_node, "http://ex.org/A"}}]
      right = [%{"x" => {:named_node, "http://ex.org/A"}, "y" => {:literal, :simple, "1"}}]
      result = Executor.join(left, right) |> Enum.to_list()
      # => [%{"x" => {:named_node, "http://ex.org/A"}, "y" => {:literal, :simple, "1"}}]

  """
  @spec join(Enumerable.t(), Enumerable.t(), keyword()) :: binding_stream()
  def join(left, right, opts \\ []) do
    strategy = Keyword.get(opts, :strategy, :auto)

    case strategy do
      :nested_loop ->
        nested_loop_join(left, right)

      :hash ->
        hash_join(left, right)

      :auto ->
        # For streams, we need to materialize to determine size
        # Default to hash join which handles both cases well
        hash_join(left, right)
    end
  end

  @doc """
  Executes a nested loop join between two binding collections.

  For each binding on the left, iterates through all bindings on the right
  and emits merged bindings where the values are compatible.

  This is O(n*m) in time complexity but O(1) in space (not counting input
  materialization). Best suited for small inputs or when the right side
  can be efficiently re-scanned.

  ## Arguments

  - `left` - Left binding stream/list
  - `right` - Right binding stream/list (will be materialized)

  ## Returns

  Stream of joined bindings.
  """
  @spec nested_loop_join(Enumerable.t(), Enumerable.t()) :: binding_stream()
  def nested_loop_join(left, right) do
    # Materialize right side to allow multiple iterations
    right_list = Enum.to_list(right)

    Stream.flat_map(left, fn left_binding ->
      Enum.flat_map(right_list, fn right_binding ->
        case merge_bindings(left_binding, right_binding) do
          {:ok, merged} -> [merged]
          :incompatible -> []
        end
      end)
    end)
  end

  @doc """
  Executes a hash join between two binding collections.

  Builds a hash table on the smaller (build) side, then probes with the
  larger (probe) side. This is O(n+m) in time but O(n) in space.

  The join key is computed from the shared variables between the two sides.
  Bindings are grouped by their join key values in a hash table, then
  probed for compatible matches.

  ## Arguments

  - `left` - Left binding stream/list (build side)
  - `right` - Right binding stream/list (probe side)

  ## Returns

  Stream of joined bindings.
  """
  @spec hash_join(Enumerable.t(), Enumerable.t()) :: binding_stream()
  def hash_join(left, right) do
    # Materialize both sides to find shared variables and build hash table
    left_list = Enum.to_list(left)
    right_list = Enum.to_list(right)

    # Handle empty cases
    if Enum.empty?(left_list) or Enum.empty?(right_list) do
      empty_stream()
    else
      # Find shared variables between the two sides
      left_vars = shared_variables(left_list)
      right_vars = shared_variables(right_list)
      join_vars = MapSet.intersection(left_vars, right_vars) |> MapSet.to_list()

      if Enum.empty?(join_vars) do
        # No shared variables - cartesian product
        cartesian_product(left_list, right_list)
      else
        # Build hash table on left side keyed by join variable values
        hash_table = build_hash_table(left_list, join_vars)

        # Probe with right side
        Stream.flat_map(right_list, fn right_binding ->
          key = extract_join_key(right_binding, join_vars)

          case Map.get(hash_table, key) do
            nil ->
              []

            left_bindings ->
              Enum.flat_map(left_bindings, fn left_binding ->
                case merge_bindings(left_binding, right_binding) do
                  {:ok, merged} -> [merged]
                  :incompatible -> []
                end
              end)
          end
        end)
      end
    end
  end

  @doc """
  Executes a left outer join between two binding collections.

  Returns all bindings from the left side, extended with matching bindings
  from the right side where compatible. If no compatible right binding exists,
  the left binding is returned unextended.

  This implements SPARQL OPTIONAL semantics.

  ## Arguments

  - `left` - Left binding stream/list
  - `right` - Right binding stream/list
  - `opts` - Options:
    - `:filter` - Optional filter expression to apply (for OPTIONAL { ... FILTER ... })

  ## Returns

  Stream of joined bindings (left bindings extended with right when compatible).

  ## Examples

      left = [%{"x" => {:named_node, "http://ex.org/A"}}]
      right = []  # No matches
      result = Executor.left_join(left, right) |> Enum.to_list()
      # => [%{"x" => {:named_node, "http://ex.org/A"}}]  # Left preserved

  """
  @spec left_join(Enumerable.t(), Enumerable.t(), keyword()) :: binding_stream()
  def left_join(left, right, opts \\ []) do
    filter_fn = Keyword.get(opts, :filter, fn _ -> true end)

    # Materialize right side for multiple iterations
    right_list = Enum.to_list(right)

    Stream.flat_map(left, fn left_binding ->
      # Find all compatible right bindings
      matches =
        Enum.flat_map(right_list, fn right_binding ->
          case merge_bindings(left_binding, right_binding) do
            {:ok, merged} ->
              # Apply filter if provided
              if filter_fn.(merged), do: [merged], else: []

            :incompatible ->
              []
          end
        end)

      if Enum.empty?(matches) do
        # No compatible match - preserve left binding (OPTIONAL semantics)
        [left_binding]
      else
        matches
      end
    end)
  end

  # ===========================================================================
  # Binding Compatibility (Task 2.4.2.4)
  # ===========================================================================

  @doc """
  Merges two bindings if they are compatible.

  Two bindings are compatible if for every shared variable, both bindings
  have the same value. Returns the merged binding with all variables from
  both inputs.

  ## Arguments

  - `binding1` - First binding map
  - `binding2` - Second binding map

  ## Returns

  - `{:ok, merged}` - Merged binding when compatible
  - `:incompatible` - When bindings have conflicting values

  ## Examples

      merge_bindings(%{"x" => 1}, %{"y" => 2})
      # => {:ok, %{"x" => 1, "y" => 2}}

      merge_bindings(%{"x" => 1}, %{"x" => 1, "y" => 2})
      # => {:ok, %{"x" => 1, "y" => 2}}

      merge_bindings(%{"x" => 1}, %{"x" => 2})
      # => :incompatible

  """
  @spec merge_bindings(binding(), binding()) :: {:ok, binding()} | :incompatible
  def merge_bindings(binding1, binding2) do
    # Check compatibility: shared variables must have same value
    compatible? =
      Enum.all?(binding1, fn {var, val} ->
        case Map.get(binding2, var) do
          nil -> true
          ^val -> true
          _ -> false
        end
      end)

    if compatible? do
      {:ok, Map.merge(binding1, binding2)}
    else
      :incompatible
    end
  end

  @doc """
  Checks if two bindings are compatible.

  ## Arguments

  - `binding1` - First binding map
  - `binding2` - Second binding map

  ## Returns

  `true` if bindings are compatible, `false` otherwise.
  """
  @spec bindings_compatible?(binding(), binding()) :: boolean()
  def bindings_compatible?(binding1, binding2) do
    Enum.all?(binding1, fn {var, val} ->
      case Map.get(binding2, var) do
        nil -> true
        ^val -> true
        _ -> false
      end
    end)
  end

  # ===========================================================================
  # Join Helper Functions
  # ===========================================================================

  # Extract the set of variable names present in any binding
  defp shared_variables(bindings) do
    bindings
    |> Enum.flat_map(&Map.keys/1)
    |> MapSet.new()
  end

  # Build a hash table grouping bindings by join key
  defp build_hash_table(bindings, join_vars) do
    Enum.group_by(bindings, &extract_join_key(&1, join_vars))
  end

  # Extract join key as a list of values for the join variables
  defp extract_join_key(binding, join_vars) do
    Enum.map(join_vars, fn var -> Map.get(binding, var) end)
  end

  # Compute cartesian product when there are no shared variables
  defp cartesian_product(left_list, right_list) do
    Stream.flat_map(left_list, fn left_binding ->
      Enum.map(right_list, fn right_binding ->
        Map.merge(left_binding, right_binding)
      end)
    end)
  end

  # ===========================================================================
  # Empty Result Handling
  # ===========================================================================

  @doc """
  Returns an empty binding stream.

  Used when a pattern cannot match (e.g., term not in dictionary).
  """
  @spec empty_stream() :: binding_stream()
  def empty_stream do
    Stream.iterate(nil, & &1) |> Stream.take(0)
  end

  @doc """
  Returns a stream with a single empty binding.

  Used for empty BGP patterns (SPARQL semantics: empty pattern matches once).
  """
  @spec unit_stream() :: binding_stream()
  def unit_stream do
    Stream.iterate(%{}, & &1) |> Stream.take(1)
  end
end
