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

  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.IdToString
  alias TripleStore.Dictionary.StringToId
  alias TripleStore.Index
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
          case IdToString.lookup_term(db, term_id) do
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

    # Emit telemetry for join materialization
    :telemetry.execute(
      [:triple_store, :sparql, :executor, :hash_join],
      %{left_count: length(left_list), right_count: length(right_list)},
      %{}
    )

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
  # Union Execution (Task 2.4.3)
  # ===========================================================================

  @doc """
  Executes a UNION between two binding streams.

  UNION in SPARQL concatenates the results of two graph patterns. Results from
  the left branch appear before results from the right branch (preserving order
  within each branch).

  Variables that appear in one branch but not the other will be unbound in the
  bindings from the other branch. This function does NOT add nil values for
  missing variables - each binding contains only the variables that were
  actually bound in that branch.

  ## Arguments

  - `left` - Left binding stream (results appear first)
  - `right` - Right binding stream (results appear second)

  ## Returns

  Stream of bindings from both branches concatenated.

  ## Examples

      # UNION of two patterns with different variables
      left = [%{"x" => 1, "y" => "a"}]
      right = [%{"x" => 2, "z" => "b"}]
      result = Executor.union(left, right) |> Enum.to_list()
      # => [%{"x" => 1, "y" => "a"}, %{"x" => 2, "z" => "b"}]

  ## SPARQL Semantics

  In SPARQL, `{ P1 } UNION { P2 }` returns all solutions from P1 followed by
  all solutions from P2. Variables not bound in a particular solution are
  simply absent from that binding (not set to nil or null).

  """
  @spec union(Enumerable.t(), Enumerable.t()) :: binding_stream()
  def union(left, right) do
    Stream.concat(left, right)
  end

  @doc """
  Executes a UNION with variable alignment.

  Similar to `union/2`, but ensures all bindings have the same set of variables.
  Variables that are missing in a binding are set to `:unbound`.

  This is useful when you need to align variables across branches for further
  processing, such as projection or serialization.

  ## Arguments

  - `left` - Left binding stream
  - `right` - Right binding stream
  - `opts` - Options:
    - `:align_variables` - When true, adds `:unbound` for missing variables

  ## Returns

  Stream of bindings with aligned variables.

  ## Examples

      left = [%{"x" => 1, "y" => "a"}]
      right = [%{"x" => 2, "z" => "b"}]
      result = Executor.union_aligned(left, right) |> Enum.to_list()
      # => [
      #   %{"x" => 1, "y" => "a", "z" => :unbound},
      #   %{"x" => 2, "y" => :unbound, "z" => "b"}
      # ]

  """
  @spec union_aligned(Enumerable.t(), Enumerable.t()) :: binding_stream()
  def union_aligned(left, right) do
    # Materialize both sides to discover all variables
    left_list = Enum.to_list(left)
    right_list = Enum.to_list(right)

    # Collect all variables from both sides
    all_vars = collect_all_variables(left_list ++ right_list)

    # Align and concatenate
    aligned_left = Enum.map(left_list, &align_binding(&1, all_vars))
    aligned_right = Enum.map(right_list, &align_binding(&1, all_vars))

    Stream.concat(aligned_left, aligned_right)
  end

  @doc """
  Executes multiple UNIONs, concatenating all branches.

  Useful for SPARQL patterns with multiple UNION branches:
  `{ P1 } UNION { P2 } UNION { P3 }`

  ## Arguments

  - `branches` - List of binding streams to concatenate

  ## Returns

  Stream of bindings from all branches concatenated in order.

  ## Examples

      branches = [
        [%{"x" => 1}],
        [%{"x" => 2}],
        [%{"x" => 3}]
      ]
      result = Executor.union_all(branches) |> Enum.to_list()
      # => [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}]

  """
  @spec union_all([Enumerable.t()]) :: binding_stream()
  def union_all([]), do: empty_stream()
  def union_all([single]), do: single
  def union_all(branches) do
    Enum.reduce(branches, fn branch, acc ->
      Stream.concat(acc, branch)
    end)
  end

  @doc """
  Returns the set of all variable names present in a collection of bindings.

  ## Arguments

  - `bindings` - List or stream of binding maps

  ## Returns

  MapSet of variable names (strings).

  ## Examples

      bindings = [%{"x" => 1, "y" => 2}, %{"x" => 3, "z" => 4}]
      vars = Executor.collect_all_variables(bindings)
      # => MapSet.new(["x", "y", "z"])

  """
  @spec collect_all_variables(Enumerable.t()) :: MapSet.t(String.t())
  def collect_all_variables(bindings) do
    bindings
    |> Enum.flat_map(&Map.keys/1)
    |> MapSet.new()
  end

  @doc """
  Aligns a binding to include all specified variables.

  Variables not present in the binding are set to `:unbound`.

  ## Arguments

  - `binding` - The binding map to align
  - `all_vars` - MapSet of all variable names that should be present

  ## Returns

  Binding map with all variables present.

  ## Examples

      binding = %{"x" => 1}
      all_vars = MapSet.new(["x", "y", "z"])
      aligned = Executor.align_binding(binding, all_vars)
      # => %{"x" => 1, "y" => :unbound, "z" => :unbound}

  """
  @spec align_binding(binding(), MapSet.t(String.t())) :: binding()
  def align_binding(binding, all_vars) do
    Enum.reduce(all_vars, binding, fn var, acc ->
      Map.put_new(acc, var, :unbound)
    end)
  end

  # ===========================================================================
  # Filter Execution (Task 2.4.4)
  # ===========================================================================

  alias TripleStore.SPARQL.Expression

  @doc """
  Filters a binding stream based on a SPARQL expression.

  Evaluates the filter expression against each binding in the stream.
  Only bindings where the expression evaluates to true (effective boolean
  value) are kept. Bindings where the expression evaluates to false or
  error are removed.

  ## Three-Valued Logic

  SPARQL uses three-valued logic for filters:
  - **true**: Binding passes the filter
  - **false**: Binding is removed
  - **error**: Binding is removed (errors propagate as false in filters)

  This differs from SQL where errors might raise exceptions. In SPARQL,
  a filter like `FILTER(?x > 5)` on an unbound `?x` simply removes the
  binding rather than failing the query.

  ## Arguments

  - `stream` - Input binding stream
  - `expression` - SPARQL expression AST from parser

  ## Returns

  Filtered binding stream.

  ## Examples

      # Filter ?age > 18
      bindings = [
        %{"name" => {:literal, :simple, "Alice"}, "age" => {:literal, :typed, "25", xsd_integer}},
        %{"name" => {:literal, :simple, "Bob"}, "age" => {:literal, :typed, "15", xsd_integer}}
      ]
      expr = {:greater, {:variable, "age"}, {:literal, :typed, "18", xsd_integer}}
      result = Executor.filter(bindings, expr) |> Enum.to_list()
      # => [%{"name" => ..., "age" => "25"}]

  """
  @spec filter(Enumerable.t(), tuple()) :: binding_stream()
  def filter(stream, expression) do
    Stream.filter(stream, fn binding ->
      evaluate_filter(expression, binding)
    end)
  end

  @doc """
  Evaluates a filter expression against a single binding.

  Returns `true` if the binding should pass the filter, `false` otherwise.
  Implements SPARQL three-valued logic where errors are treated as false.

  ## Arguments

  - `expression` - SPARQL expression AST
  - `binding` - Variable bindings map

  ## Returns

  `true` if expression evaluates to true, `false` if false or error.

  ## Examples

      expr = {:greater, {:variable, "x"}, {:literal, :typed, "5", xsd_integer}}
      binding = %{"x" => {:literal, :typed, "10", xsd_integer}}
      Executor.evaluate_filter(expr, binding)
      # => true

  """
  @spec evaluate_filter(tuple(), binding()) :: boolean()
  def evaluate_filter(expression, binding) do
    case Expression.evaluate(expression, binding) do
      {:ok, result} ->
        effective_boolean_value(result)

      :error ->
        # Three-valued logic: errors evaluate to false in filter context
        false
    end
  end

  @doc """
  Evaluates a filter expression and returns the three-valued result.

  Unlike `evaluate_filter/2`, this function distinguishes between
  false and error results.

  ## Arguments

  - `expression` - SPARQL expression AST
  - `binding` - Variable bindings map

  ## Returns

  - `{:ok, true}` - Expression is true
  - `{:ok, false}` - Expression is false
  - `:error` - Expression evaluation failed

  ## Examples

      # True case
      evaluate_filter_3vl({:bound, {:variable, "x"}}, %{"x" => 1})
      # => {:ok, true}

      # False case
      evaluate_filter_3vl({:bound, {:variable, "x"}}, %{})
      # => {:ok, false}

      # Error case (type error)
      evaluate_filter_3vl({:greater, {:variable, "x"}, {:literal, :typed, "5", xsd_integer}}, %{"x" => {:named_node, "http://ex.org"}})
      # => :error

  """
  @spec evaluate_filter_3vl(tuple(), binding()) :: {:ok, boolean()} | :error
  def evaluate_filter_3vl(expression, binding) do
    case Expression.evaluate(expression, binding) do
      {:ok, result} ->
        case to_effective_boolean(result) do
          {:ok, bool} -> {:ok, bool}
          :error -> :error
        end

      :error ->
        :error

      {:error, _} ->
        :error
    end
  end

  @doc """
  Filters bindings with multiple conjunctive filter expressions.

  Equivalent to `FILTER(e1 && e2 && e3)`. A binding passes only if
  ALL expressions evaluate to true.

  ## Arguments

  - `stream` - Input binding stream
  - `expressions` - List of SPARQL expression ASTs

  ## Returns

  Filtered binding stream.

  ## Examples

      # FILTER(?x > 0 && ?x < 10)
      exprs = [
        {:greater, {:variable, "x"}, {:literal, :typed, "0", xsd_integer}},
        {:less, {:variable, "x"}, {:literal, :typed, "10", xsd_integer}}
      ]
      result = Executor.filter_all(bindings, exprs)

  """
  @spec filter_all(Enumerable.t(), [tuple()]) :: binding_stream()
  def filter_all(stream, []), do: stream

  def filter_all(stream, expressions) when is_list(expressions) do
    Stream.filter(stream, fn binding ->
      Enum.all?(expressions, fn expr ->
        evaluate_filter(expr, binding)
      end)
    end)
  end

  @doc """
  Filters bindings with disjunctive filter expressions.

  Equivalent to `FILTER(e1 || e2 || e3)`. A binding passes if
  ANY expression evaluates to true.

  ## Arguments

  - `stream` - Input binding stream
  - `expressions` - List of SPARQL expression ASTs

  ## Returns

  Filtered binding stream.

  ## Examples

      # FILTER(?type = :Person || ?type = :Organization)
      exprs = [
        {:equal, {:variable, "type"}, {:named_node, "http://ex.org/Person"}},
        {:equal, {:variable, "type"}, {:named_node, "http://ex.org/Organization"}}
      ]
      result = Executor.filter_any(bindings, exprs)

  """
  @spec filter_any(Enumerable.t(), [tuple()]) :: binding_stream()
  def filter_any(_stream, []), do: empty_stream()

  def filter_any(stream, expressions) when is_list(expressions) do
    Stream.filter(stream, fn binding ->
      Enum.any?(expressions, fn expr ->
        evaluate_filter(expr, binding)
      end)
    end)
  end

  # ===========================================================================
  # Effective Boolean Value (SPARQL Semantics)
  # ===========================================================================

  @xsd_boolean "http://www.w3.org/2001/XMLSchema#boolean"
  @xsd_string "http://www.w3.org/2001/XMLSchema#string"
  @xsd_integer "http://www.w3.org/2001/XMLSchema#integer"
  @xsd_decimal "http://www.w3.org/2001/XMLSchema#decimal"
  @xsd_float "http://www.w3.org/2001/XMLSchema#float"
  @xsd_double "http://www.w3.org/2001/XMLSchema#double"

  # Convert RDF term to effective boolean value (two-valued for filter context)
  defp effective_boolean_value(term) do
    case to_effective_boolean(term) do
      {:ok, bool} -> bool
      :error -> false
    end
  end

  @doc """
  Computes the effective boolean value of an RDF term.

  SPARQL defines EBV (Effective Boolean Value) as:
  - xsd:boolean: the boolean value
  - xsd:string or simple literal: false if empty string, true otherwise
  - Numeric types: false if zero or NaN, true otherwise
  - Other types: error

  ## Arguments

  - `term` - RDF term tuple

  ## Returns

  - `{:ok, boolean}` - The effective boolean value
  - `:error` - Cannot compute EBV for this term type

  """
  @spec to_effective_boolean(term()) :: {:ok, boolean()} | :error
  def to_effective_boolean({:literal, :typed, "true", @xsd_boolean}), do: {:ok, true}
  def to_effective_boolean({:literal, :typed, "false", @xsd_boolean}), do: {:ok, false}
  def to_effective_boolean({:literal, :typed, "1", @xsd_boolean}), do: {:ok, true}
  def to_effective_boolean({:literal, :typed, "0", @xsd_boolean}), do: {:ok, false}

  def to_effective_boolean({:literal, :simple, s}), do: {:ok, s != ""}
  def to_effective_boolean({:literal, :typed, s, @xsd_string}), do: {:ok, s != ""}

  def to_effective_boolean({:literal, :typed, value, type})
      when type in [@xsd_integer, @xsd_decimal, @xsd_float, @xsd_double] do
    case parse_numeric(value, type) do
      {:ok, n} -> {:ok, n != 0 and not nan?(n)}
      :error -> :error
    end
  end

  def to_effective_boolean(_), do: :error

  # Parse numeric value
  defp parse_numeric(value, @xsd_integer) do
    case Integer.parse(value) do
      {n, ""} -> {:ok, n}
      _ -> :error
    end
  end

  defp parse_numeric(value, _float_type) do
    case Float.parse(value) do
      {n, ""} -> {:ok, n}
      _ -> :error
    end
  end

  # Check for NaN (not a number)
  # Uses IEEE 754 property: NaN != NaN
  defp nan?(n) when is_float(n), do: n != n
  defp nan?(_), do: false

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

  # ===========================================================================
  # Solution Modifiers (Task 2.4.5)
  # ===========================================================================

  @doc """
  Projects bindings to include only specified variables.

  This implements SPARQL SELECT projection, retaining only the listed
  variables in each binding. Variables not present in a binding are
  omitted from the result (not set to nil or :unbound).

  ## Arguments

  - `stream` - Input binding stream
  - `vars` - List of variable names to retain

  ## Returns

  Stream of projected bindings.

  ## Examples

      bindings = [
        %{"x" => 1, "y" => 2, "z" => 3},
        %{"x" => 4, "y" => 5, "z" => 6}
      ]
      result = Executor.project(bindings, ["x", "z"]) |> Enum.to_list()
      # => [%{"x" => 1, "z" => 3}, %{"x" => 4, "z" => 6}]

  """
  @spec project(Enumerable.t(), [String.t()]) :: binding_stream()
  def project(stream, vars) when is_list(vars) do
    var_set = MapSet.new(vars)

    Stream.map(stream, fn binding ->
      Map.filter(binding, fn {key, _value} ->
        MapSet.member?(var_set, key)
      end)
    end)
  end

  @doc """
  Removes duplicate bindings from a stream.

  This implements SPARQL DISTINCT, removing bindings that have already
  been seen. Uses a MapSet internally to track seen bindings.

  Note: This operation materializes the stream to track duplicates,
  so memory usage is O(n) where n is the number of unique bindings.

  ## Arguments

  - `stream` - Input binding stream

  ## Returns

  Stream of distinct bindings.

  ## Examples

      bindings = [
        %{"x" => 1},
        %{"x" => 2},
        %{"x" => 1},  # duplicate
        %{"x" => 3}
      ]
      result = Executor.distinct(bindings) |> Enum.to_list()
      # => [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}]

  """
  @spec distinct(Enumerable.t()) :: binding_stream()
  def distinct(stream) do
    Stream.transform(stream, {MapSet.new(), 0}, fn binding, {seen, count} ->
      if MapSet.member?(seen, binding) do
        {[], {seen, count}}
      else
        new_count = count + 1

        # Emit telemetry at intervals to monitor memory growth
        if rem(new_count, 10_000) == 0 do
          :telemetry.execute(
            [:triple_store, :sparql, :executor, :distinct],
            %{unique_count: new_count, seen_size: MapSet.size(seen)},
            %{}
          )
        end

        {[binding], {MapSet.put(seen, binding), new_count}}
      end
    end)
  end

  @doc """
  Removes some duplicate bindings from a stream.

  This implements SPARQL REDUCED, which allows but does not require
  duplicate elimination. This implementation is equivalent to DISTINCT
  but exists for semantic completeness.

  ## Arguments

  - `stream` - Input binding stream

  ## Returns

  Stream with some duplicates removed.

  """
  @spec reduced(Enumerable.t()) :: binding_stream()
  def reduced(stream) do
    # REDUCED is allowed to eliminate duplicates but not required
    # We implement it as DISTINCT for correctness
    distinct(stream)
  end

  @doc """
  Orders bindings by one or more comparators.

  This implements SPARQL ORDER BY, sorting bindings by the specified
  comparators. Each comparator is a tuple of `{variable, direction}`
  where direction is `:asc` or `:desc`.

  SPARQL ordering rules:
  1. Unbound < Blank nodes < IRIs < Literals
  2. Blank nodes ordered by identifier
  3. IRIs ordered lexicographically by IRI string
  4. Literals ordered by value within datatype, then by datatype
  5. For typed literals: compare values appropriately (numerics, strings, etc.)

  Note: This operation materializes the stream for sorting, so memory
  usage is O(n) where n is the total number of bindings.

  ## Arguments

  - `stream` - Input binding stream
  - `comparators` - List of `{variable, direction}` tuples or
                    `{expression, direction}` tuples

  ## Returns

  Stream of ordered bindings.

  ## Examples

      bindings = [
        %{"name" => {:literal, :simple, "Bob"}},
        %{"name" => {:literal, :simple, "Alice"}},
        %{"name" => {:literal, :simple, "Carol"}}
      ]
      result = Executor.order_by(bindings, [{"name", :asc}]) |> Enum.to_list()
      # => [%{"name" => "Alice"}, %{"name" => "Bob"}, %{"name" => "Carol"}]

  """
  @spec order_by(Enumerable.t(), [{String.t() | tuple(), :asc | :desc}]) :: binding_stream()
  def order_by(stream, []), do: stream

  def order_by(stream, comparators) when is_list(comparators) do
    # Materialize for sorting
    bindings = Enum.to_list(stream)

    # Emit telemetry for order_by materialization
    :telemetry.execute(
      [:triple_store, :sparql, :executor, :order_by],
      %{binding_count: length(bindings), comparator_count: length(comparators)},
      %{}
    )

    sorted =
      Enum.sort(bindings, fn a, b ->
        compare_bindings(a, b, comparators)
      end)

    # Return as a stream by wrapping the list
    Stream.concat(sorted, [])
  end

  # Compare two bindings using the comparator list
  defp compare_bindings(_a, _b, []), do: true

  defp compare_bindings(a, b, [{var_or_expr, direction} | rest]) do
    val_a = get_sort_value(a, var_or_expr)
    val_b = get_sort_value(b, var_or_expr)

    case compare_terms(val_a, val_b) do
      :eq -> compare_bindings(a, b, rest)
      :lt -> direction == :asc
      :gt -> direction == :desc
    end
  end

  # Get value for sorting - either from variable or expression
  defp get_sort_value(binding, var) when is_binary(var) do
    Map.get(binding, var)
  end

  defp get_sort_value(binding, {:variable, name}) do
    Map.get(binding, name)
  end

  defp get_sort_value(binding, expr) when is_tuple(expr) do
    case Expression.evaluate(expr, binding) do
      {:ok, value} -> value
      _ -> nil
    end
  end

  # Compare two RDF terms according to SPARQL ordering rules
  defp compare_terms(nil, nil), do: :eq
  defp compare_terms(nil, _), do: :lt
  defp compare_terms(_, nil), do: :gt

  defp compare_terms({:blank_node, a}, {:blank_node, b}) do
    cond do
      a < b -> :lt
      a > b -> :gt
      true -> :eq
    end
  end

  defp compare_terms({:blank_node, _}, _), do: :lt
  defp compare_terms(_, {:blank_node, _}), do: :gt

  defp compare_terms({:named_node, a}, {:named_node, b}) do
    cond do
      a < b -> :lt
      a > b -> :gt
      true -> :eq
    end
  end

  defp compare_terms({:named_node, _}, _), do: :lt
  defp compare_terms(_, {:named_node, _}), do: :gt

  defp compare_terms({:literal, _, _, _} = a, {:literal, _, _, _} = b) do
    compare_literals(a, b)
  end

  defp compare_terms({:literal, _, _} = a, {:literal, _, _} = b) do
    compare_literals(a, b)
  end

  defp compare_terms({:literal, _, _} = a, {:literal, _, _, _} = b) do
    compare_literals(a, b)
  end

  defp compare_terms({:literal, _, _, _} = a, {:literal, _, _} = b) do
    compare_literals(a, b)
  end

  defp compare_terms(a, b) do
    cond do
      a < b -> :lt
      a > b -> :gt
      true -> :eq
    end
  end

  # Compare literals - try numeric comparison first, then lexicographic
  defp compare_literals(a, b) do
    a_numeric = try_numeric_value(a)
    b_numeric = try_numeric_value(b)

    cond do
      a_numeric != nil and b_numeric != nil ->
        cond do
          a_numeric < b_numeric -> :lt
          a_numeric > b_numeric -> :gt
          true -> :eq
        end

      true ->
        # Fall back to lexicographic comparison
        a_str = literal_sort_key(a)
        b_str = literal_sort_key(b)

        cond do
          a_str < b_str -> :lt
          a_str > b_str -> :gt
          true -> :eq
        end
    end
  end

  # Try to extract numeric value from a literal
  defp try_numeric_value({:literal, :typed, value, type})
       when type in [@xsd_integer, @xsd_decimal, @xsd_float, @xsd_double] do
    case Float.parse(value) do
      {n, ""} -> n
      {n, _} -> n
      :error -> nil
    end
  end

  defp try_numeric_value(_), do: nil

  # Get sort key for a literal
  defp literal_sort_key({:literal, :simple, value}), do: value
  defp literal_sort_key({:literal, :typed, value, _}), do: value
  defp literal_sort_key({:literal, :lang, value, _}), do: value

  @doc """
  Applies offset and limit to a binding stream.

  This implements SPARQL OFFSET and LIMIT, skipping the first `offset`
  bindings and returning at most `limit` bindings.

  ## Arguments

  - `stream` - Input binding stream
  - `offset` - Number of bindings to skip (default: 0)
  - `limit` - Maximum number of bindings to return (default: nil for no limit)

  ## Returns

  Stream with offset and limit applied.

  ## Examples

      bindings = [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}, %{"x" => 4}, %{"x" => 5}]
      result = Executor.slice(bindings, 1, 2) |> Enum.to_list()
      # => [%{"x" => 2}, %{"x" => 3}]

  """
  @spec slice(Enumerable.t(), non_neg_integer(), non_neg_integer() | nil) :: binding_stream()
  def slice(stream, offset \\ 0, limit \\ nil)

  def slice(stream, 0, nil), do: stream

  def slice(stream, offset, nil) when offset > 0 do
    Stream.drop(stream, offset)
  end

  def slice(stream, 0, limit) when is_integer(limit) and limit >= 0 do
    Stream.take(stream, limit)
  end

  def slice(stream, offset, limit)
      when is_integer(offset) and offset >= 0 and is_integer(limit) and limit >= 0 do
    stream
    |> Stream.drop(offset)
    |> Stream.take(limit)
  end

  @doc """
  Applies offset to a binding stream.

  Convenience function for applying only offset without limit.

  ## Arguments

  - `stream` - Input binding stream
  - `offset` - Number of bindings to skip

  ## Returns

  Stream with first `offset` bindings skipped.

  """
  @spec offset(Enumerable.t(), non_neg_integer()) :: binding_stream()
  def offset(stream, 0), do: stream
  def offset(stream, n) when is_integer(n) and n > 0, do: Stream.drop(stream, n)

  @doc """
  Applies limit to a binding stream.

  Convenience function for applying only limit without offset.

  ## Arguments

  - `stream` - Input binding stream
  - `limit` - Maximum number of bindings to return

  ## Returns

  Stream with at most `limit` bindings.

  """
  @spec limit(Enumerable.t(), non_neg_integer()) :: binding_stream()
  def limit(stream, n) when is_integer(n) and n >= 0, do: Stream.take(stream, n)

  # ===========================================================================
  # Result Serialization (Task 2.4.6)
  # ===========================================================================

  @doc """
  Serializes SELECT query results to a list of binding maps.

  This is the standard result format for SELECT queries. Each binding
  maps variable names (without the leading '?') to RDF terms represented
  as internal tuples.

  ## Arguments

  - `stream` - Binding stream from query execution
  - `vars` - List of projected variable names (optional, defaults to all)

  ## Returns

  List of binding maps.

  ## Examples

      {:ok, stream} = Executor.execute_bgp(ctx, patterns)
      results = Executor.to_select_results(stream, ["name", "age"])
      # => [
      #   %{"name" => {:literal, :simple, "Alice"}, "age" => {:literal, :typed, "30", "xsd:integer"}},
      #   %{"name" => {:literal, :simple, "Bob"}, "age" => {:literal, :typed, "25", "xsd:integer"}}
      # ]

  """
  @spec to_select_results(Enumerable.t(), [String.t()] | nil) :: [binding()]
  def to_select_results(stream, vars \\ nil)

  def to_select_results(stream, nil) do
    Enum.to_list(stream)
  end

  def to_select_results(stream, vars) when is_list(vars) do
    stream
    |> project(vars)
    |> Enum.to_list()
  end

  @doc """
  Serializes ASK query results to a boolean.

  ASK queries return true if any solutions exist, false otherwise.

  ## Arguments

  - `stream` - Binding stream from query execution

  ## Returns

  `true` if at least one solution exists, `false` otherwise.

  ## Examples

      {:ok, stream} = Executor.execute_bgp(ctx, patterns)
      result = Executor.to_ask_result(stream)
      # => true

  """
  @spec to_ask_result(Enumerable.t()) :: boolean()
  def to_ask_result(stream) do
    case Enum.take(stream, 1) do
      [_] -> true
      [] -> false
    end
  end

  @doc """
  Serializes CONSTRUCT query results to an RDF.Graph.

  Takes a template (list of triple patterns with variables) and instantiates
  it with each binding to produce triples for the result graph.

  ## Arguments

  - `ctx` - Execution context with `:db` key
  - `stream` - Binding stream from query execution
  - `template` - List of triple patterns `{:triple, s, p, o}` where components
                 can be variables or concrete terms
  - `opts` - Options passed to `RDF.Graph.new/2` (optional)

  ## Returns

  - `{:ok, graph}` - RDF.Graph containing constructed triples
  - `{:error, reason}` - On failure

  ## Template Variable Substitution

  Variables in the template are substituted with values from each binding:
  - `{:variable, "x"}` -> value of `?x` from binding
  - Concrete terms are passed through unchanged
  - If a variable is unbound, that triple is skipped

  ## Examples

      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://xmlns.com/foaf/0.1/name"}, {:variable, "name"}}
      ]
      {:ok, graph} = Executor.to_construct_result(ctx, stream, template)

  """
  @spec to_construct_result(context(), Enumerable.t(), [tuple()], keyword()) ::
          {:ok, RDF.Graph.t()} | {:error, term()}
  def to_construct_result(ctx, stream, template, opts \\ []) do
    triples =
      stream
      |> Stream.flat_map(fn binding ->
        instantiate_template(template, binding)
      end)
      |> Enum.to_list()

    # Convert internal terms to RDF terms and build graph
    build_graph_from_terms(ctx, triples, opts)
  end

  @doc """
  Serializes DESCRIBE query results to an RDF.Graph using Concise Bounded Description.

  For each resource in the bindings, retrieves all triples where that resource
  appears as subject (and optionally follows blank nodes to their descriptions).

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager` keys
  - `stream` - Binding stream from query execution
  - `vars` - List of variable names whose values should be described
  - `opts` - Options:
    - `:follow_bnodes` - Whether to follow blank node references (default: true)
    - Other options passed to `RDF.Graph.new/2`

  ## Returns

  - `{:ok, graph}` - RDF.Graph containing descriptions
  - `{:error, reason}` - On failure

  ## Concise Bounded Description (CBD)

  CBD includes:
  1. All triples where the resource is subject
  2. Recursively, CBD of any blank nodes appearing as objects

  ## Examples

      {:ok, stream} = Executor.execute_bgp(ctx, patterns)
      {:ok, graph} = Executor.to_describe_result(ctx, stream, ["person"])

  """
  @spec to_describe_result(context(), Enumerable.t(), [String.t()], keyword()) ::
          {:ok, RDF.Graph.t()} | {:error, term()}
  def to_describe_result(ctx, stream, vars, opts \\ []) do
    {follow_bnodes, graph_opts} = Keyword.pop(opts, :follow_bnodes, true)

    # Collect all resources to describe
    resources =
      stream
      |> Stream.flat_map(fn binding ->
        Enum.flat_map(vars, fn var ->
          case Map.get(binding, var) do
            nil -> []
            term -> [term]
          end
        end)
      end)
      |> Enum.uniq()

    # Get descriptions for all resources
    describe_resources(ctx, resources, follow_bnodes, graph_opts)
  end

  # ===========================================================================
  # Result Serialization Helpers
  # ===========================================================================

  # Instantiate a template with binding values, returning list of triples
  defp instantiate_template(template, binding) do
    Enum.flat_map(template, fn {:triple, s, p, o} ->
      with {:ok, s_val} <- substitute_term(s, binding),
           {:ok, p_val} <- substitute_term(p, binding),
           {:ok, o_val} <- substitute_term(o, binding) do
        [{s_val, p_val, o_val}]
      else
        :unbound -> []
      end
    end)
  end

  # Substitute a term with its binding value if it's a variable
  defp substitute_term({:variable, name}, binding) do
    case Map.get(binding, name) do
      nil -> :unbound
      :unbound -> :unbound
      value -> {:ok, value}
    end
  end

  defp substitute_term(term, _binding), do: {:ok, term}

  # Build RDF.Graph from internal term triples
  defp build_graph_from_terms(_ctx, [], opts) do
    {:ok, RDF.Graph.new(opts)}
  end

  defp build_graph_from_terms(_ctx, triples, opts) do
    # Convert internal terms to RDF terms
    rdf_triples =
      Enum.flat_map(triples, fn {s, p, o} ->
        with {:ok, s_term} <- internal_to_rdf(s),
             {:ok, p_term} <- internal_to_rdf(p),
             {:ok, o_term} <- internal_to_rdf(o) do
          [{s_term, p_term, o_term}]
        else
          _ -> []
        end
      end)

    {:ok, RDF.Graph.new(rdf_triples, opts)}
  end

  # Convert internal term representation to RDF.ex terms
  defp internal_to_rdf({:named_node, uri}), do: {:ok, RDF.iri(uri)}
  defp internal_to_rdf({:blank_node, id}), do: {:ok, RDF.bnode(id)}
  defp internal_to_rdf({:literal, :simple, value}), do: {:ok, RDF.literal(value)}

  defp internal_to_rdf({:literal, :typed, value, datatype}) do
    {:ok, RDF.literal(value, datatype: datatype)}
  end

  defp internal_to_rdf({:literal, :lang, value, lang}) do
    {:ok, RDF.literal(value, language: lang)}
  end

  defp internal_to_rdf(_), do: :error

  # Describe resources by fetching their CBD from the database
  defp describe_resources(ctx, resources, follow_bnodes, opts) do
    %{db: db} = ctx

    # Convert resources to IDs and fetch triples
    all_triples =
      resources
      |> Enum.flat_map(fn resource ->
        case resource_to_id(db, resource) do
          {:ok, id} ->
            # Use Index pattern format: {:bound, id} for bound, :var for variable
            case Index.lookup_all(db, {{:bound, id}, :var, :var}) do
              {:ok, triples} ->
                triples

              _ ->
                []
            end

          _ ->
            []
        end
      end)
      |> Enum.uniq()

    # If following blank nodes, recursively fetch their descriptions
    all_triples =
      if follow_bnodes do
        follow_blank_nodes(ctx, all_triples, MapSet.new())
      else
        all_triples
      end

    # Convert to RDF graph
    TripleStore.Adapter.to_rdf_graph(db, all_triples, opts)
  end

  # Convert resource term to dictionary ID
  defp resource_to_id(db, {:named_node, uri}) do
    StringToId.lookup_id(db, RDF.iri(uri))
  end

  defp resource_to_id(db, {:blank_node, id}) do
    StringToId.lookup_id(db, RDF.bnode(id))
  end

  defp resource_to_id(_, _), do: :error

  # Maximum depth for blank node following to prevent stack overflow
  @max_bnode_depth 100

  # Follow blank nodes recursively for CBD with depth limiting
  defp follow_blank_nodes(ctx, triples, seen, depth \\ 0)

  defp follow_blank_nodes(_ctx, triples, _seen, depth) when depth >= @max_bnode_depth do
    # Depth limit reached - return what we have to prevent stack overflow
    triples
  end

  defp follow_blank_nodes(ctx, triples, seen, depth) do
    %{db: db} = ctx

    # Find blank node objects we haven't seen yet
    new_bnodes =
      triples
      |> Enum.flat_map(fn {_s, _p, o} ->
        if blank_node_id?(o) and not MapSet.member?(seen, o) do
          [o]
        else
          []
        end
      end)
      |> Enum.uniq()

    if Enum.empty?(new_bnodes) do
      triples
    else
      # Fetch triples for new blank nodes
      new_seen = Enum.reduce(new_bnodes, seen, &MapSet.put(&2, &1))

      new_triples =
        Enum.flat_map(new_bnodes, fn bnode_id ->
          case Index.lookup_all(db, {{:bound, bnode_id}, :var, :var}) do
            {:ok, bnode_triples} -> bnode_triples
            _ -> []
          end
        end)

      # Recursively follow more blank nodes with accumulator pattern
      all_triples = triples ++ new_triples
      follow_blank_nodes(ctx, all_triples, new_seen, depth + 1)
    end
  end

  # Check if an ID represents a blank node (type tag 1)
  defp blank_node_id?(id) when is_integer(id) do
    # Blank nodes have type tag 1 in high bits
    import Bitwise
    (id >>> 60) == 1
  end

  defp blank_node_id?(_), do: false
end
