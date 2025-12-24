defmodule TripleStore.SPARQL.PropertyPath do
  @moduledoc """
  SPARQL property path evaluation.

  This module evaluates property path expressions against the triple store.
  Property paths allow matching paths of arbitrary structure through the graph.

  ## Supported Path Types

  ### Non-Recursive Paths (Task 3.4.1)
  - **Sequence** (`p1/p2`): Match p1 followed by p2
  - **Alternative** (`p1|p2`): Match either p1 or p2
  - **Inverse** (`^p`): Match p in reverse direction
  - **Negated Property Set** (`!(p1|p2)`): Match any predicate except those listed

  ### Recursive Paths (Task 3.4.2 - Not yet implemented)
  - **Zero or More** (`p*`): Match zero or more occurrences
  - **One or More** (`p+`): Match one or more occurrences
  - **Optional** (`p?`): Match zero or one occurrence

  ## Usage

      # Evaluate a path pattern
      {:ok, stream} = PropertyPath.evaluate(ctx, binding, subject, path, object)

  ## Path Expression Format

  Path expressions are represented as tuples:
  - `{:link, iri}` - Simple predicate
  - `{:sequence, left, right}` - Sequence of two paths
  - `{:alternative, left, right}` - Alternative paths
  - `{:reverse, path}` - Inverse path
  - `{:negated_property_set, [iri1, iri2, ...]}` - Negated property set
  - `{:zero_or_more, path}` - Zero or more (recursive)
  - `{:one_or_more, path}` - One or more (recursive)
  - `{:zero_or_one, path}` - Optional (recursive)

  """

  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.IdToString
  alias TripleStore.Dictionary.StringToId
  alias TripleStore.Index

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Execution context"
  @type context :: %{db: reference(), dict_manager: GenServer.server()}

  @typedoc "Variable binding map"
  @type binding :: %{String.t() => term()}

  @typedoc "Stream of bindings"
  @type binding_stream :: Enumerable.t()

  @typedoc "RDF term in algebra format"
  @type rdf_term :: {:variable, String.t()} | {:named_node, String.t()} | term()

  @typedoc "Property path expression"
  @type path_expr ::
          {:link, String.t()}
          | {:sequence, path_expr(), path_expr()}
          | {:alternative, path_expr(), path_expr()}
          | {:reverse, path_expr()}
          | {:negated_property_set, [String.t()]}
          | {:zero_or_more, path_expr()}
          | {:one_or_more, path_expr()}
          | {:zero_or_one, path_expr()}

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Evaluates a property path pattern.

  Given a subject, path expression, and object, returns a stream of bindings
  that extend the input binding with any new variable assignments.

  ## Arguments

  - `ctx` - Execution context with `:db` and `:dict_manager`
  - `binding` - Current variable binding
  - `subject` - Subject term (variable or concrete)
  - `path` - Property path expression
  - `object` - Object term (variable or concrete)

  ## Returns

  - `{:ok, stream}` - Stream of extended bindings
  - `{:error, reason}` - On failure

  ## Examples

      # Simple link (equivalent to triple pattern)
      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, {:variable, "s"}, {:link, "http://ex.org/p"}, {:variable, "o"})

      # Sequence path
      path = {:sequence, {:link, "http://ex.org/p1"}, {:link, "http://ex.org/p2"}}
      {:ok, stream} = PropertyPath.evaluate(ctx, %{}, {:variable, "s"}, path, {:variable, "o"})

  """
  @spec evaluate(context(), binding(), rdf_term(), path_expr(), rdf_term()) ::
          {:ok, binding_stream()} | {:error, term()}
  def evaluate(ctx, binding, subject, path, object) do
    case do_evaluate(ctx, binding, subject, path, object) do
      {:ok, _} = result -> result
      {:error, _} = error -> error
    end
  end

  # ===========================================================================
  # Path Evaluation
  # ===========================================================================

  # Simple link - equivalent to a triple pattern
  defp do_evaluate(ctx, binding, subject, {:link, predicate}, object) do
    evaluate_link(ctx, binding, subject, predicate, object)
  end

  # Named node used directly as path (from parser)
  defp do_evaluate(ctx, binding, subject, {:named_node, iri}, object) do
    evaluate_link(ctx, binding, subject, iri, object)
  end

  # Sequence path: p1/p2
  # Match subject-[p1]->intermediate-[p2]->object
  defp do_evaluate(ctx, binding, subject, {:sequence, left, right}, object) do
    evaluate_sequence(ctx, binding, subject, left, right, object)
  end

  # Alternative path: p1|p2
  # Match either p1 or p2
  defp do_evaluate(ctx, binding, subject, {:alternative, left, right}, object) do
    evaluate_alternative(ctx, binding, subject, left, right, object)
  end

  # Inverse path: ^p
  # Match object-[p]->subject (reversed)
  defp do_evaluate(ctx, binding, subject, {:reverse, inner_path}, object) do
    evaluate_reverse(ctx, binding, subject, inner_path, object)
  end

  # Negated property set: !(p1|p2|...)
  # Match any predicate except those in the list
  defp do_evaluate(ctx, binding, subject, {:negated_property_set, excluded}, object) do
    evaluate_negated_property_set(ctx, binding, subject, excluded, object)
  end

  # Recursive paths - not yet implemented (Task 3.4.2)
  defp do_evaluate(_ctx, _binding, _subject, {:zero_or_more, _path}, _object) do
    {:error, :recursive_paths_not_implemented}
  end

  defp do_evaluate(_ctx, _binding, _subject, {:one_or_more, _path}, _object) do
    {:error, :recursive_paths_not_implemented}
  end

  defp do_evaluate(_ctx, _binding, _subject, {:zero_or_one, _path}, _object) do
    {:error, :recursive_paths_not_implemented}
  end

  defp do_evaluate(_ctx, _binding, _subject, path, _object) do
    {:error, {:unsupported_path, path}}
  end

  # ===========================================================================
  # Link Evaluation (Simple Predicate)
  # ===========================================================================

  defp evaluate_link(ctx, binding, subject, predicate, object) do
    %{db: db, dict_manager: dict_manager} = ctx

    # Resolve subject and object from binding
    s_resolved = resolve_term(subject, binding)
    o_resolved = resolve_term(object, binding)

    # Convert to index patterns
    with {:ok, s_pattern} <- term_to_pattern(s_resolved, dict_manager),
         {:ok, p_id} <- predicate_to_id(predicate, dict_manager),
         {:ok, o_pattern} <- term_to_pattern(o_resolved, dict_manager) do
      # Check for not-found terms
      if not_found?(s_pattern) or not_found?(o_pattern) or p_id == :not_found do
        {:ok, empty_stream()}
      else
        # Wrap bound IDs in {:bound, id} for Index module
        index_pattern = {
          wrap_bound(s_pattern),
          wrap_bound(p_id),
          wrap_bound(o_pattern)
        }

        case Index.lookup(db, index_pattern) do
          {:ok, triple_stream} ->
            binding_stream =
              Stream.flat_map(triple_stream, fn {s_id, _p_id, o_id} ->
                case extend_binding(binding, subject, object, s_id, o_id, dict_manager) do
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

  # ===========================================================================
  # Sequence Path (p1/p2)
  # ===========================================================================

  defp evaluate_sequence(ctx, binding, subject, left, right, object) do
    # Generate a unique intermediate variable name
    intermediate = {:variable, "_seq_#{:erlang.unique_integer([:positive])}"}

    # First, evaluate left path: subject-[left]->intermediate
    case do_evaluate(ctx, binding, subject, left, intermediate) do
      {:ok, left_stream} ->
        # For each result, evaluate right path: intermediate-[right]->object
        result_stream =
          Stream.flat_map(left_stream, fn intermediate_binding ->
            case do_evaluate(ctx, intermediate_binding, intermediate, right, object) do
              {:ok, right_stream} ->
                # Filter out the intermediate variable from results
                Stream.map(right_stream, fn b ->
                  {_, result} = intermediate
                  Map.delete(b, result)
                end)

              {:error, _} ->
                []
            end
          end)

        {:ok, result_stream}

      {:error, _} = error ->
        error
    end
  end

  # ===========================================================================
  # Alternative Path (p1|p2)
  # ===========================================================================

  defp evaluate_alternative(ctx, binding, subject, left, right, object) do
    # Evaluate both paths and concatenate results
    case do_evaluate(ctx, binding, subject, left, object) do
      {:ok, left_stream} ->
        case do_evaluate(ctx, binding, subject, right, object) do
          {:ok, right_stream} ->
            # Concatenate streams (left results first, then right)
            combined = Stream.concat(left_stream, right_stream)
            {:ok, combined}

          {:error, _} = error ->
            error
        end

      {:error, _} = error ->
        error
    end
  end

  # ===========================================================================
  # Inverse Path (^p)
  # ===========================================================================

  defp evaluate_reverse(ctx, binding, subject, inner_path, object) do
    # Swap subject and object for the inner path evaluation
    do_evaluate(ctx, binding, object, inner_path, subject)
  end

  # ===========================================================================
  # Negated Property Set (!(p1|p2|...))
  # ===========================================================================

  defp evaluate_negated_property_set(ctx, binding, subject, excluded_iris, object) do
    %{db: db, dict_manager: dict_manager} = ctx

    # Resolve subject and object
    s_resolved = resolve_term(subject, binding)
    o_resolved = resolve_term(object, binding)

    # Convert excluded IRIs to IDs for comparison
    excluded_ids =
      excluded_iris
      |> Enum.map(fn iri -> predicate_to_id(iri, dict_manager) end)
      |> Enum.filter(fn
        {:ok, id} when id != :not_found -> true
        _ -> false
      end)
      |> Enum.map(fn {:ok, id} -> id end)
      |> MapSet.new()

    # Convert to index patterns
    with {:ok, s_pattern} <- term_to_pattern(s_resolved, dict_manager),
         {:ok, o_pattern} <- term_to_pattern(o_resolved, dict_manager) do
      if not_found?(s_pattern) or not_found?(o_pattern) do
        {:ok, empty_stream()}
      else
        # Query all triples matching subject and object (predicate is variable)
        index_pattern = {wrap_bound(s_pattern), :var, wrap_bound(o_pattern)}

        case Index.lookup(db, index_pattern) do
          {:ok, triple_stream} ->
            # Filter out excluded predicates
            binding_stream =
              triple_stream
              |> Stream.reject(fn {_s_id, p_id, _o_id} ->
                MapSet.member?(excluded_ids, p_id)
              end)
              |> Stream.flat_map(fn {s_id, _p_id, o_id} ->
                case extend_binding(binding, subject, object, s_id, o_id, dict_manager) do
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

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  # Resolve a term using the current binding
  defp resolve_term({:variable, name}, binding) do
    case Map.get(binding, name) do
      nil -> {:variable, name}
      value -> value
    end
  end

  defp resolve_term(term, _binding), do: term

  # Convert a term to an index pattern
  defp term_to_pattern({:variable, _name}, _dict_manager), do: {:ok, :var}

  defp term_to_pattern({:named_node, iri}, dict_manager) do
    lookup_term_id(dict_manager, RDF.iri(iri))
  end

  defp term_to_pattern({:blank_node, id}, dict_manager) do
    lookup_term_id(dict_manager, RDF.bnode(id))
  end

  defp term_to_pattern({:literal, :simple, value}, dict_manager) do
    lookup_term_id(dict_manager, RDF.literal(value))
  end

  defp term_to_pattern({:literal, :typed, value, datatype}, dict_manager) do
    lookup_term_id(dict_manager, RDF.literal(value, datatype: datatype))
  end

  defp term_to_pattern({:literal, :lang, value, lang}, dict_manager) do
    lookup_term_id(dict_manager, RDF.literal(value, language: lang))
  end

  defp term_to_pattern(id, _dict_manager) when is_integer(id), do: {:ok, id}

  defp term_to_pattern(_term, _dict_manager), do: {:ok, :not_found}

  # Convert predicate IRI to dictionary ID
  defp predicate_to_id(iri, dict_manager) when is_binary(iri) do
    lookup_term_id(dict_manager, RDF.iri(iri))
  end

  defp predicate_to_id({:named_node, iri}, dict_manager) do
    lookup_term_id(dict_manager, RDF.iri(iri))
  end

  # Lookup a term ID from the dictionary
  defp lookup_term_id(dict_manager, rdf_term) do
    case GenServer.call(dict_manager, :get_db) do
      {:ok, db} ->
        case StringToId.lookup_id(db, rdf_term) do
          {:ok, id} -> {:ok, id}
          :not_found -> {:ok, :not_found}
          {:error, _} = error -> error
        end

      {:error, _} = error ->
        error
    end
  end

  # Check if a pattern element is a not-found marker
  defp not_found?(:not_found), do: true
  defp not_found?(_), do: false

  # Extend binding with matched values
  defp extend_binding(binding, subject, object, s_id, o_id, dict_manager) do
    with {:ok, binding1} <- maybe_bind(binding, subject, s_id, dict_manager),
         {:ok, binding2} <- maybe_bind(binding1, object, o_id, dict_manager) do
      {:ok, binding2}
    end
  end

  # Bind a variable to a value, or verify consistency
  defp maybe_bind(binding, {:variable, name}, id, dict_manager) do
    case Map.get(binding, name) do
      nil ->
        # Unbound - decode and bind
        case decode_term(id, dict_manager) do
          {:ok, term} -> {:ok, Map.put(binding, name, term)}
          {:error, _} = error -> error
        end

      existing_id when is_integer(existing_id) ->
        # Already bound to an ID - check consistency
        if existing_id == id do
          {:ok, binding}
        else
          {:error, :binding_mismatch}
        end

      existing_term ->
        # Already bound to a term - need to check if IDs match
        case encode_term(existing_term, dict_manager) do
          {:ok, existing_id} when existing_id == id -> {:ok, binding}
          {:ok, _} -> {:error, :binding_mismatch}
          :not_found -> {:error, :binding_mismatch}
          {:error, _} -> {:error, :binding_mismatch}
        end
    end
  end

  defp maybe_bind(binding, _concrete_term, _id, _dict_manager) do
    # Concrete term - already matched by index lookup
    {:ok, binding}
  end

  # Decode a term ID back to an algebra term
  defp decode_term(term_id, dict_manager) do
    if Dictionary.inline_encoded?(term_id) do
      decode_inline_term(term_id)
    else
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

  # Encode a term to its ID
  defp encode_term({:named_node, iri}, dict_manager) do
    lookup_term_id(dict_manager, RDF.iri(iri))
  end

  defp encode_term({:blank_node, id}, dict_manager) do
    lookup_term_id(dict_manager, RDF.bnode(id))
  end

  defp encode_term({:literal, :simple, value}, dict_manager) do
    lookup_term_id(dict_manager, RDF.literal(value))
  end

  defp encode_term({:literal, :typed, value, datatype}, dict_manager) do
    lookup_term_id(dict_manager, RDF.literal(value, datatype: datatype))
  end

  defp encode_term({:literal, :lang, value, lang}, dict_manager) do
    lookup_term_id(dict_manager, RDF.literal(value, language: lang))
  end

  defp encode_term(_term, _dict_manager), do: :not_found

  # Convert RDF.ex term to algebra term representation
  defp rdf_term_to_algebra(%RDF.IRI{value: uri}) do
    {:named_node, uri}
  end

  defp rdf_term_to_algebra(%RDF.BlankNode{value: name}) do
    {:blank_node, name}
  end

  defp rdf_term_to_algebra(%RDF.Literal{literal: %{language: lang}} = lit)
       when not is_nil(lang) do
    {:literal, :lang, to_string(lit), lang}
  end

  defp rdf_term_to_algebra(%RDF.Literal{} = lit) do
    datatype_id = RDF.Literal.datatype_id(lit)
    value = to_string(lit)

    if datatype_id == RDF.XSD.String.id() do
      {:literal, :simple, value}
    else
      {:literal, :typed, value, to_string(datatype_id)}
    end
  end

  # Wrap an ID in {:bound, id} for Index module, pass :var through
  defp wrap_bound(:var), do: :var
  defp wrap_bound(id) when is_integer(id), do: {:bound, id}

  # Empty stream helper
  defp empty_stream, do: Stream.map([], & &1)
end
