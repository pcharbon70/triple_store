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

  ### Recursive Paths (Task 3.4.2)
  - **Zero or More** (`p*`): Match zero or more occurrences (includes identity)
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

  alias TripleStore.Index
  alias TripleStore.SPARQL.Term

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
  # Configuration and Limits
  # ===========================================================================

  # Default limits - can be overridden via application config or context
  @default_max_path_depth 100
  @default_max_bidirectional_depth 50
  @default_max_frontier_size 100_000
  @default_max_visited_size 1_000_000
  @default_max_unbounded_results 100_000
  @default_max_all_nodes 50_000

  # Get configurable limit from context or application config
  defp get_limit(ctx, key, default) do
    cond do
      is_map(ctx) and Map.has_key?(ctx, key) ->
        Map.get(ctx, key)

      true ->
        Application.get_env(:triple_store, :property_path, [])
        |> Keyword.get(key, default)
    end
  end

  defp max_path_depth(ctx), do: get_limit(ctx, :max_path_depth, @default_max_path_depth)

  defp max_bidirectional_depth(ctx),
    do: get_limit(ctx, :max_bidirectional_depth, @default_max_bidirectional_depth)

  defp max_frontier_size(ctx), do: get_limit(ctx, :max_frontier_size, @default_max_frontier_size)
  defp max_visited_size(ctx), do: get_limit(ctx, :max_visited_size, @default_max_visited_size)

  defp max_unbounded_results(ctx),
    do: get_limit(ctx, :max_unbounded_results, @default_max_unbounded_results)

  defp max_all_nodes(ctx), do: get_limit(ctx, :max_all_nodes, @default_max_all_nodes)

  # Helper to check if a MapSet is empty (avoids map_size anti-pattern)
  defp mapset_empty?(set), do: MapSet.size(set) == 0

  # Helper to check if MapSets have any intersection
  defp mapsets_intersect?(set1, set2) do
    not Enum.empty?(MapSet.intersection(set1, set2))
  end

  # Helper to generate unique intermediate variable names
  defp gen_intermediate_var(prefix) do
    {:variable, "_#{prefix}_#{:erlang.unique_integer([:positive])}"}
  end

  # Emit telemetry event for resource monitoring
  defp emit_telemetry(event, measurements) do
    :telemetry.execute(
      [:triple_store, :sparql, :property_path, event],
      measurements,
      %{timestamp: System.monotonic_time()}
    )
  end

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
    # Try optimized evaluation first, fall back to standard evaluation
    case evaluate_optimized(ctx, binding, subject, path, object) do
      {:optimized, result} -> result
      :not_optimized -> do_evaluate(ctx, binding, subject, path, object)
    end
  end

  # ===========================================================================
  # Path Optimization (Task 3.4.3)
  # ===========================================================================

  # Try to apply optimizations to the path evaluation
  defp evaluate_optimized(ctx, binding, subject, path, object) do
    s_resolved = resolve_term(subject, binding)
    o_resolved = resolve_term(object, binding)

    cond do
      # Optimization 1: Fixed-length path (sequence of simple predicates)
      fixed_length_path?(path) ->
        {:optimized, evaluate_fixed_length_path(ctx, binding, subject, path, object)}

      # Optimization 2: Bidirectional search for bounded recursive paths
      both_bound?(s_resolved, o_resolved) and recursive_path?(path) ->
        {:optimized,
         evaluate_bidirectional(ctx, binding, subject, path, object, s_resolved, o_resolved)}

      true ->
        :not_optimized
    end
  end

  # Check if path is a fixed-length sequence of simple predicates
  defp fixed_length_path?({:sequence, left, right}) do
    simple_predicate?(left) and (simple_predicate?(right) or fixed_length_path?(right))
  end

  defp fixed_length_path?(_), do: false

  # Check if a path element is a simple predicate (link or named_node)
  defp simple_predicate?({:link, _}), do: true
  defp simple_predicate?({:named_node, _}), do: true
  defp simple_predicate?({:reverse, inner}), do: simple_predicate?(inner)
  defp simple_predicate?(_), do: false

  # Check if both subject and object are bound (not variables)
  defp both_bound?({:variable, _}, _), do: false
  defp both_bound?(_, {:variable, _}), do: false
  defp both_bound?(_, _), do: true

  # Check if path is a recursive path (*, +)
  defp recursive_path?({:zero_or_more, _}), do: true
  defp recursive_path?({:one_or_more, _}), do: true
  defp recursive_path?(_), do: false

  # ===========================================================================
  # Fixed-Length Path Optimization
  # ===========================================================================

  # Optimize fixed-length paths by converting to a sequence of joins
  # Instead of using intermediate streams, we extract all predicates and
  # evaluate as a multi-way join
  defp evaluate_fixed_length_path(ctx, binding, subject, path, object) do
    # Extract the predicate chain
    predicates = extract_predicate_chain(path)

    case predicates do
      [] ->
        {:error, :empty_path}

      [single] ->
        # Just one predicate - evaluate normally
        evaluate_link_predicate(ctx, binding, subject, single, object)

      chain ->
        # Multiple predicates - use chained evaluation with optimized intermediate handling
        evaluate_predicate_chain(ctx, binding, subject, chain, object)
    end
  end

  # Extract predicates from a sequence path
  defp extract_predicate_chain({:sequence, left, right}) do
    left_chain = extract_predicate_chain(left)
    right_chain = extract_predicate_chain(right)
    left_chain ++ right_chain
  end

  defp extract_predicate_chain({:link, iri}), do: [{:forward, iri}]
  defp extract_predicate_chain({:named_node, iri}), do: [{:forward, iri}]
  defp extract_predicate_chain({:reverse, {:link, iri}}), do: [{:reverse, iri}]
  defp extract_predicate_chain({:reverse, {:named_node, iri}}), do: [{:reverse, iri}]
  defp extract_predicate_chain(_), do: []

  # Evaluate a link predicate with direction
  defp evaluate_link_predicate(ctx, binding, subject, {:forward, iri}, object) do
    evaluate_link(ctx, binding, subject, iri, object)
  end

  defp evaluate_link_predicate(ctx, binding, subject, {:reverse, iri}, object) do
    # Reverse: swap subject and object
    evaluate_link(ctx, binding, object, iri, subject)
  end

  # Evaluate a chain of predicates efficiently
  defp evaluate_predicate_chain(ctx, binding, subject, [pred | rest], object) do
    case rest do
      [] ->
        # Last predicate - connect to final object
        evaluate_link_predicate(ctx, binding, subject, pred, object)

      _ ->
        # Intermediate predicate - use a generated variable
        {:variable, var_name} = intermediate = gen_intermediate_var("chain")

        case evaluate_link_predicate(ctx, binding, subject, pred, intermediate) do
          {:ok, stream} ->
            # Chain to rest of predicates
            result_stream =
              Stream.flat_map(stream, fn intermediate_binding ->
                case evaluate_predicate_chain(
                       ctx,
                       intermediate_binding,
                       intermediate,
                       rest,
                       object
                     ) do
                  {:ok, rest_stream} ->
                    # Remove intermediate variable from final bindings
                    Stream.map(rest_stream, &Map.delete(&1, var_name))

                  {:error, _} ->
                    []
                end
              end)

            {:ok, result_stream}

          {:error, _} = error ->
            error
        end
    end
  end

  # ===========================================================================
  # Bidirectional Search Optimization
  # ===========================================================================

  # Use bidirectional BFS when both endpoints are bound
  # This can be significantly faster for sparse graphs where the path
  # might be long but the search space expands exponentially
  defp evaluate_bidirectional(ctx, binding, _subject, path, _object, s_resolved, o_resolved) do
    %{dict_manager: dict_manager} = ctx

    inner_path =
      case path do
        {:zero_or_more, inner} -> inner
        {:one_or_more, inner} -> inner
      end

    include_identity = match?({:zero_or_more, _}, path)

    with {:ok, s_id} <- term_to_id(s_resolved, dict_manager),
         {:ok, o_id} <- term_to_id(o_resolved, dict_manager) do
      if s_id == :not_found or o_id == :not_found do
        {:ok, empty_stream()}
      else
        # Check identity first for zero-or-more
        if include_identity and s_id == o_id do
          {:ok, Stream.map([binding], & &1)}
        else
          # Bidirectional BFS
          case bidirectional_bfs(ctx, inner_path, s_id, o_id) do
            true -> {:ok, Stream.map([binding], & &1)}
            false -> {:ok, empty_stream()}
          end
        end
      end
    end
  end

  # Bidirectional BFS: search from both ends and meet in the middle
  # Returns true if a path exists, false otherwise
  defp bidirectional_bfs(ctx, inner_path, start_id, target_id) do
    reversed_path = reverse_path(inner_path)

    # Take one step from each side first (since we need at least one step for one-or-more paths)
    forward_step_1 =
      get_one_step_forward(ctx, inner_path, start_id)

    backward_step_1 =
      get_one_step_forward(ctx, reversed_path, target_id)

    # Check for 1-hop connection (forward reaches target directly or backward reaches start)
    if MapSet.member?(forward_step_1, target_id) or MapSet.member?(backward_step_1, start_id) do
      true
    else
      # Check for 2-hop connection (frontiers meet in the middle)
      if MapSet.intersection(forward_step_1, backward_step_1) |> MapSet.size() > 0 do
        true
      else
        # Continue with bidirectional BFS
        # IMPORTANT: visited sets track only discovered nodes, NOT the start/target
        # This ensures we don't report a path when start == target but no cycle exists
        forward_visited = forward_step_1
        backward_visited = backward_step_1

        do_bidirectional_bfs(
          ctx,
          inner_path,
          reversed_path,
          forward_step_1,
          backward_step_1,
          forward_visited,
          backward_visited,
          1
        )
      end
    end
  end

  defp do_bidirectional_bfs(
         ctx,
         inner_path,
         reversed_path,
         forward_frontier,
         backward_frontier,
         forward_visited,
         backward_visited,
         depth
       )
       when depth > 0 do
    # Check termination conditions
    max_depth = max_bidirectional_depth(ctx)
    max_frontier = max_frontier_size(ctx)

    cond do
      depth > max_depth ->
        emit_telemetry(:bidirectional_depth_limit, %{depth: depth})
        false

      mapset_empty?(forward_frontier) and mapset_empty?(backward_frontier) ->
        false

      MapSet.size(forward_visited) > max_frontier or MapSet.size(backward_visited) > max_frontier ->
        emit_telemetry(:bidirectional_frontier_limit, %{
          forward_size: MapSet.size(forward_visited),
          backward_size: MapSet.size(backward_visited)
        })

        false

      true ->
        do_bidirectional_bfs_step(
          ctx,
          inner_path,
          reversed_path,
          forward_frontier,
          backward_frontier,
          forward_visited,
          backward_visited,
          depth
        )
    end
  end

  # Initial call - start the BFS
  defp do_bidirectional_bfs(
         ctx,
         inner_path,
         reversed_path,
         forward_frontier,
         backward_frontier,
         forward_visited,
         backward_visited,
         0 = depth
       ) do
    do_bidirectional_bfs_step(
      ctx,
      inner_path,
      reversed_path,
      forward_frontier,
      backward_frontier,
      forward_visited,
      backward_visited,
      depth
    )
  end

  defp do_bidirectional_bfs_step(
         ctx,
         inner_path,
         reversed_path,
         forward_frontier,
         backward_frontier,
         forward_visited,
         backward_visited,
         depth
       ) do
    # Expand the smaller frontier (optimization)
    {new_forward_frontier, new_forward_visited, new_backward_frontier, new_backward_visited} =
      if MapSet.size(forward_frontier) <= MapSet.size(backward_frontier) do
        # Expand forward
        next_forward = expand_frontier(ctx, inner_path, forward_frontier)
        new_forward = MapSet.difference(next_forward, forward_visited)

        {new_forward, MapSet.union(forward_visited, new_forward), backward_frontier,
         backward_visited}
      else
        # Expand backward
        next_backward = expand_frontier(ctx, reversed_path, backward_frontier)
        new_backward = MapSet.difference(next_backward, backward_visited)

        {forward_frontier, forward_visited, new_backward,
         MapSet.union(backward_visited, new_backward)}
      end

    # Check for intersection (path found)
    if mapsets_intersect?(new_forward_visited, new_backward_visited) do
      true
    else
      do_bidirectional_bfs(
        ctx,
        inner_path,
        reversed_path,
        new_forward_frontier,
        new_backward_frontier,
        new_forward_visited,
        new_backward_visited,
        depth + 1
      )
    end
  end

  # Expand a frontier by one step
  defp expand_frontier(ctx, path, frontier) do
    frontier
    |> Enum.flat_map(fn node_id ->
      get_one_step_forward(ctx, path, node_id) |> MapSet.to_list()
    end)
    |> MapSet.new()
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

  # Zero-or-more path: p*
  # Match zero or more occurrences (includes identity: subject = object)
  defp do_evaluate(ctx, binding, subject, {:zero_or_more, inner_path}, object) do
    evaluate_zero_or_more(ctx, binding, subject, inner_path, object)
  end

  # One-or-more path: p+
  # Match one or more occurrences
  defp do_evaluate(ctx, binding, subject, {:one_or_more, inner_path}, object) do
    evaluate_one_or_more(ctx, binding, subject, inner_path, object)
  end

  # Zero-or-one path: p?
  # Match zero or one occurrence (optional)
  defp do_evaluate(ctx, binding, subject, {:zero_or_one, inner_path}, object) do
    evaluate_zero_or_one(ctx, binding, subject, inner_path, object)
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
    {:variable, var_name} = intermediate = gen_intermediate_var("seq")

    # First, evaluate left path: subject-[left]->intermediate
    case do_evaluate(ctx, binding, subject, left, intermediate) do
      {:ok, left_stream} ->
        # For each result, evaluate right path: intermediate-[right]->object
        result_stream =
          Stream.flat_map(left_stream, fn intermediate_binding ->
            case do_evaluate(ctx, intermediate_binding, intermediate, right, object) do
              {:ok, right_stream} ->
                # Filter out the intermediate variable from results
                Stream.map(right_stream, &Map.delete(&1, var_name))

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
  # Zero-or-More Path (p*)
  # ===========================================================================

  # Zero-or-more matches identity (subject = object) OR one-or-more matches
  # Uses breadth-first search with cycle detection
  defp evaluate_zero_or_more(ctx, binding, subject, inner_path, object) do
    %{dict_manager: dict_manager} = ctx

    # Resolve subject and object from binding
    s_resolved = resolve_term(subject, binding)
    o_resolved = resolve_term(object, binding)

    case {s_resolved, o_resolved} do
      # Both bound: check if there's a path between them (including identity)
      {{:variable, _}, {:variable, _}} ->
        # Both unbound: enumerate all reachable pairs
        evaluate_zero_or_more_both_unbound(ctx, binding, subject, inner_path, object)

      {{:variable, _}, _} ->
        # Object bound, subject unbound: find all nodes that can reach object
        evaluate_zero_or_more_reverse(ctx, binding, subject, inner_path, object, o_resolved)

      {_, {:variable, _}} ->
        # Subject bound, object unbound: find all reachable nodes
        evaluate_zero_or_more_forward(ctx, binding, subject, inner_path, object, s_resolved)

      {_, _} ->
        # Both bound: check if there's a path (including identity)
        evaluate_zero_or_more_both_bound(
          ctx,
          binding,
          subject,
          inner_path,
          object,
          s_resolved,
          o_resolved,
          dict_manager
        )
    end
  end

  # Both subject and object are bound - check for path existence (including identity)
  defp evaluate_zero_or_more_both_bound(
         ctx,
         binding,
         subject,
         inner_path,
         object,
         s_resolved,
         o_resolved,
         dict_manager
       ) do
    # Check identity first (zero steps)
    if terms_equal?(s_resolved, o_resolved, dict_manager) do
      {:ok, Stream.map([binding], & &1)}
    else
      # Check for one-or-more path
      case evaluate_one_or_more(ctx, binding, subject, inner_path, object) do
        {:ok, stream} ->
          # If any result exists, return the binding
          if Enum.any?(stream) do
            {:ok, Stream.map([binding], & &1)}
          else
            {:ok, empty_stream()}
          end

        {:error, _} = error ->
          error
      end
    end
  end

  # Subject bound, object unbound - forward BFS
  defp evaluate_zero_or_more_forward(ctx, binding, _subject, inner_path, object, s_resolved) do
    %{dict_manager: dict_manager} = ctx

    # Get subject ID for identity result
    case term_to_id(s_resolved, dict_manager) do
      {:ok, s_id} when s_id != :not_found ->
        # BFS to find all reachable nodes
        reachable = bfs_forward(ctx, inner_path, MapSet.new([s_id]), MapSet.new([s_id]))

        # Convert reachable IDs to bindings
        binding_stream =
          reachable
          |> Stream.flat_map(fn node_id ->
            case maybe_bind(binding, object, node_id, dict_manager) do
              {:ok, new_binding} -> [new_binding]
              {:error, _} -> []
            end
          end)

        {:ok, binding_stream}

      {:ok, :not_found} ->
        {:ok, empty_stream()}

      {:error, _} = error ->
        error
    end
  end

  # Object bound, subject unbound - find nodes that can reach object via path
  # This is equivalent to finding nodes reachable from object via reversed path
  defp evaluate_zero_or_more_reverse(ctx, binding, subject, inner_path, _object, o_resolved) do
    %{dict_manager: dict_manager} = ctx

    case term_to_id(o_resolved, dict_manager) do
      {:ok, o_id} when o_id != :not_found ->
        # BFS forward from object using the REVERSED path to find all sources
        reversed_path = reverse_path(inner_path)
        reachable = bfs_forward(ctx, reversed_path, MapSet.new([o_id]), MapSet.new([o_id]))

        # Convert reachable IDs to bindings
        binding_stream =
          reachable
          |> Stream.flat_map(fn node_id ->
            case maybe_bind(binding, subject, node_id, dict_manager) do
              {:ok, new_binding} -> [new_binding]
              {:error, _} -> []
            end
          end)

        {:ok, binding_stream}

      {:ok, :not_found} ->
        {:ok, empty_stream()}

      {:error, _} = error ->
        error
    end
  end

  # Both subject and object unbound - enumerate all reachable pairs
  # NOTE: This is expensive and has result limits for DoS protection
  defp evaluate_zero_or_more_both_unbound(ctx, binding, subject, inner_path, object) do
    %{db: db, dict_manager: dict_manager} = ctx
    max_results = max_unbounded_results(ctx)

    # Get all nodes in the graph (subjects and objects of any triple)
    all_nodes = get_all_nodes(ctx, db)

    # For each starting node, find all reachable nodes and emit pairs
    # Use Stream.take to limit result count for DoS protection
    binding_stream =
      all_nodes
      |> Stream.flat_map(fn start_id ->
        reachable = bfs_forward(ctx, inner_path, MapSet.new([start_id]), MapSet.new([start_id]))

        reachable
        |> Enum.flat_map(fn end_id ->
          with {:ok, binding1} <- maybe_bind(binding, subject, start_id, dict_manager),
               {:ok, binding2} <- maybe_bind(binding1, object, end_id, dict_manager) do
            [binding2]
          else
            _ -> []
          end
        end)
      end)
      |> Stream.take(max_results)

    {:ok, binding_stream}
  end

  # ===========================================================================
  # One-or-More Path (p+)
  # ===========================================================================

  # One-or-more: at least one step, then zero-or-more
  defp evaluate_one_or_more(ctx, binding, subject, inner_path, object) do
    %{dict_manager: dict_manager} = ctx

    s_resolved = resolve_term(subject, binding)
    o_resolved = resolve_term(object, binding)

    case {s_resolved, o_resolved} do
      {{:variable, _}, {:variable, _}} ->
        # Both unbound: enumerate all pairs with at least one path step
        evaluate_one_or_more_both_unbound(ctx, binding, subject, inner_path, object)

      {{:variable, _}, _} ->
        # Object bound, subject unbound
        evaluate_one_or_more_reverse(ctx, binding, subject, inner_path, object, o_resolved)

      {_, {:variable, _}} ->
        # Subject bound, object unbound
        evaluate_one_or_more_forward(ctx, binding, subject, inner_path, object, s_resolved)

      {_, _} ->
        # Both bound: check if there's a path of length >= 1
        evaluate_one_or_more_both_bound(
          ctx,
          binding,
          subject,
          inner_path,
          object,
          s_resolved,
          o_resolved,
          dict_manager
        )
    end
  end

  # Both bound - check for path of length >= 1
  defp evaluate_one_or_more_both_bound(
         ctx,
         binding,
         _subject,
         inner_path,
         _object,
         s_resolved,
         o_resolved,
         dict_manager
       ) do
    case term_to_id(s_resolved, dict_manager) do
      {:ok, s_id} when s_id != :not_found ->
        case term_to_id(o_resolved, dict_manager) do
          {:ok, o_id} when o_id != :not_found ->
            # BFS from subject, excluding the start itself
            reachable = bfs_forward(ctx, inner_path, MapSet.new([s_id]), MapSet.new())

            if MapSet.member?(reachable, o_id) do
              {:ok, Stream.map([binding], & &1)}
            else
              {:ok, empty_stream()}
            end

          {:ok, :not_found} ->
            {:ok, empty_stream()}

          {:error, _} = error ->
            error
        end

      {:ok, :not_found} ->
        {:ok, empty_stream()}

      {:error, _} = error ->
        error
    end
  end

  # Subject bound, object unbound - forward BFS excluding start
  defp evaluate_one_or_more_forward(ctx, binding, _subject, inner_path, object, s_resolved) do
    %{dict_manager: dict_manager} = ctx

    case term_to_id(s_resolved, dict_manager) do
      {:ok, s_id} when s_id != :not_found ->
        # BFS to find all reachable nodes, excluding the start itself
        reachable = bfs_forward(ctx, inner_path, MapSet.new([s_id]), MapSet.new())

        binding_stream =
          reachable
          |> Stream.flat_map(fn node_id ->
            case maybe_bind(binding, object, node_id, dict_manager) do
              {:ok, new_binding} -> [new_binding]
              {:error, _} -> []
            end
          end)

        {:ok, binding_stream}

      {:ok, :not_found} ->
        {:ok, empty_stream()}

      {:error, _} = error ->
        error
    end
  end

  # Object bound, subject unbound - find nodes that can reach object via path (at least one step)
  defp evaluate_one_or_more_reverse(ctx, binding, subject, inner_path, _object, o_resolved) do
    %{dict_manager: dict_manager} = ctx

    case term_to_id(o_resolved, dict_manager) do
      {:ok, o_id} when o_id != :not_found ->
        # BFS forward from object using REVERSED path, excluding the start itself
        reversed_path = reverse_path(inner_path)
        reachable = bfs_forward(ctx, reversed_path, MapSet.new([o_id]), MapSet.new())

        binding_stream =
          reachable
          |> Stream.flat_map(fn node_id ->
            case maybe_bind(binding, subject, node_id, dict_manager) do
              {:ok, new_binding} -> [new_binding]
              {:error, _} -> []
            end
          end)

        {:ok, binding_stream}

      {:ok, :not_found} ->
        {:ok, empty_stream()}

      {:error, _} = error ->
        error
    end
  end

  # Both unbound - enumerate all pairs
  # NOTE: This is expensive and has result limits for DoS protection
  defp evaluate_one_or_more_both_unbound(ctx, binding, subject, inner_path, object) do
    %{db: db, dict_manager: dict_manager} = ctx
    max_results = max_unbounded_results(ctx)

    all_nodes = get_all_nodes(ctx, db)

    binding_stream =
      all_nodes
      |> Stream.flat_map(fn start_id ->
        # Exclude start from results (one-or-more, not zero-or-more)
        reachable = bfs_forward(ctx, inner_path, MapSet.new([start_id]), MapSet.new())

        reachable
        |> Enum.flat_map(fn end_id ->
          with {:ok, binding1} <- maybe_bind(binding, subject, start_id, dict_manager),
               {:ok, binding2} <- maybe_bind(binding1, object, end_id, dict_manager) do
            [binding2]
          else
            _ -> []
          end
        end)
      end)
      |> Stream.take(max_results)

    {:ok, binding_stream}
  end

  # ===========================================================================
  # Zero-or-One Path (p?)
  # ===========================================================================

  # Zero-or-one: identity OR exactly one step
  defp evaluate_zero_or_one(ctx, binding, subject, inner_path, object) do
    %{dict_manager: dict_manager} = ctx

    s_resolved = resolve_term(subject, binding)
    o_resolved = resolve_term(object, binding)

    case {s_resolved, o_resolved} do
      {{:variable, _}, {:variable, _}} ->
        # Both unbound: enumerate identity + one-step pairs
        evaluate_zero_or_one_both_unbound(ctx, binding, subject, inner_path, object)

      {{:variable, _}, _} ->
        # Object bound, subject unbound
        evaluate_zero_or_one_reverse(ctx, binding, subject, inner_path, object, o_resolved)

      {_, {:variable, _}} ->
        # Subject bound, object unbound
        evaluate_zero_or_one_forward(ctx, binding, subject, inner_path, object, s_resolved)

      {_, _} ->
        # Both bound: identity or one-step path
        evaluate_zero_or_one_both_bound(
          ctx,
          binding,
          subject,
          inner_path,
          object,
          s_resolved,
          o_resolved,
          dict_manager
        )
    end
  end

  # Both bound - check identity or one-step path
  defp evaluate_zero_or_one_both_bound(
         ctx,
         binding,
         subject,
         inner_path,
         object,
         s_resolved,
         o_resolved,
         dict_manager
       ) do
    # Check identity first
    if terms_equal?(s_resolved, o_resolved, dict_manager) do
      {:ok, Stream.map([binding], & &1)}
    else
      # Check for exactly one step
      case do_evaluate(ctx, binding, subject, inner_path, object) do
        {:ok, stream} ->
          if Enum.any?(stream) do
            {:ok, Stream.map([binding], & &1)}
          else
            {:ok, empty_stream()}
          end

        {:error, _} = error ->
          error
      end
    end
  end

  # Subject bound, object unbound - identity + one step
  defp evaluate_zero_or_one_forward(ctx, binding, _subject, inner_path, object, s_resolved) do
    %{dict_manager: dict_manager} = ctx

    case term_to_id(s_resolved, dict_manager) do
      {:ok, s_id} when s_id != :not_found ->
        # Get one-step reachable nodes
        one_step = get_one_step_forward(ctx, inner_path, s_id)

        # Include identity (s_id itself) and one-step nodes
        all_reachable = MapSet.put(one_step, s_id)

        binding_stream =
          all_reachable
          |> Stream.flat_map(fn node_id ->
            case maybe_bind(binding, object, node_id, dict_manager) do
              {:ok, new_binding} -> [new_binding]
              {:error, _} -> []
            end
          end)

        {:ok, binding_stream}

      {:ok, :not_found} ->
        {:ok, empty_stream()}

      {:error, _} = error ->
        error
    end
  end

  # Object bound, subject unbound - identity + one step via reversed path
  defp evaluate_zero_or_one_reverse(ctx, binding, subject, inner_path, _object, o_resolved) do
    %{dict_manager: dict_manager} = ctx

    case term_to_id(o_resolved, dict_manager) do
      {:ok, o_id} when o_id != :not_found ->
        # Get one-step reachable nodes using REVERSED path
        reversed_path = reverse_path(inner_path)
        one_step = get_one_step_forward(ctx, reversed_path, o_id)

        # Include identity and one-step nodes
        all_reachable = MapSet.put(one_step, o_id)

        binding_stream =
          all_reachable
          |> Stream.flat_map(fn node_id ->
            case maybe_bind(binding, subject, node_id, dict_manager) do
              {:ok, new_binding} -> [new_binding]
              {:error, _} -> []
            end
          end)

        {:ok, binding_stream}

      {:ok, :not_found} ->
        {:ok, empty_stream()}

      {:error, _} = error ->
        error
    end
  end

  # Both unbound - enumerate all identity + one-step pairs
  # NOTE: This is expensive and has result limits for DoS protection
  defp evaluate_zero_or_one_both_unbound(ctx, binding, subject, inner_path, object) do
    %{db: db, dict_manager: dict_manager} = ctx
    max_results = max_unbounded_results(ctx)

    all_nodes = get_all_nodes(ctx, db)

    binding_stream =
      all_nodes
      |> Stream.flat_map(fn node_id ->
        # Identity: node -> node
        one_step = get_one_step_forward(ctx, inner_path, node_id)

        # Include identity and one-step results
        all_targets = MapSet.put(one_step, node_id)

        all_targets
        |> Enum.flat_map(fn target_id ->
          with {:ok, binding1} <- maybe_bind(binding, subject, node_id, dict_manager),
               {:ok, binding2} <- maybe_bind(binding1, object, target_id, dict_manager) do
            [binding2]
          else
            _ -> []
          end
        end)
      end)
      |> Stream.take(max_results)

    {:ok, binding_stream}
  end

  # ===========================================================================
  # BFS Traversal with Cycle Detection
  # ===========================================================================

  # BFS forward: find all nodes reachable via the inner path
  # frontier: nodes to explore next
  # visited: nodes already found (for cycle detection)
  defp bfs_forward(ctx, inner_path, frontier, visited, depth \\ 0)

  defp bfs_forward(ctx, inner_path, frontier, visited, depth) do
    max_depth = max_path_depth(ctx)
    max_visited = max_visited_size(ctx)
    max_frontier = max_frontier_size(ctx)

    cond do
      # Depth limit reached
      depth > max_depth ->
        emit_telemetry(:bfs_depth_limit, %{depth: depth, visited_size: MapSet.size(visited)})
        visited

      # No more nodes to explore
      mapset_empty?(frontier) ->
        visited

      # Visited set too large (memory protection)
      MapSet.size(visited) > max_visited ->
        emit_telemetry(:bfs_visited_limit, %{visited_size: MapSet.size(visited)})
        visited

      # Frontier too large (memory protection)
      MapSet.size(frontier) > max_frontier ->
        emit_telemetry(:bfs_frontier_limit, %{frontier_size: MapSet.size(frontier)})
        visited

      # Continue BFS
      true ->
        # Get all nodes reachable in one step from the frontier
        next_nodes = expand_frontier(ctx, inner_path, frontier)

        # Filter out already visited nodes
        new_frontier = MapSet.difference(next_nodes, visited)
        new_visited = MapSet.union(visited, new_frontier)

        # Continue BFS if there are new nodes to explore
        if mapset_empty?(new_frontier) do
          new_visited
        else
          bfs_forward(ctx, inner_path, new_frontier, new_visited, depth + 1)
        end
    end
  end

  # Get nodes reachable in exactly one step via the inner path (forward)
  defp get_one_step_forward(ctx, inner_path, node_id) do
    %{dict_manager: dict_manager} = ctx

    # Decode the node ID to an algebra term
    case Term.decode(node_id, dict_manager) do
      {:ok, node_term} ->
        # Create a variable for the target
        {:variable, var_name} = target_var = gen_intermediate_var("step")

        # Evaluate the inner path
        case do_evaluate(ctx, %{}, node_term, inner_path, target_var) do
          {:ok, stream} ->
            stream
            |> Enum.flat_map(fn binding ->
              case Map.get(binding, var_name) do
                nil ->
                  []

                term ->
                  case term_to_id(term, dict_manager) do
                    {:ok, id} when id != :not_found -> [id]
                    _ -> []
                  end
              end
            end)
            |> MapSet.new()

          {:error, _} ->
            MapSet.new()
        end

      {:error, _} ->
        MapSet.new()
    end
  end

  # Get all nodes (subjects and objects) in the graph with limit protection
  defp get_all_nodes(ctx, db) do
    max_nodes = max_all_nodes(ctx)

    # Query all triples with all variables
    case Index.lookup(db, {:var, :var, :var}) do
      {:ok, stream} ->
        # Use Stream.take to limit memory usage during collection
        nodes =
          stream
          |> Stream.take(max_nodes * 2)
          |> Enum.flat_map(fn {s, _p, o} -> [s, o] end)
          |> MapSet.new()

        if MapSet.size(nodes) >= max_nodes do
          emit_telemetry(:all_nodes_limit, %{size: MapSet.size(nodes), limit: max_nodes})
        end

        nodes

      {:error, _} ->
        MapSet.new()
    end
  end

  # Check if two terms are equal
  defp terms_equal?(term1, term2, dict_manager) do
    case {term_to_id(term1, dict_manager), term_to_id(term2, dict_manager)} do
      {{:ok, id1}, {:ok, id2}} when id1 != :not_found and id2 != :not_found ->
        id1 == id2

      _ ->
        false
    end
  end

  # Convert term to ID
  defp term_to_id({:named_node, iri}, dict_manager) do
    lookup_term_id(dict_manager, RDF.iri(iri))
  end

  defp term_to_id({:blank_node, id}, dict_manager) do
    lookup_term_id(dict_manager, RDF.bnode(id))
  end

  defp term_to_id({:literal, :simple, value}, dict_manager) do
    lookup_term_id(dict_manager, RDF.literal(value))
  end

  defp term_to_id({:literal, :typed, value, datatype}, dict_manager) do
    lookup_term_id(dict_manager, RDF.literal(value, datatype: datatype))
  end

  defp term_to_id({:literal, :lang, value, lang}, dict_manager) do
    lookup_term_id(dict_manager, RDF.literal(value, language: lang))
  end

  defp term_to_id(id, _dict_manager) when is_integer(id), do: {:ok, id}

  defp term_to_id(_term, _dict_manager), do: {:ok, :not_found}

  # ===========================================================================
  # Path Reversal
  # ===========================================================================

  # Reverse a path expression (for computing backwards reachability)
  defp reverse_path({:link, iri}), do: {:reverse, {:link, iri}}
  defp reverse_path({:named_node, iri}), do: {:reverse, {:named_node, iri}}
  defp reverse_path({:reverse, inner}), do: inner

  defp reverse_path({:sequence, left, right}),
    do: {:sequence, reverse_path(right), reverse_path(left)}

  defp reverse_path({:alternative, left, right}),
    do: {:alternative, reverse_path(left), reverse_path(right)}

  defp reverse_path({:zero_or_more, inner}), do: {:zero_or_more, reverse_path(inner)}
  defp reverse_path({:one_or_more, inner}), do: {:one_or_more, reverse_path(inner)}
  defp reverse_path({:zero_or_one, inner}), do: {:zero_or_one, reverse_path(inner)}
  defp reverse_path({:negated_property_set, iris}), do: {:negated_property_set, iris}

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

  # Handle blank nodes - they can be bound in a join context
  # The binding may have the blank node tuple as a key with an integer ID as value
  defp resolve_term({:blank_node, _id} = bnode, binding) do
    case Map.get(binding, bnode) do
      nil -> bnode
      id when is_integer(id) -> id
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
  # Note: Wraps Term.lookup_term_id to return {:ok, :not_found} instead of :not_found
  # to allow pattern matching in callers that expect all results wrapped in :ok
  defp lookup_term_id(dict_manager, rdf_term) do
    case Term.lookup_term_id(dict_manager, rdf_term) do
      {:ok, id} -> {:ok, id}
      :not_found -> {:ok, :not_found}
      {:error, _} = error -> error
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
        case Term.decode(id, dict_manager) do
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
        case Term.encode(existing_term, dict_manager) do
          {:ok, existing_id} when existing_id == id -> {:ok, binding}
          {:ok, _} -> {:error, :binding_mismatch}
          :not_found -> {:error, :binding_mismatch}
          {:error, _} -> {:error, :binding_mismatch}
        end
    end
  end

  # Bind a blank node to a value, for join context support
  # Blank nodes act like variables in joins - they need to be in the binding
  defp maybe_bind(binding, {:blank_node, _id} = bnode, id, _dict_manager) do
    case Map.get(binding, bnode) do
      nil ->
        # Unbound blank node - bind to the ID for join matching
        {:ok, Map.put(binding, bnode, id)}

      existing_id when is_integer(existing_id) ->
        # Already bound - check consistency
        if existing_id == id do
          {:ok, binding}
        else
          {:error, :binding_mismatch}
        end

      _other ->
        # Bound to something else - should not happen for blank nodes
        {:error, :binding_mismatch}
    end
  end

  defp maybe_bind(binding, _concrete_term, _id, _dict_manager) do
    # Concrete term - already matched by index lookup
    {:ok, binding}
  end

  # Wrap an ID in {:bound, id} for Index module, pass :var through
  defp wrap_bound(:var), do: :var
  defp wrap_bound(id) when is_integer(id), do: {:bound, id}

  # Empty stream helper
  defp empty_stream, do: Stream.map([], & &1)
end
