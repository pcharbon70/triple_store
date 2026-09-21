defmodule TripleStore.SPARQL.Leapfrog.QuadScanPlan do
  @moduledoc """
  Pure planning and key-decoding rules for a single quad-pattern scan.

  The module owns the representation boundary between semantic quad order
  `{subject, predicate, object, graph}` and the physical GSPO, GPOS, SPOG, and
  POSG index orders. It performs no storage I/O and owns no iterator resources.
  """

  @type variable :: {:variable, String.t()}
  @type component :: non_neg_integer() | variable()
  @type normalized_component :: component() | :not_found
  @type pattern :: {:quad, component(), component(), component(), component()}

  @type normalized_pattern ::
          {:quad, normalized_component(), normalized_component(), normalized_component(),
           normalized_component()}

  @type index :: :gspo | :gpos | :spog | :posg
  @type iterator_plan :: [{3, index(), 0..3, binary()}]

  @indexes [:gspo, :gpos, :spog, :posg]
  @positions [:subject, :predicate, :object, :graph]

  @doc "Normalizes and validates a quad pattern at the planner boundary."
  @spec normalize(tuple()) :: {:ok, normalized_pattern()} | {:error, term()}
  def normalize({:quad, s, p, o, g}) do
    [s, p, o, g]
    |> Enum.zip(@positions)
    |> Enum.reduce_while({:ok, []}, fn {component, position}, {:ok, acc} ->
      case normalize_component(component) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        :error -> {:halt, {:error, {:invalid_quad_component, position, component}}}
      end
    end)
    |> normalized_pattern()
  end

  def normalize(_pattern), do: {:error, :invalid_quad_pattern}

  @doc "Builds the single physical scan plan for a valid quad pattern."
  @spec build(tuple()) :: {:ok, iterator_plan()} | {:error, term()}
  def build(pattern) do
    with {:ok, normalized} <- normalize(pattern) do
      normalized
      |> components()
      |> build_for_components()
    end
  end

  @doc "Returns the components in semantic subject-predicate-object-graph order."
  @spec components(normalized_pattern()) :: [normalized_component()]
  def components({:quad, s, p, o, g}), do: [s, p, o, g]

  @doc "Returns true when compatibility normalization found an absent dictionary ID."
  @spec not_found?(normalized_pattern()) :: boolean()
  def not_found?(pattern), do: :not_found in components(pattern)

  @doc "Returns the distinct, non-anonymous variable names in encounter order."
  @spec variables(normalized_pattern()) :: [String.t()]
  def variables(pattern) do
    pattern
    |> components()
    |> Enum.flat_map(fn
      {:variable, "_"} -> []
      {:variable, name} -> [name]
      _bound -> []
    end)
    |> Enum.uniq()
  end

  @doc "Decodes a physical index key into semantic quad order."
  @spec decode_key(binary(), index()) ::
          {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}
  def decode_key(<<g::64-big, s::64-big, p::64-big, o::64-big>>, :gspo),
    do: {s, p, o, g}

  def decode_key(<<g::64-big, p::64-big, o::64-big, s::64-big>>, :gpos),
    do: {s, p, o, g}

  def decode_key(<<s::64-big, p::64-big, o::64-big, g::64-big>>, :spog),
    do: {s, p, o, g}

  def decode_key(<<p::64-big, o::64-big, s::64-big, g::64-big>>, :posg),
    do: {s, p, o, g}

  @doc "Matches a physical key and constructs canonical binary-key bindings."
  @spec bindings_for_key(binary(), index(), normalized_pattern()) ::
          {:ok, %{optional(String.t()) => non_neg_integer()}} | :no_match
  def bindings_for_key(key, index, pattern) do
    values = key |> decode_key(index) |> Tuple.to_list()

    pattern
    |> components()
    |> Enum.zip(values)
    |> Enum.reduce_while({:ok, %{}}, &match_component/2)
  end

  defp build_for_components(components) do
    if Enum.all?(components, &is_integer/1) do
      {:ok, []}
    else
      {index, prefix_components} = best_index_and_prefix(components)
      prefix = encode_prefix(prefix_components)
      {:ok, [{3, index, length(prefix_components), prefix}]}
    end
  end

  defp best_index_and_prefix(components) do
    @indexes
    |> Enum.map(fn index ->
      {index, contiguous_bound_prefix(components_for_index(components, index))}
    end)
    |> Enum.max_by(fn {_index, prefix_components} -> length(prefix_components) end)
  end

  defp contiguous_bound_prefix(components), do: Enum.take_while(components, &is_integer/1)
  defp encode_prefix(components), do: Enum.map_join(components, fn id -> <<id::64-big>> end)

  defp components_for_index([s, p, o, g], :gspo), do: [g, s, p, o]
  defp components_for_index([s, p, o, g], :gpos), do: [g, p, o, s]
  defp components_for_index([s, p, o, g], :spog), do: [s, p, o, g]
  defp components_for_index([s, p, o, g], :posg), do: [p, o, s, g]

  defp match_component({component, value}, {:ok, bindings}) when is_integer(component) do
    if component == value, do: {:cont, {:ok, bindings}}, else: {:halt, :no_match}
  end

  defp match_component({{:variable, "_"}, _value}, {:ok, bindings}) do
    {:cont, {:ok, bindings}}
  end

  defp match_component({{:variable, name}, value}, {:ok, bindings}) do
    case Map.fetch(bindings, name) do
      :error -> {:cont, {:ok, Map.put(bindings, name, value)}}
      {:ok, ^value} -> {:cont, {:ok, bindings}}
      {:ok, _other} -> {:halt, :no_match}
    end
  end

  defp normalize_component(component) when is_integer(component) and component >= 0,
    do: {:ok, component}

  defp normalize_component({:bound, component})
       when is_integer(component) and component >= 0,
       do: {:ok, component}

  defp normalize_component({:bound, nil}), do: {:ok, :not_found}
  defp normalize_component(:default_graph), do: {:ok, 0}

  defp normalize_component({:variable, name}) when is_binary(name) and byte_size(name) > 0,
    do: {:ok, {:variable, name}}

  defp normalize_component(_component), do: :error

  defp normalized_pattern({:ok, reversed}) do
    [s, p, o, g] = Enum.reverse(reversed)
    {:ok, {:quad, s, p, o, g}}
  end

  defp normalized_pattern({:error, _} = error), do: error
end
