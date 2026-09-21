defmodule TripleStore.SPARQL.Leapfrog.QuadLeapfrog do
  @moduledoc """
  Lazy, index-backed execution for one quad pattern.

  A quad pattern is `{:quad, subject, predicate, object, graph}`. Bound
  components are non-negative dictionary IDs and variables are represented as
  `{:variable, binary_name}`. The stream returns maps from those binary names to
  dictionary IDs. Anonymous variables named `_` participate in matching but are
  not returned.

  One quad pattern represents one relation. It is therefore executed with one
  physical index scan rather than intersecting the four component domains as if
  they represented the same join variable. Multi-pattern joins remain owned by
  the SPARQL executor.

  The planner chooses the quad index with the longest contiguous bound prefix.
  The prefix stops at the first variable; gaps are never encoded as zero. Any
  remaining bound components and repeated variables are checked while scanning.

  `stream/1` owns the iterator once enumeration begins and closes it on normal
  exhaustion, early halt, or exception. A caller that constructs a value but
  never enumerates it must call `close/1`.
  """

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.SPARQL.Leapfrog.{QuadScanPlan, QuadTrieIterator}

  @type variable :: {:variable, String.t()}
  @type component :: non_neg_integer() | variable()
  @type pattern :: {:quad, component(), component(), component(), component()}
  @type index :: :gspo | :gpos | :spog | :posg

  @typedoc """
  One physical scan plan: `{scan_level, index, prefix_depth, prefix}`.

  `scan_level` is currently `3` because execution reads complete 32-byte keys.
  `prefix_depth` is the number of contiguous bound components encoded in the
  prefix. Fully bound patterns use direct lookup and have an empty plan.
  """
  @type iterator_plan :: [{3, index(), 0..3, binary()}]

  @typedoc "Iterator and metadata needed to decode its physical key order."
  @type tagged_iterator :: %{
          iterator: QuadTrieIterator.t(),
          index: index(),
          prefix_depth: 0..3,
          position: 3,
          variable_name: nil
        }

  @type t :: %__MODULE__{
          leapfrog: nil,
          variables: [String.t()],
          pattern: pattern(),
          bindings: %{optional(String.t()) => non_neg_integer()},
          tagged_iterators: [tagged_iterator()],
          iterations: non_neg_integer(),
          yielded: boolean(),
          advanced: boolean(),
          exhausted: boolean()
        }

  @enforce_keys [:variables, :pattern]
  defstruct leapfrog: nil,
            variables: [],
            pattern: nil,
            bindings: %{},
            tagged_iterators: [],
            iterations: 0,
            yielded: false,
            advanced: false,
            exhausted: false

  @max_iterations 10_000
  @doc """
  Returns the preferred index for a semantic component position.

  Actual execution uses `plan_iterators/1`, which compares every available
  contiguous prefix.
  """
  @spec index_for_position(tuple(), non_neg_integer()) :: index()
  def index_for_position({:quad, s, _p, _o, g}, 0) do
    if bound_component?(g) and bound_component?(s), do: :gspo, else: :spog
  end

  def index_for_position({:quad, _s, _p, _o, g}, 1) do
    if bound_component?(g), do: :gpos, else: :posg
  end

  def index_for_position({:quad, _s, _p, _o, g}, 2) do
    if bound_component?(g), do: :gspo, else: :spog
  end

  def index_for_position({:quad, _s, _p, _o, _g}, 3), do: :gspo

  @doc """
  Builds the canonical physical scan plan for a quad pattern.

  Compatibility tuples such as `{:bound, id}` and `:default_graph` are
  normalized at this boundary. Newly produced patterns should use integer IDs.
  """
  @spec plan_iterators(tuple()) :: {:ok, iterator_plan()} | {:error, term()}
  defdelegate plan_iterators(pattern), to: QuadScanPlan, as: :build

  @doc """
  Constructs a lazy quad-pattern scan.

  Returns `{:exhausted, state}` when a fully bound key is absent or the selected
  prefix has no entries. Invalid input and unavailable stores return tagged
  errors without opening an iterator.
  """
  @spec from_pattern(pid(), tuple()) :: {:ok, t()} | {:exhausted, t()} | {:error, term()}
  def from_pattern(db, pattern) do
    with :ok <- validate_store(db),
         {:ok, normalized} <- QuadScanPlan.normalize(pattern) do
      state = new_state(normalized)

      if QuadScanPlan.not_found?(normalized) do
        exhausted(state)
      else
        open_pattern(db, state)
      end
    end
  end

  @doc """
  Creates the iterator described by the canonical plan.

  This function remains an expert inspection surface. Normal callers should use
  `from_pattern/2` so fully bound lookup and exhaustion are handled.
  """
  @spec create_iterators_for_pattern(pid(), tuple()) ::
          {:ok, [tagged_iterator()]} | {:error, term()}
  def create_iterators_for_pattern(db, pattern) do
    with :ok <- validate_store(db),
         {:ok, normalized} <- QuadScanPlan.normalize(pattern),
         false <- QuadScanPlan.not_found?(normalized),
         {:ok, plan} <- plan_iterators(normalized) do
      open_plan(db, plan)
    else
      true -> {:ok, []}
      {:error, _} = error -> error
    end
  end

  @doc "Returns the first matching binding without advancing past it."
  @spec search(t()) :: {:ok, t()} | {:exhausted, t()} | {:error, term()}
  def search(%__MODULE__{exhausted: true} = state), do: {:exhausted, state}
  def search(%__MODULE__{advanced: true} = state), do: {:ok, state}

  def search(%__MODULE__{tagged_iterators: [], yielded: false} = state) do
    {:ok, %{state | yielded: true, advanced: true, bindings: %{}}}
  end

  def search(%__MODULE__{tagged_iterators: []} = state), do: exhausted(state)
  def search(%__MODULE__{} = state), do: find_match(state)

  @doc "Advances to the next matching quad."
  @spec next(t()) :: {:ok, t()} | {:exhausted, t()} | {:error, term()}
  def next(%__MODULE__{exhausted: true} = state), do: {:exhausted, state}
  def next(%__MODULE__{tagged_iterators: []} = state), do: exhausted(state)

  def next(%__MODULE__{} = state) do
    case advance_iterator(state) do
      {:ok, advanced} -> find_match(%{advanced | advanced: false, bindings: %{}})
      {:exhausted, exhausted_state} -> {:exhausted, exhausted_state}
      {:error, _} = error -> error
    end
  end

  @doc "Returns the current map of binary variable names to dictionary IDs."
  @spec bindings(t()) :: %{optional(String.t()) => non_neg_integer()}
  def bindings(%__MODULE__{bindings: bindings}), do: bindings

  @doc "Returns whether the scan has no remaining result."
  @spec exhausted?(t()) :: boolean()
  def exhausted?(%__MODULE__{exhausted: exhausted}), do: exhausted

  @doc "Returns the currently owned physical iterators."
  @spec iterators(t()) :: [QuadTrieIterator.t()]
  def iterators(%__MODULE__{tagged_iterators: tagged}), do: Enum.map(tagged, & &1.iterator)

  @doc "Closes every iterator owned by the scan. Safe to call more than once."
  @spec close(t()) :: :ok
  def close(%__MODULE__{tagged_iterators: tagged}) do
    Enum.each(tagged, fn %{iterator: iterator} -> safe_close(iterator) end)
    :ok
  end

  @doc """
  Returns a single-use lazy stream of binding maps.

  Construction errors are returned by `from_pattern/2`; iteration errors are
  raised because the Enumerable protocol has no tagged-error return.
  """
  @spec stream(t()) :: Enumerable.t()
  def stream(%__MODULE__{} = state) do
    Stream.resource(
      fn -> state end,
      fn current ->
        result = if current.advanced, do: next(current), else: search(current)

        case result do
          {:ok, matched} -> {[matched.bindings], matched}
          {:exhausted, exhausted_state} -> {:halt, exhausted_state}
          {:error, reason} -> raise "QuadLeapfrog stream error: #{inspect(reason)}"
        end
      end,
      &close/1
    )
  end

  @doc """
  Orders semantic component positions with bound positions first.

  Variable positions use available distinct-count statistics as a stable
  tiebreaker. The physical scan planner remains responsible for index choice.
  """
  @spec quad_variable_ordering(tuple(), map() | nil) ::
          {:ok, [non_neg_integer()]} | {:error, term()}
  def quad_variable_ordering(pattern, stats) do
    with {:ok, normalized} <- QuadScanPlan.normalize(pattern) do
      stats = if is_map(stats), do: stats, else: %{}

      ordering =
        normalized
        |> QuadScanPlan.components()
        |> Enum.with_index()
        |> Enum.sort_by(fn {component, position} ->
          {variable_score(component, position, stats), position}
        end)
        |> Enum.map(fn {_component, position} -> position end)

      {:ok, ordering}
    end
  end

  # Construction

  defp open_pattern(db, %__MODULE__{pattern: pattern} = state) do
    components = QuadScanPlan.components(pattern)

    if Enum.all?(components, &is_integer/1) do
      fully_bound_lookup(db, state, components)
    else
      open_scanning_pattern(db, state, pattern)
    end
  end

  defp open_scanning_pattern(db, state, pattern) do
    with {:ok, plan} <- plan_iterators(pattern),
         {:ok, tagged} <- open_plan(db, plan) do
      classify_opened(%{state | tagged_iterators: tagged})
    end
  end

  defp classify_opened(%__MODULE__{tagged_iterators: [%{iterator: iterator}]} = state) do
    if QuadTrieIterator.exhausted?(iterator), do: exhausted(state), else: {:ok, state}
  end

  defp classify_opened(%__MODULE__{tagged_iterators: []} = state), do: exhausted(state)

  defp open_plan(_db, []), do: {:ok, []}

  defp open_plan(db, [{3, index, prefix_depth, prefix}]) do
    case safe_new_iterator(db, index, prefix) do
      {:ok, iterator} ->
        {:ok,
         [
           %{
             iterator: iterator,
             index: index,
             prefix_depth: prefix_depth,
             position: 3,
             variable_name: nil
           }
         ]}

      {:error, _} = error ->
        error
    end
  end

  defp fully_bound_lookup(db, state, [s, p, o, g]) do
    key = <<g::64-big, s::64-big, p::64-big, o::64-big>>

    case safe_adapter_call(fn -> ErlangAdapter.get(db, :gspo, key) end) do
      {:ok, _value} -> {:ok, state}
      :not_found -> exhausted(state)
      {:error, _} = error -> error
    end
  end

  defp safe_new_iterator(db, index, prefix) do
    safe_adapter_call(fn -> QuadTrieIterator.new(db, index, prefix, 3) end)
  end

  defp safe_adapter_call(fun) do
    fun.()
  catch
    :exit, _reason -> {:error, :store_unavailable}
  end

  defp validate_store(db) when is_pid(db) do
    if Process.alive?(db), do: :ok, else: {:error, :store_unavailable}
  end

  defp validate_store(_db), do: {:error, :store_unavailable}

  # Matching and iteration

  defp find_match(%__MODULE__{iterations: iterations}) when iterations >= @max_iterations do
    {:error, :max_iterations_exceeded}
  end

  defp find_match(%__MODULE__{tagged_iterators: [%{iterator: iterator, index: index}]} = state) do
    case QuadTrieIterator.current_key(iterator) do
      {:ok, key} ->
        match_current_key(state, key, index)

      :exhausted ->
        exhausted(state)
    end
  end

  defp match_current_key(state, key, index) do
    examined = %{state | iterations: state.iterations + 1}

    case QuadScanPlan.bindings_for_key(key, index, state.pattern) do
      {:ok, bindings} -> {:ok, %{examined | bindings: bindings, advanced: true}}
      :no_match -> advance_to_match(examined)
    end
  end

  defp advance_to_match(state) do
    case advance_iterator(state) do
      {:ok, advanced} -> find_match(advanced)
      {:exhausted, exhausted_state} -> {:exhausted, exhausted_state}
      {:error, _} = error -> error
    end
  end

  defp advance_iterator(%__MODULE__{tagged_iterators: [%{iterator: iterator} = tagged]} = state) do
    case safe_adapter_call(fn -> ErlangAdapter.iterator_next(iterator.iter_ref) end) do
      {:ok, key, _value} ->
        if prefix_match?(key, iterator.prefix) do
          updated_iterator = %{
            iterator
            | current_key: key,
              current_value: QuadTrieIterator.extract_value_at_level(key, iterator.level),
              exhausted: false
          }

          {:ok, %{state | tagged_iterators: [%{tagged | iterator: updated_iterator}]}}
        else
          exhausted(state)
        end

      :iterator_end ->
        exhausted(state)

      {:error, _} = error ->
        error
    end
  end

  defp prefix_match?(_key, <<>>), do: true

  defp prefix_match?(key, prefix) do
    :binary.longest_common_prefix([key, prefix]) == byte_size(prefix)
  end

  defp exhausted(state) do
    exhausted_state = %{state | exhausted: true, bindings: %{}}
    {:exhausted, exhausted_state}
  end

  defp safe_close(%QuadTrieIterator{iter_ref: iter_ref} = iterator) when is_pid(iter_ref) do
    if Process.alive?(iter_ref) do
      try do
        QuadTrieIterator.close(iterator)
      catch
        :exit, _reason -> :ok
      end
    else
      :ok
    end
  end

  defp safe_close(_iterator), do: :ok

  defp new_state(pattern) do
    %__MODULE__{variables: QuadScanPlan.variables(pattern), pattern: pattern}
  end

  defp bound_component?({:variable, _name}), do: false
  defp bound_component?(_component), do: true

  defp variable_score(component, _position, _stats) when is_integer(component), do: 0

  defp variable_score({:variable, _name}, position, stats) do
    key =
      Enum.at(
        [:distinct_subjects, :distinct_predicates, :distinct_objects, :distinct_graphs],
        position
      )

    Map.get(stats, key, 10_000)
  end
end
