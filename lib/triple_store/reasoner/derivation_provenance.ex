defmodule TripleStore.Reasoner.DerivationProvenance do
  @moduledoc """
  Detailed provenance tracking for derived quads.

  This module tracks the derivation chain for each derived quad, including:
  - Which rule produced the derivation
  - Which specific premise quads were used
  - The binding environment that produced the derivation

  ## Provenance Model

  Each derived quad has a derivation record containing:
  - `rule_name` - The rule that produced this derivation (e.g., `:cax_sco`)
  - `premises` - List of premise quads {g, s, p, o} used in the derivation
  - `bindings` - Variable bindings that satisfied the rule body

  This is more detailed than GraphProvenance, which only tracks graph-level
  dependencies. DerivationProvenance enables:
  - Explaining why a quad was derived
  - Recomputing derivations after deletions
  - Debugging reasoning behavior

  ## Storage

  Provenance is stored in-memory during reasoning and can be persisted
  to a separate column family for debugging and audit trails. The supported
  on-disk format is the existing unversioned derivation map. Reads use safe
  Erlang-term decoding and validate the complete record; malformed or
  incompatible records return `{:error, {:corrupt_provenance, reason}}`.

  ## Usage

      # Create a provenance tracker
      tracker = DerivationProvenance.new()

      # Record a derivation
      tracker = DerivationProvenance.record_derivation(
        tracker,
        derived_quad,
        rule_name: :cax_sco,
        premises: [premise1, premise2],
        bindings: %{"x" => {:bound, alice_id}}
      )

      # Explain a derivation
      {:ok, explanation} = DerivationProvenance.explain_inference(tracker, derived_quad, db)
  """

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Reasoner.Rule

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "ID quad: {graph_id, subject_id, predicate_id, object_id}"
  @type id_quad :: {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @typedoc "Variable binding: variable name => bound value"
  @type bindings :: %{String.t() => Rule.bound_term()}

  @typedoc "Derivation record for a single derived quad"
  @type derivation :: %{
          optional(:metadata) => map(),
          rule_name: Rule.name(),
          premises: [id_quad()],
          bindings: bindings(),
          timestamp: non_neg_integer()
        }

  @typedoc "Provenance tracker mapping derived quads to their derivations"
  @type t :: %__MODULE__{
          derivations: %{id_quad() => derivation()},
          count: non_neg_integer()
        }

  defstruct [:derivations, :count]

  # Provenance column family for persistent storage
  @provenance_cf :derivation_provenance

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Creates a new provenance tracker.
  """
  @spec new() :: t()
  def new do
    %__MODULE__{
      derivations: %{},
      count: 0
    }
  end

  @doc """
  Records a derivation for a derived quad.

  ## Parameters

  - `tracker` - The provenance tracker
  - `derived_quad` - The quad that was derived
  - `opts` - Derivation options:
    - `:rule_name` - The rule that produced this derivation (required)
    - `:premises` - List of premise quads used (default: [])
    - `:bindings` - Variable bindings (default: %{})

  ## Returns

  Updated tracker.
  """
  @spec record_derivation(t(), id_quad(), keyword()) :: t()
  def record_derivation(%__MODULE__{} = tracker, derived_quad, opts) do
    rule_name = Keyword.fetch!(opts, :rule_name)
    premises = Keyword.get(opts, :premises, [])
    bindings = Keyword.get(opts, :bindings, %{})
    metadata = Keyword.get(opts, :metadata)

    derivation = %{
      rule_name: rule_name,
      premises: premises,
      bindings: bindings,
      timestamp: System.system_time(:millisecond)
    }

    derivation =
      if is_nil(metadata), do: derivation, else: Map.put(derivation, :metadata, metadata)

    updated_derivations = Map.put(tracker.derivations, derived_quad, derivation)

    updated_count =
      if Map.has_key?(tracker.derivations, derived_quad) do
        tracker.count
      else
        tracker.count + 1
      end

    %{tracker | derivations: updated_derivations, count: updated_count}
  end

  @doc """
  Gets the derivation record for a derived quad.

  ## Returns

  - `{:ok, derivation}` if the quad has a recorded derivation
  - `:error` if not found
  """
  @spec get_derivation(t(), id_quad()) :: {:ok, derivation()} | :error
  def get_derivation(%__MODULE__{} = tracker, quad) do
    case Map.get(tracker.derivations, quad) do
      nil -> :error
      derivation -> {:ok, derivation}
    end
  end

  @doc """
  Explains how a derived quad was inferred.

  Returns a human-readable explanation of the derivation chain,
  including the rule used and the premises.

  ## Parameters

  - `tracker` - The provenance tracker
  - `quad` - The derived quad to explain
  - `db` - Database reference (for term lookups)

  ## Returns

  - `{:ok, explanation}` where explanation is a map with:
    - `:derived_quad` - The quad being explained
    - `:rule_name` - The rule that produced it
    - `:premises` - The premise quads used
    - `:bindings` - Variable bindings
    - `:formatted` - Human-readable string explanation

  - `:error` if no derivation is recorded
  """
  @spec explain_inference(t(), id_quad(), term()) :: {:ok, map()} | :error
  def explain_inference(%__MODULE__{} = tracker, quad, db) do
    with {:ok, derivation} <- get_derivation(tracker, quad) do
      explanation = %{
        derived_quad: quad,
        rule_name: derivation.rule_name,
        premises: derivation.premises,
        bindings: derivation.bindings,
        formatted: format_explanation(quad, derivation, db)
      }

      {:ok, explanation}
    end
  end

  @doc """
  Finds all derivations produced by a specific rule.

  ## Parameters

  - `tracker` - The provenance tracker
  - `rule_name` - The rule name to filter by

  ## Returns

  List of `{derived_quad, derivation}` tuples.
  """
  @spec find_by_rule(t(), Rule.name()) :: [{id_quad(), derivation()}]
  def find_by_rule(%__MODULE__{} = tracker, rule_name) do
    tracker.derivations
    |> Enum.filter(fn {_quad, derivation} -> derivation.rule_name == rule_name end)
    |> Enum.to_list()
  end

  @doc """
  Finds all derivations that depend on a specific premise quad.

  This is useful for determining which derivations may be affected
  when a premise is deleted.

  ## Parameters

  - `tracker` - The provenance tracker
  - `premise` - The premise quad to check

  ## Returns

  List of `{derived_quad, derivation}` tuples.
  """
  @spec find_dependent_derivations(t(), id_quad()) :: [{id_quad(), derivation()}]
  def find_dependent_derivations(%__MODULE__{} = tracker, premise) do
    tracker.derivations
    |> Enum.filter(fn {_quad, derivation} ->
      Enum.member?(derivation.premises, premise)
    end)
    |> Enum.to_list()
  end

  @doc """
  Removes derivation tracking for a quad.

  ## Parameters

  - `tracker` - The provenance tracker
  - `quad` - The quad to remove

  ## Returns

  Updated tracker.
  """
  @spec remove_quad(t(), id_quad()) :: t()
  def remove_quad(%__MODULE__{} = tracker, quad) do
    if Map.has_key?(tracker.derivations, quad) do
      %{tracker | derivations: Map.delete(tracker.derivations, quad), count: tracker.count - 1}
    else
      tracker
    end
  end

  @doc """
  Returns the number of derivations being tracked.
  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{} = tracker), do: tracker.count

  @doc """
  Checks if the tracker is empty.
  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{} = tracker), do: tracker.count == 0

  @doc """
  Clears all derivation tracking.
  """
  @spec clear(t()) :: t()
  def clear(%__MODULE__{} = tracker) do
    %{tracker | derivations: %{}, count: 0}
  end

  @doc """
  Merges two provenance trackers.

  When both trackers have derivations for the same quad, the one from
  tracker2 takes precedence (last-write-wins).
  """
  @spec merge(t(), t()) :: t()
  def merge(%__MODULE__{} = tracker1, %__MODULE__{} = tracker2) do
    merged_derivations = Map.merge(tracker1.derivations, tracker2.derivations)

    %__MODULE__{
      derivations: merged_derivations,
      count: map_size(merged_derivations)
    }
  end

  # ============================================================================
  # Persistent Storage
  # ============================================================================

  @doc """
  Saves provenance tracking to the database.

  ## Parameters

  - `db` - Database reference
  - `tracker` - The provenance tracker to save

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec save(term(), t()) :: :ok | {:error, term()}
  def save(db, %__MODULE__{} = tracker) do
    with {:ok, operations} <- build_persistence_operations(tracker.derivations) do
      ErlangAdapter.write_batch(db, operations, true)
    end
  rescue
    error -> {:error, error}
  end

  @doc """
  Loads provenance tracking from the database.

  ## Parameters

  - `db` - Database reference
  - `graph_id` - Optional graph ID to filter by (nil = load all)

  ## Returns

  - `{:ok, tracker}` on success
  - `{:error, reason}` on failure
  """
  @spec load(term(), non_neg_integer() | nil) :: {:ok, t()} | {:error, term()}
  def load(db, graph_id \\ nil) do
    prefix = if graph_id, do: <<graph_id::64-big>>, else: <<>>

    case ErlangAdapter.fold(
           db,
           @provenance_cf,
           prefix,
           {:ok, []},
           &collect_persisted_derivation/2
         ) do
      {:ok, derivations} ->
        {:ok,
         %__MODULE__{
           derivations: Map.new(derivations),
           count: length(derivations)
         }}

      {:error, _reason} = error ->
        error
    end
  rescue
    error -> {:error, error}
  end

  defp collect_persisted_derivation(_record, {:error, _reason} = error), do: error

  defp collect_persisted_derivation({key, value}, {:ok, acc}) do
    with {:ok, {_g, _s, _p, _o} = quad} <- decode_provenance_key(key),
         {:ok, derivation} <- decode_derivation(value) do
      {:ok, [{quad, derivation} | acc]}
    else
      {:error, reason} -> {:error, {:corrupt_provenance, reason}}
    end
  end

  @doc """
  Clears provenance for a specific graph from the database.

  ## Parameters

  - `db` - Database reference
  - `graph_id` - The graph ID to clear

  ## Returns

  - `{:ok, count}` with number of entries deleted
  - `{:error, reason}` on failure
  """
  @spec clear_graph(term(), non_neg_integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def clear_graph(db, graph_id) do
    try do
      with {:ok, tracker} <- load(db, graph_id) do
        keys = Enum.map(Map.keys(tracker.derivations), &encode_provenance_key/1)

        if keys == [] do
          {:ok, 0}
        else
          operations = Enum.map(keys, fn key -> {@provenance_cf, key} end)

          case ErlangAdapter.delete_batch(db, operations, true) do
            :ok -> {:ok, length(keys)}
            error -> error
          end
        end
      end
    rescue
      error -> {:error, error}
    end
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp format_explanation({g, s, p, o}, derivation, db) do
    # Build a human-readable explanation
    rule_str = to_string(derivation.rule_name)

    premise_str =
      derivation.premises
      |> Enum.map_join(", ", fn quad -> format_quad(quad, db) end)

    binding_str =
      derivation.bindings
      |> Enum.map_join(", ", fn {var, value} -> "#{var}=#{format_binding(value)}" end)

    """
    Derived: #{format_quad({g, s, p, o}, db)}
    Rule: #{rule_str}
    Premises: [#{premise_str}]
    Bindings: {#{binding_str}}
    """
    |> String.trim()
  end

  defp format_binding({:bound, value}), do: to_string(value)
  defp format_binding(value), do: inspect(value)

  defp format_quad({g, s, p, o}, db) do
    # Try to look up term strings for readability
    s_str = lookup_term(db, s)
    p_str = lookup_term(db, p)
    o_str = lookup_term(db, o)

    "[g:#{g} (#{s_str} #{p_str} #{o_str})]"
  rescue
    _ -> "[g:#{g} (#{s} #{p} #{o})]"
  end

  defp lookup_term(db, term_id) do
    case ErlangAdapter.get(db, :id2str, <<term_id::64-big>>) do
      {:ok, value} -> value
      _ -> "##{term_id}"
    end
  end

  # Encode a quad as a provenance key
  defp encode_provenance_key({g, s, p, o}) do
    <<g::64-big, s::64-big, p::64-big, o::64-big>>
  end

  # Decode a provenance key back to a quad
  defp decode_provenance_key(<<g::64-big, s::64-big, p::64-big, o::64-big>>) do
    {:ok, {g, s, p, o}}
  end

  defp decode_provenance_key(_other), do: {:error, :invalid_fact_key}

  # Encode a derivation record for storage
  defp encode_derivation(derivation) do
    :erlang.term_to_binary(derivation)
  end

  # Decode a derivation record from storage
  defp decode_derivation(binary) when is_binary(binary) do
    try do
      binary
      |> :erlang.binary_to_term([:safe])
      |> validate_derivation()
    rescue
      ArgumentError -> {:error, :unsafe_or_invalid_term}
    end
  end

  defp build_persistence_operations(derivations) do
    Enum.reduce_while(derivations, {:ok, []}, fn {quad, derivation}, {:ok, acc} ->
      with :ok <- validate_id_quad(quad),
           {:ok, valid_derivation} <- validate_derivation(derivation) do
        operation =
          {@provenance_cf, encode_provenance_key(quad), encode_derivation(valid_derivation)}

        {:cont, {:ok, [operation | acc]}}
      else
        {:error, reason} -> {:halt, {:error, {:invalid_provenance, reason}}}
      end
    end)
    |> case do
      {:ok, operations} -> {:ok, Enum.reverse(operations)}
      {:error, _reason} = error -> error
    end
  end

  defp validate_derivation(%{version: version}),
    do: {:error, {:unsupported_version, version}}

  defp validate_derivation(derivation) when is_map(derivation) do
    required_keys = MapSet.new([:rule_name, :premises, :bindings, :timestamp])
    allowed_keys = MapSet.put(required_keys, :metadata)
    actual_keys = MapSet.new(Map.keys(derivation))

    cond do
      not MapSet.subset?(required_keys, actual_keys) ->
        {:error, :missing_required_fields}

      not MapSet.subset?(actual_keys, allowed_keys) ->
        {:error,
         {:unsupported_fields, MapSet.difference(actual_keys, allowed_keys) |> MapSet.to_list()}}

      not valid_rule_name?(derivation.rule_name) ->
        {:error, {:invalid_rule_name, derivation.rule_name}}

      not is_list(derivation.premises) or
          not Enum.all?(derivation.premises, &(validate_id_quad(&1) == :ok)) ->
        {:error, {:invalid_premises, derivation.premises}}

      not valid_bindings?(derivation.bindings) ->
        {:error, {:invalid_bindings, derivation.bindings}}

      not is_integer(derivation.timestamp) or derivation.timestamp < 0 ->
        {:error, {:invalid_timestamp, derivation.timestamp}}

      not valid_metadata?(Map.get(derivation, :metadata)) ->
        {:error, {:invalid_metadata, Map.get(derivation, :metadata)}}

      true ->
        {:ok, derivation}
    end
  end

  defp validate_derivation(_derivation), do: {:error, :invalid_record_shape}

  defp validate_id_quad({g, s, p, o}) do
    if Enum.all?([g, s, p, o], &(is_integer(&1) and &1 >= 0 and &1 <= 0xFFFFFFFFFFFFFFFF)) do
      :ok
    else
      {:error, :invalid_fact_key}
    end
  end

  defp validate_id_quad(_quad), do: {:error, :invalid_fact_key}

  defp valid_rule_name?(name) when is_atom(name), do: name not in [nil, true, false]
  defp valid_rule_name?(name) when is_binary(name), do: byte_size(name) > 0
  defp valid_rule_name?(_name), do: false

  defp valid_bindings?(bindings) when is_map(bindings) do
    Enum.all?(bindings, fn
      {name, value} when is_binary(name) -> valid_binding_value?(value)
      _other -> false
    end)
  end

  defp valid_bindings?(_bindings), do: false

  defp valid_binding_value?({:bound, value}), do: is_integer(value) and value >= 0
  defp valid_binding_value?({:iri, value}), do: is_binary(value)
  defp valid_binding_value?({:blank_node, value}), do: is_binary(value)
  defp valid_binding_value?({:literal, :simple, value}), do: is_binary(value)

  defp valid_binding_value?({:literal, type, value, qualifier})
       when type in [:typed, :lang],
       do: is_binary(value) and is_binary(qualifier)

  defp valid_binding_value?(_value), do: false

  defp valid_metadata?(nil), do: true

  defp valid_metadata?(metadata) when is_map(metadata) do
    allowed_keys = MapSet.new([:graph_id, :scope, :iteration])
    keys = MapSet.new(Map.keys(metadata))

    MapSet.subset?(keys, allowed_keys) and
      valid_optional_non_negative(metadata, :graph_id) and
      valid_optional_non_negative(metadata, :iteration) and
      Map.get(metadata, :scope, :local) in [:local, :global]
  end

  defp valid_metadata?(_metadata), do: false

  defp valid_optional_non_negative(metadata, key) do
    case Map.fetch(metadata, key) do
      :error -> true
      {:ok, value} -> is_integer(value) and value >= 0
    end
  end
end
