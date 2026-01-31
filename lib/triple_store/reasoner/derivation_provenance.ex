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
  to a separate column family for debugging and audit trails.

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
          rule_name: atom(),
          premises: [id_quad()],
          bindings: bindings(),
          timestamp: integer()
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

    derivation = %{
      rule_name: rule_name,
      premises: premises,
      bindings: bindings,
      timestamp: System.system_time(:millisecond)
    }

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
  @spec find_by_rule(t(), atom()) :: [{id_quad(), derivation()}]
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
    operations =
      tracker.derivations
      |> Enum.map(fn {quad, derivation} ->
        key = encode_provenance_key(quad)
        value = encode_derivation(derivation)
        {@provenance_cf, key, value}
      end)

    ErlangAdapter.write_batch(db, operations, true)
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

    derivations =
      ErlangAdapter.fold(db, @provenance_cf, prefix, [], fn {key, value}, acc ->
        case decode_provenance_key(key) do
          {:ok, {_g, _s, _p, _o} = quad} ->
            case decode_derivation(value) do
              {:ok, derivation} -> [{quad, derivation} | acc]
              _error -> acc
            end

          _error ->
            acc
        end
      end)

    tracker = %__MODULE__{
      derivations: Map.new(derivations),
      count: length(derivations)
    }

    {:ok, tracker}
  rescue
    error -> {:error, error}
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
    prefix = <<graph_id::64-big>>

    try do
      # Collect all keys for this graph
      keys =
        ErlangAdapter.fold_keys(db, @provenance_cf, prefix, [], fn key, acc ->
          [key | acc]
        end)

      if keys == [] do
        {:ok, 0}
      else
        operations = Enum.map(keys, fn key -> {@provenance_cf, key} end)

        case ErlangAdapter.delete_batch(db, operations, true) do
          :ok -> {:ok, length(keys)}
          error -> error
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
    rule_str = Atom.to_string(derivation.rule_name)

    premise_str =
      derivation.premises
      |> Enum.map(fn quad -> format_quad(quad, db) end)
      |> Enum.join(", ")

    binding_str =
      derivation.bindings
      |> Enum.map(fn {var, {:bound, value}} -> "#{var}=#{value}" end)
      |> Enum.join(", ")

    """
    Derived: #{format_quad({g, s, p, o}, db)}
    Rule: #{rule_str}
    Premises: [#{premise_str}]
    Bindings: {#{binding_str}}
    """
    |> String.trim()
  end

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

  defp decode_provenance_key(_other), do: :error

  # Encode a derivation record for storage
  defp encode_derivation(derivation) do
    :erlang.term_to_binary(derivation)
  end

  # Decode a derivation record from storage
  defp decode_derivation(binary) when is_binary(binary) do
    {:ok, :erlang.binary_to_term(binary)}
  rescue
    _ -> :error
  end
end
