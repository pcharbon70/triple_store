defmodule TripleStore.Benchmark.LDBC.SPB.Semantics do
  @moduledoc """
  Graph and reasoning contract for the SPB advanced workload.

  SPB runs use a quad store with a union default graph. Explicit source graphs
  remain separate, while global OWL 2 RL inference is stored in the dedicated
  derived column family under graph ID `0`. The executor includes that derived
  surface only when this profile explicitly enables it.
  """

  alias TripleStore.Dictionary.Manager
  alias TripleStore.Exporter

  alias TripleStore.Reasoner.{
    DerivedStore,
    GraphScopedReasoner,
    ReasoningConfig,
    ReasoningProfile,
    Rule
  }

  alias TripleStore.Benchmark.LDBC.SPB.Workload

  @required_rules [
    :scm_sco,
    :scm_spo,
    :cax_sco,
    :prp_spo1,
    :prp_trp,
    :prp_symp,
    :eq_sym,
    :eq_trans,
    :eq_rep_s,
    :eq_rep_p,
    :eq_rep_o
  ]

  @doc "Returns the explicit SPB query context with ACLs disabled."
  @spec execution_context(TripleStore.store()) :: map()
  def execution_context(store) do
    %{
      db: store.db,
      dict_manager: store.dict_manager,
      schema: :quad,
      union_default_graph: true,
      include_derived: true,
      authorization: :disabled,
      permit_all: true,
      user: :public
    }
  end

  @doc "Returns the canonical reasoning profile and verifies required rule coverage."
  @spec profile() :: {:ok, map()} | {:error, term()}
  def profile do
    with {:ok, rules} <- ReasoningProfile.rules_for(:owl2rl),
         [] <- @required_rules -- Enum.map(rules, & &1.name) do
      {:ok,
       %{
         name: :spb_advanced_owl2rl,
         scope: :global,
         storage_strategy: :per_graph_cf,
         derived_graph_id: 0,
         rules: rules,
         required_rules: @required_rules,
         explicit_and_derived_distinct?: true,
         maintenance: :clear_and_rederive_after_commit
       }}
    else
      missing when is_list(missing) -> {:error, {:missing_reasoning_rules, missing}}
      {:error, _reason} = error -> error
    end
  end

  @doc "Builds the global SPB reasoner configuration after its TBox graph is loaded."
  @spec configuration(TripleStore.store()) :: {:ok, ReasoningConfig.t()} | {:error, term()}
  def configuration(store) do
    ontology = Workload.graph_contract().ontology

    with {:ok, tbox_graph} <- Manager.lookup_id(store.dict_manager, RDF.iri(ontology)) do
      ReasoningConfig.new(
        profile: :owl2rl,
        mode: :materialized,
        scope: :global,
        tbox_graph: tbox_graph,
        storage_strategy: :per_graph_cf
      )
    else
      :not_found -> {:error, {:graph_not_loaded, ontology}}
      {:error, _reason} = error -> error
    end
  end

  @doc "Clears the benchmark-derived surface and deterministically rematerializes it."
  @spec materialize(TripleStore.store()) :: {:ok, map()} | {:error, term()}
  def materialize(store) do
    with {:ok, profile} <- profile(),
         {:ok, config} <- configuration(store),
         {:ok, rules} <- encode_rules(profile.rules, store.dict_manager),
         {:ok, cleared} <- DerivedStore.clear_all(store.db),
         {:ok, stats} <-
           GraphScopedReasoner.materialize_all(store.db,
             config: config,
             rules: rules,
             parallel: false
           ) do
      {:ok,
       Map.merge(stats, %{
         cleared_before_materialization: cleared,
         profile: profile.name,
         explicit_and_derived_distinct?: true
       })}
    end
  end

  @doc "Rematerializes only after a successful committed editorial mutation."
  @spec after_commit(TripleStore.store(), :ok | {:ok, term()} | {:error, term()}) ::
          {:ok, map()} | {:error, term()}
  def after_commit(store, result) when result == :ok or elem(result, 0) == :ok,
    do: materialize(store)

  def after_commit(_store, {:error, reason}), do: {:error, {:mutation_not_committed, reason}}

  @doc "Verifies that all required SPB source contexts survive dataset export."
  @spec verify_contexts(TripleStore.store()) :: {:ok, map()} | {:error, term()}
  def verify_contexts(store) do
    contract = Workload.graph_contract()
    expected = [contract.ontology, contract.reference, contract.creative_works]

    with {:ok, nquads} <- Exporter.export_nquads_string(store.db),
         {:ok, dataset} <- RDF.NQuads.read_string(nquads) do
      present =
        dataset
        |> RDF.Dataset.quads()
        |> Enum.map(fn {_s, _p, _o, graph} -> to_string(graph) end)
        |> MapSet.new()

      missing = Enum.reject(expected, &MapSet.member?(present, &1))

      if missing == [] do
        {:ok, %{graphs: Enum.sort(expected), acl_mode: contract.acl_mode, nquads: nquads}}
      else
        {:error, {:missing_graph_contexts, missing}}
      end
    end
  end

  @doc "Qualifies reasoning only when every mandatory conformance action passes."
  @spec conformance_gate(map(), map()) :: :ok | {:error, term()}
  def conformance_gate(package, results) when is_map(results) do
    required =
      package.operations
      |> Enum.filter(&(&1.family == :conformance and &1.availability == :mandatory))
      |> Enum.map(& &1.operation.id)

    missing = Enum.reject(required, &Map.has_key?(results, &1))
    failed = Enum.filter(required, &(Map.get(results, &1) != true))

    cond do
      missing != [] -> {:error, {:missing_conformance_results, missing}}
      failed != [] -> {:error, {:failed_conformance_actions, failed}}
      true -> :ok
    end
  end

  defp encode_rules(rules, manager) do
    Enum.reduce_while(rules, {:ok, []}, fn %Rule{} = rule, {:ok, encoded} ->
      with {:ok, body} <- encode_elements(rule.body, manager),
           {:ok, head} <- encode_element(rule.head, manager) do
        {:cont, {:ok, [%Rule{rule | body: body, head: head} | encoded]}}
      else
        {:error, reason} -> {:halt, {:error, {:rule_encoding_failed, rule.name, reason}}}
      end
    end)
    |> case do
      {:ok, encoded} -> {:ok, Enum.reverse(encoded)}
      error -> error
    end
  end

  defp encode_elements(elements, manager) do
    Enum.reduce_while(elements, {:ok, []}, fn element, {:ok, encoded} ->
      case encode_element(element, manager) do
        {:ok, value} -> {:cont, {:ok, [value | encoded]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, encoded} -> {:ok, Enum.reverse(encoded)}
      error -> error
    end
  end

  defp encode_element({kind, terms}, manager) when kind in [:pattern, :quad_pattern] do
    with {:ok, encoded} <- encode_terms(terms, manager), do: {:ok, {kind, encoded}}
  end

  defp encode_element({:not_equal, left, right}, manager) do
    with {:ok, encoded_left} <- encode_term(left, manager),
         {:ok, encoded_right} <- encode_term(right, manager) do
      {:ok, {:not_equal, encoded_left, encoded_right}}
    end
  end

  defp encode_element({condition, term}, manager)
       when condition in [:is_iri, :is_blank, :is_literal, :bound] do
    with {:ok, encoded} <- encode_term(term, manager), do: {:ok, {condition, encoded}}
  end

  defp encode_element(other, _manager), do: {:ok, other}

  defp encode_terms(terms, manager) do
    Enum.reduce_while(terms, {:ok, []}, fn term, {:ok, encoded} ->
      case encode_term(term, manager) do
        {:ok, value} -> {:cont, {:ok, [value | encoded]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, encoded} -> {:ok, Enum.reverse(encoded)}
      error -> error
    end
  end

  defp encode_term({:iri, iri}, manager), do: Manager.get_or_create_id(manager, RDF.iri(iri))
  defp encode_term(term, _manager), do: {:ok, term}
end
