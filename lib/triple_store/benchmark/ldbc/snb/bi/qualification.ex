defmodule TripleStore.Benchmark.LDBC.SNB.BI.Qualification do
  @moduledoc """
  Generates the fail-closed SNB BI correctness qualification matrix.

  The matrix records evidence independently for parsing, algebra extraction,
  optimization, execution, the typed result contract, and the accepted answer
  bundle. A stage is never inferred from a later stage. In particular, parser
  and plan readiness do not make an operation executable, and an executable
  operation does not pass correctness qualification without an accepted answer.

  Structural evidence and contextual execution evidence remain separate. The
  matrix can prove that a handler is registered without claiming that it ran,
  and it can inventory a contract fixture without treating it as an accepted
  correctness oracle.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.{
    Analytics,
    GraphAdapterRegistry,
    HandlerRegistry,
    Parameters,
    ReferenceAnswers,
    StateProbe
  }

  alias TripleStore.Benchmark.LDBC.{Correctness, Result, StoreFixture}
  alias TripleStore.SPARQL.Query

  @graph_operations MapSet.new([10, 15, 19, 20])
  @numeric_types ["ID", "32-bit Integer", "64-bit Integer", "32-bit Float", "64-bit Float"]
  @qualification_identities [
    :dataset,
    :mapping,
    :translation,
    :parameters,
    :reference_answers,
    :update_stream,
    :checkpoint,
    :scorer,
    :environment
  ]

  @type stage_status :: :pass | :fail | :blocked | :not_applicable
  @type stage_evidence :: %{
          required(:status) => stage_status(),
          optional(:digest) => String.t(),
          optional(:reason) => term()
        }

  @doc "Generates a stable qualification matrix for every supplied BI variant."
  @spec matrix([map()], keyword()) :: {:ok, map()} | {:error, term()}
  def matrix(definitions, opts \\ [])

  def matrix(definitions, opts) when is_list(definitions) do
    with {:ok, inventory} <- ReferenceAnswers.inventory(Keyword.take(opts, [:root])) do
      reference_index = Map.new(inventory, &{{&1.operation_id, &1.variant}, &1})

      entries =
        definitions
        |> Enum.map(&entry(&1, reference_index))
        |> Enum.sort_by(&{&1.number, variant_order(&1.variant)})

      operations = operation_evidence(entries)

      {:ok,
       %{
         entries: entries,
         operations: operations,
         groups: Enum.frequencies_by(entries, & &1.gap_group),
         classifications: Enum.frequencies_by(entries, & &1.classification),
         reference_coverage: Enum.frequencies_by(entries, & &1.expected_result.status)
       }}
    end
  end

  def matrix(_definitions, _opts), do: {:error, :invalid_bi_definitions}

  @doc "Executes every BI variant in a validated store context and records qualification evidence."
  @spec run([map()], StoreFixture.t(), Parameters.bundle(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def run(definitions, fixture, parameter_bundle, opts \\ [])

  def run(definitions, %StoreFixture{store: store} = fixture, parameter_bundle, opts)
      when is_list(definitions) and is_map(store) and is_map(parameter_bundle) do
    validation_opts = Keyword.take(opts, [:comparable, :reference_profile])
    inventory_opts = reference_inventory_opts(opts)

    with :ok <- Parameters.validate(parameter_bundle, fixture, definitions, validation_opts),
         {:ok, inventory} <- ReferenceAnswers.inventory(inventory_opts) do
      reference_index = Map.new(inventory, &{{&1.operation_id, &1.variant}, &1})
      context = Keyword.get(opts, :context, Map.put(store, :permit_all, true))

      entries =
        definitions
        |> Enum.map(
          &execute_entry(&1, context, fixture.manifest, parameter_bundle, reference_index, opts)
        )
        |> Enum.sort_by(&{&1.number, variant_order(&1.variant)})

      {:ok,
       %{
         entries: entries,
         operations: runtime_operation_evidence(entries),
         execution: Enum.frequencies_by(entries, & &1.execution.status),
         comparisons: Enum.frequencies_by(entries, & &1.comparison.status),
         qualified?: Enum.all?(entries, & &1.qualified?)
       }}
    end
  end

  def run(_definitions, _fixture, _parameter_bundle, _opts),
    do: {:error, :invalid_qualification_context}

  @doc "Builds the fail-closed qualification envelope used by Phase 5 artifacts."
  @spec evidence([map()], map(), keyword()) :: {:ok, map()} | {:error, term()}
  def evidence(entries, identities, opts \\ [])

  def evidence(entries, identities, opts) when is_list(entries) and is_map(identities) do
    with :ok <- validate_qualification_identities(identities) do
      reconciled? = Keyword.get(opts, :reconciled?, false)
      updates_complete? = Keyword.get(opts, :updates_complete?, false)

      blockers =
        []
        |> maybe_add(
          not Enum.all?(entries, &implementation_complete?/1),
          :implementation_incomplete
        )
        |> maybe_add(not Enum.all?(entries, &execution_complete?/1), :execution_incomplete)
        |> maybe_add(
          not Enum.all?(entries, &accepted_answer_complete?/1),
          :accepted_answers_incomplete
        )
        |> maybe_add(
          not Enum.all?(entries, &result_comparison_complete?/1),
          :result_comparison_incomplete
        )
        |> maybe_add(not updates_complete?, :updates_incomplete)
        |> maybe_add(not reconciled?, :protocol_reconciliation_incomplete)
        |> Enum.reverse()

      {:ok,
       %{
         complete?: blockers == [],
         operation_count: length(entries),
         variant_count: length(entries),
         reconciled?: reconciled?,
         identities: identities,
         blockers: blockers
       }}
    end
  end

  def evidence(_entries, _identities, _opts), do: {:error, :invalid_qualification_evidence}

  @doc "Combines structural inventory with contextual execution and comparison evidence."
  @spec reconcile_entries([map()], [map()]) :: {:ok, [map()]} | {:error, term()}
  def reconcile_entries(structural, runtime) when is_list(structural) and is_list(runtime) do
    structural_keys = Enum.map(structural, &entry_key/1)
    runtime_keys = Enum.map(runtime, &entry_key/1)

    cond do
      length(runtime_keys) != length(Enum.uniq(runtime_keys)) ->
        {:error, :duplicate_runtime_qualification_entry}

      MapSet.new(structural_keys) != MapSet.new(runtime_keys) ->
        {:error, {:qualification_entry_coverage, structural_keys, runtime_keys}}

      true ->
        runtime_index = Map.new(runtime, &{entry_key(&1), &1})

        combined =
          Enum.map(structural, fn entry ->
            observed = Map.fetch!(runtime_index, entry_key(entry))

            entry
            |> put_in([:stages, :execution], observed.execution)
            |> put_in([:stages, :result_contract], runtime_result_contract(observed))
            |> update_in(
              [:expected_result],
              &Map.put(&1, :comparison_status, observed.comparison.status)
            )
            |> Map.put(:runtime_evidence, observed)
          end)

        {:ok, combined}
    end
  end

  def reconcile_entries(_structural, _runtime),
    do: {:error, :invalid_qualification_entries}

  @doc "Compares complete post-update answers for reads affected by mapped RDF mutations."
  @spec post_update_evidence(
          [map()],
          [map()],
          map(),
          (map() -> {:ok, Result.t()} | {:error, term()}),
          (map() -> {:ok, Result.t()} | {:error, term()})
        ) :: {:ok, map()} | {:error, term()}
  def post_update_evidence(definitions, mutations, receipt, actual_runner, reference_runner)
      when is_list(definitions) and is_list(mutations) and is_map(receipt) and
             is_function(actual_runner, 1) and is_function(reference_runner, 1) do
    affected = StateProbe.affected_reads(definitions, mutations)

    with {:ok, answers} <- compare_post_update_answers(affected, actual_runner, reference_runner),
         {:ok, first} <- fetch_receipt(receipt, :first_sequence),
         {:ok, last} <- fetch_receipt(receipt, :last_sequence),
         {:ok, state_checksum} <- fetch_receipt(receipt, :state_checksum) do
      {:ok,
       %{
         affected_operations: Enum.map(affected, & &1.catalog_id),
         answers: answers,
         batch_sequence: %{first: first, last: last},
         state_checksum: state_checksum
       }}
    end
  end

  def post_update_evidence(_definitions, _mutations, _receipt, _actual, _reference),
    do: {:error, :invalid_post_update_evidence_context}

  @doc "Classifies one complete stage-evidence set without weakening failed stages."
  @spec classify(map(), boolean()) :: atom()
  def classify(stages, extension?) when is_map(stages) and is_boolean(extension?) do
    statuses =
      {
        status(stages, :parse),
        status(stages, :algebra),
        status(stages, :optimized_plan),
        status(stages, :execution),
        status(stages, :result_contract)
      }

    classify_statuses(statuses, extension?, reason(stages, :result_contract))
  end

  defp entry(definition, reference_index) do
    extension? = MapSet.member?(@graph_operations, definition.number)
    stages = if extension?, do: extension_stages(definition), else: standard_stages(definition)

    %{
      operation_id: definition.catalog_id,
      execution_id: definition.id,
      number: definition.number,
      variant: definition.variant,
      strategy: definition.strategy,
      classification: classify(stages, extension?),
      gap_group: gap_group(stages, extension?),
      confirmed_gaps: confirmed_gaps(definition),
      semantic_requirements: semantic_requirements(definition),
      stages: stages,
      expected_result: expected_result(definition, reference_index)
    }
  end

  defp standard_stages(definition) do
    parse = pass(definition, :parse, definition.translation.ast)

    case Query.prepare(probe_query(definition)) do
      {:ok, prepared} ->
        %{
          parse: parse,
          algebra: pass(definition, :algebra, prepared.pattern),
          optimized_plan: pass(definition, :optimized_plan, prepared.optimized_pattern),
          implementation: implementation_evidence(definition, HandlerRegistry),
          execution: blocked(:qualification_context_required),
          result_contract: blocked(:execution_not_available)
        }

      {:error, {:parse_error, _reason} = reason} ->
        %{
          parse: fail(reason),
          algebra: blocked(:parse_failed),
          optimized_plan: blocked(:parse_failed),
          implementation: implementation_evidence(definition, HandlerRegistry),
          execution: blocked(:parse_failed),
          result_contract: blocked(:parse_failed)
        }

      {:error, reason} ->
        %{
          parse: parse,
          algebra: fail(reason),
          optimized_plan: blocked(:algebra_failed),
          implementation: implementation_evidence(definition, HandlerRegistry),
          execution: blocked(:algebra_failed),
          result_contract: blocked(:algebra_failed)
        }
    end
  end

  defp extension_stages(definition) do
    {:extension, extension_id, _module} = definition.strategy

    %{
      parse: not_applicable(:graph_extension),
      algebra: not_applicable(:graph_extension),
      optimized_plan: not_applicable(:graph_extension),
      implementation: implementation_evidence(definition, GraphAdapterRegistry),
      execution: blocked({:qualification_context_required, extension_id}),
      result_contract: blocked(:execution_not_available)
    }
  end

  defp implementation_evidence(definition, registry) do
    case registry.fetch(definition) do
      {:ok, entry} ->
        %{status: :pass, digest: digest({entry.version, entry.adapter, entry.key})}

      {:error, reason} ->
        fail(reason)
    end
  end

  defp expected_result(definition, reference_index) do
    case Map.fetch(reference_index, {definition.catalog_id, definition.variant}) do
      {:ok, entry} ->
        %{
          status: entry.purpose,
          purpose: entry.purpose,
          artifact_kind: entry.artifact.kind,
          parameter_row_index: entry.parameter_row_index,
          checksum: entry.artifact.checksum,
          reference_source: entry.reference_source
        }

      :error ->
        %{status: :missing, purpose: :missing, parameter_row_index: 0}
    end
  end

  defp gap_group(stages, extension?) do
    case {classify(stages, extension?), status(stages, :implementation)} do
      {:extension_adapter_gap, :pass} ->
        :graph_qualification_context_required

      {:execution_gap, :pass} ->
        :qualification_context_required

      {classification, _implementation} ->
        classification
    end
  end

  defp semantic_requirements(%{strategy: {:extension, id, _module}}),
    do: [{:graph_extension, id}, :typed_result_conversion]

  defp semantic_requirements(definition) do
    query = definition.translation.sparql

    []
    |> maybe_add(String.match?(query, ~r/xsd:date|dateTime|Duration|YEAR|MONTH/), :date_time)
    |> maybe_add(
      String.match?(query, ~r/\b(?:CONCAT|STR|COALESCE|IF|ABS|BOUND)\s*\(/),
      :expression_coercion
    )
    |> maybe_add(String.match?(query, ~r/\b(?:COUNT|SUM|AVG|MIN|MAX)\s*\(/), :aggregation)
    |> maybe_add(String.match?(query, ~r/\{\s*SELECT\b/), :subquery)
    |> maybe_add(String.contains?(query, "HAVING"), :having)
    |> maybe_add(definition.ordering != [], :canonical_ordering)
    |> maybe_add(is_integer(definition.limit), :limit)
    |> maybe_add(
      Enum.any?(definition.result, &(&1.type in @numeric_types)),
      :typed_numeric_result
    )
    |> Enum.reverse()
  end

  defp confirmed_gaps(_definition), do: []

  defp operation_evidence(entries) do
    entries
    |> Enum.group_by(& &1.operation_id)
    |> Enum.map(fn {operation_id, variants} -> summarize_operation(operation_id, variants) end)
    |> Enum.sort_by(& &1.operation_id)
  end

  defp summarize_operation(operation_id, variants) do
    extension? = Enum.all?(variants, &match?({:extension, _, _}, &1.strategy))
    parse_complete? = Enum.all?(variants, &parse_ready?/1)

    implementation_available? =
      Enum.all?(variants, &(status(&1.stages, :implementation) == :pass))

    execution_complete? = Enum.all?(variants, &(status(&1.stages, :execution) == :pass))

    accepted_answers_complete? =
      Enum.all?(variants, &(&1.expected_result.purpose == :accepted_answer))

    result_comparison_complete? =
      Enum.all?(variants, &(Map.get(&1.expected_result, :comparison_status) == :pass))

    blockers =
      []
      |> maybe_add(not parse_complete?, :parse_incomplete)
      |> maybe_add(not implementation_available?, :implementation_missing)
      |> maybe_add(not execution_complete?, :execution_incomplete)
      |> maybe_add(not accepted_answers_complete?, :accepted_answers_incomplete)
      |> maybe_add(not result_comparison_complete?, :result_comparison_incomplete)
      |> Enum.reverse()

    %{
      operation_id: operation_id,
      extension?: extension?,
      variants: Enum.map(variants, & &1.variant),
      parse_complete?: parse_complete?,
      implementation_available?: implementation_available?,
      execution_complete?: execution_complete?,
      accepted_answers_complete?: accepted_answers_complete?,
      result_comparison_complete?: result_comparison_complete?,
      qualified?: blockers == [],
      blockers: blockers
    }
  end

  defp parse_ready?(entry) do
    status(entry.stages, :parse) in [:pass, :not_applicable]
  end

  defp execute_entry(definition, context, dataset, parameter_bundle, references, opts) do
    parameters = parameter_row(parameter_bundle, definition)

    case Analytics.execute(definition, context, parameters, execution_opts(opts)) do
      {:ok, result} ->
        comparison =
          comparison_evidence(definition, result, dataset, parameter_bundle, references, opts)

        %{
          operation_id: definition.catalog_id,
          execution_id: definition.id,
          number: definition.number,
          variant: definition.variant,
          strategy: definition.strategy,
          execution: %{
            status: :pass,
            digest: digest(result),
            rows: length(result.rows),
            columns: result.columns,
            types: result.types
          },
          comparison: comparison,
          qualified?: comparison.status == :pass
        }

      {:error, reason} ->
        failed_execution_entry(definition, reason)
    end
  end

  defp failed_execution_entry(definition, reason) do
    %{
      operation_id: definition.catalog_id,
      execution_id: definition.id,
      number: definition.number,
      variant: definition.variant,
      strategy: definition.strategy,
      execution: fail(reason),
      comparison: blocked(:execution_failed),
      qualified?: false
    }
  end

  defp comparison_evidence(definition, result, dataset, parameter_bundle, references, opts) do
    case Map.fetch(references, {definition.catalog_id, definition.variant}) do
      {:ok, %{purpose: :accepted_answer} = reference} ->
        compare_accepted_answer(definition, result, dataset, parameter_bundle, reference, opts)

      {:ok, %{purpose: purpose}} ->
        blocked({:ineligible_reference_purpose, purpose})

      :error ->
        blocked(:accepted_answer_missing)
    end
  end

  defp compare_accepted_answer(definition, result, dataset, parameter_bundle, reference, opts) do
    compare_opts =
      [reference_source: reference.reference_source]
      |> Keyword.merge(reference_inventory_opts(opts))
      |> Keyword.merge(
        Keyword.take(opts, [:full_comparison_limit, :numeric_tolerance, :accepted_divergence])
      )

    case ReferenceAnswers.compare(definition, 0, result, dataset, parameter_bundle, compare_opts) do
      {:ok, %{status: :correct} = comparison} ->
        %{status: :pass, digest: digest(comparison.comparison)}

      {:ok, comparison} ->
        fail({:answer_mismatch, comparison.status, comparison.category})

      {:error, reason} ->
        fail(reason)
    end
  end

  defp runtime_operation_evidence(entries) do
    entries
    |> Enum.group_by(& &1.operation_id)
    |> Enum.map(fn {operation_id, variants} ->
      %{
        operation_id: operation_id,
        variants: Enum.map(variants, & &1.variant),
        execution_complete?: Enum.all?(variants, &(&1.execution.status == :pass)),
        comparison_complete?: Enum.all?(variants, &(&1.comparison.status == :pass)),
        qualified?: Enum.all?(variants, & &1.qualified?)
      }
    end)
    |> Enum.sort_by(& &1.operation_id)
  end

  defp parameter_row(bundle, definition) do
    bundle.sequences
    |> Map.fetch!(variant_label(definition))
    |> List.first()
  end

  defp compare_post_update_answers(definitions, actual_runner, reference_runner) do
    Enum.reduce_while(definitions, {:ok, []}, fn definition, {:ok, evidence} ->
      with {:ok, %Result{} = actual} <- actual_runner.(definition),
           {:ok, %Result{} = expected} <- reference_runner.(definition) do
        comparison =
          Correctness.compare(expected, actual,
            full_comparison_limit: max(length(expected.rows), length(actual.rows))
          )

        entry = %{
          operation_id: definition.catalog_id,
          variant: Map.get(definition, :variant, "default"),
          comparison: comparison
        }

        if comparison.status == :correct,
          do: {:cont, {:ok, [entry | evidence]}},
          else: {:halt, {:error, {:post_update_answer_mismatch, entry}}}
      else
        {:error, reason} ->
          {:halt, {:error, {:post_update_answer_failed, definition.catalog_id, reason}}}

        other ->
          {:halt, {:error, {:invalid_post_update_answer, definition.catalog_id, other}}}
      end
    end)
    |> case do
      {:ok, evidence} -> {:ok, Enum.reverse(evidence)}
      {:error, _} = error -> error
    end
  end

  defp fetch_receipt(receipt, field) do
    case Map.fetch(receipt, field) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:missing_correctness_evidence, field}}
    end
  end

  defp execution_opts(opts) do
    Keyword.take(opts, [:timeout, :max_visited, :max_intermediate, :cancelled?])
  end

  defp validate_qualification_identities(identities) do
    case Enum.find(@qualification_identities, &(not present_identity?(Map.get(identities, &1)))) do
      nil -> :ok
      identity -> {:error, {:qualification_identity_missing, identity}}
    end
  end

  defp entry_key(entry), do: {entry[:operation_id], entry[:variant]}

  defp runtime_result_contract(%{execution: %{status: :pass, digest: digest}}),
    do: %{status: :pass, digest: digest}

  defp runtime_result_contract(%{execution: %{status: :fail, reason: reason}}),
    do: %{status: :fail, reason: reason}

  defp present_identity?(value) when is_binary(value), do: value != ""
  defp present_identity?(value) when is_map(value), do: map_size(value) > 0
  defp present_identity?(_value), do: false

  defp implementation_complete?(entry),
    do: get_in(entry, [:stages, :implementation, :status]) == :pass

  defp execution_complete?(entry) do
    get_in(entry, [:stages, :execution, :status]) == :pass or
      get_in(entry, [:execution, :status]) == :pass
  end

  defp accepted_answer_complete?(entry),
    do: get_in(entry, [:expected_result, :purpose]) == :accepted_answer

  defp result_comparison_complete?(entry) do
    get_in(entry, [:expected_result, :comparison_status]) == :pass or
      get_in(entry, [:comparison, :status]) == :pass
  end

  defp reference_inventory_opts(opts) do
    case Keyword.fetch(opts, :reference_root) do
      {:ok, root} -> [root: root]
      :error -> []
    end
  end

  defp variant_label(definition) do
    suffix = if definition.variant == "default", do: "", else: definition.variant
    Integer.to_string(definition.number) <> suffix
  end

  defp probe_query(definition) do
    Regex.replace(
      ~r/\{\{([A-Za-z][A-Za-z0-9_]*)\}\}/,
      definition.translation.sparql,
      "$\\1"
    )
  end

  defp pass(definition, stage, term) do
    evidence = {definition.translation.checksum, stage, structural_signature(term)}
    %{status: :pass, digest: digest(evidence)}
  end

  defp fail(reason), do: %{status: :fail, reason: reason}
  defp blocked(reason), do: %{status: :blocked, reason: reason}
  defp not_applicable(reason), do: %{status: :not_applicable, reason: reason}

  defp classify_statuses({:fail, _, _, _, _}, _extension?, _reason), do: :parser_gap
  defp classify_statuses({_, :fail, _, _, _}, _extension?, _reason), do: :algebra_gap
  defp classify_statuses({_, _, :fail, _, _}, _extension?, _reason), do: :algebra_gap

  defp classify_statuses({_, _, _, :fail, _}, true, _reason),
    do: :extension_adapter_gap

  defp classify_statuses({_, _, _, :fail, _}, false, _reason), do: :execution_gap

  defp classify_statuses({_, _, _, _, :fail}, _extension?, :ordering_mismatch),
    do: :ordering_mismatch

  defp classify_statuses({_, _, _, _, :fail}, _extension?, _reason), do: :type_mismatch
  defp classify_statuses({_, _, _, _, :pass}, _extension?, _reason), do: :passing
  defp classify_statuses(_statuses, true, _reason), do: :extension_adapter_gap
  defp classify_statuses(_statuses, false, _reason), do: :execution_gap

  defp status(stages, stage), do: get_in(stages, [stage, :status])
  defp reason(stages, stage), do: get_in(stages, [stage, :reason])

  defp digest(term) do
    "sha256:" <>
      (term
       |> :erlang.term_to_binary([:deterministic])
       |> then(&:crypto.hash(:sha256, &1))
       |> Base.encode16(case: :lower))
  end

  defp structural_signature(tuple) when is_tuple(tuple) do
    {:tuple, tuple |> Tuple.to_list() |> Enum.map(&structural_signature/1)}
  end

  defp structural_signature(list) when is_list(list),
    do: {:list, Enum.map(list, &structural_signature/1)}

  defp structural_signature(map) when is_map(map) do
    entries =
      map
      |> Enum.map(fn {key, value} -> {structural_signature(key), structural_signature(value)} end)
      |> Enum.sort()

    {:map, entries}
  end

  defp structural_signature(value) when is_atom(value), do: value
  defp structural_signature(value) when is_integer(value), do: :integer
  defp structural_signature(value) when is_float(value), do: :float
  defp structural_signature(value) when is_binary(value), do: :binary
  defp structural_signature(value) when is_function(value), do: :function
  defp structural_signature(value) when is_reference(value), do: :reference
  defp structural_signature(value) when is_pid(value), do: :pid
  defp structural_signature(_value), do: :other

  defp maybe_add(values, true, value), do: [value | values]
  defp maybe_add(values, false, _value), do: values

  defp variant_order("default"), do: {0, ""}
  defp variant_order(variant), do: {1, variant}
end
