defmodule TripleStore.Benchmark.LDBC.SNB.BI.Protocol do
  @moduledoc """
  Pinned SNB BI validation, power, and throughput scheduling.

  A block applies its dated update batch, performs declared precomputations,
  then executes the official query variant order. The first block is power;
  later blocks are throughput. Comparable runs require official parameter
  provenance and the one-hour throughput termination condition.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.{Parameters, StateIdentity, Workload}

  @start_date ~D[2012-11-29]
  @end_date ~D[2013-01-01]
  @official_variants ~w(1 2a 2b 2m 3 4 5 6 7 8a 8b 8m 9 10a 10b 11 12 13 14a 14b 15a 15b 16a 16b 17 18 19a 19b 20a 20b 20m)

  @doc "Returns the immutable official read order used for power scoring."
  @spec official_variants() :: [String.t()]
  def official_variants, do: @official_variants

  @doc "Builds a dated protocol schedule for validation, smoke, or comparable execution."
  @spec schedule(:validation | :power | :throughput, keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def schedule(mode, opts \\ []) when mode in [:validation, :power, :throughput] do
    comparable? = Keyword.get(opts, :comparable, false)
    max_batches = Keyword.get(opts, :max_batches, default_batches(mode))

    cond do
      comparable? and mode == :throughput and max_batches < Date.diff(@end_date, @start_date) ->
        {:error, :comparable_schedule_cannot_be_reduced}

      not is_integer(max_batches) or max_batches <= 0 ->
        {:error, :invalid_batch_count}

      true ->
        blocks =
          0..(max_batches - 1)
          |> Enum.map(fn offset ->
            date = Date.add(@start_date, offset)

            %{
              position: offset + 1,
              date: date,
              kind: block_kind(mode, offset),
              stages: [:updates, :precomputations, :reads],
              variants: @official_variants
            }
          end)

        {:ok, blocks}
    end
  end

  @doc "Executes a schedule with explicit engine callbacks and reconciled receipts."
  @spec run([map()], map(), keyword()) :: {:ok, map()} | {:error, map()}
  def run(schedule, parameter_bundle, opts) when is_list(schedule) and is_map(parameter_bundle) do
    with :ok <- validate_callbacks(opts),
         {:ok, definitions} <- Workload.load(),
         :ok <- validate_parameters(parameter_bundle, opts, definitions),
         {:ok, identity} <- fetch_state_identity(opts),
         :ok <- StateIdentity.verify_manifest(identity, Keyword.fetch!(opts, :manifest)) do
      definitions_by_variant = Map.new(definitions, &{variant_label(&1), &1})
      execute_blocks(schedule, parameter_bundle, identity, definitions_by_variant, opts)
    else
      {:error, reason} -> {:error, %{stage: :setup, reason: reason, score_eligible?: false}}
    end
  end

  defp execute_blocks(schedule, bundle, identity, definitions, opts) do
    initial = %{blocks: [], cursors: %{}, state_identity: identity, score_eligible?: true}

    schedule
    |> Enum.reduce_while({:ok, initial}, fn block, {:ok, run} ->
      case execute_block(block, bundle, run.cursors, run.state_identity, definitions, opts) do
        {:ok, receipt, cursors, next_identity} ->
          {:cont,
           {:ok,
            %{
              run
              | blocks: run.blocks ++ [receipt],
                cursors: cursors,
                state_identity: next_identity
            }}}

        {:error, stage, reason, partial} ->
          {:halt,
           {:error,
            %{
              stage: stage,
              reason: reason,
              partial_blocks: run.blocks,
              partial: partial,
              score_eligible?: false
            }}}
      end
    end)
    |> case do
      {:ok, run} ->
        {:ok,
         Map.merge(run, %{
           operation_count: Enum.sum(Enum.map(run.blocks, & &1.operation_count)),
           parameter_sequence: Enum.flat_map(run.blocks, & &1.parameter_sequence)
         })}

      error ->
        error
    end
  end

  defp execute_block(block, bundle, cursors, identity, definitions, opts) do
    started = System.monotonic_time()
    update_block = Map.put(block, :continuation_state_identity, identity)

    with :ok <- validate_continuation_position(identity, block),
         {:ok, update} <- timed_call(Keyword.fetch!(opts, :apply_update), update_block, :update),
         {:ok, next_identity} <- StateIdentity.advance(identity, block.position, update),
         state_block <- Map.put(block, :state_identity, next_identity),
         {:ok, precompute} <-
           timed_call(Keyword.fetch!(opts, :precompute), state_block, :precompute),
         {:ok, reads, next_cursors} <-
           execute_reads(state_block, bundle, cursors, definitions, opts) do
      duration_s = elapsed_seconds(started)

      {:ok,
       %{
         position: block.position,
         date: block.date,
         kind: block.kind,
         start_state_identity: identity,
         end_state_identity: next_identity,
         state_identity: next_identity,
         update: update,
         precompute: precompute,
         reads: reads,
         duration_s: duration_s,
         operation_count: 1 + length(reads),
         parameter_sequence: Enum.map(reads, &{&1.variant, &1.parameter_index}),
         complete?: length(reads) == length(@official_variants)
       }, next_cursors, next_identity}
    else
      {:error, {:state_identity_mismatch, _, _, _} = reason} ->
        {:error, :updates, reason, block}

      {:error, {:invalid_state_identity, _} = reason} ->
        {:error, :updates, reason, block}

      {:error, {:callback_failed, :update, reason}} ->
        {:error, :updates, reason, block}

      {:error, {:callback_failed, :precompute, reason}} ->
        {:error, :precomputations, reason, block}

      {:error, {:read_failed, reason, reads}} ->
        {:error, :reads, reason, %{block: block, reads: reads}}
    end
  end

  defp execute_reads(block, bundle, cursors, definitions, opts) do
    Enum.reduce_while(block.variants, {:ok, [], cursors}, fn variant, {:ok, reads, positions} ->
      sequence = Map.fetch!(bundle.sequences, variant)
      index = Map.get(positions, variant, 0)
      parameters = Enum.at(sequence, rem(index, length(sequence)))
      definition = Map.fetch!(definitions, variant)
      parameter_identity = parameter_row_identity(bundle, variant, index, parameters)

      operation = %{
        block: block,
        variant: variant,
        operation_id: definition.catalog_id,
        parameters: parameters,
        parameter_index: index,
        parameter_row_identity: parameter_identity
      }

      case timed_call(Keyword.fetch!(opts, :execute_read), operation, :read) do
        {:ok, result} ->
          receipt =
            result
            |> normalize_read_receipt()
            |> Map.merge(%{
              variant: variant,
              operation_id: definition.catalog_id,
              parameter_index: index,
              parameter_row_identity: parameter_identity
            })

          case read_eligibility(receipt) do
            :ok ->
              {:cont, {:ok, reads ++ [receipt], Map.put(positions, variant, index + 1)}}

            {:error, status} ->
              {:halt, {:error, {:read_failed, {:ineligible_read, variant, status}, reads}}}
          end

        {:error, {:callback_failed, :read, reason}} ->
          {:halt, {:error, {:read_failed, {variant, reason}, reads}}}
      end
    end)
  end

  defp normalize_read_receipt(result) do
    correct? = Map.get(result, :correct?) == true
    complete? = Map.get(result, :complete?) == true
    warmup? = Map.get(result, :mode) == :warmup

    score_eligible? =
      correct? and complete? and not warmup? and Map.get(result, :score_eligible?, true)

    result
    |> Map.put_new(:result_checksum, digest(Map.drop(result, [:duration_s])))
    |> Map.put(:correctness_status, correctness_status(correct?, complete?))
    |> Map.put(:score_eligible?, score_eligible?)
  end

  defp read_eligibility(%{correctness_status: :correct, score_eligible?: true}), do: :ok
  defp read_eligibility(%{correctness_status: status}), do: {:error, status}

  defp correctness_status(_correct?, false), do: :incomplete
  defp correctness_status(true, true), do: :correct
  defp correctness_status(false, true), do: :incorrect

  defp parameter_row_identity(bundle, variant, index, parameters) do
    %{
      variant: variant,
      row_index: index,
      dataset_checksum: bundle.dataset_checksum,
      source_id: bundle.source_id,
      generator_pin: bundle.generator_pin,
      checksum: digest(parameters)
    }
  end

  defp timed_call(callback, argument, stage) do
    started = System.monotonic_time()

    case callback.(argument) do
      {:ok, value} ->
        {:ok, Map.put(normalize_receipt(value), :duration_s, elapsed_seconds(started))}

      {:error, reason} ->
        {:error, {:callback_failed, stage, reason}}

      other ->
        {:error, {:callback_failed, stage, {:invalid_result, other}}}
    end
  end

  defp normalize_receipt(value) when is_map(value), do: value
  defp normalize_receipt(value), do: %{value: value}

  defp validate_callbacks(opts) do
    if Enum.all?([:apply_update, :precompute, :execute_read], fn key ->
         is_function(Keyword.get(opts, key), 1)
       end) do
      :ok
    else
      {:error, :missing_protocol_callback}
    end
  end

  defp validate_parameters(bundle, opts, definitions) do
    manifest = Keyword.fetch!(opts, :manifest)
    comparable? = Keyword.get(opts, :comparable, false)
    validation_target = Keyword.get(opts, :fixture, manifest)

    validation_opts =
      [comparable: comparable?]
      |> maybe_put_reference_profile(Keyword.get(opts, :reference_profile))

    Parameters.validate(bundle, validation_target, definitions, validation_opts)
  end

  defp fetch_state_identity(opts) do
    case Keyword.fetch(opts, :state_identity) do
      {:ok, %StateIdentity{} = identity} ->
        with :ok <- StateIdentity.validate(identity), do: {:ok, identity}

      _other ->
        {:error, {:invalid_state_identity, :missing}}
    end
  end

  defp validate_continuation_position(identity, %{position: position}) do
    expected = identity.last_applied_batch + 1

    if position == expected,
      do: :ok,
      else: {:error, {:state_identity_mismatch, :last_applied_batch, expected, position}}
  end

  defp maybe_put_reference_profile(opts, nil), do: opts

  defp maybe_put_reference_profile(opts, profile),
    do: Keyword.put(opts, :reference_profile, profile)

  defp variant_label(definition) do
    suffix = if definition.variant == "default", do: "", else: definition.variant
    Integer.to_string(definition.number) <> suffix
  end

  defp default_batches(:validation), do: 1
  defp default_batches(:power), do: 1
  defp default_batches(:throughput), do: Date.diff(@end_date, @start_date)

  defp block_kind(:validation, _offset), do: :validation
  defp block_kind(:power, _offset), do: :power
  defp block_kind(:throughput, 0), do: :power
  defp block_kind(:throughput, _offset), do: :throughput

  defp elapsed_seconds(started) do
    elapsed =
      (System.monotonic_time() - started)
      |> System.convert_time_unit(:native, :nanosecond)
      |> Kernel./(1_000_000_000)

    max(elapsed, 1.0e-9)
  end

  defp digest(term) do
    "sha256:" <>
      (term
       |> :erlang.term_to_binary([:deterministic])
       |> then(&:crypto.hash(:sha256, &1))
       |> Base.encode16(case: :lower))
  end
end
