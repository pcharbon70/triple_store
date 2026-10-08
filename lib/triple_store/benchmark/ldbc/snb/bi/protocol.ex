defmodule TripleStore.Benchmark.LDBC.SNB.BI.Protocol do
  @moduledoc """
  Pinned SNB BI validation, power, and throughput scheduling.

  A block applies its dated update batch, performs declared precomputations,
  then executes the official query variant order. The first block is power;
  later blocks are throughput. Comparable runs require official parameter
  provenance and the one-hour throughput termination condition.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.{Parameters, Workload}

  @start_date ~D[2012-11-29]
  @end_date ~D[2013-01-01]
  @official_variants ~w(1 2a 2b 3 4 5 6 7 8a 8b 9 10a 10b 11 12 13 14a 14b 15a 15b 16a 16b 17 18 19a 19b 20a 20b)

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
              kind: if(offset == 0, do: :power, else: :throughput),
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
         :ok <- validate_parameters(parameter_bundle, opts, definitions) do
      execute_blocks(schedule, parameter_bundle, opts)
    else
      {:error, reason} -> {:error, %{stage: :setup, reason: reason, score_eligible?: false}}
    end
  end

  defp execute_blocks(schedule, bundle, opts) do
    initial = %{blocks: [], cursors: %{}, score_eligible?: true}

    schedule
    |> Enum.reduce_while({:ok, initial}, fn block, {:ok, run} ->
      case execute_block(block, bundle, run.cursors, opts) do
        {:ok, receipt, cursors} ->
          {:cont, {:ok, %{run | blocks: run.blocks ++ [receipt], cursors: cursors}}}

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

  defp execute_block(block, bundle, cursors, opts) do
    started = System.monotonic_time()

    with {:ok, update} <- timed_call(Keyword.fetch!(opts, :apply_update), block, :update),
         {:ok, precompute} <- timed_call(Keyword.fetch!(opts, :precompute), block, :precompute),
         {:ok, reads, next_cursors} <- execute_reads(block, bundle, cursors, opts) do
      duration_s = elapsed_seconds(started)

      {:ok,
       %{
         position: block.position,
         date: block.date,
         kind: block.kind,
         update: update,
         precompute: precompute,
         reads: reads,
         duration_s: duration_s,
         operation_count: 1 + length(reads),
         parameter_sequence: Enum.map(reads, &{&1.variant, &1.parameter_index}),
         complete?: length(reads) == length(@official_variants)
       }, next_cursors}
    else
      {:error, {:callback_failed, :update, reason}} ->
        {:error, :updates, reason, block}

      {:error, {:callback_failed, :precompute, reason}} ->
        {:error, :precomputations, reason, block}

      {:error, {:read_failed, reason, reads}} ->
        {:error, :reads, reason, %{block: block, reads: reads}}
    end
  end

  defp execute_reads(block, bundle, cursors, opts) do
    Enum.reduce_while(block.variants, {:ok, [], cursors}, fn variant, {:ok, reads, positions} ->
      sequence = Map.fetch!(bundle.sequences, variant)
      index = Map.get(positions, variant, 0)
      parameters = Enum.at(sequence, rem(index, length(sequence)))

      operation = %{
        block: block,
        variant: variant,
        parameters: parameters,
        parameter_index: index
      }

      case timed_call(Keyword.fetch!(opts, :execute_read), operation, :read) do
        {:ok, result} ->
          receipt = Map.merge(result, %{variant: variant, parameter_index: index})
          {:cont, {:ok, reads ++ [receipt], Map.put(positions, variant, index + 1)}}

        {:error, {:callback_failed, :read, reason}} ->
          {:halt, {:error, {:read_failed, {variant, reason}, reads}}}
      end
    end)
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
    Parameters.validate(bundle, manifest, definitions, comparable: comparable?)
  end

  defp default_batches(:validation), do: 1
  defp default_batches(:power), do: 1
  defp default_batches(:throughput), do: Date.diff(@end_date, @start_date)

  defp elapsed_seconds(started),
    do:
      (System.monotonic_time() - started)
      |> System.convert_time_unit(:native, :microsecond)
      |> Kernel./(1_000_000)
end
