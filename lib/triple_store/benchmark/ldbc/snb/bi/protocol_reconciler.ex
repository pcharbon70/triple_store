defmodule TripleStore.Benchmark.LDBC.SNB.BI.ProtocolReconciler do
  @moduledoc """
  Reconciles SNB BI protocol execution with the exact local scoring inputs.

  Score records are derived from completed protocol receipts so setup and
  artifact-writing time cannot enter measurements. Each record retains its
  block position and persisted state checksum, allowing missing, duplicated,
  reordered, or stale-state inputs to fail before scoring.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.StateIdentity

  @comparison_fields [
    :block_position,
    :batch_type,
    :day,
    :label,
    :state_checksum,
    :operation_id,
    :parameter_row_identity,
    :result_checksum,
    :duration_s,
    :correct?,
    :complete?,
    :score_eligible?
  ]

  @doc "Builds canonical score inputs from one completed protocol run."
  @spec score_records(map()) :: {:ok, [map()]} | {:error, term()}
  def score_records(%{blocks: blocks}) when is_list(blocks) do
    if Enum.all?(blocks, &valid_block?/1) do
      {:ok, Enum.flat_map(blocks, &block_records/1)}
    else
      {:error, :incomplete_protocol_run}
    end
  end

  def score_records(_run), do: {:error, :invalid_protocol_run}

  @doc "Proves schedule, execution, state, and scoring records agree one-to-one."
  @spec reconcile([map()], map(), [map()]) :: {:ok, map()} | {:error, term()}
  def reconcile(schedule, %{blocks: blocks} = run, records)
      when is_list(schedule) and is_list(blocks) and is_list(records) do
    with :ok <- reconcile_schedule(schedule, blocks),
         :ok <- reconcile_state_chain(blocks),
         {:ok, expected} <- score_records(run),
         :ok <- reject_duplicate_records(records),
         :ok <- compare_record_count(expected, records),
         :ok <- compare_records(expected, records) do
      {:ok,
       %{
         block_count: length(blocks),
         operation_count: length(records),
         input_checksum: digest(records),
         first_state_identity: blocks |> List.first() |> Map.fetch!(:start_state_identity),
         final_state_identity: blocks |> List.last() |> Map.fetch!(:end_state_identity),
         valid?: true
       }}
    end
  end

  def reconcile(_schedule, _run, _records), do: {:error, :invalid_reconciliation_input}

  defp block_records(%{kind: :validation}), do: []

  defp block_records(%{kind: :power} = block) do
    [write_record(block, :power) | Enum.map(block.reads, &power_read_record(block, &1))]
  end

  defp block_records(%{kind: :throughput} = block) do
    [write_record(block, :throughput), throughput_read_record(block)]
  end

  defp write_record(block, batch_type) do
    eligible? =
      block.complete? and Map.get(block.update, :complete?, true) and
        Map.get(block.update, :score_eligible?, true)

    base_record(block, batch_type, "writes")
    |> Map.merge(%{
      operation_id: "ldbc/snb-bi/update-batch@v1.0.3",
      duration_s: block.update.duration_s + block.precompute.duration_s,
      result_checksum: Map.get(block.update, :checksum),
      parameter_row_identity: nil,
      correct?: eligible?,
      complete?: eligible?,
      score_eligible?: eligible?
    })
  end

  defp power_read_record(block, read) do
    base_record(block, :power, read.variant)
    |> Map.merge(%{
      operation_id: read.operation_id,
      duration_s: read.duration_s,
      result_checksum: read.result_checksum,
      parameter_row_identity: read.parameter_row_identity,
      correct?: read.correctness_status == :correct,
      complete?: read.complete?,
      score_eligible?: read.score_eligible?
    })
  end

  defp throughput_read_record(block) do
    eligible? = Enum.all?(block.reads, & &1.score_eligible?)

    base_record(block, :throughput, "reads")
    |> Map.merge(%{
      operation_id: "ldbc/snb-bi/read-block@v1.0.3",
      duration_s: Enum.sum(Enum.map(block.reads, & &1.duration_s)),
      result_checksum: digest(Enum.map(block.reads, & &1.result_checksum)),
      parameter_row_identity: Enum.map(block.reads, & &1.parameter_row_identity),
      correct?: eligible?,
      complete?: eligible? and block.complete?,
      score_eligible?: eligible? and block.complete?
    })
  end

  defp base_record(block, batch_type, label) do
    %{
      block_position: block.position,
      batch_type: batch_type,
      day: block.date,
      label: label,
      state_checksum: block.end_state_identity.state_checksum
    }
  end

  defp valid_block?(block) do
    block[:complete?] == true and is_list(block[:reads]) and is_map(block[:update]) and
      is_map(block[:precompute]) and not is_nil(block[:start_state_identity]) and
      not is_nil(block[:end_state_identity])
  end

  defp reconcile_schedule(schedule, blocks) do
    if length(schedule) == length(blocks) do
      Enum.zip(schedule, blocks)
      |> Enum.with_index()
      |> Enum.reduce_while(:ok, fn {{planned, actual}, index}, :ok ->
        reconcile_block(planned, actual, index)
      end)
    else
      {:error, {:protocol_block_count, length(schedule), length(blocks)}}
    end
  end

  defp reconcile_block(planned, actual, index) do
    expected = {planned.position, planned.date, planned.kind, planned.variants}
    observed = {actual.position, actual.date, actual.kind, Enum.map(actual.reads, & &1.variant)}

    if expected == observed,
      do: {:cont, :ok},
      else: {:halt, {:error, {:protocol_block_mismatch, index, expected, observed}}}
  end

  defp reconcile_state_chain([]), do: {:error, :empty_protocol_run}

  defp reconcile_state_chain(blocks) do
    with :ok <- validate_block_state_identities(blocks) do
      blocks
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.reduce_while(:ok, fn [left, right], :ok ->
        if left.end_state_identity == right.start_state_identity,
          do: {:cont, :ok},
          else: {:halt, {:error, {:stale_protocol_state, left.position, right.position}}}
      end)
    end
  end

  defp validate_block_state_identities(blocks) do
    Enum.reduce_while(blocks, :ok, fn block, :ok ->
      case validate_block_state_identity(block) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_block_state_identity(block) do
    start_identity = block.start_state_identity
    end_identity = block.end_state_identity

    valid? =
      StateIdentity.validate(start_identity) == :ok and
        StateIdentity.validate(end_identity) == :ok and
        StateIdentity.compare(end_identity, block.state_identity) == :ok and
        start_identity.last_applied_batch + 1 == block.position and
        end_identity.last_applied_batch == block.position and
        Map.get(block.update, :state_checksum) == end_identity.state_checksum

    if valid?,
      do: :ok,
      else: {:error, {:protocol_state_identity_mismatch, block.position}}
  end

  defp reject_duplicate_records(records) do
    records
    |> Enum.frequencies_by(&record_identity/1)
    |> Enum.find(fn {_identity, count} -> count > 1 end)
    |> case do
      nil -> :ok
      {identity, _count} -> {:error, {:duplicate_score_record, identity}}
    end
  end

  defp compare_record_count(expected, actual) do
    if length(expected) == length(actual),
      do: :ok,
      else: {:error, {:score_record_count, length(expected), length(actual)}}
  end

  defp compare_records(expected, actual) do
    expected
    |> Enum.zip(actual)
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {{left, right}, index}, :ok ->
      case Enum.find(@comparison_fields, &(Map.get(left, &1) != Map.get(right, &1))) do
        nil ->
          {:cont, :ok}

        field ->
          {:halt, {:error, {:score_record_mismatch, index, field, left[field], right[field]}}}
      end
    end)
  end

  defp record_identity(record),
    do: {record[:block_position], record[:batch_type], record[:label]}

  defp digest(term) do
    "sha256:" <>
      (term
       |> :erlang.term_to_binary([:deterministic])
       |> then(&:crypto.hash(:sha256, &1))
       |> Base.encode16(case: :lower))
  end
end
