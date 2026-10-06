defmodule TripleStore.Benchmark.LDBC.Execution do
  @moduledoc """
  Fail-fast, phase-aware execution for canonical LDBC operations.

  A run becomes a latency sample only after materialization and correctness
  validation succeed. Warmup runs and every error class remain visible but are
  never included in measured latency distributions.
  """

  alias TripleStore.Benchmark.LDBC.Operation

  @phases [:setup, :parse, :plan, :execute, :materialize, :validation]
  @default_timeouts %{short: 5_000, standard: 30_000, long: 120_000}

  @type phase_fun :: (term() -> {:ok, term()} | {:error, term()})

  @doc "Runs configured phases, always invokes teardown, and returns a complete record."
  @spec run(Operation.t(), keyword()) :: map()
  def run(%Operation{} = operation, opts) do
    mode = Keyword.get(opts, :mode, :measured)
    started_at = System.system_time(:millisecond)
    total_started = System.monotonic_time()
    timeout = operation_timeout(operation, opts)

    {outcome, timings} = run_phases(operation, opts, timeout)
    {teardown, teardown_us} = timed(fn -> invoke(Keyword.get(opts, :teardown), outcome) end)
    total_us = elapsed_us(total_started)

    outcome = combine_teardown(outcome, teardown)
    success? = match?({:ok, _value}, outcome)

    %{
      operation_id: operation.id,
      mode: mode,
      started_at_unix_ms: started_at,
      status: if(success?, do: :success, else: :error),
      result: outcome,
      timings_us: Map.put(timings, :teardown, teardown_us),
      total_us: total_us,
      sample_us: if(success? and mode == :measured, do: total_us, else: nil),
      score_eligible?: success? and mode == :measured
    }
  end

  defp run_phases(operation, opts, timeout) do
    Enum.reduce_while(@phases, {{:ok, nil}, %{}}, fn phase, {{:ok, value}, timings} ->
      fun = phase_fun(phase, opts)
      phase_timeout = Keyword.get(opts, :phase_timeouts, %{}) |> Map.get(phase, timeout)

      case timed_timeout(fn -> invoke(fun, value) end, phase_timeout) do
        {{:ok, next}, duration} ->
          {:cont, {{:ok, next}, Map.put(timings, phase, duration)}}

        {{:error, reason}, duration} ->
          error = {:error, %{class: error_class(phase, reason), phase: phase, reason: reason}}
          {:halt, {error, Map.put(timings, phase, duration)}}
      end
    end)
    |> ensure_validation(operation)
  end

  defp phase_fun(phase, opts) do
    default =
      case phase do
        :materialize -> &materialize/1
        :validation -> fn _value -> {:error, :validation_callback_required} end
        _other -> fn value -> {:ok, value} end
      end

    opts |> Keyword.get(:phases, %{}) |> Map.get(phase, default)
  end

  defp materialize(value) when is_list(value), do: {:ok, value}

  defp materialize(value) when is_struct(value) do
    if Enumerable.impl_for(value), do: {:ok, Enum.to_list(value)}, else: {:ok, value}
  end

  defp materialize(value) when is_map(value) or is_boolean(value), do: {:ok, value}

  defp materialize(value) do
    if Enumerable.impl_for(value), do: {:ok, Enum.to_list(value)}, else: {:ok, value}
  rescue
    error -> {:error, {:materialization_failed, Exception.message(error)}}
  end

  defp invoke(nil, value), do: {:ok, value}

  defp invoke(fun, value) when is_function(fun, 1) do
    case fun.(value) do
      {:ok, _result} = ok -> ok
      :ok -> {:ok, value}
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_phase_response, other}}
    end
  rescue
    error -> {:error, {:exception, error.__struct__, Exception.message(error)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp timed_timeout(fun, timeout) when is_integer(timeout) and timeout > 0 do
    started = System.monotonic_time()
    task = Task.async(fun)

    result =
      case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
        {:ok, value} -> value
        nil -> {:error, :timeout}
      end

    {result, elapsed_us(started)}
  end

  defp timed(fun) do
    started = System.monotonic_time()
    {fun.(), elapsed_us(started)}
  end

  defp combine_teardown({:ok, value}, {:ok, _teardown}), do: {:ok, value}

  defp combine_teardown({:ok, _value}, {:error, reason}),
    do: {:error, %{class: :teardown, reason: reason}}

  defp combine_teardown(error, _teardown), do: error

  defp ensure_validation({{:ok, _value}, timings} = outcome, _operation) do
    if Map.has_key?(timings, :validation),
      do: outcome,
      else: {{:error, %{class: :validation, reason: :not_run}}, timings}
  end

  defp ensure_validation(outcome, _operation), do: outcome

  defp error_class(_phase, :timeout), do: :timeout
  defp error_class(:parse, _reason), do: :parse
  defp error_class(:validation, _reason), do: :validation
  defp error_class(_phase, _reason), do: :execution

  defp operation_timeout(operation, opts) do
    opts
    |> Keyword.get(:timeout_classes, @default_timeouts)
    |> Map.fetch!(operation.timeout_class)
  end

  defp elapsed_us(started),
    do: System.convert_time_unit(System.monotonic_time() - started, :native, :microsecond)
end
