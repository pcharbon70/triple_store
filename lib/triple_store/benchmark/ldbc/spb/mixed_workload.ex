defmodule TripleStore.Benchmark.LDBC.SPB.MixedWorkload do
  @moduledoc """
  Bounded, serialized scheduling for SPB aggregation and editorial agents.

  Agents submit concurrently, while one scheduler establishes the accepted
  embedded-store isolation boundary. Operations from each editorial agent must
  arrive in sequence; the scheduler rejects gaps before invoking a handler.
  Warmup and measured records are accounted separately.
  """

  use GenServer

  @types [:aggregation, :editorial]
  @phases [:warmup, :measured]

  @doc "Starts a scheduler with explicit operation handlers and queue bound."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @doc "Submits one correlated operation through the serialized boundary."
  @spec submit(
          GenServer.server(),
          atom(),
          String.t(),
          non_neg_integer(),
          atom(),
          term(),
          timeout()
        ) ::
          {:ok, map()} | {:error, term()}
  def submit(server, type, agent_id, sequence, phase, payload, timeout \\ 30_000) do
    GenServer.call(server, {:submit, type, agent_id, sequence, phase, payload}, timeout)
  end

  @doc "Runs each agent's script sequentially while agents execute concurrently."
  @spec run_agents(GenServer.server(), [map()], atom(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def run_agents(server, agents, phase, opts \\ []) when phase in @phases do
    timeout = Keyword.get(opts, :timeout, 30_000)
    concurrency = Keyword.get(opts, :concurrency, length(agents))

    results =
      Task.async_stream(
        agents,
        fn agent ->
          agent.operations
          |> Enum.with_index()
          |> Enum.map(fn {payload, sequence} ->
            submit(server, agent.type, agent.id, sequence, phase, payload, timeout)
          end)
        end,
        ordered: false,
        max_concurrency: max(concurrency, 1),
        timeout: :infinity
      )
      |> Enum.to_list()

    successful? =
      Enum.all?(results, fn
        {:ok, operations} -> Enum.all?(operations, &match?({:ok, _}, &1))
        _ -> false
      end)

    if successful? do
      summary(server)
    else
      {:error, {:agent_failure, results}}
    end
  end

  @doc "Returns reconciled per-agent counts and score-gated rates."
  @spec summary(GenServer.server()) :: {:ok, map()}
  def summary(server), do: GenServer.call(server, :summary)

  @doc "Returns a handler suitable for the Phase 3 local driver bridge."
  @spec bridge_handler(GenServer.server()) :: (atom(), map() -> {:ok, term()} | {:error, term()})
  def bridge_handler(server) do
    fn
      :execute, %{"parameters" => parameters} ->
        submit(
          server,
          String.to_existing_atom(parameters["type"]),
          parameters["agent_id"],
          parameters["sequence"],
          String.to_existing_atom(parameters["phase"]),
          parameters["payload"]
        )

      :health, _frame ->
        {:ok, %{status: :ready}}

      action, _frame ->
        {:error, {:unsupported_bridge_action, action}}
    end
  end

  @impl true
  def init(opts) do
    handlers = Keyword.fetch!(opts, :handlers)

    if Enum.all?(@types, &is_function(Map.get(handlers, &1), 1)) do
      {:ok,
       %{
         handlers: handlers,
         max_queue: Keyword.get(opts, :max_queue, 1_000),
         retries: Keyword.get(opts, :retries, 0),
         editorial_sequences: %{},
         records: []
       }}
    else
      {:stop, :invalid_handlers}
    end
  end

  @impl true
  def handle_call({:submit, type, agent_id, sequence, phase, payload}, _from, state) do
    with :ok <- validate_envelope(type, agent_id, sequence, phase),
         :ok <- enforce_backpressure(state),
         :ok <- enforce_editorial_sequence(state, type, agent_id, sequence) do
      started = System.monotonic_time()
      {result, retries} = invoke_with_retries(state.handlers[type], payload, state.retries)
      latency = System.convert_time_unit(System.monotonic_time() - started, :native, :microsecond)

      record = %{
        operation_id: operation_id(payload),
        type: type,
        agent_id: agent_id,
        sequence: sequence,
        phase: phase,
        status: if(match?({:ok, _}, result), do: :success, else: :error),
        latency_us: latency,
        retries: retries,
        result: result,
        timestamp_us: System.monotonic_time(:microsecond)
      }

      next_state =
        state
        |> advance_sequence(type, agent_id, sequence, result)
        |> Map.update!(:records, &[record | &1])

      {:reply, {:ok, record}, next_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:summary, _from, state) do
    records = Enum.reverse(state.records)
    measured = Enum.filter(records, &(&1.phase == :measured))
    warmup = Enum.filter(records, &(&1.phase == :warmup))
    failures = Enum.filter(measured, &(&1.status == :error))
    valid? = measured != [] and failures == []
    duration_s = measured_duration_seconds(measured)

    rates =
      if valid? do
        measured
        |> Enum.frequencies_by(& &1.type)
        |> Map.new(fn {type, count} -> {type, count / duration_s} end)
      else
        %{}
      end

    reply = %{
      warmup_count: length(warmup),
      measured_count: length(measured),
      failures: failures,
      score_eligible?: valid?,
      rates_per_second: rates,
      agents: agent_metrics(records),
      records: records
    }

    {:reply, {:ok, reply}, state}
  end

  @impl true
  def handle_info({:DOWN, _reference, :process, _pid, _reason}, state) do
    # Query execution may use monitored workers internally. Their result is
    # consumed synchronously by the handler; a late monitor notification does
    # not represent a scheduler-owned process failure.
    {:noreply, state}
  end

  defp validate_envelope(type, agent_id, sequence, phase)
       when type in @types and is_binary(agent_id) and agent_id != "" and
              is_integer(sequence) and sequence >= 0 and phase in @phases,
       do: :ok

  defp validate_envelope(_type, _agent_id, _sequence, _phase), do: {:error, :invalid_operation}

  defp enforce_backpressure(state) do
    {:message_queue_len, queued} = Process.info(self(), :message_queue_len)
    if queued <= state.max_queue, do: :ok, else: {:error, :backpressure}
  end

  defp enforce_editorial_sequence(_state, :aggregation, _agent_id, _sequence), do: :ok

  defp enforce_editorial_sequence(state, :editorial, agent_id, sequence) do
    expected = Map.get(state.editorial_sequences, agent_id, 0)
    if sequence == expected, do: :ok, else: {:error, {:editorial_sequence, expected, sequence}}
  end

  defp advance_sequence(state, :editorial, agent_id, sequence, {:ok, _value}) do
    put_in(state, [:editorial_sequences, agent_id], sequence + 1)
  end

  defp advance_sequence(state, _type, _agent_id, _sequence, _result), do: state

  defp invoke_with_retries(handler, payload, retries),
    do: invoke_with_retries(handler, payload, retries, 0)

  defp invoke_with_retries(handler, payload, retries_left, attempted) do
    case invoke(handler, payload) do
      {:retry, _reason} when retries_left > 0 ->
        invoke_with_retries(handler, payload, retries_left - 1, attempted + 1)

      {:retry, reason} ->
        {{:error, {:retries_exhausted, reason}}, attempted}

      result ->
        {result, attempted}
    end
  end

  defp invoke(handler, payload) do
    case handler.(payload) do
      {:ok, _value} = ok -> ok
      {:error, _reason} = error -> error
      {:retry, _reason} = retry -> retry
      other -> {:error, {:invalid_handler_response, other}}
    end
  rescue
    error -> {:error, {:exception, error.__struct__, Exception.message(error)}}
  end

  defp operation_id(%{operation_id: id}) when is_binary(id), do: id
  defp operation_id(%{"operation_id" => id}) when is_binary(id), do: id
  defp operation_id(_payload), do: "unknown"

  defp measured_duration_seconds([]), do: 1.0

  defp measured_duration_seconds(records) do
    timestamps = Enum.map(records, & &1.timestamp_us)
    max((Enum.max(timestamps) - Enum.min(timestamps)) / 1_000_000, 1.0e-6)
  end

  defp agent_metrics(records) do
    records
    |> Enum.group_by(&{&1.type, &1.agent_id})
    |> Map.new(fn {{type, id}, agent_records} ->
      successes = Enum.count(agent_records, &(&1.status == :success))

      {{type, id},
       %{
         operations: length(agent_records),
         successes: successes,
         failures: length(agent_records) - successes,
         retries: Enum.sum(Enum.map(agent_records, & &1.retries)),
         latency_us: Enum.map(agent_records, & &1.latency_us)
       }}
    end)
  end
end
