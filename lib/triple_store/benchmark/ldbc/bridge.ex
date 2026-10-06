defmodule TripleStore.Benchmark.LDBC.Bridge do
  @moduledoc """
  Explicitly started, benchmark-only driver bridge.

  The bridge is intentionally absent from normal application supervision. It
  correlates requests, bounds work, and owns every request task so cancellation,
  caller exits, and bridge shutdown cannot leave benchmark work running.
  """

  use GenServer

  alias TripleStore.Benchmark.LDBC.Bridge.Protocol

  @default_limits %{
    frame_bytes: 1_048_576,
    operations: 1_000,
    parameter_bytes: 262_144,
    concurrency: 8,
    timeout_ms: 30_000
  }

  @type handler :: (atom(), map() -> {:ok, term()} | {:error, term()})

  @doc "Starts a bridge only when explicit benchmark mode is enabled."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    if Keyword.get(opts, :benchmark_mode, false) and benchmark_environment?() do
      GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
    else
      {:error, :benchmark_mode_required}
    end
  end

  @doc "Submits a typed bridge request and waits for its correlated response."
  @spec request(GenServer.server(), map(), timeout()) :: {:ok, term()} | {:error, term()}
  def request(server, frame, timeout \\ 35_000),
    do: GenServer.call(server, {:request, frame}, timeout)

  @doc "Cancels an in-flight request by correlation ID."
  @spec cancel(GenServer.server(), String.t()) :: :ok | {:error, :not_found}
  def cancel(server, request_id), do: GenServer.call(server, {:cancel, request_id})

  @impl true
  def init(opts) do
    {:ok, supervisor} = Task.Supervisor.start_link()

    state = %{
      task_supervisor: supervisor,
      handler: Keyword.fetch!(opts, :handler),
      limits: Map.merge(@default_limits, Map.new(Keyword.get(opts, :limits, []))),
      handshake: Keyword.fetch!(opts, :handshake),
      negotiated?: false,
      requests: %{},
      refs: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:request, frame}, from, state) do
    with :ok <- validate_request(frame, state),
         :ok <- ensure_handshake(frame, state),
         :ok <- ensure_capacity(state) do
      action = frame |> Map.fetch!("action") |> String.to_existing_atom()

      if action in [:health, :checkpoint] do
        {:reply, invoke(state.handler, action, frame), negotiated(state, action)}
      else
        start_request(action, frame, from, state)
      end
    else
      {:error, reason} -> {:reply, {:error, classify(reason)}, state}
    end
  end

  def handle_call({:cancel, request_id}, _from, state) do
    case Map.fetch(state.requests, request_id) do
      {:ok, %{task: task, from: from}} ->
        Task.shutdown(task, :brutal_kill)
        GenServer.reply(from, {:error, %{class: :cancellation, reason: :cancelled}})
        {:reply, :ok, drop_request(state, request_id, task.ref)}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    case Map.pop(state.refs, ref) do
      {nil, _refs} ->
        {:noreply, state}

      {request_id, refs} ->
        %{from: from} = Map.fetch!(state.requests, request_id)
        GenServer.reply(from, result)
        {:noreply, %{state | refs: refs, requests: Map.delete(state.requests, request_id)}}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.refs, ref) do
      {nil, _refs} ->
        {:noreply, state}

      {request_id, refs} ->
        %{from: from} = Map.fetch!(state.requests, request_id)
        GenServer.reply(from, {:error, %{class: :bridge, reason: {:task_exit, reason}}})
        {:noreply, %{state | refs: refs, requests: Map.delete(state.requests, request_id)}}
    end
  end

  defp start_request(action, frame, from, state) do
    request_id = Map.fetch!(frame, "id")
    timeout = min(Map.get(frame, "timeout_ms", state.limits.timeout_ms), state.limits.timeout_ms)
    handler = state.handler

    task =
      Task.Supervisor.async_nolink(state.task_supervisor, fn ->
        task = Task.async(fn -> invoke(handler, action, frame) end)

        case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
          {:ok, response} -> response
          nil -> {:error, %{class: :timeout, reason: :operation_timeout}}
        end
      end)

    request = %{task: task, from: from}
    requests = Map.put(state.requests, request_id, request)
    refs = Map.put(state.refs, task.ref, request_id)
    {:noreply, %{negotiated(state, action) | requests: requests, refs: refs}}
  end

  defp validate_request(frame, state) when is_map(frame) do
    with :ok <- Protocol.validate(frame),
         :ok <- unique_id(frame, state),
         :ok <- parameter_limit(frame, state.limits.parameter_bytes),
         :ok <- operation_limit(frame, state.limits.operations) do
      :ok
    end
  end

  defp validate_request(_frame, _state), do: {:error, :invalid_frame_envelope}

  defp unique_id(%{"id" => id}, state) do
    if Map.has_key?(state.requests, id), do: {:error, :duplicate_request_id}, else: :ok
  end

  defp parameter_limit(frame, limit) do
    if byte_size(:erlang.term_to_binary(Map.get(frame, "parameters", %{}))) <= limit,
      do: :ok,
      else: {:error, :parameters_too_large}
  end

  defp operation_limit(%{"action" => "batch"} = frame, limit) do
    if length(Map.get(frame, "operations", [])) <= limit,
      do: :ok,
      else: {:error, :too_many_operations}
  end

  defp operation_limit(_frame, _limit), do: :ok

  defp ensure_handshake(%{"action" => "handshake"} = frame, state) do
    expected = Map.put(state.handshake, "protocol_version", Protocol.version())
    supplied = Map.take(frame, Map.keys(expected))
    if supplied == expected, do: :ok, else: {:error, {:handshake_mismatch, expected, supplied}}
  end

  defp ensure_handshake(_frame, %{negotiated?: true}), do: :ok
  defp ensure_handshake(_frame, _state), do: {:error, :handshake_required}

  defp ensure_capacity(state) do
    if map_size(state.requests) < state.limits.concurrency,
      do: :ok,
      else: {:error, :concurrency_limit}
  end

  defp invoke(handler, action, frame) do
    case handler.(action, frame) do
      {:ok, _value} = ok -> ok
      {:error, reason} -> {:error, classify(reason)}
      other -> {:error, classify({:invalid_handler_response, other})}
    end
  rescue
    error -> {:error, %{class: :execution, reason: Exception.message(error)}}
  end

  defp classify(%{class: _class, reason: _reason} = error), do: error

  defp classify({class, reason})
       when class in [
              :parse,
              :validation,
              :execution,
              :timeout,
              :cancellation,
              :storage,
              :reasoning,
              :bridge
            ],
       do: %{class: class, reason: reason}

  defp classify(reason), do: %{class: :bridge, reason: reason}

  defp negotiated(state, :handshake), do: %{state | negotiated?: true}
  defp negotiated(state, _action), do: state

  defp drop_request(state, request_id, ref),
    do: %{
      state
      | requests: Map.delete(state.requests, request_id),
        refs: Map.delete(state.refs, ref)
    }

  defp benchmark_environment?, do: Mix.env() in [:dev, :test]
end
