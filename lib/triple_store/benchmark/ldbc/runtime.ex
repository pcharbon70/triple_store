defmodule TripleStore.Benchmark.LDBC.Runtime do
  @moduledoc """
  Owns the embedded store and optional services for one LDBC benchmark run.

  Services are started only from profile declarations and are stopped in reverse
  order before the store closes. The final accounting record makes remaining
  tasks, ports, snapshots, and streams visible to the run artifact pipeline.
  """

  use GenServer

  @doc "Starts an explicitly configured benchmark runtime."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))

  @doc "Returns the owned store handle for coordinated benchmark execution."
  @spec store(GenServer.server()) :: {:ok, TripleStore.store()}
  def store(server), do: GenServer.call(server, :store)

  @doc "Serializes stateful benchmark work through the runtime owner."
  @spec run(GenServer.server(), (TripleStore.store() -> term())) :: term()
  def run(server, operation) when is_function(operation, 1),
    do: GenServer.call(server, {:run, operation}, :infinity)

  @doc "Stops services and the store and returns final resource accounting."
  @spec shutdown(GenServer.server()) :: {:ok, map()} | {:error, term()}
  def shutdown(server), do: GenServer.call(server, :shutdown, :infinity)

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    schema = Keyword.get(opts, :schema, :quad)

    with {:ok, store} <- TripleStore.open(path, schema: schema),
         {:ok, services} <- start_services(Keyword.get(opts, :services, []), store) do
      {:ok, %{store: store, services: services, resources: MapSet.new(), shutdown?: false}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:store, _from, state), do: {:reply, {:ok, state.store}, state}

  def handle_call({:run, operation}, _from, state) do
    {:reply, operation.(state.store), state}
  rescue
    error -> {:reply, {:error, {:execution, Exception.message(error)}}, state}
  end

  def handle_call(:shutdown, _from, state) do
    {service_errors, stopped} = stop_services(state.services)
    close_result = TripleStore.close(state.store)

    accounting = %{
      services_started: length(state.services),
      services_stopped: stopped,
      service_errors: service_errors,
      outstanding_resources: MapSet.to_list(state.resources),
      store_close: close_result
    }

    result =
      if service_errors == [] and close_result == :ok,
        do: {:ok, accounting},
        else: {:error, accounting}

    {:stop, :normal, result, %{state | shutdown?: true}}
  end

  @impl true
  def terminate(_reason, %{shutdown?: true}), do: :ok

  def terminate(_reason, state) do
    stop_services(state.services)
    TripleStore.close(state.store)
    :ok
  end

  defp start_services(specs, store) do
    Enum.reduce_while(specs, {:ok, []}, fn
      {name, start, stop}, {:ok, services} when is_function(start, 1) and is_function(stop, 1) ->
        case start.(store) do
          {:ok, resource} ->
            {:cont, {:ok, [%{name: name, resource: resource, stop: stop} | services]}}

          {:error, reason} ->
            stop_services(services)
            {:halt, {:error, {:service_start_failed, name, reason}}}
        end

      invalid, _acc ->
        {:halt, {:error, {:invalid_service_spec, invalid}}}
    end)
  end

  defp stop_services(services) do
    Enum.reduce(services, {[], 0}, fn service, {errors, count} ->
      case service.stop.(service.resource) do
        :ok -> {errors, count + 1}
        {:error, reason} -> {[{service.name, reason} | errors], count}
      end
    end)
  end
end
