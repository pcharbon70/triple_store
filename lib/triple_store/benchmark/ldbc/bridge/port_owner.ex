defmodule TripleStore.Benchmark.LDBC.Bridge.PortOwner do
  @moduledoc """
  Owns the external LDBC driver child as a four-byte framed Erlang Port.

  The child has no listening socket. Its stdout carries requests to the bridge,
  stdin carries correlated responses, and any driver exit is reported to the
  benchmark owner before the Port is released.
  """

  use GenServer

  alias TripleStore.Benchmark.LDBC.Bridge
  alias TripleStore.Benchmark.LDBC.Bridge.Protocol

  @doc "Starts an external driver process owned by the caller."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))

  @impl true
  def init(opts) do
    executable = Keyword.fetch!(opts, :executable)
    arguments = Keyword.get(opts, :arguments, [])
    owner = Keyword.get(opts, :owner, self())

    port =
      Port.open({:spawn_executable, executable}, [
        :binary,
        :exit_status,
        {:packet, 4},
        {:args, arguments}
      ])

    {:ok,
     %{
       port: port,
       bridge: Keyword.fetch!(opts, :bridge),
       owner: owner,
       max_frame_bytes: Keyword.get(opts, :max_frame_bytes, 1_048_576)
     }}
  end

  @impl true
  def handle_info({port, {:data, payload}}, %{port: port} = state) do
    response =
      case Protocol.decode_payload(payload, state.max_frame_bytes) do
        {:ok, frame} -> response(frame, Bridge.request(state.bridge, frame))
        {:error, reason} -> %{"id" => nil, "status" => "error", "error" => inspect(reason)}
      end

    send(state.owner, {:ldbc_driver_frame, response})

    case Protocol.encode_payload(response, state.max_frame_bytes) do
      {:ok, encoded} -> Port.command(port, encoded)
      {:error, reason} -> send(state.owner, {:ldbc_driver_error, reason})
    end

    {:noreply, state}
  end

  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    send(state.owner, {:ldbc_driver_exit, status})
    reason = if status == 0, do: :normal, else: {:driver_exit, status}
    {:stop, reason, state}
  end

  @impl true
  def terminate(_reason, %{port: port}) do
    if Port.info(port), do: Port.close(port)
    :ok
  end

  defp response(frame, {:ok, value}),
    do: %{"id" => frame["id"], "status" => "success", "value" => json_safe(value)}

  defp response(frame, {:error, error}),
    do: %{"id" => frame["id"], "status" => "error", "error" => json_safe(error)}

  defp json_safe(value) when is_map(value),
    do: Map.new(value, fn {key, item} -> {to_string(key), json_safe(item)} end)

  defp json_safe(value) when is_list(value), do: Enum.map(value, &json_safe/1)
  defp json_safe(value) when is_tuple(value), do: value |> Tuple.to_list() |> json_safe()
  defp json_safe(value) when is_atom(value), do: Atom.to_string(value)
  defp json_safe(value), do: value
end
