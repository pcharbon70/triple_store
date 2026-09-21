defmodule TripleStore.SPARQL.Update.SessionIterator do
  @moduledoc """
  In-memory iterator over an update session's merged base and staged view.

  The process implements the same movement messages used by the RocksDB iterator
  wrapper so existing index and query code can read staged mutations without
  publishing them to RocksDB.
  """

  use GenServer

  @type entry :: {binary(), binary()}

  @spec start(pid(), [entry()]) :: GenServer.on_start()
  def start(owner, entries) do
    GenServer.start(__MODULE__, {owner, entries})
  end

  @impl true
  def init({owner, entries}) do
    monitor = Process.monitor(owner)
    {:ok, %{entries: entries, position: nil, seek: nil, monitor: monitor}}
  end

  @impl true
  def handle_call({:move, action}, _from, state) do
    {reply, state} = move(action, state)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:seek, key}, _from, state) do
    {:reply, :ok, %{state | seek: key, position: nil}}
  end

  @impl true
  def handle_call(:next, _from, state) do
    action = if state.seek, do: state.seek, else: :next
    {reply, state} = move(action, state)
    {:reply, reply, %{state | seek: nil}}
  end

  @impl true
  def handle_call(:collect, _from, state) do
    start = collection_start(state)
    entries = Enum.drop(state.entries, start)
    {:reply, {:ok, entries}, %{state | position: length(state.entries), seek: nil}}
  end

  @impl true
  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{monitor: monitor} = state) do
    {:stop, :normal, state}
  end

  defp move(:first, state), do: entry_at(state, 0)
  defp move(:last, state), do: entry_at(state, length(state.entries) - 1)

  defp move(:next, %{position: nil} = state), do: entry_at(state, 0)
  defp move(:next, state), do: entry_at(state, state.position + 1)

  defp move(:prev, %{position: nil} = state), do: entry_at(state, length(state.entries) - 1)
  defp move(:prev, state), do: entry_at(state, state.position - 1)

  defp move(key, state) when is_binary(key) do
    position = Enum.find_index(state.entries, fn {entry_key, _value} -> entry_key >= key end)
    entry_at(state, position)
  end

  defp entry_at(state, nil), do: {:iterator_end, %{state | position: length(state.entries)}}

  defp entry_at(state, position) when position < 0,
    do: {:iterator_end, %{state | position: -1}}

  defp entry_at(state, position) do
    case Enum.at(state.entries, position) do
      {key, value} -> {{:ok, key, value}, %{state | position: position}}
      nil -> {:iterator_end, %{state | position: length(state.entries)}}
    end
  end

  defp collection_start(%{seek: seek, entries: entries}) when is_binary(seek) do
    Enum.find_index(entries, fn {key, _value} -> key >= seek end) || length(entries)
  end

  defp collection_start(%{position: nil}), do: 0
  defp collection_start(%{position: position}), do: max(position + 1, 0)
end
