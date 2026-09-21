defmodule TripleStore.SPARQL.UpdateSession do
  @moduledoc """
  Stages one parsed SPARQL Update request before a single RocksDB commit.

  The session presents the subset of the RocksDB adapter protocol used by update
  planning. Point reads, folds, and iterators merge the persisted database with
  pending puts and deletes, so later operations observe earlier operations in
  the same request. Writes only update the overlay. `commit/1` submits the final
  canonical key state as one mixed batch.

  Dictionary allocation deliberately remains on the base store. A request that
  fails before the explicit-index commit can therefore leave unused dictionary
  IDs, but it cannot leave partial explicit-index fanout.
  """

  use GenServer

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.SPARQL.Update.SessionIterator

  @triple_column_families [:spo, :pos, :osp, :derived, :numeric_range, :id2str, :str2id]
  @quad_column_families [
    :gspo,
    :gpos,
    :spog,
    :posg,
    :derived,
    :derivation_provenance,
    :numeric_range,
    :acl,
    :id2str,
    :str2id
  ]

  @type mutation :: {:put, atom(), binary(), binary()} | {:delete, atom(), binary()}
  @type summary :: %{
          mutation_count: non_neg_integer(),
          column_families: MapSet.t(atom()),
          affected_graphs: MapSet.t(non_neg_integer())
        }

  @spec start_link(map()) :: GenServer.on_start()
  def start_link(%{db: base_db}) do
    GenServer.start_link(__MODULE__, base_db)
  end

  @spec context(pid(), map()) :: map()
  def context(session, ctx), do: Map.put(ctx, :db, session)

  @spec commit(pid()) :: {:ok, summary()} | {:error, {:storage, term()}}
  def commit(session), do: GenServer.call(session, :commit, :infinity)

  @spec summary(pid()) :: summary()
  def summary(session), do: GenServer.call(session, :summary)

  @spec stop(pid()) :: :ok
  def stop(session) do
    if Process.alive?(session), do: GenServer.stop(session, :normal)
    :ok
  end

  @spec staging?(pid()) :: boolean()
  def staging?(pid) when is_pid(pid) do
    case Process.info(pid, :dictionary) do
      {:dictionary, dictionary} ->
        Keyword.get(dictionary, :"$initial_call") == {__MODULE__, :init, 1}

      nil ->
        false
    end
  end

  def staging?(_pid), do: false

  @impl true
  def init(base_db) do
    with {:ok, quad_store?} <- ErlangAdapter.is_quad_store?(base_db),
         {:ok, instance_id} <- ErlangAdapter.instance_id(base_db) do
      {:ok,
       %{
         base_db: base_db,
         quad_store?: quad_store?,
         instance_id: instance_id,
         overlay: %{},
         column_families: MapSet.new(),
         affected_graphs: MapSet.new()
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:is_open, _from, state), do: {:reply, Process.alive?(state.base_db), state}

  @impl true
  def handle_call(:is_quad_store, _from, state), do: {:reply, {:ok, state.quad_store?}, state}

  @impl true
  def handle_call(:instance_id, _from, state), do: {:reply, {:ok, state.instance_id}, state}

  @impl true
  def handle_call(:get_path, _from, state) do
    {:reply, ErlangAdapter.get_path(state.base_db), state}
  end

  @impl true
  def handle_call({:get, cf, key}, _from, state) do
    result =
      case Map.get(state.overlay, {cf, key}) do
        {:put, value} -> {:ok, value}
        :delete -> :not_found
        nil -> ErlangAdapter.get(state.base_db, cf, key)
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call({:exists, cf, key}, _from, state) do
    result =
      case Map.get(state.overlay, {cf, key}) do
        {:put, _value} -> {:ok, true}
        :delete -> {:ok, false}
        nil -> ErlangAdapter.exists(state.base_db, cf, key)
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call({:put, cf, key, value}, _from, state) do
    reply_with_staged(state, [{:put, cf, key, value}])
  end

  @impl true
  def handle_call({:delete, cf, key}, _from, state) do
    reply_with_staged(state, [{:delete, cf, key}])
  end

  @impl true
  def handle_call({:write_batch, operations, _sync}, _from, state) do
    mutations = Enum.map(operations, fn {cf, key, value} -> {:put, cf, key, value} end)
    reply_with_staged(state, mutations)
  end

  @impl true
  def handle_call({:delete_batch, operations, _sync}, _from, state) do
    mutations = Enum.map(operations, fn {cf, key} -> {:delete, cf, key} end)
    reply_with_staged(state, mutations)
  end

  @impl true
  def handle_call({:mixed_batch, operations, _sync}, _from, state) do
    reply_with_staged(state, operations)
  end

  @impl true
  def handle_call({:fold, cf, prefix, acc, fun}, _from, state) do
    {:reply, fold_entries(state, cf, prefix, [], acc, fun, :entries), state}
  end

  @impl true
  def handle_call({:fold, cf, prefix, acc, fun, opts}, _from, state) do
    {:reply, fold_entries(state, cf, prefix, opts, acc, fun, :entries), state}
  end

  @impl true
  def handle_call({:fold_keys, cf, prefix, acc, fun}, _from, state) do
    {:reply, fold_entries(state, cf, prefix, [], acc, fun, :keys), state}
  end

  @impl true
  def handle_call({:fold_keys, cf, prefix, acc, fun, opts}, _from, state) do
    {:reply, fold_entries(state, cf, prefix, opts, acc, fun, :keys), state}
  end

  @impl true
  def handle_call({:prefix_iterator, cf, prefix}, _from, state) do
    start_iterator(state, cf, prefix, [])
  end

  @impl true
  def handle_call({:prefix_iterator, cf, prefix, opts}, _from, state) do
    start_iterator(state, cf, prefix, opts)
  end

  @impl true
  def handle_call({:iterator, cf, opts}, _from, state) do
    start_iterator(state, cf, Keyword.get(opts, :prefix, <<>>), opts)
  end

  @impl true
  def handle_call(:summary, _from, state) do
    {:reply, build_summary(state), state}
  end

  @impl true
  def handle_call(:commit, _from, state) do
    mutations = canonical_mutations(state.overlay)

    result =
      case mutations do
        [] -> :ok
        _ -> ErlangAdapter.mixed_batch(state.base_db, mutations, true)
      end

    reply =
      case result do
        :ok -> {:ok, build_summary(state)}
        {:error, reason} -> {:error, {:storage, reason}}
      end

    {:reply, reply, state}
  end

  defp reply_with_staged(state, mutations) do
    case validate_mutations(mutations, state.quad_store?) do
      :ok -> {:reply, :ok, Enum.reduce(mutations, state, &stage_mutation/2)}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  defp validate_mutations(mutations, quad_store?) when is_list(mutations) do
    allowed = if quad_store?, do: @quad_column_families, else: @triple_column_families

    case Enum.find(mutations, &(not valid_mutation?(&1, allowed))) do
      nil -> :ok
      mutation -> {:error, {:invalid_staged_mutation, mutation}}
    end
  end

  defp validate_mutations(other, _quad_store?), do: {:error, {:invalid_staged_mutation, other}}

  defp valid_mutation?({:put, cf, key, value}, allowed),
    do: cf in allowed and is_binary(key) and is_binary(value)

  defp valid_mutation?({:delete, cf, key}, allowed),
    do: cf in allowed and is_binary(key)

  defp valid_mutation?(_mutation, _allowed), do: false

  defp stage_mutation({:put, cf, key, value}, state) do
    state
    |> put_overlay(cf, key, {:put, value})
    |> track_graph(cf, key)
  end

  defp stage_mutation({:delete, cf, key}, state) do
    state
    |> put_overlay(cf, key, :delete)
    |> track_graph(cf, key)
  end

  defp put_overlay(state, cf, key, value) do
    %{
      state
      | overlay: Map.put(state.overlay, {cf, key}, value),
        column_families: MapSet.put(state.column_families, cf)
    }
  end

  defp track_graph(state, :gspo, <<graph_id::unsigned-big-integer-size(64), _::binary>>) do
    %{state | affected_graphs: MapSet.put(state.affected_graphs, graph_id)}
  end

  defp track_graph(state, _cf, _key), do: state

  defp fold_entries(state, cf, prefix, opts, acc, fun, mode) do
    state
    |> merged_entries(cf, prefix, opts)
    |> Enum.reduce(acc, fn
      {key, value}, inner_acc when mode == :entries -> fun.({key, value}, inner_acc)
      {key, _value}, inner_acc -> fun.(key, inner_acc)
    end)
  end

  defp start_iterator(state, cf, prefix, opts) do
    entries = merged_entries(state, cf, prefix, opts)

    case SessionIterator.start(self(), entries) do
      {:ok, iterator} -> {:reply, {:ok, iterator}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp merged_entries(state, cf, prefix, opts) do
    base_entries =
      ErlangAdapter.fold(state.base_db, cf, prefix, %{}, fn {key, value}, acc ->
        Map.put(acc, key, value)
      end)

    state.overlay
    |> Enum.reduce(base_entries, fn
      {{^cf, key}, {:put, value}}, acc ->
        if in_range?(key, prefix, opts), do: Map.put(acc, key, value), else: acc

      {{^cf, key}, :delete}, acc ->
        if in_range?(key, prefix, opts), do: Map.delete(acc, key), else: acc

      _, acc ->
        acc
    end)
    |> Enum.filter(fn {key, _value} -> in_range?(key, prefix, opts) end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp in_range?(key, prefix, opts) do
    String.starts_with?(key, prefix) and below_upper_bound?(key, opts)
  end

  defp below_upper_bound?(key, opts) do
    case Keyword.get(opts, :iterate_upper_bound) do
      upper when is_binary(upper) -> key < upper
      _ -> true
    end
  end

  defp canonical_mutations(overlay) do
    overlay
    |> Enum.map(fn
      {{cf, key}, {:put, value}} -> {:put, cf, key, value}
      {{cf, key}, :delete} -> {:delete, cf, key}
    end)
    |> Enum.sort()
  end

  defp build_summary(state) do
    %{
      mutation_count: map_size(state.overlay),
      column_families: state.column_families,
      affected_graphs: state.affected_graphs
    }
  end
end
