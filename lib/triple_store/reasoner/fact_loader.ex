defmodule TripleStore.Reasoner.FactLoader do
  @moduledoc """
  Loads explicit triple facts for local in-memory materialization.

  The loader owns the SPO iterator from creation through close and converts
  iterator setup, scan, storage-process, and persisted-key decoding failures
  into tagged errors. It never returns a partial fact set.
  """

  alias TripleStore.Backend.RocksDB.ErlangAdapter

  @typedoc "A dictionary-encoded explicit triple"
  @type fact :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @typedoc "Tagged fact-loading failure"
  @type load_error ::
          {:fact_iterator_failed, term()}
          | {:fact_scan_failed, term()}
          | {:fact_decode_failed, term()}
          | {:fact_storage_failed, term()}

  @doc """
  Loads all explicit triples from the SPO index.

  The optional `:iterator_opts` value is forwarded to the storage adapter. It
  is intended for adapter-level tuning and deterministic failure tests.
  """
  @spec load_facts_from_db(TripleStore.db_ref(), keyword()) ::
          {:ok, MapSet.t(fact())} | {:error, load_error()}
  def load_facts_from_db(db, opts \\ []) do
    iterator_opts = Keyword.get(opts, :iterator_opts, [])

    case open_iterator(db, iterator_opts) do
      {:ok, iterator} -> collect_owned_iterator(iterator)
      {:error, {:storage_exit, reason}} -> {:error, {:fact_storage_failed, reason}}
      {:error, reason} -> {:error, {:fact_iterator_failed, reason}}
    end
  end

  defp open_iterator(db, iterator_opts) do
    ErlangAdapter.prefix_iterator(db, :spo, <<>>, iterator_opts)
  catch
    :exit, reason -> {:error, {:storage_exit, reason}}
  end

  defp collect_owned_iterator(iterator) do
    try do
      collect_facts(iterator, MapSet.new())
    rescue
      exception ->
        {:error, {:fact_storage_failed, {exception.__struct__, Exception.message(exception)}}}
    catch
      :exit, reason -> {:error, {:fact_storage_failed, reason}}
    after
      close_iterator(iterator)
    end
  end

  defp collect_facts(iterator, facts) do
    case ErlangAdapter.iterator_next(iterator) do
      {:ok, key, _value} ->
        case decode_spo_key(key) do
          {:ok, fact} -> collect_facts(iterator, MapSet.put(facts, fact))
          {:error, reason} -> {:error, {:fact_decode_failed, reason}}
        end

      :iterator_end ->
        {:ok, facts}

      {:error, reason} ->
        {:error, {:fact_scan_failed, reason}}

      other ->
        {:error, {:fact_scan_failed, {:invalid_iterator_result, other}}}
    end
  end

  defp decode_spo_key(<<subject::64-big, predicate::64-big, object::64-big>>) do
    {:ok, {subject, predicate, object}}
  end

  defp decode_spo_key(key) when is_binary(key) do
    {:error, {:invalid_spo_key_size, byte_size(key)}}
  end

  defp decode_spo_key(_key), do: {:error, :invalid_spo_key}

  defp close_iterator(iterator) do
    ErlangAdapter.iterator_close(iterator)
  catch
    :exit, _reason -> :ok
  end
end
