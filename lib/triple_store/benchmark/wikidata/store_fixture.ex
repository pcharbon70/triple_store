defmodule TripleStore.Benchmark.Wikidata.StoreFixture do
  @moduledoc """
  Helpers for loading registered benchmark fixtures into TripleStore stores.

  The helper keeps the setup path explicit:

  1. open a benchmark store
  2. load the registered dataset fixture
  3. optionally warm the store
  4. optionally compact the store
  5. return structured load metrics
  """

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Benchmark.Wikidata.DatasetManifest

  defstruct dataset_manifest: nil,
            fixture_root: nil,
            store: nil,
            store_path: nil,
            schema: :triple,
            load_metrics: nil

  @type load_metrics :: %{
          count: non_neg_integer(),
          elapsed_us: non_neg_integer(),
          throughput_tps: float(),
          warnings: [String.t()],
          warning_count: non_neg_integer(),
          failure_class: atom() | nil,
          compacted?: boolean(),
          warmed?: boolean()
        }

  @type t :: %__MODULE__{
          dataset_manifest: DatasetManifest.t(),
          fixture_root: String.t(),
          store: map() | nil,
          store_path: String.t(),
          schema: :triple | :quad,
          load_metrics: load_metrics() | nil
        }

  @doc """
  Returns named load presets for benchmark fixture ingestion.
  """
  @spec load_presets() :: %{atom() => keyword()}
  def load_presets do
    %{
      truthy_only: [
        format: :ntriples,
        batch_size: 10_000
      ],
      full_rdf: [
        format: :ntriples,
        batch_size: 5_000
      ]
    }
  end

  @doc """
  Returns a named load preset.
  """
  @spec load_preset(atom()) :: {:ok, keyword()} | {:error, :unknown_preset}
  def load_preset(name) when is_atom(name) do
    case Map.fetch(load_presets(), name) do
      {:ok, preset} -> {:ok, preset}
      :error -> {:error, :unknown_preset}
    end
  end

  @doc """
  Opens a fresh store, loads the dataset fixture, and returns structured metrics.
  """
  @spec setup(Path.t(), DatasetManifest.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def setup(fixture_root, %DatasetManifest{} = dataset_manifest, opts \\ [])
      when is_binary(fixture_root) and is_list(opts) do
    schema = Keyword.get(opts, :schema, :triple)
    store_id = Keyword.get(opts, :store_id, dataset_manifest.dataset_id)
    store_path = Path.join([fixture_root, "stores", store_id])
    load_opts = resolve_load_opts(opts, dataset_manifest)

    with true <- dataset_manifest.local_data_path != nil or {:error, :missing_local_data_path},
         :ok <- File.mkdir_p(Path.dirname(store_path)),
         {:ok, store} <- TripleStore.open(store_path, schema: schema),
         {:ok, load_result} <-
           load_fixture(
             store,
             dataset_manifest.local_data_path,
             dataset_manifest.format,
             load_opts
           ),
         {:ok, warmed?} <- maybe_warm(store, Keyword.get(opts, :warmup)),
         :ok <- maybe_compact(store, Keyword.get(opts, :compact, false)) do
      {:ok,
       %__MODULE__{
         dataset_manifest: dataset_manifest,
         fixture_root: fixture_root,
         store: store,
         store_path: store_path,
         schema: schema,
         load_metrics:
           Map.merge(load_result, %{
             compacted?: Keyword.get(opts, :compact, false),
             warmed?: warmed?
           })
       }}
    else
      {:error, _reason} = error ->
        error

      reason ->
        {:error, reason}
    end
  end

  @doc """
  Reopens a previously loaded store without reimporting the dataset fixture.
  """
  @spec reopen(t()) :: {:ok, t()} | {:error, term()}
  def reopen(%__MODULE__{store_path: store_path, schema: schema} = fixture_state) do
    with {:ok, store} <- TripleStore.open(store_path, schema: schema, create_if_missing: false) do
      {:ok, %{fixture_state | store: store}}
    end
  end

  @doc """
  Closes the open store and optionally removes its on-disk state.
  """
  @spec teardown(t(), keyword()) :: :ok | {:error, term()}
  def teardown(%__MODULE__{} = fixture_state, opts \\ []) do
    delete_store = Keyword.get(opts, :delete_store, false)

    close_result =
      case fixture_state.store do
        nil -> :ok
        store -> TripleStore.close(store)
      end

    case close_result do
      :ok ->
        if delete_store, do: File.rm_rf(fixture_state.store_path)
        :ok

      {:error, _} = error ->
        error
    end
  end

  defp load_fixture(store, data_path, format, load_opts) do
    {elapsed_us, result} =
      :timer.tc(fn ->
        TripleStore.load(store, data_path, Keyword.merge([format: format], load_opts))
      end)

    case result do
      {:ok, count} ->
        throughput_tps =
          if elapsed_us == 0 do
            0.0
          else
            count * 1_000_000 / elapsed_us
          end

        {:ok,
         %{
           count: count,
           elapsed_us: elapsed_us,
           throughput_tps: throughput_tps,
           warnings: [],
           warning_count: 0,
           failure_class: nil
         }}

      {:error, reason} ->
        {:error, {:load_failed, classify_failure(reason), reason}}
    end
  end

  defp maybe_warm(_store, nil), do: {:ok, false}

  defp maybe_warm(store, warmup_fun) when is_function(warmup_fun, 1) do
    case warmup_fun.(store) do
      :ok -> {:ok, true}
      {:ok, _} -> {:ok, true}
      other -> {:error, {:warmup_failed, other}}
    end
  end

  defp maybe_compact(_store, false), do: :ok

  defp maybe_compact(%{db: db}, true) do
    if function_exported?(ErlangAdapter, :compact, 1) do
      apply(ErlangAdapter, :compact, [db])
    else
      :ok
    end
  end

  defp classify_failure(:file_not_found), do: :file_not_found
  defp classify_failure({:parse_error, _}), do: :parse_error
  defp classify_failure(:database_closed), do: :database_closed
  defp classify_failure(_), do: :load_failed

  defp resolve_load_opts(opts, dataset_manifest) do
    explicit_load_opts = Keyword.get(opts, :load_opts, [])

    case Keyword.get(opts, :load_preset) do
      nil ->
        Keyword.put_new(explicit_load_opts, :format, dataset_manifest.format)

      preset_name ->
        preset =
          case load_preset(preset_name) do
            {:ok, preset} -> preset
            {:error, :unknown_preset} -> []
          end

        preset
        |> Keyword.merge(explicit_load_opts)
        |> Keyword.put_new(:format, dataset_manifest.format)
    end
  end
end
