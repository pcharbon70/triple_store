defmodule TripleStore.Benchmark.Wikidata.SmokeFixture do
  @moduledoc """
  Built-in smoke fixture and baseline asset helpers for the Wikidata benchmark.

  The smoke tier is designed to work from a clean checkout with no external
  data dependencies.
  """

  alias TripleStore.Benchmark.Wikidata.{DatasetManifest, Fixture}

  @dataset_id "wikidata-built-in-smoke"
  @source_url "repo://priv/benchmarks/wikidata/fixtures/smoke.nt"
  @dump_version "2024-10-smoke"

  @doc """
  Returns the built-in smoke fixture path.
  """
  @spec source_path() :: String.t()
  def source_path do
    Application.app_dir(:triple_store, "priv/benchmarks/wikidata/fixtures/smoke.nt")
  end

  @doc """
  Returns the default answer-baseline JSON path for the smoke fixture.
  """
  @spec answer_baseline_path() :: String.t()
  def answer_baseline_path do
    Application.app_dir(
      :triple_store,
      "priv/benchmarks/wikidata/baselines/smoke/reference_answers.json"
    )
  end

  @doc """
  Returns the default accepted-divergence JSON path for the smoke fixture.
  """
  @spec accepted_divergences_path() :: String.t()
  def accepted_divergences_path do
    Application.app_dir(
      :triple_store,
      "priv/benchmarks/wikidata/baselines/smoke/accepted_divergences.json"
    )
  end

  @doc """
  Returns the directory containing accepted smoke report artifacts.
  """
  @spec accepted_report_dir() :: String.t()
  def accepted_report_dir do
    Application.app_dir(:triple_store, "priv/benchmarks/wikidata/baselines/smoke/accepted_report")
  end

  @doc """
  Builds the dataset manifest for the built-in smoke fixture.
  """
  @spec manifest() :: {:ok, DatasetManifest.t()} | {:error, term()}
  def manifest do
    DatasetManifest.from_source(
      source_path(),
      dataset_id: @dataset_id,
      tier: :smoke,
      source_url: @source_url,
      dump_version: @dump_version,
      source_date: ~D[2024-10-01],
      normalization_flags: [:truthy_only, :repo_smoke_fixture]
    )
  end

  @doc """
  Registers the built-in smoke fixture in a fixture root.
  """
  @spec register(Path.t()) :: {:ok, DatasetManifest.t()} | {:error, term()}
  def register(fixture_root) when is_binary(fixture_root) do
    with {:ok, manifest} <- manifest() do
      Fixture.register_dataset(fixture_root, manifest, source_path())
    end
  end
end
