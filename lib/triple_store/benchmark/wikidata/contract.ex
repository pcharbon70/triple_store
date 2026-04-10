defmodule TripleStore.Benchmark.Wikidata.Contract do
  @moduledoc """
  Canonical contract definitions for Wikidata-style benchmark workloads.

  This module centralizes the machine-readable vocabulary used by the
  benchmark foundation layer:

  - workload families
  - execution variants
  - required result artifacts
  - dataset tiers
  - initial success criteria
  - required runtime and hardware metadata
  """

  @type workload_family :: :wgpb | :wdbench | :wdqs | :scholia
  @type execution_variant :: :raw | :count_only | :distinct_only
  @type result_artifact ::
          :raw_timings | :adjusted_timings | :errors | :timeouts | :divergences | :metadata
  @type dataset_tier :: :smoke | :medium | :large | :full_dump

  @dataset_tiers %{
    smoke: %{
      id: :smoke,
      label: "Smoke",
      intended_use: [:ci, :local_debugging],
      recommended_max_triples: 100_000
    },
    medium: %{
      id: :medium,
      label: "Medium",
      intended_use: [:developer_workstation, :regression_validation],
      recommended_max_triples: 1_000_000
    },
    large: %{
      id: :large,
      label: "Large",
      intended_use: [:workstation_benchmarking, :pre_release_validation],
      recommended_max_triples: 50_000_000
    },
    full_dump: %{
      id: :full_dump,
      label: "Full Dump",
      intended_use: [:long_running_validation, :publication_grade_runs],
      recommended_max_triples: :full_dataset
    }
  }

  @required_run_metadata [
    :captured_at,
    :hostname,
    :elixir_version,
    :otp_release,
    :system_architecture,
    :word_size,
    :logical_processors,
    :logical_processors_available,
    :schedulers,
    :schedulers_online,
    :dirty_cpu_schedulers,
    :dirty_io_schedulers
  ]

  @doc """
  Returns the supported workload families.
  """
  @spec workload_families() :: [workload_family()]
  def workload_families, do: [:wgpb, :wdbench, :wdqs, :scholia]

  @doc """
  Returns the supported execution variants.
  """
  @spec execution_variants() :: [execution_variant()]
  def execution_variants, do: [:raw, :count_only, :distinct_only]

  @doc """
  Returns the required result artifact types for benchmark runs.
  """
  @spec required_result_artifacts() :: [result_artifact()]
  def required_result_artifacts do
    [:raw_timings, :adjusted_timings, :errors, :timeouts, :divergences, :metadata]
  end

  @doc """
  Returns all supported dataset tiers.
  """
  @spec dataset_tiers() :: [map()]
  def dataset_tiers do
    @dataset_tiers
    |> Map.values()
    |> Enum.sort_by(&Enum.find_index(Map.keys(@dataset_tiers), fn id -> id == &1.id end))
  end

  @doc """
  Returns metadata for a dataset tier.
  """
  @spec dataset_tier(dataset_tier()) :: {:ok, map()} | {:error, :unknown_tier}
  def dataset_tier(tier) when is_atom(tier) do
    case Map.fetch(@dataset_tiers, tier) do
      {:ok, metadata} -> {:ok, metadata}
      :error -> {:error, :unknown_tier}
    end
  end

  @doc """
  Returns the initial benchmark success criteria.
  """
  @spec success_criteria() :: map()
  def success_criteria do
    %{
      load_completion: %{
        required: true,
        description: "The benchmark dataset must load successfully into TripleStore."
      },
      query_completion_rate: %{
        required: true,
        metric: :completion_rate,
        description: "Benchmark runs must record the proportion of queries that complete."
      },
      report_generation: %{
        required: true,
        artifacts: required_result_artifacts(),
        description:
          "Benchmark runs must emit the required machine-readable and summary artifacts."
      }
    }
  end

  @doc """
  Returns the runtime and hardware metadata fields required for each benchmark run.
  """
  @spec required_run_metadata_fields() :: [atom()]
  def required_run_metadata_fields, do: @required_run_metadata

  @doc """
  Captures the currently available runtime and hardware metadata for a benchmark run.

  The returned map always includes the required fields defined by
  `required_run_metadata_fields/0`.
  """
  @spec capture_runtime_metadata() :: map()
  def capture_runtime_metadata do
    %{
      captured_at: DateTime.utc_now(),
      hostname: hostname(),
      elixir_version: System.version(),
      otp_release: List.to_string(:erlang.system_info(:otp_release)),
      system_architecture: List.to_string(:erlang.system_info(:system_architecture)),
      word_size: :erlang.system_info(:wordsize),
      logical_processors: system_info_or_nil(:logical_processors),
      logical_processors_available: system_info_or_nil(:logical_processors_available),
      schedulers: :erlang.system_info(:schedulers),
      schedulers_online: :erlang.system_info(:schedulers_online),
      dirty_cpu_schedulers: :erlang.system_info(:dirty_cpu_schedulers),
      dirty_io_schedulers: :erlang.system_info(:dirty_io_schedulers)
    }
  end

  @doc """
  Returns true when the workload family is part of the benchmark contract.
  """
  @spec workload_family?(term()) :: boolean()
  def workload_family?(family), do: family in workload_families()

  @doc """
  Returns true when the execution variant is part of the benchmark contract.
  """
  @spec execution_variant?(term()) :: boolean()
  def execution_variant?(variant), do: variant in execution_variants()

  @doc """
  Returns true when the dataset tier is part of the benchmark contract.
  """
  @spec dataset_tier?(term()) :: boolean()
  def dataset_tier?(tier), do: Map.has_key?(@dataset_tiers, tier)

  @doc """
  Returns true when the result artifact type is part of the benchmark contract.
  """
  @spec result_artifact?(term()) :: boolean()
  def result_artifact?(artifact), do: artifact in required_result_artifacts()

  defp hostname do
    {:ok, hostname} = :inet.gethostname()
    List.to_string(hostname)
  end

  defp system_info_or_nil(key) do
    case :erlang.system_info(key) do
      :unknown -> nil
      value -> value
    end
  end
end
