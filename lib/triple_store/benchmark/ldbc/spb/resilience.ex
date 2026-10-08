defmodule TripleStore.Benchmark.LDBC.SPB.Resilience do
  @moduledoc """
  Honest SPB backup/recovery execution and availability capability gates.

  The supported profile is a coordinated full backup followed by restoration
  into a fresh path. Online replication and failover are rejected before any
  measurement because TripleStore is an embedded store without a replicated
  deployment surface.
  """

  alias TripleStore.Backup
  alias TripleStore.Benchmark.LDBC.DatasetManifest
  alias TripleStore.Benchmark.LDBC.SPB.{Aggregation, Semantics}
  alias TripleStore.Reasoner.DerivedStore
  alias TripleStore.SPARQL.Query

  @source_commit "ce6323c0936306729408233dc70d26f2389b34c6"
  @unsupported_profiles [:online_replication, :failover]

  @doc "Returns the pinned SPB resilience actions and their local disposition."
  @spec requirements() :: map()
  def requirements do
    %{
      source_commit: @source_commit,
      source_path: "datasets_and_queries/scripts/enterprise",
      availability: :audit_only,
      vendor_implementation_required?: true,
      actions: [
        %{id: "benchmarkOnlineReplicationAndBackup", support: :unsupported},
        %{id: "full_backup_start", support: :supported},
        %{id: "full_backup_restore", support: :supported},
        %{id: "system_shutdown", support: :unsupported},
        %{id: "system_start", support: :unsupported}
      ]
    }
  end

  @doc "Describes supported profiles and the score-output policy."
  @spec disclosure() :: map()
  def disclosure do
    %{
      supported_profiles: [:coordinated_backup_restore],
      unsupported_profiles: @unsupported_profiles,
      backup_is_replication?: false,
      availability_score_eligible?: false,
      backup_metrics_eligible?: true,
      coordination: :quiesced_writes,
      limitations: [
        "writes must be quiesced by the benchmark harness during filesystem backup",
        "read and write availability during backup is not scored",
        "restoration opens a fresh local embedded store",
        "no replica promotion or failover consistency claim is made"
      ]
    }
  end

  @doc "Rejects unsupported availability profiles before measurement starts."
  @spec authorize_profile(atom()) :: :ok | {:error, term()}
  def authorize_profile(:coordinated_backup_restore), do: :ok

  def authorize_profile(profile) when profile in @unsupported_profiles,
    do: {:error, {:unsupported_spb_profile, profile, :no_score_output}}

  def authorize_profile(profile), do: {:error, {:unknown_spb_profile, profile}}

  @doc "Runs a coordinated backup/restore and proves state equivalence."
  @spec run_backup_restore(TripleStore.store(), Path.t(), Path.t(), keyword()) ::
          {:ok, map()} | {:error, map()}
  def run_backup_restore(store, backup_path, restore_path, opts \\ []) do
    profile = Keyword.get(opts, :profile, :coordinated_backup_restore)
    probe = Keyword.get(opts, :probe, &default_probe/1)
    manifest = Keyword.get(opts, :manifest)

    with :ok <- authorize_profile(profile),
         :ok <- validate_probe(probe),
         {:ok, manifest_identity} <- validate_manifest(manifest),
         {:ok, before} <- capture_state(store, probe),
         {:ok, backup, backup_us} <- timed(fn -> Backup.create(store, backup_path) end),
         {:ok, :valid} <- Backup.verify_quad_backup(backup_path),
         {:ok, restored, restore_us} <- timed(fn -> Backup.restore(backup_path, restore_path) end) do
      validate_and_close(
        restored,
        before,
        probe,
        backup,
        backup_us,
        restore_us,
        manifest_identity
      )
    else
      {:error, %{stage: _stage} = error} -> {:error, error}
      {:error, reason} -> {:error, %{stage: stage(reason), reason: reason}}
    end
  end

  defp validate_and_close(
         restored,
         before,
         probe,
         backup,
         backup_us,
         restore_us,
         manifest_identity
       ) do
    result =
      with {:ok, after_state} <- capture_state(restored, probe),
           :ok <- compare_state(before, after_state) do
        {:ok,
         %{
           profile: :coordinated_backup_restore,
           manifest: manifest_identity,
           backup: %{
             duration_us: backup_us,
             artifact_size_bytes: backup.size_bytes,
             file_count: backup.file_count,
             errors: []
           },
           restore: %{duration_us: restore_us, errors: []},
           state: %{before: before, after: after_state, equivalent?: true},
           workload_impact: %{
             mode: :coordinated,
             writes: :quiesced,
             concurrent_operations_measured: 0
           },
           lifecycle: %{restored_store: :stopped, scheduled_helpers: :not_started},
           disclosure: disclosure()
         }}
      else
        {:error, reason} -> {:error, %{stage: :restored_state_validation, reason: reason}}
      end

    close_result = TripleStore.close(restored)

    case {result, close_result} do
      {{:ok, report}, :ok} -> {:ok, report}
      {{:error, error}, :ok} -> {:error, error}
      {_, {:error, reason}} -> {:error, %{stage: :restored_store_close, reason: reason}}
    end
  end

  defp capture_state(store, probe) do
    with {:ok, contexts} <- Semantics.verify_contexts(store),
         {:ok, derived_count} <- DerivedStore.count(store.db),
         {:ok, answer} <- invoke_probe(probe, store) do
      {:ok,
       %{
         graph_contexts: contexts.graphs,
         explicit_digest: nquads_digest(contexts.nquads),
         derived_count: derived_count,
         accepted_answer: canonical(answer)
       }}
    end
  end

  defp compare_state(before, after_state) do
    if before == after_state,
      do: :ok,
      else: {:error, {:restored_state_mismatch, before, after_state}}
  end

  defp invoke_probe(probe, store) do
    case probe.(store) do
      {:ok, answer} -> {:ok, answer}
      {:error, reason} -> {:error, {:accepted_answer_probe_failed, reason}}
      other -> {:error, {:invalid_probe_response, other}}
    end
  rescue
    error -> {:error, {:probe_exception, error.__struct__, Exception.message(error)}}
  end

  defp default_probe(store) do
    Query.query(
      Semantics.execution_context(store),
      "SELECT ?work WHERE { ?work <http://schema.org/about> ?entity }"
    )
  end

  defp validate_probe(probe) when is_function(probe, 1), do: :ok
  defp validate_probe(_probe), do: {:error, :invalid_probe}

  defp validate_manifest(nil), do: {:ok, :not_supplied}

  defp validate_manifest(%DatasetManifest{suite: :spb} = manifest) do
    with :ok <- DatasetManifest.validate(manifest), do: {:ok, DatasetManifest.identity(manifest)}
  end

  defp validate_manifest(%DatasetManifest{}), do: {:error, :not_spb_manifest}
  defp validate_manifest(_manifest), do: {:error, :invalid_manifest}

  defp timed(fun) do
    started = System.monotonic_time()

    case fun.() do
      {:ok, value} ->
        duration = System.monotonic_time() - started
        {:ok, value, System.convert_time_unit(duration, :native, :microsecond)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp canonical(answer), do: Aggregation.canonical_answer(answer, :unordered)

  defp nquads_digest(nquads) do
    nquads
    |> String.split("\n", trim: true)
    |> Enum.sort()
    |> Enum.join("\n")
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp stage({:unsupported_spb_profile, _profile, :no_score_output}), do: :profile_gate
  defp stage(:invalid_probe), do: :configuration
  defp stage(:invalid_manifest), do: :manifest_validation
  defp stage(:not_spb_manifest), do: :manifest_validation
  defp stage(_reason), do: :backup_or_restore
end
