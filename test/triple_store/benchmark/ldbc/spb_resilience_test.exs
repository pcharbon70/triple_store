defmodule TripleStore.Benchmark.LDBC.SPB.ResilienceTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.LDBC.{SPB.Pipeline, StoreFixture}
  alias TripleStore.Benchmark.LDBC.SPB.{Resilience, Semantics}

  test "coordinated backup restores manifests, graphs, derived facts, and accepted answers",
       %{test: test} do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} = Pipeline.generate_smoke(Path.join(root, "generated"), seed: 411)
    assert {:ok, fixture} = StoreFixture.setup(root, manifest)
    on_exit(fn -> StoreFixture.teardown(fixture, delete: true) end)
    assert {:ok, stats} = Semantics.materialize(fixture.store)
    assert stats.total_derived > 0

    backup_path = Path.join(root, "backup")
    restore_path = Path.join(root, "restored")

    assert {:ok, report} =
             Resilience.run_backup_restore(fixture.store, backup_path, restore_path,
               manifest: manifest
             )

    assert report.profile == :coordinated_backup_restore
    assert report.manifest.dataset_id == manifest.dataset_id
    assert report.state.equivalent?
    assert report.state.before.derived_count > 0
    assert report.backup.artifact_size_bytes > 0
    assert report.backup.duration_us >= 0
    assert report.restore.duration_us >= 0
    assert report.workload_impact.writes == :quiesced
    assert report.lifecycle.restored_store == :stopped
    refute report.disclosure.backup_is_replication?
    refute report.disclosure.availability_score_eligible?

    assert {:ok, reopened} =
             TripleStore.open(restore_path, schema: :quad, create_if_missing: false)

    assert :ok = TripleStore.close(reopened)
  end

  test "replication and failover profiles fail before creating measurement artifacts",
       %{test: test} do
    root = tmp_dir(test)
    backup_path = Path.join(root, "backup")
    restore_path = Path.join(root, "restored")
    store = %{db: :unused, dict_manager: :unused}

    for profile <- [:online_replication, :failover] do
      assert {:error,
              %{
                stage: :profile_gate,
                reason: {:unsupported_spb_profile, ^profile, :no_score_output}
              }} =
               Resilience.run_backup_restore(store, backup_path, restore_path, profile: profile)
    end

    refute File.exists?(backup_path)
    refute File.exists?(restore_path)
  end

  test "pinned resilience actions keep backup distinct from replication" do
    requirements = Resilience.requirements()
    disclosure = Resilience.disclosure()

    assert requirements.source_commit == "ce6323c0936306729408233dc70d26f2389b34c6"
    assert requirements.availability == :audit_only
    assert Enum.find(requirements.actions, &(&1.id == "full_backup_start")).support == :supported

    assert Enum.find(
             requirements.actions,
             &(&1.id == "benchmarkOnlineReplicationAndBackup")
           ).support == :unsupported

    refute disclosure.backup_is_replication?
    assert disclosure.unsupported_profiles == [:online_replication, :failover]
  end

  defp tmp_dir(test) do
    Path.join(System.tmp_dir!(), "spb_resilience_#{test}_#{System.unique_integer([:positive])}")
  end
end
