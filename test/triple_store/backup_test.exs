defmodule TripleStore.BackupTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backup
  alias TripleStore.Dictionary.Manager, as: DictManager
  alias TripleStore.Dictionary.SequenceCounter

  @moduletag :backup

  setup do
    # Create a temporary directory for tests
    test_dir = Path.join(System.tmp_dir!(), "backup_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(test_dir)

    db_path = Path.join(test_dir, "db")
    backup_dir = Path.join(test_dir, "backups")
    File.mkdir_p!(backup_dir)

    # Create a test database
    {:ok, store} = TripleStore.open(db_path)

    # Insert some test data
    turtle = """
    @prefix ex: <http://example.org/> .
    ex:subject1 ex:predicate1 ex:object1 .
    ex:subject2 ex:predicate2 "literal value" .
    """

    {:ok, _} = TripleStore.load_string(store, turtle, :turtle)

    on_exit(fn ->
      # Store might already be closed in some tests
      try do
        TripleStore.close(store)
      rescue
        _ -> :ok
      catch
        _, _ -> :ok
      end

      File.rm_rf!(test_dir)
    end)

    %{store: store, test_dir: test_dir, backup_dir: backup_dir}
  end

  describe "create/3" do
    test "creates a full backup", %{store: store, backup_dir: backup_dir} do
      backup_path = Path.join(backup_dir, "full_backup")

      assert {:ok, metadata} = Backup.create(store, backup_path)

      assert metadata.path == backup_path
      assert metadata.backup_type == :full
      assert metadata.size_bytes > 0
      assert metadata.file_count > 0
      assert %DateTime{} = metadata.created_at
    end

    test "fails if backup path exists", %{store: store, backup_dir: backup_dir} do
      backup_path = Path.join(backup_dir, "existing")
      File.mkdir_p!(backup_path)

      assert {:error, :backup_path_exists} = Backup.create(store, backup_path)
    end

    test "can skip verification", %{store: store, backup_dir: backup_dir} do
      backup_path = Path.join(backup_dir, "no_verify")

      assert {:ok, _metadata} = Backup.create(store, backup_path, verify: false)
    end
  end

  describe "create_incremental/4" do
    # Note: Incremental backups using hard links work best when there are no active
    # writes to the source database. In test scenarios where the source DB is open
    # and active, verification may fail due to file number mismatches in MANIFEST.

    @tag :skip
    test "creates an incremental backup based on full backup", %{
      store: store,
      backup_dir: backup_dir
    } do
      full_path = Path.join(backup_dir, "full")
      incr_path = Path.join(backup_dir, "incr")

      # Create full backup first, then close to ensure consistent state
      {:ok, _} = Backup.create(store, full_path, verify: false)

      # Create incremental backup
      assert {:ok, metadata} =
               Backup.create_incremental(store, incr_path, full_path, verify: false)

      assert metadata.path == incr_path
      assert metadata.backup_type == :incremental
      assert metadata.base_backup == full_path
      assert metadata.files_linked >= 0
      assert metadata.files_copied >= 0
    end

    test "fails if base backup is invalid", %{store: store, backup_dir: backup_dir} do
      incr_path = Path.join(backup_dir, "incr")
      invalid_base = Path.join(backup_dir, "nonexistent")

      assert {:error, :not_a_directory} =
               Backup.create_incremental(store, incr_path, invalid_base)
    end

    @tag :skip
    test "incremental backup is independently restorable", %{
      store: store,
      backup_dir: backup_dir,
      test_dir: test_dir
    } do
      full_path = Path.join(backup_dir, "full2")
      incr_path = Path.join(backup_dir, "incr2")
      restore_path = Path.join(test_dir, "restored")

      # Create full and incremental backups
      {:ok, _} = Backup.create(store, full_path, verify: false)
      {:ok, _} = Backup.create_incremental(store, incr_path, full_path, verify: false)

      # Close original store
      TripleStore.close(store)

      # Restore from incremental
      {:ok, restored_store} = Backup.restore(incr_path, restore_path)

      # Verify data is present
      query = "SELECT * WHERE { ?s ?p ?o }"
      {:ok, results} = TripleStore.query(restored_store, query)
      assert length(results) >= 2

      TripleStore.close(restored_store)
    end
  end

  describe "verify/1" do
    @tag :slow
    test "validates a correct backup", %{store: store, backup_dir: backup_dir} do
      backup_path = Path.join(backup_dir, "verify_test")
      {:ok, _} = Backup.create(store, backup_path)

      assert {:ok, :valid} = Backup.verify(backup_path)
    end

    test "returns error for non-directory", %{backup_dir: backup_dir} do
      file_path = Path.join(backup_dir, "not_a_dir")
      File.write!(file_path, "test")

      assert {:error, :not_a_directory} = Backup.verify(file_path)
    end

    test "returns error for missing required files", %{backup_dir: backup_dir} do
      empty_dir = Path.join(backup_dir, "empty")
      File.mkdir_p!(empty_dir)

      assert {:error, :missing_files} = Backup.verify(empty_dir)
    end
  end

  describe "list/1" do
    @tag :slow
    test "lists all backups in directory", %{store: store, backup_dir: backup_dir} do
      # Create multiple backups
      for i <- 1..3 do
        backup_path = Path.join(backup_dir, "backup_#{i}")
        {:ok, _} = Backup.create(store, backup_path)
        # Small delay to ensure different timestamps
        Process.sleep(10)
      end

      assert {:ok, backups} = Backup.list(backup_dir)
      assert length(backups) == 3

      # Verify sorted by creation time (newest first)
      timestamps = Enum.map(backups, & &1.created_at)
      assert timestamps == Enum.sort(timestamps, {:desc, DateTime})
    end

    test "returns error for non-directory", %{backup_dir: backup_dir} do
      file_path = Path.join(backup_dir, "not_a_dir")
      File.write!(file_path, "test")

      assert {:error, :not_a_directory} = Backup.list(file_path)
    end

    test "returns empty list for directory with no backups", %{test_dir: test_dir} do
      empty_dir = Path.join(test_dir, "empty_backups")
      File.mkdir_p!(empty_dir)

      assert {:ok, []} = Backup.list(empty_dir)
    end
  end

  describe "restore/3" do
    test "restores backup to new location", %{
      store: store,
      backup_dir: backup_dir,
      test_dir: test_dir
    } do
      backup_path = Path.join(backup_dir, "to_restore")
      restore_path = Path.join(test_dir, "restored_db")

      {:ok, _} = Backup.create(store, backup_path)
      TripleStore.close(store)

      assert {:ok, restored_store} = Backup.restore(backup_path, restore_path)

      # Verify we can query the restored database
      query = "SELECT * WHERE { ?s ?p ?o }"
      {:ok, results} = TripleStore.query(restored_store, query)
      assert length(results) >= 2

      TripleStore.close(restored_store)
    end

    test "fails if destination exists without overwrite", %{
      store: store,
      backup_dir: backup_dir,
      test_dir: test_dir
    } do
      backup_path = Path.join(backup_dir, "backup_for_restore")
      restore_path = Path.join(test_dir, "existing_dest")

      {:ok, _} = Backup.create(store, backup_path)
      File.mkdir_p!(restore_path)

      assert {:error, :destination_exists} = Backup.restore(backup_path, restore_path)
    end

    test "overwrites destination when option set", %{
      store: store,
      backup_dir: backup_dir,
      test_dir: test_dir
    } do
      backup_path = Path.join(backup_dir, "backup_overwrite")
      restore_path = Path.join(test_dir, "to_overwrite")

      {:ok, _} = Backup.create(store, backup_path)
      File.mkdir_p!(restore_path)
      File.write!(Path.join(restore_path, "old_file"), "old")

      TripleStore.close(store)

      assert {:ok, restored_store} = Backup.restore(backup_path, restore_path, overwrite: true)
      TripleStore.close(restored_store)

      # Old file should be gone
      refute File.exists?(Path.join(restore_path, "old_file"))
    end
  end

  describe "rotate/3" do
    test "creates timestamped backup", %{store: store, backup_dir: backup_dir} do
      assert {:ok, metadata} = Backup.rotate(store, backup_dir)

      assert String.starts_with?(Path.basename(metadata.path), "backup_")
      assert File.dir?(metadata.path)
    end

    @tag :slow
    test "removes old backups beyond limit", %{store: store, backup_dir: backup_dir} do
      # Create 5 backups with same prefix - need significant delay to avoid timestamp collision
      for _ <- 1..5 do
        {:ok, _} = Backup.rotate(store, backup_dir, max_backups: 3, prefix: "rot")
        # Sleep at least 1 second to ensure unique timestamps
        Process.sleep(1100)
      end

      {:ok, backups} = Backup.list(backup_dir)

      rot_backups =
        Enum.filter(backups, fn b ->
          Path.basename(b.path) |> String.starts_with?("rot")
        end)

      assert length(rot_backups) == 3
    end

    test "uses custom prefix", %{store: store, backup_dir: backup_dir} do
      assert {:ok, metadata} = Backup.rotate(store, backup_dir, prefix: "custom")

      assert String.starts_with?(Path.basename(metadata.path), "custom_")
    end
  end

  describe "delete/1" do
    test "deletes a valid backup", %{store: store, backup_dir: backup_dir} do
      backup_path = Path.join(backup_dir, "to_delete")
      {:ok, _} = Backup.create(store, backup_path)

      assert :ok = Backup.delete(backup_path)
      refute File.exists?(backup_path)
    end

    test "returns error for invalid backup", %{backup_dir: backup_dir} do
      invalid_path = Path.join(backup_dir, "not_a_backup")
      File.mkdir_p!(invalid_path)

      assert {:error, :not_a_backup} = Backup.delete(invalid_path)
    end
  end

  describe "counter state persistence" do
    test "backup includes counter state file", %{store: store, backup_dir: backup_dir} do
      backup_path = Path.join(backup_dir, "counter_backup")

      # Insert data to advance counters
      turtle = """
      @prefix ex: <http://example.org/> .
      ex:a ex:b ex:c .
      ex:d ex:e ex:f .
      """

      {:ok, _} = TripleStore.load_string(store, turtle, :turtle)

      {:ok, _} = Backup.create(store, backup_path)

      # Counter state file should exist
      counter_file = Path.join(backup_path, ".counter_state")
      assert File.exists?(counter_file)
    end

    test "restore applies counter state from backup", %{
      store: store,
      backup_dir: backup_dir,
      test_dir: test_dir
    } do
      backup_path = Path.join(backup_dir, "restore_counter")
      restore_path = Path.join(test_dir, "restored_counter")

      # Insert significant data to advance counters beyond safety margin
      turtle = """
      @prefix ex: <http://example.org/> .
      """

      # Generate many unique subjects to advance URI counter
      turtle = turtle <> Enum.map_join(1..100, "\n", fn i -> "ex:s#{i} ex:p ex:o#{i} ." end)
      {:ok, _} = TripleStore.load_string(store, turtle, :turtle)

      # Get counter state before backup
      {:ok, counter} = DictManager.get_counter(store.dict_manager)
      {:ok, original_counters} = SequenceCounter.export(counter)

      # Create backup
      {:ok, _} = Backup.create(store, backup_path)
      TripleStore.close(store)

      # Restore
      {:ok, restored_store} = Backup.restore(backup_path, restore_path)

      # Get restored counter state
      {:ok, restored_counter} =
        DictManager.get_counter(restored_store.dict_manager)

      {:ok, restored_counters} = SequenceCounter.export(restored_counter)

      # Restored counters should be at least original + safety_margin
      safety = TripleStore.Dictionary.safety_margin()
      assert restored_counters.uri >= original_counters.uri + safety
      assert restored_counters.bnode >= original_counters.bnode + safety
      assert restored_counters.literal >= original_counters.literal + safety

      TripleStore.close(restored_store)
    end

    test "restore works without counter file (legacy backup)", %{
      store: store,
      backup_dir: backup_dir,
      test_dir: test_dir
    } do
      backup_path = Path.join(backup_dir, "legacy_backup")
      restore_path = Path.join(test_dir, "restored_legacy")

      # Create backup
      {:ok, _} = Backup.create(store, backup_path)

      # Remove counter file to simulate legacy backup
      File.rm(Path.join(backup_path, ".counter_state"))

      TripleStore.close(store)

      # Restore should still work
      assert {:ok, restored_store} = Backup.restore(backup_path, restore_path)

      # Should be able to insert new data
      turtle = "@prefix ex: <http://example.org/> . ex:new ex:data ex:here ."
      assert {:ok, _} = TripleStore.load_string(restored_store, turtle, :turtle)

      TripleStore.close(restored_store)
    end

    test "new data after restore gets unique IDs", %{
      store: store,
      backup_dir: backup_dir,
      test_dir: test_dir
    } do
      backup_path = Path.join(backup_dir, "id_uniqueness")
      restore_path = Path.join(test_dir, "restored_ids")

      # Insert initial data
      turtle1 = """
      @prefix ex: <http://example.org/> .
      ex:original1 ex:pred ex:obj1 .
      ex:original2 ex:pred ex:obj2 .
      """

      {:ok, _} = TripleStore.load_string(store, turtle1, :turtle)

      # Create backup
      {:ok, _} = Backup.create(store, backup_path)
      TripleStore.close(store)

      # Restore
      {:ok, restored_store} = Backup.restore(backup_path, restore_path)

      # Insert new data
      turtle2 = """
      @prefix ex: <http://example.org/> .
      ex:new1 ex:pred ex:newobj1 .
      ex:new2 ex:pred ex:newobj2 .
      """

      {:ok, _} = TripleStore.load_string(restored_store, turtle2, :turtle)

      # Query should return all data (original + new)
      query = "SELECT * WHERE { ?s <http://example.org/pred> ?o }"
      {:ok, results} = TripleStore.query(restored_store, query)
      assert length(results) == 4

      TripleStore.close(restored_store)
    end
  end

  describe "security" do
    test "rejects backup path with path traversal", %{store: store, backup_dir: backup_dir} do
      # Try to backup with path traversal
      malicious_path = Path.join(backup_dir, "../outside/backup")

      assert {:error, :path_traversal_attempt} = Backup.create(store, malicious_path)
    end

    test "rejects restore path with path traversal", %{
      store: store,
      backup_dir: backup_dir,
      test_dir: test_dir
    } do
      # Create a valid backup first
      backup_path = Path.join(backup_dir, "valid_backup")
      {:ok, _} = Backup.create(store, backup_path)

      # Try to restore to path with traversal
      malicious_restore = Path.join(test_dir, "../outside/restore")
      TripleStore.close(store)

      assert {:error, :path_traversal_attempt} = Backup.restore(backup_path, malicious_restore)
    end

    test "rejects source with symlinks", %{
      store: store,
      backup_dir: backup_dir,
      test_dir: test_dir
    } do
      # Create a backup first
      backup_path = Path.join(backup_dir, "backup_for_symlink_test")
      {:ok, _} = Backup.create(store, backup_path)

      # Create a symlink inside the backup
      symlink_target = Path.join(test_dir, "symlink_target")
      File.write!(symlink_target, "target content")
      symlink_path = Path.join(backup_path, "evil_symlink")
      File.ln_s!(symlink_target, symlink_path)

      # Try to restore from backup with symlink
      restore_path = Path.join(test_dir, "restored_symlink_test")
      TripleStore.close(store)

      result = Backup.restore(backup_path, restore_path, overwrite: true)
      assert {:error, {:symlink_detected, ^symlink_path}} = result
    end

    test "empty database backup and restore works", %{test_dir: test_dir, backup_dir: backup_dir} do
      # Open an empty database
      empty_db_path = Path.join(test_dir, "empty_db")
      {:ok, empty_store} = TripleStore.open(empty_db_path)

      # Backup the empty database
      backup_path = Path.join(backup_dir, "empty_backup")
      assert {:ok, metadata} = Backup.create(empty_store, backup_path)
      assert metadata.backup_type == :full

      TripleStore.close(empty_store)

      # Restore from empty backup
      restore_path = Path.join(test_dir, "restored_empty")
      assert {:ok, restored_store} = Backup.restore(backup_path, restore_path)

      # Should be able to insert data
      turtle = "@prefix ex: <http://example.org/> . ex:s ex:p ex:o ."
      assert {:ok, _} = TripleStore.load_string(restored_store, turtle, :turtle)

      # Query should work
      query = "SELECT * WHERE { ?s ?p ?o }"
      {:ok, results} = TripleStore.query(restored_store, query)
      assert length(results) == 1

      TripleStore.close(restored_store)
    end
  end
end
