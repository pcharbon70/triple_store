defmodule TripleStore.SnapshotTest do
  use ExUnit.Case, async: false

  alias TripleStore.Snapshot
  alias TripleStore.Backend.RocksDB.NIF

  @moduletag :capture_log

  setup do
    # Create a temporary database for testing
    path = Path.join(System.tmp_dir!(), "snapshot_test_#{System.unique_integer([:positive])}")

    {:ok, db} = NIF.open(path)

    # Insert some test data
    :ok = NIF.put(db, :spo, "key1", "value1")
    :ok = NIF.put(db, :spo, "key2", "value2")

    on_exit(fn ->
      NIF.close(db)
      File.rm_rf!(path)
    end)

    {:ok, db: db, path: path}
  end

  describe "create/2" do
    test "creates a snapshot with default TTL", %{db: db} do
      assert {:ok, snapshot} = Snapshot.create(db)
      assert is_reference(snapshot)

      # Verify it's registered
      assert Snapshot.count() >= 1

      # Clean up
      :ok = Snapshot.release(snapshot)
    end

    test "creates a snapshot with custom TTL", %{db: db} do
      assert {:ok, snapshot} = Snapshot.create(db, ttl: 1000)

      info = Snapshot.info()
      snapshot_info = Enum.find(info.snapshots, &(&1.ref == snapshot))
      assert snapshot_info.ttl == 1000

      :ok = Snapshot.release(snapshot)
    end

    test "snapshot provides consistent view", %{db: db} do
      # Create snapshot
      {:ok, snapshot} = Snapshot.create(db)

      # Update data after snapshot
      :ok = NIF.put(db, :spo, "key1", "updated_value")

      # Snapshot should still see old value
      assert {:ok, "value1"} = NIF.snapshot_get(snapshot, :spo, "key1")

      # Direct read should see new value
      assert {:ok, "updated_value"} = NIF.get(db, :spo, "key1")

      :ok = Snapshot.release(snapshot)
    end
  end

  describe "release/1" do
    test "releases a snapshot", %{db: db} do
      {:ok, snapshot} = Snapshot.create(db)
      initial_count = Snapshot.count()

      assert :ok = Snapshot.release(snapshot)
      assert Snapshot.count() == initial_count - 1
    end

    test "handles double release gracefully", %{db: db} do
      {:ok, snapshot} = Snapshot.create(db)
      assert :ok = Snapshot.release(snapshot)

      # Second release should not error
      assert {:error, :snapshot_released} = Snapshot.release(snapshot)
    end
  end

  describe "with_snapshot/3" do
    test "executes function with snapshot", %{db: db} do
      result =
        Snapshot.with_snapshot(db, fn snapshot ->
          {:ok, value} = NIF.snapshot_get(snapshot, :spo, "key1")
          value
        end)

      assert result == "value1"
    end

    test "releases snapshot after successful execution", %{db: db} do
      initial_count = Snapshot.count()

      Snapshot.with_snapshot(db, fn _snapshot ->
        # Count should be higher during execution
        assert Snapshot.count() == initial_count + 1
        :ok
      end)

      # Count should be back to initial after
      assert Snapshot.count() == initial_count
    end

    test "releases snapshot on exception", %{db: db} do
      initial_count = Snapshot.count()

      assert_raise RuntimeError, "test error", fn ->
        Snapshot.with_snapshot(db, fn _snapshot ->
          raise "test error"
        end)
      end

      # Count should be back to initial after exception
      assert Snapshot.count() == initial_count
    end

    test "accepts custom TTL option", %{db: db} do
      Snapshot.with_snapshot(db, [ttl: 500], fn snapshot ->
        info = Snapshot.info()
        snapshot_info = Enum.find(info.snapshots, &(&1.ref == snapshot))
        assert snapshot_info.ttl == 500
      end)
    end
  end

  describe "owner process monitoring" do
    test "releases snapshot when owner process terminates", %{db: db} do
      initial_count = Snapshot.count()

      # Spawn a process that creates a snapshot and then dies
      parent = self()

      pid =
        spawn(fn ->
          {:ok, snapshot} = Snapshot.create(db)
          send(parent, {:snapshot_created, snapshot})
          # Wait to be killed
          receive do
            :exit -> :ok
          end
        end)

      # Wait for snapshot creation
      assert_receive {:snapshot_created, _snapshot}, 1000

      # Verify snapshot is registered
      assert Snapshot.count() == initial_count + 1

      # Kill the process
      Process.exit(pid, :kill)

      # Wait for monitor to clean up
      Process.sleep(100)

      # Snapshot should be released
      assert Snapshot.count() == initial_count
    end
  end

  describe "TTL expiration" do
    test "expires snapshots after TTL", %{db: db} do
      initial_count = Snapshot.count()

      # Create snapshot with very short TTL
      {:ok, _snapshot} = Snapshot.create(db, ttl: 50)
      assert Snapshot.count() == initial_count + 1

      # Wait for expiration + cleanup interval
      # Note: cleanup runs every 60s by default, but we can trigger it manually
      Process.sleep(100)

      # Trigger cleanup by sending the message directly
      send(TripleStore.Snapshot, :cleanup)
      Process.sleep(50)

      # Snapshot should be expired
      assert Snapshot.count() == initial_count
    end
  end

  describe "info/0" do
    test "returns snapshot information", %{db: db} do
      {:ok, snapshot} = Snapshot.create(db, ttl: 10_000)

      info = Snapshot.info()
      assert info.count >= 1

      snapshot_info = Enum.find(info.snapshots, &(&1.ref == snapshot))
      assert snapshot_info != nil
      assert snapshot_info.owner == self()
      assert snapshot_info.ttl == 10_000
      assert snapshot_info.age_ms >= 0
      assert snapshot_info.remaining_ms <= 10_000

      :ok = Snapshot.release(snapshot)
    end
  end

  describe "telemetry events" do
    test "emits created event", %{db: db} do
      ref = make_ref()
      parent = self()

      :telemetry.attach(
        "test-snapshot-created-#{inspect(ref)}",
        [:triple_store, :snapshot, :created],
        fn _event, measurements, metadata, _config ->
          send(parent, {:created, measurements, metadata})
        end,
        nil
      )

      {:ok, snapshot} = Snapshot.create(db)

      assert_receive {:created, %{count: 1}, %{snapshot_ref: ^snapshot, ttl: _, owner: _}}, 1000

      :ok = Snapshot.release(snapshot)

      :telemetry.detach("test-snapshot-created-#{inspect(ref)}")
    end

    test "emits released event", %{db: db} do
      ref = make_ref()
      parent = self()

      :telemetry.attach(
        "test-snapshot-released-#{inspect(ref)}",
        [:triple_store, :snapshot, :released],
        fn _event, measurements, metadata, _config ->
          send(parent, {:released, measurements, metadata})
        end,
        nil
      )

      {:ok, snapshot} = Snapshot.create(db)
      :ok = Snapshot.release(snapshot)

      assert_receive {:released, %{count: 1}, %{snapshot_ref: ^snapshot, reason: :manual}}, 1000

      :telemetry.detach("test-snapshot-released-#{inspect(ref)}")
    end

    test "emits released event with owner_down reason", %{db: db} do
      ref = make_ref()
      parent = self()

      :telemetry.attach(
        "test-snapshot-owner-down-#{inspect(ref)}",
        [:triple_store, :snapshot, :released],
        fn _event, _measurements, metadata, _config ->
          send(parent, {:released, metadata})
        end,
        nil
      )

      # Spawn a process that creates a snapshot and dies
      spawn(fn ->
        {:ok, _snapshot} = Snapshot.create(db)
        # Process exits immediately
      end)

      assert_receive {:released, %{reason: :owner_down}}, 1000

      :telemetry.detach("test-snapshot-owner-down-#{inspect(ref)}")
    end
  end

  describe "count/0" do
    test "returns number of active snapshots", %{db: db} do
      initial = Snapshot.count()

      {:ok, s1} = Snapshot.create(db)
      assert Snapshot.count() == initial + 1

      {:ok, s2} = Snapshot.create(db)
      assert Snapshot.count() == initial + 2

      :ok = Snapshot.release(s1)
      assert Snapshot.count() == initial + 1

      :ok = Snapshot.release(s2)
      assert Snapshot.count() == initial
    end
  end
end
