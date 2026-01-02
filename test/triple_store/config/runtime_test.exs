defmodule TripleStore.Config.RuntimeTest do
  use ExUnit.Case, async: false

  alias TripleStore.Config.Runtime
  alias TripleStore.Config.Compaction
  alias TripleStore.Backend.RocksDB.NIF

  @moduletag :integration

  setup do
    # Create a temporary database for testing
    path = Path.join(System.tmp_dir!(), "runtime_config_test_#{:erlang.unique_integer([:positive])}")
    File.rm_rf!(path)

    {:ok, db} = NIF.open(path)

    on_exit(fn ->
      NIF.close(db)
      File.rm_rf!(path)
    end)

    %{db: db, path: path}
  end

  describe "set_options/2 NIF" do
    test "sets level0 compaction trigger", %{db: db} do
      assert :ok = NIF.set_options(db, [{"level0_file_num_compaction_trigger", "16"}])
    end

    test "sets multiple options", %{db: db} do
      options = [
        {"level0_file_num_compaction_trigger", "16"},
        {"level0_slowdown_writes_trigger", "64"},
        {"level0_stop_writes_trigger", "128"}
      ]

      assert :ok = NIF.set_options(db, options)
    end

    test "sets disable_auto_compactions", %{db: db} do
      assert :ok = NIF.set_options(db, [{"disable_auto_compactions", "true"}])
      assert :ok = NIF.set_options(db, [{"disable_auto_compactions", "false"}])
    end

    test "returns error for invalid option", %{db: db} do
      # Invalid option name should fail
      assert {:error, {:set_options_failed, _reason}} =
               NIF.set_options(db, [{"invalid_option_name", "123"}])
    end

    test "returns error for closed database" do
      path = Path.join(System.tmp_dir!(), "runtime_closed_test_#{:erlang.unique_integer([:positive])}")
      {:ok, db} = NIF.open(path)
      NIF.close(db)

      assert {:error, :already_closed} = NIF.set_options(db, [{"level0_file_num_compaction_trigger", "16"}])

      File.rm_rf!(path)
    end
  end

  describe "prepare_for_bulk_load/2" do
    test "returns saved config for restoration", %{db: db} do
      assert {:ok, saved} = Runtime.prepare_for_bulk_load(db)
      assert is_map(saved)
      assert is_list(saved.options)
      assert saved.preset == :default
    end

    test "options contain expected keys", %{db: db} do
      {:ok, saved} = Runtime.prepare_for_bulk_load(db)

      option_keys = Enum.map(saved.options, fn {k, _v} -> k end)
      assert "level0_file_num_compaction_trigger" in option_keys
      assert "level0_slowdown_writes_trigger" in option_keys
      assert "level0_stop_writes_trigger" in option_keys
    end

    test "with disable_compaction option", %{db: db} do
      {:ok, saved} = Runtime.prepare_for_bulk_load(db, disable_compaction: true)

      option_keys = Enum.map(saved.options, fn {k, _v} -> k end)
      assert "disable_auto_compactions" in option_keys
    end
  end

  describe "restore_config/2" do
    test "restores saved configuration", %{db: db} do
      {:ok, saved} = Runtime.prepare_for_bulk_load(db)
      assert :ok = Runtime.restore_config(db, saved)
    end

    test "round-trip preserves database operation", %{db: db} do
      {:ok, saved} = Runtime.prepare_for_bulk_load(db)
      :ok = Runtime.restore_config(db, saved)

      # Verify we can still write to the database
      assert :ok = NIF.write_batch(db, [{:put, :spo, "test_key", "test_value"}], true)
    end
  end

  describe "restore_normal_config/1" do
    test "restores to default settings", %{db: db} do
      # First apply bulk load settings
      {:ok, _saved} = Runtime.prepare_for_bulk_load(db)

      # Then restore to normal
      assert :ok = Runtime.restore_normal_config(db)
    end
  end

  describe "apply_preset/2" do
    test "applies bulk_load preset", %{db: db} do
      assert :ok = Runtime.apply_preset(db, :bulk_load)
    end

    test "applies default preset", %{db: db} do
      assert :ok = Runtime.apply_preset(db, :default)
    end

    test "applies write_heavy preset", %{db: db} do
      assert :ok = Runtime.apply_preset(db, :write_heavy)
    end

    test "applies all presets successfully", %{db: db} do
      for preset <- Compaction.preset_names() do
        assert :ok = Runtime.apply_preset(db, preset),
               "Failed to apply preset #{preset}"
      end
    end
  end

  describe "set_options/2 with keyword list" do
    test "sets options with atom keys", %{db: db} do
      assert :ok =
               Runtime.set_options(db,
                 level0_file_num_compaction_trigger: 16,
                 level0_slowdown_writes_trigger: 64
               )
    end

    test "sets boolean option", %{db: db} do
      assert :ok = Runtime.set_options(db, disable_auto_compactions: true)
      assert :ok = Runtime.set_options(db, disable_auto_compactions: false)
    end
  end

  describe "with_bulk_config/3" do
    test "executes function with bulk config and restores", %{db: db} do
      result =
        Runtime.with_bulk_config(db, [], fn _db ->
          :success
        end)

      assert {:ok, :success} = result
    end

    test "restores config on success", %{db: db} do
      {:ok, _result} =
        Runtime.with_bulk_config(db, [], fn _db ->
          # Function completes normally
          :done
        end)

      # Database should still work with normal settings
      assert :ok = NIF.write_batch(db, [{:put, :spo, "key1", "value1"}], true)
    end

    test "restores config on error", %{db: db} do
      result =
        Runtime.with_bulk_config(db, [], fn _db ->
          {:error, :simulated_failure}
        end)

      # Result is wrapped
      assert {:ok, {:error, :simulated_failure}} = result

      # Database should still work
      assert :ok = NIF.write_batch(db, [{:put, :spo, "key2", "value2"}], true)
    end

    test "restores config on raise", %{db: db} do
      assert_raise RuntimeError, "test error", fn ->
        Runtime.with_bulk_config(db, [], fn _db ->
          raise "test error"
        end)
      end

      # Database should still work after exception
      assert :ok = NIF.write_batch(db, [{:put, :spo, "key3", "value3"}], true)
    end

    test "works with disable_compaction option", %{db: db} do
      {:ok, result} =
        Runtime.with_bulk_config(db, [disable_compaction: true], fn _db ->
          :completed
        end)

      assert result == :completed
    end
  end

  describe "configuration round-trip" do
    test "bulk load then restore cycle", %{db: db} do
      # Apply bulk load config
      {:ok, saved} = Runtime.prepare_for_bulk_load(db)

      # Simulate some work
      for i <- 1..10 do
        NIF.write_batch(db, [{:put, :spo, "key#{i}", "value#{i}"}], false)
      end

      # Restore original config
      :ok = Runtime.restore_config(db, saved)

      # Verify database still works
      {:ok, value} = NIF.get(db, :spo, "key5")
      assert value == "value5"
    end

    test "multiple preset switches", %{db: db} do
      presets = [:default, :bulk_load, :write_heavy, :default, :bulk_load, :default]

      for preset <- presets do
        assert :ok = Runtime.apply_preset(db, preset)
      end

      # Database should still work
      assert :ok = NIF.write_batch(db, [{:put, :spo, "final_key", "final_value"}], true)
    end
  end
end
