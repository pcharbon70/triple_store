defmodule TripleStore.Config.RocksDBTest do
  use ExUnit.Case, async: true

  alias TripleStore.Config.RocksDB

  describe "recommended/1" do
    test "returns valid configuration with default options" do
      config = RocksDB.recommended()

      assert is_integer(config.block_cache_size)
      assert config.block_cache_size > 0
      assert is_integer(config.write_buffer_size)
      assert config.write_buffer_size > 0
      assert is_integer(config.max_write_buffer_number)
      assert config.max_write_buffer_number >= 1
      assert is_integer(config.max_open_files)
    end

    test "respects available_memory override" do
      # 8 GB system
      memory = 8 * 1024 * 1024 * 1024
      config = RocksDB.recommended(available_memory: memory)

      # Block cache should be ~40% of 8 GB = ~3.2 GB
      expected_min = trunc(memory * 0.35)
      expected_max = trunc(memory * 0.45)

      assert config.block_cache_size >= expected_min
      assert config.block_cache_size <= expected_max
    end

    test "respects block_cache_percentage override" do
      memory = 4 * 1024 * 1024 * 1024
      config = RocksDB.recommended(available_memory: memory, block_cache_percentage: 0.25)

      # 25% of 4 GB = 1 GB
      expected = trunc(memory * 0.25)
      assert config.block_cache_size == expected
    end
  end

  describe "for_memory_budget/2" do
    test "allocates block cache at 40% by default" do
      budget = 10 * 1024 * 1024 * 1024
      config = RocksDB.for_memory_budget(budget)

      # 40% of 10 GB = 4 GB
      expected = trunc(budget * 0.40)
      assert config.block_cache_size == expected
    end

    test "respects minimum block cache size" do
      # Very small budget: 100 MB
      budget = 100 * 1024 * 1024
      config = RocksDB.for_memory_budget(budget)

      # Should use minimum of 64 MB
      assert config.block_cache_size == 64 * 1024 * 1024
    end

    test "respects maximum block cache size" do
      # Very large budget: 100 GB
      budget = 100 * 1024 * 1024 * 1024
      config = RocksDB.for_memory_budget(budget)

      # Should cap at 32 GB
      assert config.block_cache_size == 32 * 1024 * 1024 * 1024
    end

    test "configures write buffers based on remaining memory" do
      # 4 GB budget
      budget = 4 * 1024 * 1024 * 1024
      config = RocksDB.for_memory_budget(budget)

      # Write buffer should be reasonable
      assert config.write_buffer_size >= 16 * 1024 * 1024
      assert config.write_buffer_size <= 512 * 1024 * 1024
      assert config.max_write_buffer_number in [2, 4]
    end

    test "scales level configuration with write buffer" do
      budget = 4 * 1024 * 1024 * 1024
      config = RocksDB.for_memory_budget(budget)

      # target_file_size should equal write_buffer_size
      assert config.target_file_size_base == config.write_buffer_size

      # max_bytes_for_level_base should be 10× target_file_size
      assert config.max_bytes_for_level_base == config.target_file_size_base * 10
    end
  end

  describe "preset/1" do
    test "returns development preset" do
      config = RocksDB.preset(:development)

      assert config.block_cache_size == 128 * 1024 * 1024
      assert config.write_buffer_size == 32 * 1024 * 1024
      assert config.max_open_files == 256
    end

    test "returns production_low_memory preset" do
      config = RocksDB.preset(:production_low_memory)

      assert config.block_cache_size == 256 * 1024 * 1024
      assert config.max_open_files == 512
    end

    test "returns production_high_memory preset" do
      config = RocksDB.preset(:production_high_memory)

      assert config.block_cache_size == 4 * 1024 * 1024 * 1024
      assert config.write_buffer_size == 128 * 1024 * 1024
      assert config.max_write_buffer_number == 4
      assert config.max_open_files == 4096
    end

    test "returns write_heavy preset" do
      config = RocksDB.preset(:write_heavy)

      assert config.write_buffer_size == 256 * 1024 * 1024
      assert config.max_write_buffer_number == 4
    end

    test "raises for unknown preset" do
      assert_raise FunctionClauseError, fn ->
        RocksDB.preset(:nonexistent)
      end
    end
  end

  describe "preset_names/0" do
    test "returns all preset names" do
      names = RocksDB.preset_names()

      assert :development in names
      assert :production_low_memory in names
      assert :production_high_memory in names
      assert :write_heavy in names
    end
  end

  describe "calculate_block_cache_size/1" do
    test "calculates 40% of available memory" do
      memory = 8 * 1024 * 1024 * 1024
      size = RocksDB.calculate_block_cache_size(memory)

      expected = trunc(memory * 0.40)
      assert size == expected
    end

    test "clamps to minimum of 64 MB" do
      memory = 100 * 1024 * 1024
      size = RocksDB.calculate_block_cache_size(memory)

      assert size == 64 * 1024 * 1024
    end

    test "clamps to maximum of 32 GB" do
      memory = 100 * 1024 * 1024 * 1024
      size = RocksDB.calculate_block_cache_size(memory)

      assert size == 32 * 1024 * 1024 * 1024
    end
  end

  describe "detect_system_memory/0" do
    test "returns a positive integer" do
      memory = RocksDB.detect_system_memory()

      assert is_integer(memory)
      assert memory > 0
    end

    test "returns reasonable value (at least 256 MB)" do
      memory = RocksDB.detect_system_memory()

      # Any real system should have at least 256 MB
      assert memory >= 256 * 1024 * 1024
    end
  end

  describe "calculate_max_open_files/0" do
    test "returns a reasonable value" do
      max_files = RocksDB.calculate_max_open_files()

      assert is_integer(max_files)
      # Should be at least 256
      assert max_files >= 256
      # Should not exceed 65536
      assert max_files <= 65_536
    end
  end

  describe "estimate_memory_usage/1" do
    test "includes block cache in estimate" do
      config = RocksDB.preset(:development)
      usage = RocksDB.estimate_memory_usage(config)

      # Should be at least block cache size
      assert usage >= config.block_cache_size
    end

    test "accounts for write buffers across column families" do
      config = RocksDB.preset(:production_high_memory)
      usage = RocksDB.estimate_memory_usage(config)

      # 6 column families × 4 buffers × 128 MB = 3 GB write buffers
      write_buffer_total = 6 * 4 * 128 * 1024 * 1024

      # Should include write buffers
      assert usage >= config.block_cache_size + write_buffer_total
    end

    test "adds overhead for indices and filters" do
      config = RocksDB.preset(:development)
      usage = RocksDB.estimate_memory_usage(config)

      # Overhead should be ~10% of block cache
      write_buffers = 6 * config.max_write_buffer_number * config.write_buffer_size
      min_expected = config.block_cache_size + write_buffers

      assert usage > min_expected
    end
  end

  describe "format_bytes/1" do
    test "formats gigabytes" do
      assert RocksDB.format_bytes(1024 * 1024 * 1024) == "1.00 GB"
      assert RocksDB.format_bytes(4 * 1024 * 1024 * 1024) == "4.00 GB"
      assert RocksDB.format_bytes(trunc(2.5 * 1024 * 1024 * 1024)) == "2.50 GB"
    end

    test "formats megabytes" do
      assert RocksDB.format_bytes(64 * 1024 * 1024) == "64.00 MB"
      assert RocksDB.format_bytes(128 * 1024 * 1024) == "128.00 MB"
    end

    test "formats kilobytes" do
      assert RocksDB.format_bytes(512 * 1024) == "512 KB"
    end

    test "formats bytes" do
      assert RocksDB.format_bytes(100) == "100 B"
    end
  end

  describe "format_summary/1" do
    test "returns string summary" do
      config = RocksDB.preset(:development)
      summary = RocksDB.format_summary(config)

      assert is_binary(summary)
      assert String.contains?(summary, "Block Cache")
      assert String.contains?(summary, "Write Buffer")
      assert String.contains?(summary, "Max Open Files")
    end
  end

  describe "validate/1" do
    test "validates correct configuration" do
      config = RocksDB.recommended()
      assert RocksDB.validate(config) == :ok
    end

    test "validates all presets" do
      for name <- RocksDB.preset_names() do
        config = RocksDB.preset(name)
        assert RocksDB.validate(config) == :ok
      end
    end

    test "rejects negative block_cache_size" do
      config = %{RocksDB.default() | block_cache_size: -1}
      assert {:error, _} = RocksDB.validate(config)
    end

    test "rejects zero write_buffer_size" do
      config = %{RocksDB.default() | write_buffer_size: 0}
      assert {:error, _} = RocksDB.validate(config)
    end

    test "rejects zero max_write_buffer_number" do
      config = %{RocksDB.default() | max_write_buffer_number: 0}
      assert {:error, _} = RocksDB.validate(config)
    end

    test "rejects non-integer max_open_files" do
      config = %{RocksDB.default() | max_open_files: "1000"}
      assert {:error, _} = RocksDB.validate(config)
    end

    test "rejects zero target_file_size_base" do
      config = %{RocksDB.default() | target_file_size_base: 0}
      assert {:error, _} = RocksDB.validate(config)
    end

    test "rejects zero max_bytes_for_level_base" do
      config = %{RocksDB.default() | max_bytes_for_level_base: 0}
      assert {:error, _} = RocksDB.validate(config)
    end
  end

  describe "default/0" do
    test "returns valid configuration" do
      config = RocksDB.default()

      assert :ok = RocksDB.validate(config)
    end

    test "uses system detection" do
      config = RocksDB.default()

      # Should have reasonable values
      assert config.block_cache_size >= 64 * 1024 * 1024
      assert config.write_buffer_size >= 16 * 1024 * 1024
    end
  end

  describe "configuration loads without errors" do
    test "all configurations pass validation" do
      # Test recommended
      assert :ok = RocksDB.validate(RocksDB.recommended())

      # Test with different memory budgets
      for budget <- [512 * 1024 * 1024, 4 * 1024 * 1024 * 1024, 32 * 1024 * 1024 * 1024] do
        config = RocksDB.for_memory_budget(budget)
        assert :ok = RocksDB.validate(config)
      end

      # Test all presets
      for name <- RocksDB.preset_names() do
        config = RocksDB.preset(name)
        assert :ok = RocksDB.validate(config)
      end
    end
  end
end
