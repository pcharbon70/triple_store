defmodule TripleStore.Config.CompressionTest do
  use ExUnit.Case, async: true

  alias TripleStore.Config.Compression

  describe "for_column_family/1" do
    test "returns LZ4 for id2str" do
      config = Compression.for_column_family(:id2str)

      assert config.algorithm == :lz4
      assert config.enabled == true
    end

    test "returns LZ4 for str2id" do
      config = Compression.for_column_family(:str2id)

      assert config.algorithm == :lz4
      assert config.enabled == true
    end

    test "returns LZ4 for spo index" do
      config = Compression.for_column_family(:spo)

      assert config.algorithm == :lz4
      assert config.enabled == true
    end

    test "returns LZ4 for pos index" do
      config = Compression.for_column_family(:pos)

      assert config.algorithm == :lz4
      assert config.enabled == true
    end

    test "returns LZ4 for osp index" do
      config = Compression.for_column_family(:osp)

      assert config.algorithm == :lz4
      assert config.enabled == true
    end

    test "returns Zstd for derived column family" do
      config = Compression.for_column_family(:derived)

      assert config.algorithm == :zstd
      assert config.level == 3
      assert config.enabled == true
    end

    test "raises for unknown column family" do
      assert_raise FunctionClauseError, fn ->
        Compression.for_column_family(:unknown)
      end
    end
  end

  describe "all_column_families/0" do
    test "returns configuration for all 6 column families" do
      configs = Compression.all_column_families()

      assert map_size(configs) == 6
      assert Map.has_key?(configs, :id2str)
      assert Map.has_key?(configs, :str2id)
      assert Map.has_key?(configs, :spo)
      assert Map.has_key?(configs, :pos)
      assert Map.has_key?(configs, :osp)
      assert Map.has_key?(configs, :derived)
    end

    test "index column families use LZ4" do
      configs = Compression.all_column_families()

      for cf <- [:spo, :pos, :osp] do
        assert configs[cf].algorithm == :lz4
      end
    end

    test "derived column family uses Zstd" do
      configs = Compression.all_column_families()

      assert configs[:derived].algorithm == :zstd
    end
  end

  describe "per_level_compression/1" do
    test "dictionary column families use LZ4 at all levels" do
      for cf <- [:id2str, :str2id] do
        levels = Compression.per_level_compression(cf)

        assert levels.level_0 == :none
        assert levels.level_1 == :lz4
        assert levels.level_2 == :lz4
        assert levels.level_3_plus == :lz4
      end
    end

    test "index column families use LZ4 and Zstd" do
      for cf <- [:spo, :pos, :osp] do
        levels = Compression.per_level_compression(cf)

        assert levels.level_0 == :none
        assert levels.level_1 == :lz4
        assert levels.level_2 == :lz4
        assert levels.level_3_plus == :zstd
      end
    end

    test "derived column family uses more Zstd" do
      levels = Compression.per_level_compression(:derived)

      assert levels.level_0 == :none
      assert levels.level_1 == :lz4
      assert levels.level_2 == :zstd
      assert levels.level_3_plus == :zstd
    end
  end

  describe "algorithm_spec/1" do
    test "returns specification for LZ4" do
      spec = Compression.algorithm_spec(:lz4)

      assert spec.name == "LZ4"
      assert spec.decode_speed_mbps == 400
      assert spec.ratio == 2.1
      assert spec.cpu_usage == :low
    end

    test "returns specification for Zstd" do
      spec = Compression.algorithm_spec(:zstd)

      assert spec.name == "Zstandard"
      assert spec.decode_speed_mbps == 300
      assert spec.ratio == 3.5
      assert spec.cpu_usage == :medium
    end

    test "returns specification for no compression" do
      spec = Compression.algorithm_spec(:none)

      assert spec.ratio == 1.0
      assert spec.cpu_usage == :none
    end
  end

  describe "algorithms/0" do
    test "returns all supported algorithms" do
      algos = Compression.algorithms()

      assert :none in algos
      assert :snappy in algos
      assert :lz4 in algos
      assert :lz4hc in algos
      assert :zstd in algos
    end
  end

  describe "column_families/0" do
    test "returns all column family names" do
      cfs = Compression.column_families()

      assert length(cfs) == 6
      assert :id2str in cfs
      assert :str2id in cfs
      assert :spo in cfs
      assert :pos in cfs
      assert :osp in cfs
      assert :derived in cfs
    end
  end

  describe "zstd_default_level/0" do
    test "returns level 3" do
      assert Compression.zstd_default_level() == 3
    end
  end

  describe "zstd_high_level/0" do
    test "returns level 6" do
      assert Compression.zstd_high_level() == 6
    end
  end

  describe "custom/1" do
    test "creates custom configuration with defaults" do
      configs = Compression.custom()

      assert configs[:spo].algorithm == :lz4
      assert configs[:derived].algorithm == :zstd
    end

    test "allows custom index algorithm" do
      configs = Compression.custom(index_algorithm: :zstd)

      assert configs[:spo].algorithm == :zstd
      assert configs[:pos].algorithm == :zstd
      assert configs[:osp].algorithm == :zstd
    end

    test "allows custom dictionary algorithm" do
      configs = Compression.custom(dictionary_algorithm: :snappy)

      assert configs[:id2str].algorithm == :snappy
      assert configs[:str2id].algorithm == :snappy
    end

    test "allows custom derived algorithm" do
      configs = Compression.custom(derived_algorithm: :lz4)

      assert configs[:derived].algorithm == :lz4
    end

    test "allows custom zstd level" do
      configs = Compression.custom(derived_algorithm: :zstd, zstd_level: 10)

      assert configs[:derived].level == 10
    end

    test "disables compression when algorithm is :none" do
      configs = Compression.custom(index_algorithm: :none)

      assert configs[:spo].enabled == false
    end
  end

  describe "preset/1" do
    test "default preset uses LZ4 and Zstd" do
      configs = Compression.preset(:default)

      assert configs[:spo].algorithm == :lz4
      assert configs[:derived].algorithm == :zstd
    end

    test "fast preset uses LZ4 everywhere" do
      configs = Compression.preset(:fast)

      for {_cf, config} <- configs do
        assert config.algorithm == :lz4
      end
    end

    test "compact preset uses Zstd everywhere" do
      configs = Compression.preset(:compact)

      for {_cf, config} <- configs do
        assert config.algorithm == :zstd
        assert config.level == 6
      end
    end

    test "none preset disables compression" do
      configs = Compression.preset(:none)

      for {_cf, config} <- configs do
        assert config.algorithm == :none
        assert config.enabled == false
      end
    end
  end

  describe "preset_names/0" do
    test "returns all preset names" do
      names = Compression.preset_names()

      assert :default in names
      assert :fast in names
      assert :compact in names
      assert :none in names
    end
  end

  describe "estimated_ratio/1" do
    test "returns correct ratio for LZ4" do
      assert Compression.estimated_ratio(:lz4) == 2.1
    end

    test "returns correct ratio for Zstd" do
      assert Compression.estimated_ratio(:zstd) == 3.5
    end

    test "returns 1.0 for no compression" do
      assert Compression.estimated_ratio(:none) == 1.0
    end
  end

  describe "estimate_savings/2" do
    test "estimates savings for LZ4 compression" do
      # 1 GB of data
      result = Compression.estimate_savings(:spo, 1024 * 1024 * 1024)

      assert result.original_bytes == 1024 * 1024 * 1024
      # LZ4 ratio is 2.1x
      assert result.compressed_bytes < result.original_bytes
      assert result.savings_percent > 50
    end

    test "estimates savings for Zstd compression" do
      result = Compression.estimate_savings(:derived, 1024 * 1024 * 1024)

      # Zstd ratio is 3.5x
      assert result.savings_percent > 70
    end

    test "handles zero bytes" do
      result = Compression.estimate_savings(:spo, 0)

      assert result.original_bytes == 0
      assert result.compressed_bytes == 0
    end
  end

  describe "validate/1" do
    test "validates correct configuration" do
      config = Compression.for_column_family(:spo)
      assert Compression.validate(config) == :ok
    end

    test "rejects unknown algorithm" do
      config = %{algorithm: :unknown, level: 0, enabled: true}
      assert {:error, _} = Compression.validate(config)
    end

    test "rejects negative level" do
      config = %{algorithm: :lz4, level: -1, enabled: true}
      assert {:error, _} = Compression.validate(config)
    end

    test "rejects zstd level > 22" do
      config = %{algorithm: :zstd, level: 23, enabled: true}
      assert {:error, _} = Compression.validate(config)
    end

    test "rejects non-boolean enabled" do
      config = %{algorithm: :lz4, level: 0, enabled: "yes"}
      assert {:error, _} = Compression.validate(config)
    end
  end

  describe "validate_all/1" do
    test "validates all default configurations" do
      configs = Compression.all_column_families()
      assert Compression.validate_all(configs) == :ok
    end

    test "validates all presets" do
      for name <- Compression.preset_names() do
        configs = Compression.preset(name)
        assert Compression.validate_all(configs) == :ok
      end
    end

    test "reports errors for invalid configurations" do
      configs = %{
        spo: %{algorithm: :lz4, level: 0, enabled: true},
        derived: %{algorithm: :unknown, level: 0, enabled: true}
      }

      assert {:error, msg} = Compression.validate_all(configs)
      assert String.contains?(msg, "derived")
      assert String.contains?(msg, "unknown")
    end
  end

  describe "format_summary/0" do
    test "returns formatted string" do
      summary = Compression.format_summary()

      assert is_binary(summary)
      assert String.contains?(summary, "Compression")
      assert String.contains?(summary, "LZ4")
      assert String.contains?(summary, "Zstd")
    end
  end

  describe "benchmark_data/0" do
    test "returns benchmark information" do
      data = Compression.benchmark_data()

      assert data.test_data_size_mb == 100
      assert is_map(data.algorithms)
      assert Map.has_key?(data.algorithms, :lz4)
      assert Map.has_key?(data.algorithms, :zstd_level_3)
    end

    test "benchmark ratios are consistent with algorithm specs" do
      data = Compression.benchmark_data()

      assert data.algorithms.lz4.ratio == Compression.estimated_ratio(:lz4)
      assert data.algorithms.none.ratio == 1.0
    end
  end

  describe "compression achieves expected ratio" do
    test "LZ4 ratio is between 1.5 and 3.0" do
      ratio = Compression.estimated_ratio(:lz4)
      assert ratio >= 1.5
      assert ratio <= 3.0
    end

    test "Zstd ratio is between 2.5 and 5.0" do
      ratio = Compression.estimated_ratio(:zstd)
      assert ratio >= 2.5
      assert ratio <= 5.0
    end

    test "no compression has ratio of 1.0" do
      assert Compression.estimated_ratio(:none) == 1.0
    end

    test "LZ4HC has better ratio than LZ4" do
      lz4 = Compression.estimated_ratio(:lz4)
      lz4hc = Compression.estimated_ratio(:lz4hc)
      assert lz4hc > lz4
    end

    test "Zstd has better ratio than LZ4" do
      lz4 = Compression.estimated_ratio(:lz4)
      zstd = Compression.estimated_ratio(:zstd)
      assert zstd > lz4
    end
  end
end
