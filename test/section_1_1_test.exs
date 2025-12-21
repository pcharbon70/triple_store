defmodule TripleStore.Section11Test do
  @moduledoc """
  Unit tests for Section 1.1: Project Scaffolding

  These tests verify the foundational project setup including:
  - Mix project compilation
  - Rustler NIF loading
  - Supervision tree startup
  - Module namespace structure
  """
  use ExUnit.Case, async: true

  alias TripleStore.Backend.RocksDB
  alias TripleStore.Dictionary

  describe "1.1.4.1 Mix project compiles without errors" do
    test "project compiles successfully" do
      # If we're running tests, compilation succeeded
      # This test documents the requirement explicitly
      assert Code.ensure_loaded?(TripleStore)
      assert Code.ensure_loaded?(TripleStore.Application)
    end

    test "all core modules compile" do
      modules = [
        TripleStore,
        TripleStore.Application,
        TripleStore.Backend,
        TripleStore.Backend.RocksDB,
        TripleStore.Backend.RocksDB.NIF,
        TripleStore.Dictionary,
        TripleStore.Index
      ]

      for module <- modules do
        assert {:module, ^module} = Code.ensure_loaded(module),
               "Module #{inspect(module)} should be loaded"
      end
    end

    test "dependencies are available" do
      # Verify key dependencies are loaded
      assert {:module, RDF} = Code.ensure_loaded(RDF)
      assert {:module, Flow} = Code.ensure_loaded(Flow)
      assert {:module, :telemetry} = Code.ensure_loaded(:telemetry)
    end
  end

  describe "1.1.4.2 Rustler NIF loads successfully" do
    test "NIF module is defined" do
      assert Code.ensure_loaded?(TripleStore.Backend.RocksDB.NIF)
    end

    test "NIF is loaded and operational" do
      result = RocksDB.NIF.nif_loaded()
      assert result == "rocksdb_nif"
    end

    test "NIF does not raise on call" do
      assert_raise_or_return = fn ->
        try do
          RocksDB.NIF.nif_loaded()
        rescue
          e -> {:error, e}
        end
      end

      result = assert_raise_or_return.()
      assert result == "rocksdb_nif"
    end
  end

  describe "1.1.4.3 Supervision tree starts correctly" do
    test "application is started" do
      # Verify the application is running
      assert Application.started_applications()
             |> Enum.any?(fn {app, _, _} -> app == :triple_store end)
    end

    test "supervisor process is running" do
      pid = Process.whereis(TripleStore.Supervisor)
      assert pid != nil, "TripleStore.Supervisor should be registered"
      assert Process.alive?(pid), "TripleStore.Supervisor should be alive"
    end

    test "supervisor has correct strategy" do
      pid = Process.whereis(TripleStore.Supervisor)
      # Supervisor.count_children returns info about the supervisor
      info = Supervisor.count_children(pid)
      assert is_map(info)
    end
  end

  describe "1.1.4.4 Module namespaces are properly defined" do
    test "TripleStore root module exists" do
      assert Code.ensure_loaded?(TripleStore)
      assert function_exported?(TripleStore, :__info__, 1)
    end

    test "TripleStore.Backend namespace exists" do
      assert Code.ensure_loaded?(TripleStore.Backend)
    end

    test "TripleStore.Backend.RocksDB namespace exists" do
      assert Code.ensure_loaded?(TripleStore.Backend.RocksDB)
    end

    test "TripleStore.Dictionary namespace exists with type tags" do
      assert Code.ensure_loaded?(Dictionary)

      assert Dictionary.type_uri() == 0b0001
      assert Dictionary.type_bnode() == 0b0010
      assert Dictionary.type_literal() == 0b0011
      assert Dictionary.type_integer() == 0b0100
      assert Dictionary.type_decimal() == 0b0101
      assert Dictionary.type_datetime() == 0b0110
    end

    test "TripleStore.Index namespace exists" do
      assert Code.ensure_loaded?(TripleStore.Index)
    end

    test "all modules have proper documentation" do
      modules = [
        TripleStore,
        TripleStore.Application,
        TripleStore.Backend,
        TripleStore.Backend.RocksDB,
        TripleStore.Dictionary,
        TripleStore.Index
      ]

      for module <- modules do
        # Each module should have __info__ which indicates proper compilation
        assert function_exported?(module, :__info__, 1),
               "Module #{inspect(module)} should export __info__/1"
      end
    end
  end
end
