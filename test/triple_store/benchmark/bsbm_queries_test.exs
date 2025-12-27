defmodule TripleStore.Benchmark.BSBMQueriesTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.BSBMQueries

  @moduletag :benchmark

  describe "all/0" do
    test "returns 12 queries" do
      queries = BSBMQueries.all()
      assert length(queries) == 12
    end

    test "all queries have required fields" do
      for query <- BSBMQueries.all() do
        assert Map.has_key?(query, :id)
        assert Map.has_key?(query, :name)
        assert Map.has_key?(query, :description)
        assert Map.has_key?(query, :sparql)
        assert Map.has_key?(query, :params)
        assert Map.has_key?(query, :complexity)
        assert Map.has_key?(query, :operation_type)
        assert Map.has_key?(query, :expected_result_factor)
      end
    end

    test "query IDs are unique" do
      ids = Enum.map(BSBMQueries.all(), & &1.id)
      assert length(ids) == length(Enum.uniq(ids))
    end

    test "query IDs follow naming convention" do
      for query <- BSBMQueries.all() do
        assert query.id |> Atom.to_string() |> String.starts_with?("q")
      end
    end
  end

  describe "get/1" do
    test "returns query by ID" do
      {:ok, query} = BSBMQueries.get(:q1)
      assert query.id == :q1
      assert String.contains?(query.name, "Q1")
    end

    test "returns error for unknown ID" do
      assert {:error, :not_found} = BSBMQueries.get(:unknown)
    end

    test "can retrieve all queries by ID" do
      for i <- 1..12 do
        id = String.to_atom("q#{i}")
        {:ok, query} = BSBMQueries.get(id)
        assert query.id == id
      end
    end
  end

  describe "get/2 with parameters" do
    test "substitutes product_type parameter" do
      {:ok, query} = BSBMQueries.get(:q1, product_type: 5)
      assert String.contains?(query.sparql, "ProductType5")
    end

    test "substitutes product parameter" do
      {:ok, query} = BSBMQueries.get(:q6, product: 42)
      assert String.contains?(query.sparql, "Product42")
    end

    test "substitutes feature parameters" do
      {:ok, query} = BSBMQueries.get(:q4, feature1: 3, feature2: 7)
      assert String.contains?(query.sparql, "ProductFeature1_3")
      assert String.contains?(query.sparql, "ProductFeature1_7")
    end

    test "substitutes price parameters" do
      {:ok, query} = BSBMQueries.get(:q7, min_price: 100, max_price: 1000)
      assert String.contains?(query.sparql, "100")
      assert String.contains?(query.sparql, "1000")
    end

    test "uses default values when not specified" do
      {:ok, query} = BSBMQueries.get(:q1, [])
      assert String.contains?(query.sparql, "ProductType1")
      assert String.contains?(query.sparql, "ProductFeature1_1")
    end
  end

  describe "namespace/0" do
    test "returns BSBM vocabulary namespace" do
      ns = BSBMQueries.namespace()
      assert String.contains?(ns, "wiwiss.fu-berlin.de")
      assert String.contains?(ns, "bsbm")
    end
  end

  describe "instances_namespace/0" do
    test "returns BSBM instances namespace" do
      ns = BSBMQueries.instances_namespace()
      assert String.contains?(ns, "wiwiss.fu-berlin.de")
      assert String.contains?(ns, "instances")
    end
  end

  describe "estimate_results/2" do
    test "returns estimated count for queries" do
      result = BSBMQueries.estimate_results(:q2, 100)
      assert is_integer(result)
      assert result > 0
    end

    test "scales with product count" do
      result100 = BSBMQueries.estimate_results(:q7, 100)
      result1000 = BSBMQueries.estimate_results(:q7, 1000)

      assert result1000 == result100 * 10
    end

    test "returns :varies for unknown query" do
      assert :varies == BSBMQueries.estimate_results(:unknown, 100)
    end
  end

  describe "query content validation" do
    test "all queries have valid SPARQL structure" do
      for query <- BSBMQueries.all() do
        assert String.contains?(query.sparql, "PREFIX") or String.contains?(query.sparql, "SELECT") or String.contains?(query.sparql, "CONSTRUCT")
        assert String.contains?(query.sparql, "WHERE") or String.contains?(query.sparql, "CONSTRUCT")
      end
    end

    test "queries with parameters have no remaining placeholders after substitution" do
      for query <- BSBMQueries.all(), length(query.params) > 0 do
        {:ok, substituted} = BSBMQueries.get(query.id, [])
        refute String.contains?(substituted.sparql, "{product_type}")
        refute String.contains?(substituted.sparql, "{product}")
        refute String.contains?(substituted.sparql, "{feature1}")
        refute String.contains?(substituted.sparql, "{feature2}")
      end
    end

    test "complexity values are valid" do
      valid_complexities = [:simple, :medium, :complex]

      for query <- BSBMQueries.all() do
        assert query.complexity in valid_complexities
      end
    end

    test "operation_type values are valid" do
      valid_types = [:search, :lookup, :join, :analytics, :describe, :export]

      for query <- BSBMQueries.all() do
        assert query.operation_type in valid_types
      end
    end
  end

  describe "specific query validation" do
    test "Q1 searches products by type and features" do
      {:ok, query} = BSBMQueries.get(:q1)
      assert String.contains?(query.sparql, "ProductType")
      assert String.contains?(query.sparql, "productFeature")
      assert String.contains?(query.sparql, "FILTER")
    end

    test "Q4 uses UNION for alternative features" do
      {:ok, query} = BSBMQueries.get(:q4)
      assert String.contains?(query.sparql, "UNION")
    end

    test "Q6 is a product lookup" do
      {:ok, query} = BSBMQueries.get(:q6)
      assert query.operation_type == :lookup
      assert String.contains?(query.sparql, "BIND")
    end

    test "Q7 joins products and offers" do
      {:ok, query} = BSBMQueries.get(:q7)
      assert query.operation_type == :join
      assert String.contains?(query.sparql, "bsbm:product")
      assert String.contains?(query.sparql, "bsbm:price")
    end

    test "Q8 retrieves product reviews" do
      {:ok, query} = BSBMQueries.get(:q8)
      assert String.contains?(query.sparql, "reviewFor")
      assert String.contains?(query.sparql, "rating1")
    end

    test "Q12 uses CONSTRUCT for export" do
      {:ok, query} = BSBMQueries.get(:q12)
      assert query.operation_type == :export
      assert String.contains?(query.sparql, "CONSTRUCT")
    end
  end

  describe "query distribution" do
    test "has search queries" do
      search_queries = Enum.filter(BSBMQueries.all(), &(&1.operation_type == :search))
      assert length(search_queries) >= 4
    end

    test "has join queries" do
      join_queries = Enum.filter(BSBMQueries.all(), &(&1.operation_type == :join))
      assert length(join_queries) >= 2
    end

    test "has analytics queries" do
      analytics_queries = Enum.filter(BSBMQueries.all(), &(&1.operation_type == :analytics))
      assert length(analytics_queries) >= 2
    end
  end
end
