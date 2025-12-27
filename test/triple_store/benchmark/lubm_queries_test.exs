defmodule TripleStore.Benchmark.LUBMQueriesTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LUBMQueries

  @moduletag :benchmark

  describe "all/0" do
    test "returns 14 queries" do
      queries = LUBMQueries.all()
      assert length(queries) == 14
    end

    test "all queries have required fields" do
      for query <- LUBMQueries.all() do
        assert Map.has_key?(query, :id)
        assert Map.has_key?(query, :name)
        assert Map.has_key?(query, :description)
        assert Map.has_key?(query, :sparql)
        assert Map.has_key?(query, :params)
        assert Map.has_key?(query, :complexity)
        assert Map.has_key?(query, :requires_inference)
        assert Map.has_key?(query, :expected_result_factor)
      end
    end

    test "query IDs are unique" do
      ids = Enum.map(LUBMQueries.all(), & &1.id)
      assert length(ids) == length(Enum.uniq(ids))
    end

    test "query IDs follow naming convention" do
      for query <- LUBMQueries.all() do
        assert query.id |> Atom.to_string() |> String.starts_with?("q")
      end
    end
  end

  describe "get/1" do
    test "returns query by ID" do
      {:ok, query} = LUBMQueries.get(:q1)
      assert query.id == :q1
      assert String.contains?(query.name, "Q1")
    end

    test "returns error for unknown ID" do
      assert {:error, :not_found} = LUBMQueries.get(:unknown)
    end

    test "can retrieve all queries by ID" do
      for i <- 1..14 do
        id = String.to_atom("q#{i}")
        {:ok, query} = LUBMQueries.get(id)
        assert query.id == id
      end
    end
  end

  describe "get/2 with parameters" do
    test "substitutes university parameter" do
      {:ok, query} = LUBMQueries.get(:q1, uni: 5)
      assert String.contains?(query.sparql, "University5")
    end

    test "substitutes department parameter" do
      {:ok, query} = LUBMQueries.get(:q1, dept: 3)
      assert String.contains?(query.sparql, "Department3")
    end

    test "substitutes course parameter" do
      {:ok, query} = LUBMQueries.get(:q1, course: 7)
      assert String.contains?(query.sparql, "GraduateCourse7")
    end

    test "substitutes multiple parameters" do
      {:ok, query} = LUBMQueries.get(:q1, uni: 2, dept: 4, course: 6)
      assert String.contains?(query.sparql, "University2")
      assert String.contains?(query.sparql, "Department4")
      assert String.contains?(query.sparql, "GraduateCourse6")
    end

    test "uses default values when not specified" do
      {:ok, query} = LUBMQueries.get(:q1, [])
      assert String.contains?(query.sparql, "University1")
      assert String.contains?(query.sparql, "Department0")
    end
  end

  describe "namespace/0" do
    test "returns LUBM namespace" do
      ns = LUBMQueries.namespace()
      assert String.contains?(ns, "lehigh.edu")
      assert String.contains?(ns, "univ-bench")
    end
  end

  describe "estimate_results/2" do
    test "returns estimated count for simple queries" do
      result = LUBMQueries.estimate_results(:q14, 1)
      assert is_integer(result)
      assert result > 0
    end

    test "scales with scale factor" do
      result1 = LUBMQueries.estimate_results(:q6, 1)
      result5 = LUBMQueries.estimate_results(:q6, 5)

      assert result5 == result1 * 5
    end

    test "returns :varies for unknown query" do
      assert :varies == LUBMQueries.estimate_results(:unknown, 1)
    end
  end

  describe "query content validation" do
    test "all queries have valid SPARQL PREFIX declarations" do
      for query <- LUBMQueries.all() do
        assert String.contains?(query.sparql, "PREFIX")
        assert String.contains?(query.sparql, "SELECT") or String.contains?(query.sparql, "CONSTRUCT")
        assert String.contains?(query.sparql, "WHERE")
      end
    end

    test "queries with parameters have placeholder patterns" do
      for query <- LUBMQueries.all(), length(query.params) > 0 do
        # After substitution with defaults, no placeholders should remain
        {:ok, substituted} = LUBMQueries.get(query.id, [])
        refute String.contains?(substituted.sparql, "{uni}")
        refute String.contains?(substituted.sparql, "{dept}")
        refute String.contains?(substituted.sparql, "{course}")
        refute String.contains?(substituted.sparql, "{faculty}")
      end
    end

    test "complexity values are valid" do
      valid_complexities = [:simple, :medium, :complex]

      for query <- LUBMQueries.all() do
        assert query.complexity in valid_complexities
      end
    end

    test "requires_inference is boolean" do
      for query <- LUBMQueries.all() do
        assert is_boolean(query.requires_inference)
      end
    end
  end

  describe "specific query validation" do
    test "Q1 targets GraduateStudent" do
      {:ok, query} = LUBMQueries.get(:q1)
      assert String.contains?(query.sparql, "GraduateStudent")
      assert String.contains?(query.sparql, "takesCourse")
    end

    test "Q6 finds all students" do
      {:ok, query} = LUBMQueries.get(:q6)
      assert String.contains?(query.sparql, "Student")
      assert query.requires_inference == true
    end

    test "Q14 finds undergraduate students without inference" do
      {:ok, query} = LUBMQueries.get(:q14)
      assert String.contains?(query.sparql, "UndergraduateStudent")
      assert query.requires_inference == false
    end
  end
end
