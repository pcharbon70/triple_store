defmodule TripleStore.Benchmark.GMarkQueriesTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.GMarkQueries

  @moduletag :benchmark

  describe "all/0" do
    test "returns 11 queries" do
      queries = GMarkQueries.all()
      assert length(queries) == 11
    end

    test "all queries have required fields" do
      for query <- GMarkQueries.all() do
        assert Map.has_key?(query, :id)
        assert Map.has_key?(query, :name)
        assert Map.has_key?(query, :description)
        assert Map.has_key?(query, :sparql)
        assert Map.has_key?(query, :params)
        assert Map.has_key?(query, :selectivity)
        assert Map.has_key?(query, :complexity)
      end
    end

    test "query IDs are unique" do
      ids = Enum.map(GMarkQueries.all(), & &1.id)
      assert length(ids) == length(Enum.uniq(ids))
    end

    test "query IDs follow naming convention" do
      for query <- GMarkQueries.all() do
        id_str = Atom.to_string(query.id)
        assert String.length(id_str) <= 3
        assert id_str =~ ~r/^[clq]\d+$/i
      end
    end
  end

  describe "by_selectivity/1" do
    test "returns constant queries" do
      constant_queries = GMarkQueries.by_selectivity(:constant)

      assert length(constant_queries) == 3

      for q <- constant_queries do
        assert q.selectivity == :constant
      end
    end

    test "returns linear queries" do
      linear_queries = GMarkQueries.by_selectivity(:linear)

      assert length(linear_queries) == 5

      for q <- linear_queries do
        assert q.selectivity == :linear
      end
    end

    test "returns quadratic queries" do
      quadratic_queries = GMarkQueries.by_selectivity(:quadratic)

      assert length(quadratic_queries) == 3

      for q <- quadratic_queries do
        assert q.selectivity == :quadratic
      end
    end
  end

  describe "get/1" do
    test "returns query by ID" do
      {:ok, query} = GMarkQueries.get(:c1)
      assert query.id == :c1
      assert query.selectivity == :constant
    end

    test "returns error for unknown ID" do
      assert {:error, :not_found} = GMarkQueries.get(:unknown)
    end

    test "can retrieve all constant queries by ID" do
      for id <- [:c1, :c2, :c3] do
        {:ok, query} = GMarkQueries.get(id)
        assert query.id == id
        assert query.selectivity == :constant
      end
    end

    test "can retrieve all linear queries by ID" do
      for id <- [:l1, :l2, :l3, :l4, :l5] do
        {:ok, query} = GMarkQueries.get(id)
        assert query.id == id
        assert query.selectivity == :linear
      end
    end

    test "can retrieve all quadratic queries by ID" do
      for id <- [:q1, :q2, :q3] do
        {:ok, query} = GMarkQueries.get(id)
        assert query.id == id
        assert query.selectivity == :quadratic
      end
    end
  end

  describe "get/2 with parameters" do
    test "substitutes researcher parameter" do
      {:ok, query} = GMarkQueries.get(:l1, researcher: "Researcher42")
      assert String.contains?(query.sparql, "Researcher42")
    end

    test "substitutes conference parameter" do
      {:ok, query} = GMarkQueries.get(:l2, conference: "Conference10")
      assert String.contains?(query.sparql, "Conference10")
    end

    test "substitutes journal parameter" do
      {:ok, query} = GMarkQueries.get(:l3, journal: "Journal5")
      assert String.contains?(query.sparql, "Journal5")
    end

    test "substitutes year parameter" do
      {:ok, query} = GMarkQueries.get(:l5, year: "2020")
      assert String.contains?(query.sparql, "2020")
    end

    test "substitutes multiple parameters" do
      {:ok, query} = GMarkQueries.get(:l4, researcher: "Researcher1", year: "2021")

      assert String.contains?(query.sparql, "Researcher1")
      assert String.contains?(query.sparql, "2021")
    end

    test "uses full URI if provided" do
      {:ok, query} = GMarkQueries.get(:l1, researcher: "http://example.org/Researcher1")
      assert String.contains?(query.sparql, "http://example.org/Researcher1")
    end
  end

  describe "namespace/0" do
    test "returns gMark namespace" do
      ns = GMarkQueries.namespace()
      assert String.contains?(ns, "gmark.example.org")
    end
  end

  describe "query content validation" do
    test "all queries have valid SPARQL structure" do
      for query <- GMarkQueries.all() do
        assert String.contains?(query.sparql, "PREFIX")
        assert String.contains?(query.sparql, "SELECT")
        assert String.contains?(query.sparql, "WHERE")
      end
    end

    test "all queries define gMark prefix" do
      for query <- GMarkQueries.all() do
        assert String.contains?(query.sparql, "PREFIX gmark:")
        assert String.contains?(query.sparql, "gmark.example.org")
      end
    end

    test "complexity values are valid" do
      valid_complexities = [:simple, :medium, :complex]

      for query <- GMarkQueries.all() do
        assert query.complexity in valid_complexities
      end
    end

    test "selectivity values are valid" do
      valid_selectivities = [:constant, :linear, :quadratic]

      for query <- GMarkQueries.all() do
        assert query.selectivity in valid_selectivities
      end
    end
  end

  describe "constant queries (C1-C3)" do
    test "C1 queries cities" do
      {:ok, query} = GMarkQueries.get(:c1)

      assert query.id == :c1
      assert query.selectivity == :constant
      assert String.contains?(query.sparql, "gmark:City")
      assert String.contains?(query.sparql, "gmark:heldIn")
    end

    test "C2 queries journal publishers" do
      {:ok, query} = GMarkQueries.get(:c2)

      assert query.id == :c2
      assert query.selectivity == :constant
      assert String.contains?(query.sparql, "gmark:Journal")
      assert String.contains?(query.sparql, "gmark:publisher")
    end

    test "C3 queries affiliations" do
      {:ok, query} = GMarkQueries.get(:c3)

      assert query.id == :c3
      assert query.selectivity == :constant
      assert String.contains?(query.sparql, "gmark:Researcher")
      assert String.contains?(query.sparql, "gmark:affiliation")
    end
  end

  describe "linear queries (L1-L5)" do
    test "L1 queries papers by researcher" do
      {:ok, query} = GMarkQueries.get(:l1)

      assert query.id == :l1
      assert query.selectivity == :linear
      assert String.contains?(query.sparql, "gmark:Paper")
      assert String.contains?(query.sparql, "gmark:authors")
    end

    test "L2 queries papers by conference" do
      {:ok, query} = GMarkQueries.get(:l2)

      assert query.id == :l2
      assert query.selectivity == :linear
      assert String.contains?(query.sparql, "gmark:publishedIn")
    end

    test "L3 queries papers by journal" do
      {:ok, query} = GMarkQueries.get(:l3)

      assert query.id == :l3
      assert query.selectivity == :linear
      assert String.contains?(query.sparql, "gmark:extendedTo")
    end

    test "L4 uses researcher and year filters" do
      {:ok, query} = GMarkQueries.get(:l4)

      assert query.id == :l4
      assert query.selectivity == :linear
      assert String.contains?(query.sparql, "<%researcher%>")
      assert String.contains?(query.sparql, "<%year%>")
    end

    test "L5 queries conferences by year" do
      {:ok, query} = GMarkQueries.get(:l5)

      assert query.id == :l5
      assert query.selectivity == :linear
      assert String.contains?(query.sparql, "gmark:Conference")
      assert String.contains?(query.sparql, "<%year%>")
    end
  end

  describe "quadratic queries (Q1-Q3)" do
    test "Q1 finds co-authorship pairs" do
      {:ok, query} = GMarkQueries.get(:q1)

      assert query.id == :q1
      assert query.selectivity == :quadratic
      assert String.contains?(query.sparql, "gmark:authors")
      assert String.contains?(query.sparql, "FILTER(?researcher1 < ?researcher2)")
    end

    test "Q2 finds papers in same conference" do
      {:ok, query} = GMarkQueries.get(:q2)

      assert query.id == :q2
      assert query.selectivity == :quadratic
      assert String.contains?(query.sparql, "gmark:publishedIn")
      assert String.contains?(query.sparql, "FILTER(?paper1 < ?paper2)")
    end

    test "Q3 uses transitive closure (recursive)" do
      {:ok, query} = GMarkQueries.get(:q3)

      assert query.id == :q3
      assert query.selectivity == :quadratic
      # Kleene plus
      assert String.contains?(query.sparql, "+")
      assert query.complexity == :complex
    end
  end

  describe "query distribution" do
    test "has correct number of constant queries" do
      constant_queries = Enum.filter(GMarkQueries.all(), &(&1.selectivity == :constant))
      assert length(constant_queries) == 3
    end

    test "has correct number of linear queries" do
      linear_queries = Enum.filter(GMarkQueries.all(), &(&1.selectivity == :linear))
      assert length(linear_queries) == 5
    end

    test "has correct number of quadratic queries" do
      quadratic_queries = Enum.filter(GMarkQueries.all(), &(&1.selectivity == :quadratic))
      assert length(quadratic_queries) == 3
    end

    test "has queries of varying complexity" do
      queries = GMarkQueries.all()

      simple_queries = Enum.filter(queries, &(&1.complexity == :simple))
      medium_queries = Enum.filter(queries, &(&1.complexity == :medium))
      complex_queries = Enum.filter(queries, &(&1.complexity == :complex))

      # Should have some of each complexity level
      assert length(simple_queries) > 0
      assert length(medium_queries) > 0
      assert length(complex_queries) > 0
    end
  end
end
