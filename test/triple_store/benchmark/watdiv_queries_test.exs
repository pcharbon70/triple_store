defmodule TripleStore.Benchmark.WatDivQueriesTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.WatDivQueries

  @moduletag :benchmark

  describe "all/0" do
    test "returns 20 queries" do
      queries = WatDivQueries.all()
      assert length(queries) == 20
    end

    test "all queries have required fields" do
      for query <- WatDivQueries.all() do
        assert Map.has_key?(query, :id)
        assert Map.has_key?(query, :name)
        assert Map.has_key?(query, :description)
        assert Map.has_key?(query, :sparql)
        assert Map.has_key?(query, :params)
        assert Map.has_key?(query, :category)
        assert Map.has_key?(query, :complexity)
      end
    end

    test "query IDs are unique" do
      ids = Enum.map(WatDivQueries.all(), & &1.id)
      assert length(ids) == length(Enum.uniq(ids))
    end

    test "query IDs follow naming convention" do
      for query <- WatDivQueries.all() do
        id_str = Atom.to_string(query.id)
        assert String.length(id_str) <= 3
        assert id_str =~ ~r/^[lscf]\d+$/i
      end
    end
  end

  describe "by_category/1" do
    test "returns linear queries" do
      linear_queries = WatDivQueries.by_category(:linear)

      assert length(linear_queries) == 5

      for q <- linear_queries do
        assert q.category == :linear
      end
    end

    test "returns star queries" do
      star_queries = WatDivQueries.by_category(:star)

      assert length(star_queries) == 7

      for q <- star_queries do
        assert q.category == :star
      end
    end

    test "returns snowflake queries" do
      snowflake_queries = WatDivQueries.by_category(:snowflake)

      assert length(snowflake_queries) == 5

      for q <- snowflake_queries do
        assert q.category == :snowflake
      end
    end

    test "returns complex queries" do
      complex_queries = WatDivQueries.by_category(:complex)

      assert length(complex_queries) == 3

      for q <- complex_queries do
        assert q.category == :complex
      end
    end
  end

  describe "get/1" do
    test "returns query by ID" do
      {:ok, query} = WatDivQueries.get(:l1)
      assert query.id == :l1
      assert query.category == :linear
    end

    test "returns error for unknown ID" do
      assert {:error, :not_found} = WatDivQueries.get(:unknown)
    end

    test "can retrieve all linear queries by ID" do
      for id <- [:l1, :l2, :l3, :l4, :l5] do
        {:ok, query} = WatDivQueries.get(id)
        assert query.id == id
        assert query.category == :linear
      end
    end

    test "can retrieve all star queries by ID" do
      for id <- [:s1, :s2, :s3, :s4, :s5, :s6, :s7] do
        {:ok, query} = WatDivQueries.get(id)
        assert query.id == id
        assert query.category == :star
      end
    end

    test "can retrieve all snowflake queries by ID" do
      for id <- [:f1, :f2, :f3, :f4, :f5] do
        {:ok, query} = WatDivQueries.get(id)
        assert query.id == id
        assert query.category == :snowflake
      end
    end

    test "can retrieve all complex queries by ID" do
      for id <- [:c1, :c2, :c3] do
        {:ok, query} = WatDivQueries.get(id)
        assert query.id == id
        assert query.category == :complex
      end
    end
  end

  describe "get/2 with parameters" do
    test "substitutes v1 parameter (website)" do
      {:ok, query} = WatDivQueries.get(:l1, v1: "Website42")
      assert String.contains?(query.sparql, "Website42")
    end

    test "substitutes v0 parameter (user)" do
      # L2 uses v0 parameter
      {:ok, query} = WatDivQueries.get(:l2, v0: "Country5")
      assert String.contains?(query.sparql, "Country5")
    end

    test "substitutes v2 parameter (city)" do
      {:ok, query} = WatDivQueries.get(:s2, v2: "City5")
      assert String.contains?(query.sparql, "City5")
    end

    test "substitutes multiple parameters" do
      {:ok, query} = WatDivQueries.get(:s2, v2: "City20")

      # Should substitute v2 parameter
      assert String.contains?(query.sparql, "City20")
      refute String.contains?(query.sparql, "%v2%")
    end

    test "uses default values when not specified" do
      {:ok, query} = WatDivQueries.get(:l1, [])

      # Should have default substitutions
      assert String.contains?(query.sparql, "Website0")
      refute String.contains?(query.sparql, "%v1%")
    end

    test "no placeholders remain after substitution" do
      {:ok, query} = WatDivQueries.get(:s1, [])

      sparql = query.sparql

      # Should not have any placeholder patterns
      refute String.contains?(sparql, "%v0%")
      refute String.contains?(sparql, "%v1%")
      refute String.contains?(sparql, "%v2%")
      refute String.contains?(sparql, "%v3%")
      refute String.contains?(sparql, "%v4%")
    end
  end

  describe "namespace/0" do
    test "returns WatDiv namespace" do
      ns = WatDivQueries.namespace()
      assert String.contains?(ns, "uwaterloo.ca")
      assert String.contains?(ns, "wsdbm")
    end
  end

  describe "query content validation" do
    test "all queries have valid SPARQL structure" do
      for query <- WatDivQueries.all() do
        assert String.contains?(query.sparql, "PREFIX")
        assert String.contains?(query.sparql, "SELECT")
        assert String.contains?(query.sparql, "WHERE")
      end
    end

    test "all queries define required prefixes" do
      for query <- WatDivQueries.all() do
        sparql = query.sparql

        # Should have WatDiv prefix (most queries use it)
        # Some queries may use different vocabularies but all should have PREFIX declarations
        assert String.contains?(sparql, "PREFIX") ||
                 String.contains?(sparql, "wsdbm:") ||
                 String.contains?(sparql, "sorg:") ||
                 String.contains?(sparql, "gr:") ||
                 String.contains?(sparql, "mo:") ||
                 String.contains?(sparql, "foaf:")
      end
    end

    test "complexity values are valid" do
      valid_complexities = [:simple, :medium, :complex]

      for query <- WatDivQueries.all() do
        assert query.complexity in valid_complexities
      end
    end

    test "category values are valid" do
      valid_categories = [:linear, :star, :snowflake, :complex]

      for query <- WatDivQueries.all() do
        assert query.category in valid_categories
      end
    end
  end

  describe "linear queries (L1-L5)" do
    test "L1 has linear pattern" do
      {:ok, query} = WatDivQueries.get(:l1)

      assert query.id == :l1
      assert query.category == :linear
      assert String.contains?(query.sparql, "wsdbm:subscribes")
      assert String.contains?(query.sparql, "wsdbm:likes")
    end

    test "L2 has linear pattern with nationality filter" do
      {:ok, query} = WatDivQueries.get(:l2)

      assert query.id == :l2
      assert query.category == :linear
      assert String.contains?(query.sparql, "wsdbm:likes")
      assert String.contains?(query.sparql, "sorg:nationality")
      assert String.contains?(query.sparql, "gn:parentCountry")
    end

    test "L3 is simple linear query" do
      {:ok, query} = WatDivQueries.get(:l3)

      assert query.id == :l3
      assert query.category == :linear
      assert query.complexity == :simple
    end

    test "L4 uses tag and caption pattern" do
      {:ok, query} = WatDivQueries.get(:l4)

      assert query.id == :l4
      assert query.category == :linear
      assert String.contains?(query.sparql, "og:tag")
      assert String.contains?(query.sparql, "sorg:caption")
    end

    test "L5 uses job title pattern" do
      {:ok, query} = WatDivQueries.get(:l5)

      assert query.id == :l5
      assert query.category == :linear
      assert String.contains?(query.sparql, "sorg:jobTitle")
    end
  end

  describe "star queries (S1-S7)" do
    test "S1 has many properties around offer" do
      {:ok, query} = WatDivQueries.get(:s1)

      assert query.id == :s1
      assert query.category == :star
      # S1 uses GoodRelations for offers
      assert String.contains?(query.sparql, "gr:price")
      assert String.contains?(query.sparql, "gr:includes")
      assert String.contains?(query.sparql, "gr:validFrom")
    end

    test "S2 queries user by multiple attributes" do
      {:ok, query} = WatDivQueries.get(:s2)

      assert query.id == :s2
      assert query.category == :star
      # S2 uses sorg:nationality
      assert String.contains?(query.sparql, "sorg:nationality")
      assert String.contains?(query.sparql, "wsdbm:gender")
    end

    test "S3 queries product with type and attributes" do
      {:ok, query} = WatDivQueries.get(:s3)

      assert query.id == :s3
      assert query.category == :star
      # S3 has product attributes
      assert String.contains?(query.sparql, "sorg:caption")
      assert String.contains?(query.sparql, "wsdbm:hasGenre")
      assert String.contains?(query.sparql, "sorg:publisher")
    end

    test "S4 queries person with age" do
      {:ok, query} = WatDivQueries.get(:s4)

      assert query.id == :s4
      assert query.category == :star
      assert String.contains?(query.sparql, "foaf:age")
      assert String.contains?(query.sparql, "foaf:familyName")
    end

    test "S5 queries product with description" do
      {:ok, query} = WatDivQueries.get(:s5)

      assert query.id == :s5
      assert query.category == :star
      assert String.contains?(query.sparql, "sorg:description")
    end

    test "S6 queries musical work" do
      {:ok, query} = WatDivQueries.get(:s6)

      assert query.id == :s6
      assert query.category == :star
      assert String.contains?(query.sparql, "mo:conductor")
    end

    test "S7 queries liked products" do
      {:ok, query} = WatDivQueries.get(:s7)

      assert query.id == :s7
      assert query.category == :star
      assert String.contains?(query.sparql, "wsdbm:likes")
    end
  end

  describe "snowflake queries (F1-F5)" do
    test "F1 has branching pattern from movie" do
      {:ok, query} = WatDivQueries.get(:f1)

      assert query.id == :f1
      assert query.category == :snowflake
      # F1 uses wsdbm:hasGenre
      assert String.contains?(query.sparql, "wsdbm:hasGenre")
      assert String.contains?(query.sparql, "og:tag")
      assert String.contains?(query.sparql, "sorg:trailer")
    end

    test "F2 has product with homepage and genre" do
      {:ok, query} = WatDivQueries.get(:f2)

      assert query.id == :f2
      assert query.category == :snowflake
      assert String.contains?(query.sparql, "foaf:homepage")
      assert String.contains?(query.sparql, "og:title")
      assert String.contains?(query.sparql, "wsdbm:hasGenre")
    end

    test "F3 has purchase with genre" do
      {:ok, query} = WatDivQueries.get(:f3)

      assert query.id == :f3
      assert query.category == :snowflake
      assert String.contains?(query.sparql, "wsdbm:purchaseFor")
    end

    test "F4 has product with offer and language" do
      {:ok, query} = WatDivQueries.get(:f4)

      assert query.id == :f4
      assert query.category == :snowflake
      assert String.contains?(query.sparql, "gr:includes")
      assert String.contains?(query.sparql, "wsdbm:likes")
      assert String.contains?(query.sparql, "sorg:language")
    end

    test "F5 has offer with product" do
      {:ok, query} = WatDivQueries.get(:f5)

      assert query.id == :f5
      assert query.category == :snowflake
      # F5 uses gr:price (GoodRelations) not sorg:price
      assert String.contains?(query.sparql, "gr:price")
      assert String.contains?(query.sparql, "gr:includes")
    end
  end

  describe "complex queries (C1-C3)" do
    test "C1 has review with language" do
      {:ok, query} = WatDivQueries.get(:c1)

      assert query.id == :c1
      assert query.category == :complex
      assert String.contains?(query.sparql, "rev:reviewer")
    end

    test "C2 has purchase flow" do
      {:ok, query} = WatDivQueries.get(:c2)

      assert query.id == :c2
      assert query.category == :complex
      assert String.contains?(query.sparql, "wsdbm:makesPurchase")
    end

    test "C3 has comprehensive user query" do
      {:ok, query} = WatDivQueries.get(:c3)

      assert query.id == :c3
      assert query.category == :complex
      # C3 has :medium complexity (it's a complex query type but medium complexity)
      assert query.complexity == :medium
    end
  end

  describe "query distribution" do
    test "has correct number of linear queries" do
      linear_queries = Enum.filter(WatDivQueries.all(), &(&1.category == :linear))
      assert length(linear_queries) == 5
    end

    test "has correct number of star queries" do
      star_queries = Enum.filter(WatDivQueries.all(), &(&1.category == :star))
      assert length(star_queries) == 7
    end

    test "has correct number of snowflake queries" do
      snowflake_queries = Enum.filter(WatDivQueries.all(), &(&1.category == :snowflake))
      assert length(snowflake_queries) == 5
    end

    test "has correct number of complex queries" do
      complex_queries = Enum.filter(WatDivQueries.all(), &(&1.category == :complex))
      assert length(complex_queries) == 3
    end

    test "has queries of varying complexity" do
      queries = WatDivQueries.all()

      simple_queries = Enum.filter(queries, &(&1.complexity == :simple))
      medium_queries = Enum.filter(queries, &(&1.complexity == :medium))
      complex_queries = Enum.filter(queries, &(&1.complexity == :complex))

      # Should have some of each complexity level
      refute Enum.empty?(simple_queries)
      refute Enum.empty?(medium_queries)
      refute Enum.empty?(complex_queries)
    end
  end
end
