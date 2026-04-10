defmodule TripleStore.Benchmark.Wikidata.PublicWorkloadsTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Wikidata.{Corpus, PublicWorkloads, Query}
  alias TripleStore.SPARQL.Parser

  describe "corpus/2" do
    test "imports WGPB queries with stable groupings and metadata" do
      assert {:ok, corpus} = PublicWorkloads.corpus(:wgpb)

      assert corpus.suite == :wgpb

      assert Corpus.query_ids(corpus) == [
               "wgpb-single-bgp-001",
               "wgpb-path-bgp-001",
               "wgpb-star-bgp-001",
               "wgpb-multi-bgp-001"
             ]

      assert Enum.map(corpus.queries, & &1.group) == [
               :single_bgp,
               :path_bgp,
               :star_bgp,
               :multi_bgp
             ]

      assert corpus.metadata.source_origin.family == :graph_patterns
    end

    test "imports WDQS queries with normalized hints and query metadata" do
      assert {:ok, corpus} = PublicWorkloads.corpus(:wdqs)
      assert {:ok, query} = Corpus.get(corpus, "wdqs-people-occupation-001")

      assert query.name == "Humans with writer occupation"
      assert query.shape == :star_bgp
      assert :label_projection in query.feature_tags
      assert query.answer_size_class == :small
      refute String.contains?(query.sparql, "hint:Query")
      assert {:ok, _ast} = Parser.parse(query.sparql)
    end

    test "expands WDBench fragments and tracks exclusions explicitly" do
      assert {:ok, corpus} = PublicWorkloads.corpus(:wdbench, tier: :smoke)

      assert Enum.map(corpus.queries, & &1.group) == [
               :single_bgp,
               :multiple_bgps,
               :optional,
               :property_paths,
               :other
             ]

      assert length(corpus.exclusions) == 1
      assert hd(corpus.exclusions).classification == :requires_manual_rewrite
      assert Enum.all?(corpus.queries, &String.contains?(&1.sparql, "LIMIT 25"))
    end
  end

  describe "all_queries/1" do
    test "returns normalized query structs across all public suites" do
      queries = PublicWorkloads.all_queries(tier: :medium)

      assert Enum.all?(queries, &match?(%Query{}, &1))

      assert Enum.map(queries, & &1.manifest.suite) |> Enum.sort() == [
               :wdbench,
               :wdbench,
               :wdbench,
               :wdbench,
               :wdbench,
               :wdqs,
               :wdqs,
               :wdqs,
               :wgpb,
               :wgpb,
               :wgpb,
               :wgpb
             ]
    end
  end
end
