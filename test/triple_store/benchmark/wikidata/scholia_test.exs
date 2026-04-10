defmodule TripleStore.Benchmark.Wikidata.ScholiaTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Wikidata.{Corpus, Scholia}
  alias TripleStore.SPARQL.Parser

  test "loads normalized templates from local assets" do
    templates = Scholia.templates()

    assert Enum.map(templates, & &1.template_id) == [
             "scholia-author-works",
             "scholia-work-citations",
             "scholia-organization-members",
             "scholia-field-members"
           ]

    author_template = Enum.find(templates, &(&1.template_id == "scholia-author-works"))
    refute String.contains?(author_template.sparql, "SERVICE wikibase:label")

    citation_template = Enum.find(templates, &(&1.template_id == "scholia-work-citations"))
    refute String.contains?(citation_template.sparql, "hint:Query")
  end

  test "materializes a smoke corpus with variants and explicit fallback exclusions" do
    assert {:ok, corpus} = Scholia.corpus(:smoke)

    assert corpus.suite == :scholia
    assert Enum.all?(corpus.queries, &(&1.manifest.suite == :scholia))
    assert Enum.any?(corpus.queries, &(&1.manifest.execution_variant == :count_only))
    assert Enum.any?(corpus.queries, &(&1.manifest.execution_variant == :distinct_only))
    assert Enum.any?(corpus.exclusions, &(&1.reason == :no_valid_instantiation))

    sample_query =
      Enum.find(corpus.queries, &(&1.manifest.benchmark_id == "scholia-author-works-q42-raw"))

    assert sample_query.template_metadata.representative_label == "Douglas Adams"
    assert sample_query.template_metadata.fallback_behavior == :skip_when_missing
    refute String.contains?(sample_query.sparql, "%entity%")
    assert {:ok, _ast} = Parser.parse(sample_query.sparql)
  end

  test "parameter bindings include entity, class, and common template variables" do
    template = Enum.find(Scholia.templates(), &(&1.template_id == "scholia-organization-members"))
    organization = hd(Scholia.representatives(:smoke).organization)
    bindings = Scholia.parameter_bindings(template, organization, language: "en", limit: 40)

    assert bindings.entity == "http://www.wikidata.org/entity/QOrg1"
    assert bindings.class == "http://www.wikidata.org/entity/Q5"
    assert bindings.language == "en"
    assert bindings.limit == "40"
  end

  test "medium-tier corpus instantiates templates that fall back on smoke" do
    assert {:ok, corpus} = Scholia.corpus(:medium)

    assert {:ok, query} = Corpus.get(corpus, "scholia-field-members-qfow1-raw")
    assert query.template_metadata.class_id == :field_of_work
    assert {:ok, _ast} = Parser.parse(query.sparql)
  end
end
