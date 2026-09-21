defmodule TripleStore.ExternalIdentifierSafetyTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.Wikidata.Baseline
  alias TripleStore.Query.Cache
  alias TripleStore.Reasoner.{Rule, RuleCompiler, RuleOptimizer}
  alias TripleStore.SPARQL.PlanCache
  alias TripleStore.SPARQL.Update.Helpers

  test "reasoning specialization and batching keep external identifiers as binaries" do
    properties = Enum.map(1..250, &"https://example.test/property/#{&1}")
    schema = %{RuleCompiler.empty_schema_info() | transitive_properties: properties}

    # Warm modules and finite built-in rule atoms before taking the baseline.
    {:ok, warmed} = compile_schema(schema)
    RuleOptimizer.batch_rules(warmed.specialized_rules)
    :erlang.garbage_collect()
    before_count = :erlang.system_info(:atom_count)

    {:ok, first} = compile_schema(schema)
    {:ok, second} = compile_schema(schema)
    first_names = Enum.map(first.specialized_rules, & &1.name)
    second_names = Enum.map(second.specialized_rules, & &1.name)

    assert first_names == second_names
    assert length(first_names) == 250
    assert Enum.all?(first_names, &is_binary/1)
    assert length(Enum.uniq(first_names)) == 250

    batches = RuleOptimizer.batch_rules(first.specialized_rules)
    assert batches != []
    assert Enum.all?(batches, &is_binary(&1.name))

    :erlang.garbage_collect()
    assert :erlang.system_info(:atom_count) == before_count
  end

  test "rule constructors accept stable binary identifiers" do
    rule =
      Rule.new(
        "caller-rule-1",
        [{:pattern, [{:var, "s"}, {:iri, "https://example.test/p"}, {:var, "o"}]}],
        {:pattern, [{:var, "s"}, {:iri, "https://example.test/q"}, {:var, "o"}]}
      )

    assert rule.name == "caller-rule-1"
  end

  test "cache ETS tables are unnamed and independent of registered process names" do
    query_name = Module.concat(__MODULE__, QueryCache)
    plan_name = Module.concat(__MODULE__, QueryPlanCache)
    start_supervised!({Cache, name: query_name})
    start_supervised!({PlanCache, name: plan_name})

    query_state = :sys.get_state(query_name)
    plan_state = :sys.get_state(plan_name)

    assert is_reference(query_state.results_table)
    assert is_reference(query_state.lru_table)
    assert is_reference(query_state.predicate_index_table)
    assert is_reference(plan_state.plans_table)
    assert is_reference(plan_state.lru_table)
  end

  test "update property lookup compares atom keys without creating atoms" do
    external_key = "never_existing_update_key_#{System.unique_integer([:positive])}"

    assert Helpers.get_prop([graph: "https://example.test/g"], external_key, :missing) ==
             :missing

    before_count = :erlang.system_info(:atom_count)

    assert Helpers.get_prop([graph: "https://example.test/g"], external_key, :missing) ==
             :missing

    assert :erlang.system_info(:atom_count) == before_count
  end

  test "benchmark JSON preserves unknown keys and enum values as binaries" do
    path = Path.join(System.tmp_dir!(), "baseline-#{System.unique_integer([:positive])}.json")

    json = %{
      "schema_version" => 1,
      "baseline_id" => "external-identifiers",
      "generated_at" => "2026-09-21T00:00:00Z",
      "entries" => [
        %{
          "benchmark_id" => "q1",
          "query_name" => "query",
          "suite" => "external-suite",
          "execution_variant" => "external-variant",
          "answer_record" => %{"external-field" => "value"}
        }
      ]
    }

    File.write!(path, Jason.encode!(json))
    on_exit(fn -> File.rm(path) end)

    assert {:ok, baseline} = Baseline.load_json(path)
    [entry] = baseline.entries
    assert entry.suite == "external-suite"
    assert entry.execution_variant == "external-variant"
    assert entry.answer_record["external-field"] == "value"
  end

  defp compile_schema(schema) do
    RuleCompiler.compile_with_schema(schema,
      profile: :owl2rl,
      specialize: true,
      max_specializations: 500
    )
  end
end
