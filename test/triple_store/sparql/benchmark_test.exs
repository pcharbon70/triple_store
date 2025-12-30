defmodule TripleStore.SPARQL.BenchmarkTest do
  @moduledoc """
  Performance benchmarks for the SPARQL query engine.

  Task 2.7.4: Performance Benchmarking
  - 2.7.4.1 Benchmark simple BGP query: target <10ms on 1M triples
  - 2.7.4.2 Benchmark star query (5 patterns): target <100ms on 1M triples
  - 2.7.4.3 Benchmark OPTIONAL query: measure overhead vs inner join
  - 2.7.4.4 Benchmark aggregation: measure grouping cost

  Run benchmarks with:
    mix test test/triple_store/sparql/benchmark_test.exs --include benchmark

  These tests are excluded by default due to long runtime.
  """

  use ExUnit.Case, async: false

  alias TripleStore.SPARQL.Query
  alias TripleStore.Index
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Backend.RocksDB.NIF

  @moduletag :tmp_dir
  @moduletag :benchmark

  # Dataset sizes for benchmarks
  # Use smaller sizes for CI, larger for local benchmarking
  @small_dataset 10_000
  @medium_dataset 100_000
  @large_dataset 1_000_000

  # Number of warmup and measurement iterations
  @warmup_runs 3
  @measurement_runs 10

  # ===========================================================================
  # Setup Helpers
  # ===========================================================================

  defp setup_db(tmp_dir) do
    db_path = Path.join(tmp_dir, "bench_db_#{:erlang.unique_integer([:positive])}")
    {:ok, db} = NIF.open(db_path)
    {:ok, manager} = Manager.start_link(db: db)
    {db, manager}
  end

  defp cleanup({_db, manager}) do
    Manager.stop(manager)
  end

  defp add_triple(db, manager, {s_term, p_term, o_term}) do
    {:ok, s_id} = Manager.get_or_create_id(manager, term_to_rdf(s_term))
    {:ok, p_id} = Manager.get_or_create_id(manager, term_to_rdf(p_term))
    {:ok, o_id} = Manager.get_or_create_id(manager, term_to_rdf(o_term))
    :ok = Index.insert_triple(db, {s_id, p_id, o_id})
  end

  defp term_to_rdf({:named_node, uri}), do: RDF.iri(uri)
  defp term_to_rdf({:literal, :simple, value}), do: RDF.literal(value)

  defp term_to_rdf({:literal, :typed, value, type}) do
    RDF.literal(value, datatype: type)
  end

  defp iri(uri), do: {:named_node, uri}
  defp literal(value), do: {:literal, :simple, value}
  defp typed_literal(value, type), do: {:literal, :typed, value, type}

  # ===========================================================================
  # Data Generation
  # ===========================================================================

  defp generate_simple_dataset(db, manager, count) do
    # Generate triples of the form: subject_N predicate object_N
    # Uses a single predicate for simple BGP pattern matching
    predicate = iri("http://ex.org/value")

    for i <- 1..count do
      subject = iri("http://ex.org/entity/#{i}")
      object = literal("value_#{i}")
      add_triple(db, manager, {subject, predicate, object})
    end

    :ok
  end

  defp generate_star_dataset(db, manager, count) do
    # Generate entities with 5 properties each for star queries
    # Each entity has: name, age, city, category, score
    predicates = [
      iri("http://ex.org/name"),
      iri("http://ex.org/age"),
      iri("http://ex.org/city"),
      iri("http://ex.org/category"),
      iri("http://ex.org/score")
    ]

    cities = ["NYC", "LA", "Chicago", "Houston", "Phoenix"]
    categories = ["A", "B", "C", "D", "E"]

    for i <- 1..count do
      subject = iri("http://ex.org/entity/#{i}")
      city_idx = rem(i - 1, 5)
      cat_idx = rem(div(i - 1, 5), 5)

      add_triple(db, manager, {subject, Enum.at(predicates, 0), literal("Entity_#{i}")})

      add_triple(
        db,
        manager,
        {subject, Enum.at(predicates, 1),
         typed_literal(20 + rem(i, 50), "http://www.w3.org/2001/XMLSchema#integer")}
      )

      add_triple(
        db,
        manager,
        {subject, Enum.at(predicates, 2), literal(Enum.at(cities, city_idx))}
      )

      add_triple(
        db,
        manager,
        {subject, Enum.at(predicates, 3), literal(Enum.at(categories, cat_idx))}
      )

      add_triple(
        db,
        manager,
        {subject, Enum.at(predicates, 4),
         typed_literal(rem(i, 100), "http://www.w3.org/2001/XMLSchema#integer")}
      )
    end

    :ok
  end

  defp generate_optional_dataset(db, manager, count) do
    # Generate dataset where ~50% have optional property
    required_pred = iri("http://ex.org/name")
    optional_pred = iri("http://ex.org/email")

    for i <- 1..count do
      subject = iri("http://ex.org/person/#{i}")
      add_triple(db, manager, {subject, required_pred, literal("Person_#{i}")})

      # Only 50% have email
      if rem(i, 2) == 0 do
        add_triple(db, manager, {subject, optional_pred, literal("person#{i}@ex.org")})
      end
    end

    :ok
  end

  defp generate_aggregation_dataset(db, manager, count) do
    # Generate dataset for aggregation benchmarks
    # Entities grouped by category with numeric values
    category_pred = iri("http://ex.org/category")
    value_pred = iri("http://ex.org/amount")

    categories = for i <- 1..100, do: "Category_#{i}"
    num_categories = length(categories)

    for i <- 1..count do
      subject = iri("http://ex.org/sale/#{i}")
      category = Enum.at(categories, rem(i - 1, num_categories))
      amount = 10 + rem(i, 990)

      add_triple(db, manager, {subject, category_pred, literal(category)})

      add_triple(
        db,
        manager,
        {subject, value_pred, typed_literal(amount, "http://www.w3.org/2001/XMLSchema#integer")}
      )
    end

    :ok
  end

  # ===========================================================================
  # Benchmark Utilities
  # ===========================================================================

  defp measure_query(ctx, query, runs \\ @measurement_runs) do
    # Warmup
    for _ <- 1..@warmup_runs do
      {:ok, _} = Query.query(ctx, query)
    end

    # Measure
    times =
      for _ <- 1..runs do
        {time_us, {:ok, results}} = :timer.tc(fn -> Query.query(ctx, query) end)
        {time_us, length(results)}
      end

    timings = Enum.map(times, fn {t, _} -> t end)
    result_counts = Enum.map(times, fn {_, c} -> c end)

    %{
      min_us: Enum.min(timings),
      max_us: Enum.max(timings),
      avg_us: div(Enum.sum(timings), runs),
      median_us: Enum.sort(timings) |> Enum.at(div(runs, 2)),
      result_count: hd(result_counts)
    }
  end

  defp format_time(us) when us < 1_000, do: "#{us}µs"
  defp format_time(us) when us < 1_000_000, do: "#{Float.round(us / 1_000, 2)}ms"
  defp format_time(us), do: "#{Float.round(us / 1_000_000, 2)}s"

  defp report_benchmark(name, stats, target_us \\ nil) do
    IO.puts("")
    IO.puts("=== #{name} ===")
    IO.puts("  Min:    #{format_time(stats.min_us)}")
    IO.puts("  Max:    #{format_time(stats.max_us)}")
    IO.puts("  Avg:    #{format_time(stats.avg_us)}")
    IO.puts("  Median: #{format_time(stats.median_us)}")
    IO.puts("  Results: #{stats.result_count}")

    if target_us do
      status = if stats.median_us <= target_us, do: "✅ PASS", else: "❌ FAIL"
      IO.puts("  Target: #{format_time(target_us)} #{status}")
    end

    stats
  end

  # ===========================================================================
  # 2.7.4.1 - Simple BGP Query Benchmark
  # ===========================================================================

  describe "2.7.4.1 - Simple BGP Query Benchmark" do
    @describetag :benchmark

    @tag timeout: 600_000
    test "benchmark simple BGP on small dataset (10K triples)", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      IO.puts("\n\n📊 Generating #{@small_dataset} triples for simple BGP benchmark...")
      {gen_time, :ok} = :timer.tc(fn -> generate_simple_dataset(db, manager, @small_dataset) end)
      IO.puts("   Data generation: #{format_time(gen_time)}")

      # Query: Find all entities with a value
      query = """
      SELECT ?s ?v
      WHERE { ?s <http://ex.org/value> ?v }
      LIMIT 1000
      """

      stats = measure_query(ctx, query)
      report_benchmark("Simple BGP (10K triples, LIMIT 1000)", stats)

      # Target: <10ms for 10K (scaled from 1M)
      assert stats.median_us < 10_000, "Simple BGP should complete in <10ms"

      cleanup({db, manager})
    end

    @tag timeout: 600_000
    test "benchmark simple BGP on medium dataset (100K triples)", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      IO.puts("\n\n📊 Generating #{@medium_dataset} triples for simple BGP benchmark...")
      {gen_time, :ok} = :timer.tc(fn -> generate_simple_dataset(db, manager, @medium_dataset) end)
      IO.puts("   Data generation: #{format_time(gen_time)}")

      # Query with LIMIT to simulate typical usage
      query = """
      SELECT ?s ?v
      WHERE { ?s <http://ex.org/value> ?v }
      LIMIT 1000
      """

      stats = measure_query(ctx, query)
      report_benchmark("Simple BGP (100K triples, LIMIT 1000)", stats)

      # Target: Median <50ms for 100K
      assert stats.median_us < 50_000, "Simple BGP should complete in <50ms"

      cleanup({db, manager})
    end

    @tag timeout: 1_200_000
    @tag :large_dataset
    test "benchmark simple BGP on large dataset (1M triples)", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      IO.puts("\n\n📊 Generating #{@large_dataset} triples for simple BGP benchmark...")
      IO.puts("   This may take a few minutes...")
      {gen_time, :ok} = :timer.tc(fn -> generate_simple_dataset(db, manager, @large_dataset) end)
      IO.puts("   Data generation: #{format_time(gen_time)}")

      # Query with LIMIT
      query = """
      SELECT ?s ?v
      WHERE { ?s <http://ex.org/value> ?v }
      LIMIT 1000
      """

      stats = measure_query(ctx, query)
      report_benchmark("Simple BGP (1M triples, LIMIT 1000)", stats, 10_000)

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # 2.7.4.2 - Star Query Benchmark (5 patterns)
  # ===========================================================================

  describe "2.7.4.2 - Star Query Benchmark" do
    @describetag :benchmark

    @tag timeout: 600_000
    test "benchmark star query on small dataset (10K entities = 50K triples)", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      entity_count = div(@small_dataset, 5)

      IO.puts(
        "\n\n📊 Generating #{entity_count} entities (#{@small_dataset} triples) for star query..."
      )

      {gen_time, :ok} = :timer.tc(fn -> generate_star_dataset(db, manager, entity_count) end)
      IO.puts("   Data generation: #{format_time(gen_time)}")

      # Star query: 5 patterns on same subject
      query = """
      SELECT ?s ?name ?age ?city ?category ?score
      WHERE {
        ?s <http://ex.org/name> ?name .
        ?s <http://ex.org/age> ?age .
        ?s <http://ex.org/city> ?city .
        ?s <http://ex.org/category> ?category .
        ?s <http://ex.org/score> ?score
      }
      LIMIT 100
      """

      stats = measure_query(ctx, query)
      report_benchmark("Star Query 5 patterns (10K entities, LIMIT 100)", stats)

      # Target: <100ms for 10K entities
      assert stats.median_us < 100_000, "Star query should complete in <100ms"

      cleanup({db, manager})
    end

    @tag timeout: 600_000
    test "benchmark star query on medium dataset (20K entities = 100K triples)", %{
      tmp_dir: tmp_dir
    } do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      entity_count = div(@medium_dataset, 5)

      IO.puts(
        "\n\n📊 Generating #{entity_count} entities (#{@medium_dataset} triples) for star query..."
      )

      {gen_time, :ok} = :timer.tc(fn -> generate_star_dataset(db, manager, entity_count) end)
      IO.puts("   Data generation: #{format_time(gen_time)}")

      # Star query with filter
      query = """
      SELECT ?s ?name ?age ?city ?category ?score
      WHERE {
        ?s <http://ex.org/name> ?name .
        ?s <http://ex.org/age> ?age .
        ?s <http://ex.org/city> ?city .
        ?s <http://ex.org/category> ?category .
        ?s <http://ex.org/score> ?score
      }
      LIMIT 100
      """

      stats = measure_query(ctx, query)
      report_benchmark("Star Query 5 patterns (20K entities, LIMIT 100)", stats)

      # Target: <200ms for 20K entities
      assert stats.median_us < 200_000, "Star query should complete in <200ms"

      cleanup({db, manager})
    end

    @tag timeout: 1_200_000
    @tag :large_dataset
    test "benchmark star query on large dataset (200K entities = 1M triples)", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      entity_count = div(@large_dataset, 5)

      IO.puts(
        "\n\n📊 Generating #{entity_count} entities (#{@large_dataset} triples) for star query..."
      )

      IO.puts("   This may take several minutes...")
      {gen_time, :ok} = :timer.tc(fn -> generate_star_dataset(db, manager, entity_count) end)
      IO.puts("   Data generation: #{format_time(gen_time)}")

      query = """
      SELECT ?s ?name ?age ?city ?category ?score
      WHERE {
        ?s <http://ex.org/name> ?name .
        ?s <http://ex.org/age> ?age .
        ?s <http://ex.org/city> ?city .
        ?s <http://ex.org/category> ?category .
        ?s <http://ex.org/score> ?score
      }
      LIMIT 100
      """

      stats = measure_query(ctx, query)
      report_benchmark("Star Query 5 patterns (200K entities, LIMIT 100)", stats, 100_000)

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # 2.7.4.3 - OPTIONAL Query Benchmark
  # ===========================================================================

  describe "2.7.4.3 - OPTIONAL Query Benchmark" do
    @describetag :benchmark

    @tag timeout: 600_000
    test "compare OPTIONAL vs inner join on 10K entities", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      IO.puts("\n\n📊 Generating #{@small_dataset} entities for OPTIONAL comparison...")

      {gen_time, :ok} =
        :timer.tc(fn -> generate_optional_dataset(db, manager, @small_dataset) end)

      IO.puts("   Data generation: #{format_time(gen_time)}")

      # Inner join query (only entities with email)
      inner_query = """
      SELECT ?s ?name ?email
      WHERE {
        ?s <http://ex.org/name> ?name .
        ?s <http://ex.org/email> ?email
      }
      LIMIT 500
      """

      # OPTIONAL query (all entities, email optional)
      optional_query = """
      SELECT ?s ?name ?email
      WHERE {
        ?s <http://ex.org/name> ?name
        OPTIONAL { ?s <http://ex.org/email> ?email }
      }
      LIMIT 500
      """

      inner_stats = measure_query(ctx, inner_query)
      optional_stats = measure_query(ctx, optional_query)

      report_benchmark("Inner Join (10K entities, LIMIT 500)", inner_stats)
      report_benchmark("OPTIONAL (10K entities, LIMIT 500)", optional_stats)

      overhead_pct =
        (optional_stats.median_us - inner_stats.median_us) / max(inner_stats.median_us, 1) * 100

      IO.puts("\n  OPTIONAL overhead: #{Float.round(overhead_pct, 1)}%")

      # Record the overhead for benchmark reporting
      # OPTIONAL can be significantly slower due to left join semantics
      # This is expected behavior; the benchmark documents actual performance
      IO.puts("  Note: OPTIONAL overhead is expected due to left join processing")

      cleanup({db, manager})
    end

    @tag timeout: 600_000
    test "compare OPTIONAL vs inner join on 100K entities", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      IO.puts("\n\n📊 Generating #{@medium_dataset} entities for OPTIONAL comparison...")

      {gen_time, :ok} =
        :timer.tc(fn -> generate_optional_dataset(db, manager, @medium_dataset) end)

      IO.puts("   Data generation: #{format_time(gen_time)}")

      inner_query = """
      SELECT ?s ?name ?email
      WHERE {
        ?s <http://ex.org/name> ?name .
        ?s <http://ex.org/email> ?email
      }
      LIMIT 500
      """

      optional_query = """
      SELECT ?s ?name ?email
      WHERE {
        ?s <http://ex.org/name> ?name
        OPTIONAL { ?s <http://ex.org/email> ?email }
      }
      LIMIT 500
      """

      inner_stats = measure_query(ctx, inner_query)
      optional_stats = measure_query(ctx, optional_query)

      report_benchmark("Inner Join (100K entities, LIMIT 500)", inner_stats)
      report_benchmark("OPTIONAL (100K entities, LIMIT 500)", optional_stats)

      overhead_pct =
        (optional_stats.median_us - inner_stats.median_us) / max(inner_stats.median_us, 1) * 100

      IO.puts("\n  OPTIONAL overhead: #{Float.round(overhead_pct, 1)}%")

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # 2.7.4.4 - Aggregation Benchmark
  # ===========================================================================

  describe "2.7.4.4 - Aggregation Benchmark" do
    @describetag :benchmark

    @tag timeout: 600_000
    test "benchmark GROUP BY with COUNT on 10K triples", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Half the count since we add 2 triples per entity
      entity_count = div(@small_dataset, 2)

      IO.puts(
        "\n\n📊 Generating #{entity_count} sales (#{@small_dataset} triples) for aggregation..."
      )

      {gen_time, :ok} =
        :timer.tc(fn -> generate_aggregation_dataset(db, manager, entity_count) end)

      IO.puts("   Data generation: #{format_time(gen_time)}")

      # GROUP BY with COUNT
      count_query = """
      SELECT ?category (COUNT(?sale) AS ?count)
      WHERE {
        ?sale <http://ex.org/category> ?category
      }
      GROUP BY ?category
      """

      stats = measure_query(ctx, count_query)
      report_benchmark("GROUP BY COUNT (5K sales, 100 categories)", stats)

      # Should complete in <100ms
      assert stats.median_us < 100_000, "Aggregation should complete in <100ms"

      cleanup({db, manager})
    end

    @tag timeout: 600_000
    test "benchmark GROUP BY with SUM on 10K triples", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      entity_count = div(@small_dataset, 2)
      IO.puts("\n\n📊 Generating #{entity_count} sales for SUM aggregation...")

      {gen_time, :ok} =
        :timer.tc(fn -> generate_aggregation_dataset(db, manager, entity_count) end)

      IO.puts("   Data generation: #{format_time(gen_time)}")

      # GROUP BY with SUM
      sum_query = """
      SELECT ?category (SUM(?amount) AS ?total)
      WHERE {
        ?sale <http://ex.org/category> ?category .
        ?sale <http://ex.org/amount> ?amount
      }
      GROUP BY ?category
      """

      stats = measure_query(ctx, sum_query)
      report_benchmark("GROUP BY SUM (5K sales, 100 categories)", stats)

      # Should complete in <200ms (join + aggregation)
      assert stats.median_us < 200_000, "SUM aggregation should complete in <200ms"

      cleanup({db, manager})
    end

    @tag timeout: 600_000
    test "benchmark implicit grouping on 10K triples", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      entity_count = div(@small_dataset, 2)
      IO.puts("\n\n📊 Generating #{entity_count} sales for implicit grouping...")

      {gen_time, :ok} =
        :timer.tc(fn -> generate_aggregation_dataset(db, manager, entity_count) end)

      IO.puts("   Data generation: #{format_time(gen_time)}")

      # Implicit grouping (aggregate all)
      implicit_query = """
      SELECT (COUNT(?sale) AS ?total_count) (SUM(?amount) AS ?total_amount) (AVG(?amount) AS ?avg_amount)
      WHERE {
        ?sale <http://ex.org/category> ?category .
        ?sale <http://ex.org/amount> ?amount
      }
      """

      stats = measure_query(ctx, implicit_query)
      report_benchmark("Implicit Grouping COUNT+SUM+AVG (5K sales)", stats)

      # Should complete in <200ms
      assert stats.median_us < 200_000, "Implicit grouping should complete in <200ms"

      cleanup({db, manager})
    end

    @tag timeout: 600_000
    test "benchmark GROUP BY with HAVING on 50K triples", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      entity_count = div(@medium_dataset, 2)
      IO.puts("\n\n📊 Generating #{entity_count} sales for HAVING benchmark...")

      {gen_time, :ok} =
        :timer.tc(fn -> generate_aggregation_dataset(db, manager, entity_count) end)

      IO.puts("   Data generation: #{format_time(gen_time)}")

      # GROUP BY with HAVING filter
      having_query = """
      SELECT ?category (COUNT(?sale) AS ?count) (SUM(?amount) AS ?total)
      WHERE {
        ?sale <http://ex.org/category> ?category .
        ?sale <http://ex.org/amount> ?amount
      }
      GROUP BY ?category
      HAVING (COUNT(?sale) > 100)
      """

      stats = measure_query(ctx, having_query)
      report_benchmark("GROUP BY with HAVING (50K sales)", stats)

      # Should complete in <500ms for larger dataset
      assert stats.median_us < 500_000, "HAVING query should complete in <500ms"

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # Summary Benchmark
  # ===========================================================================

  describe "Benchmark Summary" do
    @describetag :benchmark

    @tag timeout: 600_000
    test "full benchmark suite on small dataset", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      IO.puts("\n")
      IO.puts("=" |> String.duplicate(60))
      IO.puts("  SPARQL Query Engine Performance Summary")
      IO.puts("  Dataset: Small (10K base triples)")
      IO.puts("=" |> String.duplicate(60))

      # Generate all datasets
      IO.puts("\n📦 Generating test data...")

      # Simple BGP data
      {t1, :ok} = :timer.tc(fn -> generate_simple_dataset(db, manager, @small_dataset) end)
      IO.puts("   Simple BGP data: #{format_time(t1)}")

      # Run simple BGP
      bgp_query = "SELECT ?s ?v WHERE { ?s <http://ex.org/value> ?v } LIMIT 1000"
      bgp_stats = measure_query(ctx, bgp_query)

      # Cleanup and regenerate for star query
      cleanup({db, manager})
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {t2, :ok} = :timer.tc(fn -> generate_star_dataset(db, manager, div(@small_dataset, 5)) end)
      IO.puts("   Star query data: #{format_time(t2)}")

      star_query = """
      SELECT ?s ?name ?age ?city ?category ?score
      WHERE {
        ?s <http://ex.org/name> ?name .
        ?s <http://ex.org/age> ?age .
        ?s <http://ex.org/city> ?city .
        ?s <http://ex.org/category> ?category .
        ?s <http://ex.org/score> ?score
      }
      LIMIT 100
      """

      star_stats = measure_query(ctx, star_query)

      # Cleanup and regenerate for OPTIONAL
      cleanup({db, manager})
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {t3, :ok} = :timer.tc(fn -> generate_optional_dataset(db, manager, @small_dataset) end)
      IO.puts("   OPTIONAL data: #{format_time(t3)}")

      optional_query = """
      SELECT ?s ?name ?email
      WHERE {
        ?s <http://ex.org/name> ?name
        OPTIONAL { ?s <http://ex.org/email> ?email }
      }
      LIMIT 500
      """

      optional_stats = measure_query(ctx, optional_query)

      # Cleanup and regenerate for aggregation
      cleanup({db, manager})
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {t4, :ok} =
        :timer.tc(fn -> generate_aggregation_dataset(db, manager, div(@small_dataset, 2)) end)

      IO.puts("   Aggregation data: #{format_time(t4)}")

      agg_query = """
      SELECT ?category (COUNT(?sale) AS ?count) (SUM(?amount) AS ?total)
      WHERE {
        ?sale <http://ex.org/category> ?category .
        ?sale <http://ex.org/amount> ?amount
      }
      GROUP BY ?category
      """

      agg_stats = measure_query(ctx, agg_query)

      # Print summary
      IO.puts("\n")
      IO.puts("-" |> String.duplicate(60))
      IO.puts("  Results Summary")
      IO.puts("-" |> String.duplicate(60))
      IO.puts("")
      IO.puts("  Query Type                    Median      Target    Status")
      IO.puts("  " <> String.duplicate("-", 56))

      print_result("Simple BGP (LIMIT 1000)", bgp_stats.median_us, 10_000)
      print_result("Star Query 5 patterns", star_stats.median_us, 100_000)
      print_result("OPTIONAL (LIMIT 500)", optional_stats.median_us, 100_000)
      print_result("GROUP BY + SUM", agg_stats.median_us, 200_000)

      IO.puts("")
      IO.puts("=" |> String.duplicate(60))

      cleanup({db, manager})
    end
  end

  defp print_result(name, actual_us, target_us) do
    padded_name = String.pad_trailing(name, 28)
    actual_str = String.pad_leading(format_time(actual_us), 10)
    target_str = String.pad_leading(format_time(target_us), 10)
    status = if actual_us <= target_us, do: "  ✅", else: "  ❌"
    IO.puts("  #{padded_name}#{actual_str}#{target_str}#{status}")
  end
end
