alias TripleStore.Benchmark.{GMark, Runner}

defmodule GMarkBenchmarkScript do
  @switches [
    scale: :integer,
    warmup: :integer,
    iterations: :integer,
    include_q3: :boolean,
    output_dir: :string
  ]

  @default_queries [:c1, :c2, :c3, :l1, :l2, :l3, :l4, :l5, :q1, :q2]

  def run(args) do
    args = Enum.drop_while(args, &(&1 == "--"))
    {opts, positional, invalid} = OptionParser.parse(args, strict: @switches)

    if positional != [] or invalid != [] do
      raise ArgumentError, "invalid arguments; run scripts/run_gmark_benchmark.sh --help"
    end

    scale = positive_option!(opts, :scale, 1)
    warmup = non_negative_option!(opts, :warmup, 2)
    iterations = positive_option!(opts, :iterations, 5)
    queries = if Keyword.get(opts, :include_q3, false), do: nil, else: @default_queries
    output_dir = Keyword.get(opts, :output_dir, default_output_dir())
    store_path = Path.join(System.tmp_dir!(), "triple_store_gmark_#{unique_id()}")

    graph = timed("Generated gMark scale #{scale}", fn -> GMark.generate(scale) end)
    triple_count = RDF.Graph.triple_count(graph)
    IO.puts("Dataset contains #{triple_count} triples")

    {:ok, store} = TripleStore.open(store_path)

    try do
      {:ok, loaded} =
        timed("Loaded dataset", fn ->
          TripleStore.load_graph(store, graph)
        end)

      IO.puts("Loaded #{loaded} triples")

      {:ok, result} =
        Runner.run(store, :gmark,
          scale: scale,
          warmup: warmup,
          iterations: iterations,
          queries: queries
        )

      assert_successful_queries!(result)
      Runner.print_summary(result)
      write_results!(output_dir, result)
    after
      TripleStore.close(store)
      File.rm_rf!(store_path)
    end
  end

  defp positive_option!(opts, key, default) do
    value = Keyword.get(opts, key, default)

    if is_integer(value) and value > 0,
      do: value,
      else: raise(ArgumentError, "--#{key} must be positive")
  end

  defp non_negative_option!(opts, key, default) do
    value = Keyword.get(opts, key, default)

    if is_integer(value) and value >= 0,
      do: value,
      else: raise(ArgumentError, "--#{key} must be non-negative")
  end

  defp default_output_dir do
    timestamp = DateTime.utc_now() |> Calendar.strftime("%Y%m%dT%H%M%SZ")
    Path.join([File.cwd!(), "tmp", "gmark_benchmark_runs", timestamp])
  end

  defp write_results!(output_dir, result) do
    File.mkdir_p!(output_dir)
    json_path = Path.join(output_dir, "results.json")
    csv_path = Path.join(output_dir, "results.csv")
    File.write!(json_path, Runner.to_json(result))
    File.write!(csv_path, Runner.to_csv(result))
    IO.puts("Results written to #{output_dir}")
  end

  defp assert_successful_queries!(result) do
    failed = for query <- result.query_results, query.result_count == :error, do: query.query_id

    if failed != [] do
      raise "gMark queries failed: #{Enum.map_join(failed, ", ", &to_string/1)}"
    end
  end

  defp timed(label, function) do
    {elapsed_us, result} = :timer.tc(function)
    IO.puts("#{label} in #{Runner.format_duration(elapsed_us)}")
    result
  end

  defp unique_id, do: System.unique_integer([:positive, :monotonic])
end

GMarkBenchmarkScript.run(System.argv())
