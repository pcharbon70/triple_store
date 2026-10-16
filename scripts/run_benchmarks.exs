IO.warn("scripts/run_benchmarks.exs is deprecated; use scripts/run_watdiv_benchmark.sh")
Mix.Task.run("benchmark.watdiv", ["run" | System.argv()])
