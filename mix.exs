defmodule TripleStore.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/pcharbon70/triple_store"

  def project do
    [
      app: :triple_store,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      dialyzer: dialyzer(),

      # Docs
      name: "TripleStore",
      source_url: @source_url,
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {TripleStore.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # RDF parsing and data structures
      {:rdf, "~> 2.0"},

      # RocksDB storage backend (erlang-rocksdb C++ NIF)
      {:rocksdb, "~> 1.9"},

      # Rustler for SPARQL parser NIF (separate from RocksDB NIF)
      {:rustler, "~> 0.35"},

      # Concurrent processing for bulk loading
      {:flow, "~> 1.2"},

      # Telemetry for metrics
      {:telemetry, "~> 1.2"},

      # Documentation
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},

      # Code quality
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},

      # Static analysis
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},

      # Property-based testing
      {:stream_data, "~> 1.0", only: :test}
    ]
  end

  defp dialyzer do
    [
      plt_add_apps: [:mix, :ex_unit]
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: documentation_extras(),
      # These specification indexes link to historical delivery records. The
      # records remain in the repository but are intentionally not published as
      # normative ExDoc pages.
      skip_undefined_reference_warnings_on: [
        "specs/getting-started.md",
        "specs/operations/README.md",
        "specs/planning/README.md"
      ],
      groups_for_extras: [
        "User Guides": ~r{^guides/user/},
        "Developer Guides": ~r{^guides/developer/},
        "Benchmark Guides": ~r{^guides/benchmarks/},
        "Ontology Guides": ~r{^guides/ontology/},
        Specifications: ~r{^specs/},
        "Production Operations": ~r{^docs/production/}
      ]
    ]
  end

  defp documentation_extras do
    ["README.md", "LICENSE.md"]
    |> Kernel.++(Path.wildcard("guides/**/*.md"))
    |> Kernel.++(Path.wildcard("specs/**/*.md"))
    |> Kernel.++(Path.wildcard("docs/production/**/*.md"))
    |> Enum.map(fn path ->
      filename =
        path
        |> Path.rootname()
        |> String.replace(~r{[^a-zA-Z0-9]+}, "-")
        |> String.trim("-")
        |> String.downcase()

      {path, filename: filename}
    end)
  end
end
