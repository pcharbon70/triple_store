defmodule TripleStore.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/your-org/triple_store"

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
      plt_add_apps: [:mix]
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: [
        "README.md",
        "guides/user/README.md",
        "guides/developer/README.md"
      ],
      groups_for_extras: [
        "User Guides": Path.wildcard("guides/user/*.md") -- ["guides/user/README.md"],
        "Developer Guides":
          Path.wildcard("guides/developer/*.md") -- ["guides/developer/README.md"]
      ]
    ]
  end
end
