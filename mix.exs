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

      # NIF compilation
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

  defp docs do
    [
      main: "TripleStore",
      extras: ["README.md"]
    ]
  end
end
