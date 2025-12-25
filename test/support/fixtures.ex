defmodule TripleStore.Test.Fixtures do
  @moduledoc """
  Test fixtures and helpers for TripleStore tests.

  Provides common test data and utility functions for testing
  the triple store components.

  ## Graph Fixtures

  The following graph fixtures are available for integration tests:

  - `social_network/0` - 100-node social network with foaf:knows relationships
  - `class_hierarchy/0` - 10-level deep class hierarchy with rdfs:subClassOf
  - `property_chain/0` - 50-hop property chain for path traversal testing
  - `diamond_hierarchy/0` - Multiple inheritance diamond pattern

  ## Namespace Constants

  Use the `@rdf`, `@rdfs`, `@foaf`, `@ex` module attributes for standard namespaces.
  """

  # Standard namespaces
  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @foaf "http://xmlns.com/foaf/0.1/"
  @ex "http://example.org/"

  @doc """
  Returns a temporary directory path for test databases.
  The directory is cleaned up after the test.
  """
  def tmp_db_path do
    path =
      Path.join(System.tmp_dir!(), "triple_store_test_#{:erlang.unique_integer([:positive])}")

    File.mkdir_p!(path)
    path
  end

  @doc """
  Cleans up a test database directory.
  """
  def cleanup_db(path) do
    File.rm_rf!(path)
  end

  # ===========================================================================
  # Graph Fixtures for Integration Tests
  # ===========================================================================

  @doc """
  Generate a social network graph with the specified number of nodes.

  Each person knows 2-5 other people randomly. Creates cyclic relationships.

  ## Example

      triples = social_network(100)
      # => [{"http://example.org/person1", "http://xmlns.com/foaf/0.1/knows", "http://example.org/person42"}, ...]
  """
  def social_network(node_count \\ 100) do
    for i <- 1..node_count,
        j <- Enum.take_random(1..node_count, Enum.random(2..5)),
        i != j do
      {"#{@ex}person#{i}", "#{@foaf}knows", "#{@ex}person#{j}"}
    end
  end

  @doc """
  Generate a class hierarchy with the specified number of levels.

  Creates a chain of rdfs:subClassOf relationships.

  ## Example

      #       Thing
      #         |
      #       Agent
      #         |
      #      Person
      #         |
      #      Student

      triples = class_hierarchy(4)
  """
  def class_hierarchy(levels \\ 10) do
    classes = ["Thing", "Agent", "Person"] ++ for i <- 1..(levels - 3), do: "Class#{i}"

    for {child, parent} <- Enum.zip(Enum.drop(classes, 1), classes) do
      {"#{@ex}#{child}", "#{@rdfs}subClassOf", "#{@ex}#{parent}"}
    end
  end

  @doc """
  Generate a property chain with the specified number of hops.

  Creates a linear chain of nodes connected by a property.

  ## Example

      triples = property_chain(50, "next")
      # node1 -> node2 -> node3 -> ... -> node50
  """
  def property_chain(hops \\ 50, predicate \\ "next") do
    for i <- 1..(hops - 1) do
      {"#{@ex}node#{i}", "#{@ex}#{predicate}", "#{@ex}node#{i + 1}"}
    end
  end

  @doc """
  Generate a diamond inheritance pattern.

  Creates a classic diamond pattern for testing multiple inheritance scenarios.

  ## Example

      #       Thing
      #      /     \\
      #   Agent   Physical
      #      \\     /
      #      Person
  """
  def diamond_hierarchy do
    [
      {"#{@ex}Agent", "#{@rdfs}subClassOf", "#{@ex}Thing"},
      {"#{@ex}Physical", "#{@rdfs}subClassOf", "#{@ex}Thing"},
      {"#{@ex}Person", "#{@rdfs}subClassOf", "#{@ex}Agent"},
      {"#{@ex}Person", "#{@rdfs}subClassOf", "#{@ex}Physical"}
    ]
  end

  @doc """
  Generate a complete graph where every node is connected to every other node.

  Useful for testing dense graph traversal performance.
  """
  def complete_graph(node_count \\ 20, predicate \\ "connected") do
    for i <- 1..node_count, j <- 1..node_count, i != j do
      {"#{@ex}node#{i}", "#{@ex}#{predicate}", "#{@ex}node#{j}"}
    end
  end

  @doc """
  Generate a binary tree hierarchy.

  Creates a binary tree with the specified number of levels.
  Level 0 has 1 node, level 1 has 2, level n has 2^n nodes.
  Total nodes = 2^(levels) - 1
  """
  def binary_tree(levels \\ 7) do
    for level <- 0..(levels - 2),
        i <- 0..(round(:math.pow(2, level)) - 1) do
      parent = "#{@ex}node_#{level}_#{i}"
      [
        {parent, "#{@rdfs}subClassOf", "#{@ex}node_#{level + 1}_#{i * 2}"},
        {parent, "#{@rdfs}subClassOf", "#{@ex}node_#{level + 1}_#{i * 2 + 1}"}
      ]
    end
    |> List.flatten()
  end

  @doc """
  Generate typed instances for class hierarchy testing.

  Creates instances of each class in the hierarchy.
  """
  def typed_instances(classes, instances_per_class \\ 3) do
    for class <- classes, i <- 1..instances_per_class do
      {"#{@ex}#{String.downcase(class)}#{i}", "#{@rdf}type", "#{@ex}#{class}"}
    end
  end
end
