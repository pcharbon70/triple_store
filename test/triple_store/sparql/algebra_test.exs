defmodule TripleStore.SPARQL.AlgebraTest do
  @moduledoc """
  Tests for the SPARQL Algebra module.
  """

  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.Algebra

  # ===========================================================================
  # Node Types List
  # ===========================================================================

  describe "node_types/0" do
    test "returns all supported algebra node types" do
      types = Algebra.node_types()

      assert :bgp in types
      assert :join in types
      assert :left_join in types
      assert :minus in types
      assert :union in types
      assert :filter in types
      assert :extend in types
      assert :group in types
      assert :project in types
      assert :distinct in types
      assert :reduced in types
      assert :order_by in types
      assert :slice in types
      assert :values in types
      assert :service in types
      assert :graph in types
      assert :path in types
    end
  end

  # ===========================================================================
  # Basic Pattern Nodes
  # ===========================================================================

  describe "bgp/1" do
    test "creates empty BGP" do
      assert {:bgp, []} = Algebra.bgp([])
    end

    test "creates BGP with single triple" do
      triple = {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      assert {:bgp, [^triple]} = Algebra.bgp([triple])
    end

    test "creates BGP with multiple triples" do
      t1 = {:triple, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"}}
      t2 = {:triple, {:variable, "s"}, {:named_node, "http://example.org/age"}, {:variable, "age"}}
      result = Algebra.bgp([t1, t2])

      assert {:bgp, [^t1, ^t2]} = result
    end
  end

  describe "triple/3" do
    test "creates triple with variables" do
      result = Algebra.triple({:variable, "s"}, {:variable, "p"}, {:variable, "o"})
      assert {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}} = result
    end

    test "creates triple with named nodes" do
      result =
        Algebra.triple(
          {:named_node, "http://example.org/subject"},
          {:named_node, "http://example.org/predicate"},
          {:named_node, "http://example.org/object"}
        )

      assert {:triple,
              {:named_node, "http://example.org/subject"},
              {:named_node, "http://example.org/predicate"},
              {:named_node, "http://example.org/object"}} = result
    end

    test "creates triple with literals" do
      result =
        Algebra.triple(
          {:variable, "s"},
          {:named_node, "http://example.org/name"},
          {:literal, :simple, "John"}
        )

      assert {:triple,
              {:variable, "s"},
              {:named_node, "http://example.org/name"},
              {:literal, :simple, "John"}} = result
    end

    test "creates triple with typed literal" do
      result =
        Algebra.triple(
          {:variable, "s"},
          {:named_node, "http://example.org/age"},
          {:literal, :typed, "30", "http://www.w3.org/2001/XMLSchema#integer"}
        )

      assert {:triple,
              {:variable, "s"},
              {:named_node, "http://example.org/age"},
              {:literal, :typed, "30", "http://www.w3.org/2001/XMLSchema#integer"}} = result
    end

    test "creates triple with language-tagged literal" do
      result =
        Algebra.triple(
          {:variable, "s"},
          {:named_node, "http://example.org/label"},
          {:literal, :lang, "Hello", "en"}
        )

      assert {:triple,
              {:variable, "s"},
              {:named_node, "http://example.org/label"},
              {:literal, :lang, "Hello", "en"}} = result
    end
  end

  # ===========================================================================
  # Join Nodes
  # ===========================================================================

  describe "join/2" do
    test "creates join between two BGPs" do
      left = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p1"}, {:variable, "o1"}}])
      right = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p2"}, {:variable, "o2"}}])

      result = Algebra.join(left, right)

      assert {:join, ^left, ^right} = result
    end

    test "creates nested joins" do
      a = Algebra.bgp([{:triple, {:variable, "a"}, {:variable, "p"}, {:variable, "b"}}])
      b = Algebra.bgp([{:triple, {:variable, "b"}, {:variable, "p"}, {:variable, "c"}}])
      c = Algebra.bgp([{:triple, {:variable, "c"}, {:variable, "p"}, {:variable, "d"}}])

      inner = Algebra.join(a, b)
      outer = Algebra.join(inner, c)

      assert {:join, {:join, ^a, ^b}, ^c} = outer
    end
  end

  describe "left_join/3" do
    test "creates left join without filter" do
      required = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      optional = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p2"}, {:variable, "o2"}}])

      result = Algebra.left_join(required, optional)

      assert {:left_join, ^required, ^optional, nil} = result
    end

    test "creates left join with filter" do
      required = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      optional = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p2"}, {:variable, "o2"}}])
      filter_expr = {:bound, {:variable, "o2"}}

      result = Algebra.left_join(required, optional, filter_expr)

      assert {:left_join, ^required, ^optional, ^filter_expr} = result
    end
  end

  describe "minus/2" do
    test "creates minus node" do
      pattern = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])

      exclude =
        Algebra.bgp([
          {:triple, {:variable, "s"}, {:named_node, "http://example.org/deleted"},
           {:literal, :simple, "true"}}
        ])

      result = Algebra.minus(pattern, exclude)

      assert {:minus, ^pattern, ^exclude} = result
    end
  end

  # ===========================================================================
  # Set Operations
  # ===========================================================================

  describe "union/2" do
    test "creates union of two BGPs" do
      left =
        Algebra.bgp([
          {:triple, {:variable, "s"}, {:named_node, "http://example.org/a"}, {:variable, "o"}}
        ])

      right =
        Algebra.bgp([
          {:triple, {:variable, "s"}, {:named_node, "http://example.org/b"}, {:variable, "o"}}
        ])

      result = Algebra.union(left, right)

      assert {:union, ^left, ^right} = result
    end
  end

  # ===========================================================================
  # Filter Node
  # ===========================================================================

  describe "filter/2" do
    test "creates filter with comparison expression" do
      bgp =
        Algebra.bgp([
          {:triple, {:variable, "s"}, {:named_node, "http://example.org/age"}, {:variable, "age"}}
        ])

      expr =
        {:greater, {:variable, "age"},
         {:literal, :typed, "18", "http://www.w3.org/2001/XMLSchema#integer"}}

      result = Algebra.filter(expr, bgp)

      assert {:filter, ^expr, ^bgp} = result
    end

    test "creates filter with logical expression" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      expr = {:and, {:bound, {:variable, "s"}}, {:bound, {:variable, "o"}}}

      result = Algebra.filter(expr, bgp)

      assert {:filter, ^expr, ^bgp} = result
    end
  end

  # ===========================================================================
  # Extension Nodes
  # ===========================================================================

  describe "extend/3" do
    test "creates extend node for BIND" do
      bgp =
        Algebra.bgp([
          {:triple, {:variable, "s"}, {:named_node, "http://example.org/age"}, {:variable, "age"}}
        ])

      expr =
        {:multiply, {:variable, "age"},
         {:literal, :typed, "12", "http://www.w3.org/2001/XMLSchema#integer"}}

      result = Algebra.extend(bgp, {:variable, "months"}, expr)

      assert {:extend, ^bgp, {:variable, "months"}, ^expr} = result
    end
  end

  describe "group/3" do
    test "creates group node with aggregates" do
      bgp =
        Algebra.bgp([
          {:triple, {:variable, "s"}, {:named_node, "http://example.org/type"},
           {:variable, "type"}}
        ])

      group_vars = [variable: "s"]
      aggregates = [{{:variable, "count"}, {:count, {:variable, "type"}, false}}]

      result = Algebra.group(bgp, group_vars, aggregates)

      assert {:group, ^bgp, ^group_vars, ^aggregates} = result
    end

    test "creates group node with multiple aggregates" do
      bgp =
        Algebra.bgp([
          {:triple, {:variable, "s"}, {:named_node, "http://example.org/value"},
           {:variable, "value"}}
        ])

      group_vars = [variable: "s"]

      aggregates = [
        {{:variable, "total"}, {:sum, {:variable, "value"}, false}},
        {{:variable, "avg_val"}, {:avg, {:variable, "value"}, false}},
        {{:variable, "cnt"}, {:count, :star, false}}
      ]

      result = Algebra.group(bgp, group_vars, aggregates)

      assert {:group, ^bgp, ^group_vars, ^aggregates} = result
    end
  end

  # ===========================================================================
  # Projection
  # ===========================================================================

  describe "project/2" do
    test "creates project node" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      vars = [variable: "s", variable: "p"]

      result = Algebra.project(bgp, vars)

      assert {:project, ^bgp, ^vars} = result
    end
  end

  # ===========================================================================
  # Duplicate Modifiers
  # ===========================================================================

  describe "distinct/1" do
    test "creates distinct node" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      project = Algebra.project(bgp, [variable: "s"])

      result = Algebra.distinct(project)

      assert {:distinct, ^project} = result
    end
  end

  describe "reduced/1" do
    test "creates reduced node" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      project = Algebra.project(bgp, [variable: "s"])

      result = Algebra.reduced(project)

      assert {:reduced, ^project} = result
    end
  end

  # ===========================================================================
  # Solution Modifiers
  # ===========================================================================

  describe "order_by/2" do
    test "creates order_by with single ascending condition" do
      bgp =
        Algebra.bgp([
          {:triple, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"}}
        ])

      conditions = [asc: {:variable, "name"}]

      result = Algebra.order_by(bgp, conditions)

      assert {:order_by, ^bgp, ^conditions} = result
    end

    test "creates order_by with multiple conditions" do
      bgp =
        Algebra.bgp([
          {:triple, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"}}
        ])

      conditions = [
        {:asc, {:variable, "name"}},
        {:desc, {:variable, "age"}}
      ]

      result = Algebra.order_by(bgp, conditions)

      assert {:order_by, ^bgp, ^conditions} = result
    end
  end

  describe "slice/3" do
    test "creates slice with offset and limit" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])

      result = Algebra.slice(bgp, 10, 5)

      assert {:slice, ^bgp, 10, 5} = result
    end

    test "creates slice with zero offset" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])

      result = Algebra.slice(bgp, 0, 100)

      assert {:slice, ^bgp, 0, 100} = result
    end

    test "creates slice with infinity limit" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])

      result = Algebra.slice(bgp, 10, :infinity)

      assert {:slice, ^bgp, 10, :infinity} = result
    end
  end

  # ===========================================================================
  # Values Node
  # ===========================================================================

  describe "values/2" do
    test "creates values node with inline data" do
      vars = [{:variable, "x"}, {:variable, "y"}]

      data = [
        [{:literal, :simple, "1"}, {:literal, :simple, "2"}],
        [{:literal, :simple, "3"}, :undef]
      ]

      result = Algebra.values(vars, data)

      assert {:values, ^vars, ^data} = result
    end
  end

  # ===========================================================================
  # Graph and Service Nodes
  # ===========================================================================

  describe "graph/2" do
    test "creates graph node with named graph" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      graph_name = {:named_node, "http://example.org/graph1"}

      result = Algebra.graph(graph_name, bgp)

      assert {:graph, ^graph_name, ^bgp} = result
    end

    test "creates graph node with variable" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      graph_var = {:variable, "g"}

      result = Algebra.graph(graph_var, bgp)

      assert {:graph, ^graph_var, ^bgp} = result
    end
  end

  describe "service/3" do
    test "creates service node" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      endpoint = {:named_node, "http://dbpedia.org/sparql"}

      result = Algebra.service(endpoint, bgp)

      assert {:service, ^endpoint, ^bgp, false} = result
    end

    test "creates silent service node" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      endpoint = {:named_node, "http://dbpedia.org/sparql"}

      result = Algebra.service(endpoint, bgp, true)

      assert {:service, ^endpoint, ^bgp, true} = result
    end
  end

  # ===========================================================================
  # Path Node
  # ===========================================================================

  describe "path/3" do
    test "creates path node with property path" do
      path_expr = {:one_or_more, {:link, "http://example.org/knows"}}

      result = Algebra.path({:variable, "s"}, path_expr, {:variable, "o"})

      assert {:path, {:variable, "s"}, ^path_expr, {:variable, "o"}} = result
    end
  end

  # ===========================================================================
  # Node Analysis
  # ===========================================================================

  describe "node_type/1" do
    test "returns type for all node kinds" do
      assert Algebra.node_type({:bgp, []}) == :bgp
      assert Algebra.node_type({:join, {:bgp, []}, {:bgp, []}}) == :join
      assert Algebra.node_type({:left_join, {:bgp, []}, {:bgp, []}, nil}) == :left_join
      assert Algebra.node_type({:minus, {:bgp, []}, {:bgp, []}}) == :minus
      assert Algebra.node_type({:union, {:bgp, []}, {:bgp, []}}) == :union
      assert Algebra.node_type({:filter, nil, {:bgp, []}}) == :filter
      assert Algebra.node_type({:extend, {:bgp, []}, nil, nil}) == :extend
      assert Algebra.node_type({:group, {:bgp, []}, [], []}) == :group
      assert Algebra.node_type({:project, {:bgp, []}, []}) == :project
      assert Algebra.node_type({:distinct, {:bgp, []}}) == :distinct
      assert Algebra.node_type({:reduced, {:bgp, []}}) == :reduced
      assert Algebra.node_type({:order_by, {:bgp, []}, []}) == :order_by
      assert Algebra.node_type({:slice, {:bgp, []}, 0, 10}) == :slice
      assert Algebra.node_type({:values, [], []}) == :values
      assert Algebra.node_type({:graph, nil, {:bgp, []}}) == :graph
      assert Algebra.node_type({:service, nil, {:bgp, []}, false}) == :service
      assert Algebra.node_type({:path, nil, nil, nil}) == :path
    end
  end

  describe "is_type?/2" do
    test "returns true for matching type" do
      assert Algebra.is_type?({:bgp, []}, :bgp)
      assert Algebra.is_type?({:filter, nil, {:bgp, []}}, :filter)
    end

    test "returns false for non-matching type" do
      refute Algebra.is_type?({:bgp, []}, :filter)
      refute Algebra.is_type?({:filter, nil, {:bgp, []}}, :bgp)
    end
  end

  describe "variables/1" do
    test "extracts variables from BGP" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      vars = Algebra.variables(bgp)

      assert {:variable, "s"} in vars
      assert {:variable, "p"} in vars
      assert {:variable, "o"} in vars
      assert length(vars) == 3
    end

    test "extracts variables from nested structure" do
      bgp1 = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p1"}, {:variable, "o1"}}])
      bgp2 = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p2"}, {:variable, "o2"}}])
      join = Algebra.join(bgp1, bgp2)

      vars = Algebra.variables(join)

      assert {:variable, "s"} in vars
      assert {:variable, "p1"} in vars
      assert {:variable, "o1"} in vars
      assert {:variable, "p2"} in vars
      assert {:variable, "o2"} in vars
    end

    test "returns unique variables" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "s"}}])
      vars = Algebra.variables(bgp)

      s_count = Enum.count(vars, fn v -> v == {:variable, "s"} end)
      assert s_count == 1
    end

    test "excludes non-variable terms" do
      bgp =
        Algebra.bgp([
          {:triple, {:variable, "s"}, {:named_node, "http://example.org/pred"},
           {:literal, :simple, "value"}}
        ])

      vars = Algebra.variables(bgp)

      assert vars == [{:variable, "s"}]
    end
  end

  describe "children/1" do
    test "BGP has no children" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      assert Algebra.children(bgp) == []
    end

    test "join has two children" do
      left = Algebra.bgp([])
      right = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      join = Algebra.join(left, right)

      children = Algebra.children(join)

      assert children == [left, right]
    end

    test "filter has one child" do
      bgp = Algebra.bgp([])
      filter = Algebra.filter({:bound, {:variable, "x"}}, bgp)

      children = Algebra.children(filter)

      assert children == [bgp]
    end

    test "project has one child" do
      bgp = Algebra.bgp([])
      project = Algebra.project(bgp, [variable: "s"])

      children = Algebra.children(project)

      assert children == [bgp]
    end
  end

  describe "fold/3" do
    test "counts nodes in tree" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      filter = Algebra.filter({:bound, {:variable, "s"}}, bgp)
      project = Algebra.project(filter, [variable: "s"])

      count = Algebra.fold(project, 0, fn _node, acc -> acc + 1 end)

      assert count == 3
    end

    test "collects node types" do
      bgp = Algebra.bgp([])
      filter = Algebra.filter({:bound, {:variable, "s"}}, bgp)
      project = Algebra.project(filter, [variable: "s"])

      types = Algebra.fold(project, [], fn node, acc -> [Algebra.node_type(node) | acc] end)

      assert :bgp in types
      assert :filter in types
      assert :project in types
    end
  end

  describe "map/2" do
    test "transforms nodes" do
      bgp =
        Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])

      filter = Algebra.filter({:bound, {:variable, "s"}}, bgp)

      result =
        Algebra.map(filter, fn
          {:bgp, _} -> {:bgp, []}
          node -> node
        end)

      assert {:filter, {:bound, {:variable, "s"}}, {:bgp, []}} = result
    end
  end

  # ===========================================================================
  # Validation
  # ===========================================================================

  describe "validate/1" do
    test "validates empty BGP" do
      assert :ok = Algebra.validate({:bgp, []})
    end

    test "validates BGP with valid triples" do
      bgp =
        Algebra.bgp([
          {:triple, {:variable, "s"}, {:named_node, "http://example.org/p"}, {:variable, "o"}}
        ])

      assert :ok = Algebra.validate(bgp)
    end

    test "rejects BGP with invalid patterns list" do
      assert {:error, _} = Algebra.validate({:bgp, "not a list"})
    end

    test "validates join" do
      left = Algebra.bgp([])
      right = Algebra.bgp([])
      join = Algebra.join(left, right)

      assert :ok = Algebra.validate(join)
    end

    test "validates left_join" do
      left = Algebra.bgp([])
      right = Algebra.bgp([])
      left_join = Algebra.left_join(left, right)

      assert :ok = Algebra.validate(left_join)
    end

    test "validates slice with valid offset/limit" do
      bgp = Algebra.bgp([])
      slice = Algebra.slice(bgp, 10, 5)

      assert :ok = Algebra.validate(slice)
    end

    test "rejects slice with negative offset" do
      # Can't create with function, but can validate raw tuple
      assert {:error, _} = Algebra.validate({:slice, {:bgp, []}, -1, 5})
    end

    test "validates nested structure" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      filter = Algebra.filter({:bound, {:variable, "s"}}, bgp)
      project = Algebra.project(filter, [variable: "s"])
      distinct = Algebra.distinct(project)

      assert :ok = Algebra.validate(distinct)
    end

    test "rejects unknown node type" do
      assert {:error, _} = Algebra.validate({:unknown_type, []})
    end

    test "rejects non-tuple" do
      assert {:error, _} = Algebra.validate("not a node")
    end
  end

  # ===========================================================================
  # Pretty Printing
  # ===========================================================================

  describe "to_string/1" do
    test "formats BGP" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      str = Algebra.to_string(bgp)

      assert str =~ "BGP"
      assert str =~ "?s"
      assert str =~ "?p"
      assert str =~ "?o"
    end

    test "formats nested structure" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      project = Algebra.project(bgp, [variable: "s"])
      str = Algebra.to_string(project)

      assert str =~ "Project"
      assert str =~ "BGP"
    end

    test "formats filter" do
      bgp = Algebra.bgp([{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}])
      filter = Algebra.filter({:bound, {:variable, "s"}}, bgp)
      str = Algebra.to_string(filter)

      assert str =~ "Filter"
      assert str =~ "bound"
    end

    test "formats distinct" do
      bgp = Algebra.bgp([])
      distinct = Algebra.distinct(bgp)
      str = Algebra.to_string(distinct)

      assert str =~ "Distinct"
    end

    test "formats slice" do
      bgp = Algebra.bgp([])
      slice = Algebra.slice(bgp, 10, 5)
      str = Algebra.to_string(slice)

      assert str =~ "Slice"
      assert str =~ "10"
      assert str =~ "5"
    end
  end

  # ===========================================================================
  # Integration with Parser Output
  # ===========================================================================

  describe "parser compatibility" do
    test "parser output matches algebra node structure" do
      {:ok, {:select, props}} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      pattern = Enum.find_value(props, fn {k, v} -> if k == "pattern", do: v end)

      # Parser produces {:project, {:bgp, triples}, vars}
      assert {:project, {:bgp, triples}, _vars} = pattern
      assert is_list(triples)
      assert [{:triple, _, _, _}] = triples
    end

    test "validates parser output" do
      {:ok, {:select, props}} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      pattern = Enum.find_value(props, fn {k, v} -> if k == "pattern", do: v end)

      assert :ok = Algebra.validate(pattern)
    end

    test "extracts variables from parser output" do
      {:ok, {:select, props}} =
        TripleStore.SPARQL.Parser.parse("SELECT ?s ?name WHERE { ?s <http://example.org/name> ?name }")

      pattern = Enum.find_value(props, fn {k, v} -> if k == "pattern", do: v end)
      vars = Algebra.variables(pattern)

      assert {:variable, "s"} in vars
      assert {:variable, "name"} in vars
    end

    test "handles complex parser output with OPTIONAL" do
      {:ok, {:select, props}} =
        TripleStore.SPARQL.Parser.parse("""
        SELECT ?s ?name ?age WHERE {
          ?s <http://example.org/name> ?name .
          OPTIONAL { ?s <http://example.org/age> ?age }
        }
        """)

      pattern = Enum.find_value(props, fn {k, v} -> if k == "pattern", do: v end)

      assert :ok = Algebra.validate(pattern)
      assert Algebra.node_type(pattern) == :project

      # Should have left_join in the tree
      types = Algebra.fold(pattern, [], fn node, acc -> [Algebra.node_type(node) | acc] end)
      assert :left_join in types
    end

    test "handles FILTER in parser output" do
      {:ok, {:select, props}} =
        TripleStore.SPARQL.Parser.parse("""
        SELECT ?s ?age WHERE {
          ?s <http://example.org/age> ?age .
          FILTER(?age > 18)
        }
        """)

      pattern = Enum.find_value(props, fn {k, v} -> if k == "pattern", do: v end)

      assert :ok = Algebra.validate(pattern)

      types = Algebra.fold(pattern, [], fn node, acc -> [Algebra.node_type(node) | acc] end)
      assert :filter in types
    end

    test "handles UNION in parser output" do
      {:ok, {:select, props}} =
        TripleStore.SPARQL.Parser.parse("""
        SELECT ?s WHERE {
          { ?s <http://example.org/a> ?o }
          UNION
          { ?s <http://example.org/b> ?o }
        }
        """)

      pattern = Enum.find_value(props, fn {k, v} -> if k == "pattern", do: v end)

      assert :ok = Algebra.validate(pattern)

      types = Algebra.fold(pattern, [], fn node, acc -> [Algebra.node_type(node) | acc] end)
      assert :union in types
    end

    test "handles ORDER BY and LIMIT in parser output" do
      {:ok, {:select, props}} =
        TripleStore.SPARQL.Parser.parse("""
        SELECT ?s ?name WHERE {
          ?s <http://example.org/name> ?name
        }
        ORDER BY ?name
        LIMIT 10
        """)

      pattern = Enum.find_value(props, fn {k, v} -> if k == "pattern", do: v end)

      assert :ok = Algebra.validate(pattern)

      types = Algebra.fold(pattern, [], fn node, acc -> [Algebra.node_type(node) | acc] end)
      assert :order_by in types
      assert :slice in types
    end

    test "handles DISTINCT in parser output" do
      {:ok, {:select, props}} =
        TripleStore.SPARQL.Parser.parse("SELECT DISTINCT ?s WHERE { ?s ?p ?o }")

      pattern = Enum.find_value(props, fn {k, v} -> if k == "pattern", do: v end)

      assert :ok = Algebra.validate(pattern)
      assert Algebra.node_type(pattern) == :distinct
    end

    test "handles GROUP BY with aggregates in parser output" do
      {:ok, {:select, props}} =
        TripleStore.SPARQL.Parser.parse("""
        SELECT ?s (COUNT(?type) AS ?count) WHERE {
          ?s <http://example.org/type> ?type
        }
        GROUP BY ?s
        """)

      pattern = Enum.find_value(props, fn {k, v} -> if k == "pattern", do: v end)

      assert :ok = Algebra.validate(pattern)

      types = Algebra.fold(pattern, [], fn node, acc -> [Algebra.node_type(node) | acc] end)
      assert :group in types
      assert :extend in types
    end
  end

  # ===========================================================================
  # AST Compilation (Task 2.2.2)
  # ===========================================================================

  describe "from_ast/1" do
    test "compiles SELECT query" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      {:ok, compiled} = Algebra.from_ast(ast)

      assert compiled.type == :select
      assert Algebra.node_type(compiled.pattern) == :project
      assert is_nil(compiled.dataset)
      assert is_nil(compiled.base_iri)
      assert is_nil(compiled.template)
    end

    test "compiles CONSTRUCT query" do
      {:ok, ast} =
        TripleStore.SPARQL.Parser.parse(
          "CONSTRUCT { ?s <http://new> ?o } WHERE { ?s <http://old> ?o }"
        )

      {:ok, compiled} = Algebra.from_ast(ast)

      assert compiled.type == :construct
      assert Algebra.node_type(compiled.pattern) == :bgp
      assert is_list(compiled.template)
      assert length(compiled.template) == 1
    end

    test "compiles ASK query" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("ASK WHERE { ?s ?p ?o }")
      {:ok, compiled} = Algebra.from_ast(ast)

      assert compiled.type == :ask
      assert Algebra.node_type(compiled.pattern) == :bgp
    end

    test "compiles DESCRIBE query" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("DESCRIBE ?s WHERE { ?s ?p ?o }")
      {:ok, compiled} = Algebra.from_ast(ast)

      assert compiled.type == :describe
      assert Algebra.node_type(compiled.pattern) == :project
    end

    test "compiles complex SELECT with modifiers" do
      {:ok, ast} =
        TripleStore.SPARQL.Parser.parse("""
        SELECT DISTINCT ?s ?name WHERE {
          ?s <http://example.org/name> ?name .
          OPTIONAL { ?s <http://example.org/age> ?age }
          FILTER(?name != "test")
        }
        ORDER BY ?name
        LIMIT 10
        OFFSET 5
        """)

      {:ok, compiled} = Algebra.from_ast(ast)

      assert compiled.type == :select
      # Outermost should be slice (for OFFSET/LIMIT)
      assert Algebra.node_type(compiled.pattern) == :slice
    end

    test "returns error for invalid AST" do
      assert {:error, msg} = Algebra.from_ast({:invalid, []})
      assert msg =~ "Invalid AST"
    end

    test "returns error for non-tuple" do
      assert {:error, _} = Algebra.from_ast("not an ast")
    end
  end

  describe "from_ast!/1" do
    test "returns compiled query for valid AST" do
      ast = TripleStore.SPARQL.Parser.parse!("SELECT ?s WHERE { ?s ?p ?o }")
      compiled = Algebra.from_ast!(ast)

      assert compiled.type == :select
    end

    test "raises for invalid AST" do
      assert_raise ArgumentError, ~r/AST compilation failed/, fn ->
        Algebra.from_ast!({:invalid, []})
      end
    end
  end

  describe "extract_pattern/1" do
    test "extracts pattern from raw AST" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      {:ok, pattern} = Algebra.extract_pattern(ast)

      assert Algebra.node_type(pattern) == :project
    end

    test "extracts pattern from compiled query" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      {:ok, compiled} = Algebra.from_ast(ast)
      {:ok, pattern} = Algebra.extract_pattern(compiled)

      assert Algebra.node_type(pattern) == :project
    end

    test "returns error for invalid structure" do
      assert {:error, _} = Algebra.extract_pattern("not valid")
    end
  end

  describe "result_variables/1" do
    test "returns projected variables for SELECT" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s ?name WHERE { ?s ?p ?name }")
      vars = Algebra.result_variables(ast)

      assert vars == [{:variable, "s"}, {:variable, "name"}]
    end

    test "returns all variables for SELECT *" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT * WHERE { ?s ?p ?o }")
      vars = Algebra.result_variables(ast)

      # Parser expands SELECT * to all in-scope variables
      assert length(vars) == 3
      assert {:variable, "s"} in vars
      assert {:variable, "p"} in vars
      assert {:variable, "o"} in vars
    end

    test "handles DISTINCT modifier" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT DISTINCT ?s WHERE { ?s ?p ?o }")
      vars = Algebra.result_variables(ast)

      assert vars == [{:variable, "s"}]
    end

    test "handles ORDER BY and LIMIT" do
      {:ok, ast} =
        TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 10")

      vars = Algebra.result_variables(ast)

      assert vars == [{:variable, "s"}]
    end

    test "returns empty list for non-SELECT queries" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("ASK WHERE { ?s ?p ?o }")
      assert Algebra.result_variables(ast) == []
    end

    test "works with compiled query" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s ?p WHERE { ?s ?p ?o }")
      {:ok, compiled} = Algebra.from_ast(ast)
      vars = Algebra.result_variables(compiled)

      assert vars == [{:variable, "s"}, {:variable, "p"}]
    end
  end

  describe "collect_bgps/1" do
    test "collects single BGP" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      {:ok, pattern} = Algebra.extract_pattern(ast)
      bgps = Algebra.collect_bgps(pattern)

      assert length(bgps) == 1
      assert [{:bgp, triples}] = bgps
      assert length(triples) == 1
    end

    test "collects multiple BGPs from OPTIONAL" do
      {:ok, ast} =
        TripleStore.SPARQL.Parser.parse("""
        SELECT ?s WHERE {
          ?s <http://a> ?o .
          OPTIONAL { ?s <http://b> ?o2 }
        }
        """)

      {:ok, pattern} = Algebra.extract_pattern(ast)
      bgps = Algebra.collect_bgps(pattern)

      assert length(bgps) == 2
    end

    test "collects multiple BGPs from UNION" do
      {:ok, ast} =
        TripleStore.SPARQL.Parser.parse("""
        SELECT ?s WHERE {
          { ?s <http://a> ?o }
          UNION
          { ?s <http://b> ?o }
        }
        """)

      {:ok, pattern} = Algebra.extract_pattern(ast)
      bgps = Algebra.collect_bgps(pattern)

      assert length(bgps) == 2
    end
  end

  describe "triple_count/1" do
    test "counts single triple" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      {:ok, pattern} = Algebra.extract_pattern(ast)

      assert Algebra.triple_count(pattern) == 1
    end

    test "counts multiple triples in BGP" do
      {:ok, ast} =
        TripleStore.SPARQL.Parser.parse("""
        SELECT ?s WHERE {
          ?s <http://a> ?o1 .
          ?s <http://b> ?o2 .
          ?s <http://c> ?o3
        }
        """)

      {:ok, pattern} = Algebra.extract_pattern(ast)

      assert Algebra.triple_count(pattern) == 3
    end

    test "counts triples across OPTIONAL" do
      {:ok, ast} =
        TripleStore.SPARQL.Parser.parse("""
        SELECT ?s WHERE {
          ?s <http://a> ?o .
          OPTIONAL { ?s <http://b> ?o2 }
        }
        """)

      {:ok, pattern} = Algebra.extract_pattern(ast)

      assert Algebra.triple_count(pattern) == 2
    end
  end

  describe "has_optional?/1" do
    test "returns true when OPTIONAL present" do
      {:ok, ast} =
        TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o OPTIONAL { ?s ?p2 ?o2 } }")

      {:ok, pattern} = Algebra.extract_pattern(ast)

      assert Algebra.has_optional?(pattern)
    end

    test "returns false when no OPTIONAL" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      {:ok, pattern} = Algebra.extract_pattern(ast)

      refute Algebra.has_optional?(pattern)
    end
  end

  describe "has_union?/1" do
    test "returns true when UNION present" do
      {:ok, ast} =
        TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { { ?s ?p ?o } UNION { ?s ?p2 ?o2 } }")

      {:ok, pattern} = Algebra.extract_pattern(ast)

      assert Algebra.has_union?(pattern)
    end

    test "returns false when no UNION" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      {:ok, pattern} = Algebra.extract_pattern(ast)

      refute Algebra.has_union?(pattern)
    end
  end

  describe "has_filter?/1" do
    test "returns true when FILTER present" do
      {:ok, ast} =
        TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o FILTER(?o > 10) }")

      {:ok, pattern} = Algebra.extract_pattern(ast)

      assert Algebra.has_filter?(pattern)
    end

    test "returns false when no FILTER" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      {:ok, pattern} = Algebra.extract_pattern(ast)

      refute Algebra.has_filter?(pattern)
    end
  end

  describe "has_aggregation?/1" do
    test "returns true when GROUP BY present" do
      {:ok, ast} =
        TripleStore.SPARQL.Parser.parse(
          "SELECT ?s (COUNT(?o) AS ?cnt) WHERE { ?s ?p ?o } GROUP BY ?s"
        )

      {:ok, pattern} = Algebra.extract_pattern(ast)

      assert Algebra.has_aggregation?(pattern)
    end

    test "returns false when no GROUP BY" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      {:ok, pattern} = Algebra.extract_pattern(ast)

      refute Algebra.has_aggregation?(pattern)
    end
  end

  describe "collect_filters/1" do
    test "collects single filter" do
      {:ok, ast} =
        TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o FILTER(?o > 10) }")

      {:ok, pattern} = Algebra.extract_pattern(ast)
      filters = Algebra.collect_filters(pattern)

      assert length(filters) == 1
    end

    test "collects combined filter (parser combines multiple FILTERs with AND)" do
      {:ok, ast} =
        TripleStore.SPARQL.Parser.parse("""
        SELECT ?s WHERE {
          ?s ?p ?o
          FILTER(?o > 10)
          FILTER(?s != ?o)
        }
        """)

      {:ok, pattern} = Algebra.extract_pattern(ast)
      filters = Algebra.collect_filters(pattern)

      # Parser combines multiple FILTERs into a single AND expression
      assert length(filters) == 1
      [{:and, _, _}] = filters
    end

    test "returns empty list when no filters" do
      {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      {:ok, pattern} = Algebra.extract_pattern(ast)
      filters = Algebra.collect_filters(pattern)

      assert filters == []
    end
  end
end
