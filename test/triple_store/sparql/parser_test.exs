defmodule TripleStore.SPARQL.ParserTest do
  @moduledoc """
  Tests for the SPARQL parser NIF and Elixir interface.

  Task 2.1.1: Parser Crate Setup
  Task 2.1.2: Query Parsing - All SPARQL query forms
  """

  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.Parser

  # ===========================================================================
  # NIF Loading
  # ===========================================================================

  describe "nif_loaded?/0" do
    test "returns true when NIF is loaded" do
      assert Parser.nif_loaded?() == true
    end
  end

  # ===========================================================================
  # Basic Query Parsing
  # ===========================================================================

  describe "parse/1" do
    test "parses simple SELECT query" do
      assert {:ok, {:select, props}} = Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      assert is_list(props)
    end

    test "parses SELECT * query" do
      assert {:ok, {:select, _props}} = Parser.parse("SELECT * WHERE { ?s ?p ?o }")
    end

    test "parses SELECT with multiple variables" do
      assert {:ok, {:select, _props}} = Parser.parse("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
    end

    test "parses CONSTRUCT query" do
      query = "CONSTRUCT { ?s <http://example.org/hasName> ?name } WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name }"
      assert {:ok, {:construct, props}} = Parser.parse(query)
      assert is_list(props)
    end

    test "parses ASK query" do
      assert {:ok, {:ask, props}} = Parser.parse("ASK WHERE { ?s ?p ?o }")
      assert is_list(props)
    end

    test "parses DESCRIBE query" do
      assert {:ok, {:describe, props}} = Parser.parse("DESCRIBE ?s WHERE { ?s ?p ?o }")
      assert is_list(props)
    end

    test "returns error for invalid query" do
      assert {:error, {:parse_error, msg}} = Parser.parse("INVALID QUERY")
      assert is_binary(msg)
    end
  end

  describe "parse!/1" do
    test "returns AST for valid query" do
      ast = Parser.parse!("SELECT ?s WHERE { ?s ?p ?o }")
      assert {:select, _} = ast
    end

    test "raises ArgumentError for invalid query" do
      assert_raise ArgumentError, ~r/SPARQL parse error/, fn ->
        Parser.parse!("NOT A QUERY")
      end
    end
  end

  # ===========================================================================
  # Query Type Helpers
  # ===========================================================================

  describe "query_type/1" do
    test "returns :select for SELECT query" do
      {:ok, ast} = Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      assert Parser.query_type(ast) == :select
    end

    test "returns :construct for CONSTRUCT query" do
      {:ok, ast} = Parser.parse("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")
      assert Parser.query_type(ast) == :construct
    end

    test "returns :ask for ASK query" do
      {:ok, ast} = Parser.parse("ASK WHERE { ?s ?p ?o }")
      assert Parser.query_type(ast) == :ask
    end

    test "returns :describe for DESCRIBE query" do
      {:ok, ast} = Parser.parse("DESCRIBE ?s WHERE { ?s ?p ?o }")
      assert Parser.query_type(ast) == :describe
    end
  end

  describe "select?/1" do
    test "returns true for SELECT query" do
      {:ok, ast} = Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      assert Parser.select?(ast)
    end

    test "returns false for non-SELECT query" do
      {:ok, ast} = Parser.parse("ASK WHERE { ?s ?p ?o }")
      refute Parser.select?(ast)
    end
  end

  # ===========================================================================
  # Pattern Extraction
  # ===========================================================================

  describe "get_pattern/1" do
    test "extracts pattern from SELECT query" do
      {:ok, ast} = Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      pattern = Parser.get_pattern(ast)
      assert is_tuple(pattern)
    end

    test "extracts pattern from complex query" do
      {:ok, ast} = Parser.parse("SELECT ?s WHERE { ?s ?p ?o . ?s <http://example.org/name> ?name }")
      pattern = Parser.get_pattern(ast)
      assert pattern != nil
    end
  end

  describe "extract_variables/1" do
    test "extracts variables from simple BGP" do
      {:ok, ast} = Parser.parse("SELECT ?s ?p WHERE { ?s ?p ?o }")
      pattern = Parser.get_pattern(ast)
      vars = Parser.extract_variables(pattern)

      assert "s" in vars
      assert "p" in vars
      assert "o" in vars
    end

    test "extracts variables from multiple triple patterns" do
      {:ok, ast} = Parser.parse("SELECT * WHERE { ?s ?p ?o . ?s <http://example.org/name> ?name }")
      pattern = Parser.get_pattern(ast)
      vars = Parser.extract_variables(pattern)

      assert "s" in vars
      assert "p" in vars
      assert "o" in vars
      assert "name" in vars
    end
  end

  describe "extract_bgp_triples/1" do
    test "extracts triples from simple BGP" do
      {:ok, ast} = Parser.parse("SELECT * WHERE { ?s ?p ?o }")
      pattern = Parser.get_pattern(ast)
      triples = Parser.extract_bgp_triples(pattern)

      assert length(triples) == 1
    end

    test "extracts triples from multiple patterns" do
      {:ok, ast} = Parser.parse("SELECT * WHERE { ?s ?p ?o . ?s <http://example.org/name> ?name }")
      pattern = Parser.get_pattern(ast)
      triples = Parser.extract_bgp_triples(pattern)

      assert length(triples) == 2
    end
  end

  # ===========================================================================
  # Complex Query Features
  # ===========================================================================

  describe "FILTER queries" do
    test "parses FILTER with comparison" do
      query = "SELECT ?age WHERE { ?s <http://example.org/age> ?age . FILTER(?age > 30) }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses FILTER with string function" do
      query = "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name . FILTER(CONTAINS(?name, \"John\")) }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses FILTER with AND/OR" do
      query = "SELECT ?age WHERE { ?s <http://example.org/age> ?age . FILTER(?age > 18 && ?age < 65) }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end
  end

  describe "OPTIONAL queries" do
    test "parses simple OPTIONAL" do
      query = """
      SELECT ?name ?age WHERE {
        ?s <http://xmlns.com/foaf/0.1/name> ?name .
        OPTIONAL { ?s <http://xmlns.com/foaf/0.1/age> ?age }
      }
      """
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses multiple OPTIONAL clauses" do
      query = """
      SELECT ?name ?age ?email WHERE {
        ?s <http://xmlns.com/foaf/0.1/name> ?name .
        OPTIONAL { ?s <http://xmlns.com/foaf/0.1/age> ?age } .
        OPTIONAL { ?s <http://xmlns.com/foaf/0.1/mbox> ?email }
      }
      """
      assert {:ok, {:select, _}} = Parser.parse(query)
    end
  end

  describe "UNION queries" do
    test "parses simple UNION" do
      query = """
      SELECT ?name WHERE {
        { ?s <http://xmlns.com/foaf/0.1/name> ?name }
        UNION
        { ?s <http://schema.org/name> ?name }
      }
      """
      assert {:ok, {:select, _}} = Parser.parse(query)
    end
  end

  describe "ORDER BY queries" do
    test "parses ORDER BY ASC" do
      query = "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name } ORDER BY ?name"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses ORDER BY DESC" do
      query = "SELECT ?age WHERE { ?s <http://xmlns.com/foaf/0.1/age> ?age } ORDER BY DESC(?age)"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses multiple ORDER BY expressions" do
      query = "SELECT ?name ?age WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name . ?s <http://xmlns.com/foaf/0.1/age> ?age } ORDER BY ?name DESC(?age)"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end
  end

  describe "LIMIT and OFFSET" do
    test "parses LIMIT" do
      query = "SELECT ?s WHERE { ?s ?p ?o } LIMIT 10"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses OFFSET" do
      query = "SELECT ?s WHERE { ?s ?p ?o } OFFSET 5"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses LIMIT and OFFSET together" do
      query = "SELECT ?s WHERE { ?s ?p ?o } LIMIT 10 OFFSET 5"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end
  end

  describe "DISTINCT and REDUCED" do
    test "parses DISTINCT" do
      query = "SELECT DISTINCT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses REDUCED" do
      query = "SELECT REDUCED ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end
  end

  describe "GROUP BY and aggregates" do
    test "parses GROUP BY with COUNT" do
      query = "SELECT ?type (COUNT(?s) AS ?count) WHERE { ?s a ?type } GROUP BY ?type"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses GROUP BY with multiple aggregates" do
      query = "SELECT ?type (COUNT(?s) AS ?count) (AVG(?age) AS ?avgAge) WHERE { ?s a ?type . ?s <http://xmlns.com/foaf/0.1/age> ?age } GROUP BY ?type"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses HAVING clause" do
      query = "SELECT ?type (COUNT(?s) AS ?count) WHERE { ?s a ?type } GROUP BY ?type HAVING (COUNT(?s) > 10)"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end
  end

  describe "VALUES clause" do
    test "parses inline VALUES" do
      query = """
      SELECT ?name WHERE {
        VALUES ?s { <http://example.org/person1> <http://example.org/person2> }
        ?s <http://xmlns.com/foaf/0.1/name> ?name
      }
      """
      assert {:ok, {:select, _}} = Parser.parse(query)
    end
  end

  describe "PREFIX declarations" do
    test "parses query with PREFIX" do
      query = """
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      SELECT ?name WHERE { ?s foaf:name ?name }
      """
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses query with multiple PREFIXes" do
      query = """
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      PREFIX schema: <http://schema.org/>
      SELECT ?name ?description WHERE {
        ?s foaf:name ?name .
        ?s schema:description ?description
      }
      """
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses query with BASE" do
      query = """
      BASE <http://example.org/>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      SELECT ?name WHERE { ?s foaf:name ?name }
      """
      assert {:ok, {:select, _}} = Parser.parse(query)
    end
  end

  # ===========================================================================
  # Property Paths
  # ===========================================================================

  describe "property paths" do
    test "parses inverse path (^)" do
      query = "SELECT ?parent WHERE { <http://example.org/child> ^<http://example.org/hasChild> ?parent }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses sequence path (/)" do
      query = "SELECT ?grandparent WHERE { ?s <http://example.org/parent>/<http://example.org/parent> ?grandparent }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses alternative path (|)" do
      query = "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name>|<http://schema.org/name> ?name }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses zero or more path (*)" do
      query = "SELECT ?ancestor WHERE { ?s <http://example.org/parent>* ?ancestor }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses one or more path (+)" do
      query = "SELECT ?ancestor WHERE { ?s <http://example.org/parent>+ ?ancestor }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses zero or one path (?)" do
      query = "SELECT ?person WHERE { ?s <http://example.org/knows>? ?person }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end
  end

  # ===========================================================================
  # Subqueries
  # ===========================================================================

  describe "subqueries" do
    test "parses simple subquery" do
      query = """
      SELECT ?name WHERE {
        {
          SELECT ?s WHERE { ?s a <http://xmlns.com/foaf/0.1/Person> } LIMIT 10
        }
        ?s <http://xmlns.com/foaf/0.1/name> ?name
      }
      """
      assert {:ok, {:select, _}} = Parser.parse(query)
    end
  end

  # ===========================================================================
  # Literals
  # ===========================================================================

  describe "literal parsing" do
    test "parses simple string literal" do
      query = "SELECT ?s WHERE { ?s <http://xmlns.com/foaf/0.1/name> \"John\" }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses language-tagged literal" do
      query = "SELECT ?s WHERE { ?s <http://xmlns.com/foaf/0.1/name> \"John\"@en }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses typed literal" do
      query = "SELECT ?s WHERE { ?s <http://xmlns.com/foaf/0.1/age> \"30\"^^<http://www.w3.org/2001/XMLSchema#integer> }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses integer literal" do
      query = "SELECT ?s WHERE { ?s <http://xmlns.com/foaf/0.1/age> 30 }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end

    test "parses boolean literal" do
      query = "SELECT ?s WHERE { ?s <http://example.org/active> true }"
      assert {:ok, {:select, _}} = Parser.parse(query)
    end
  end

  # ===========================================================================
  # Task 2.1.2: Query Parsing - All SPARQL Query Forms
  # ===========================================================================

  # Task 2.1.2.2: SELECT queries with all projection forms
  describe "Task 2.1.2.2: SELECT projection forms" do
    test "SELECT with explicit variables projects specified variables" do
      {:ok, {:select, props}} = Parser.parse("SELECT ?s ?p WHERE { ?s ?p ?o }")
      pattern = Enum.find_value(props, fn {"pattern", p} -> p; _ -> nil end)

      # Should have project node with selected variables
      assert {:project, _inner, vars} = pattern
      var_names = Enum.map(vars, fn {:variable, name} -> name end)
      assert "s" in var_names
      assert "p" in var_names
      refute "o" in var_names
    end

    test "SELECT * projects all in-scope variables" do
      {:ok, {:select, props}} = Parser.parse("SELECT * WHERE { ?s ?p ?o }")
      pattern = Enum.find_value(props, fn {"pattern", p} -> p; _ -> nil end)

      # SELECT * should still have a project node
      assert {:project, _inner, vars} = pattern
      var_names = Enum.map(vars, fn {:variable, name} -> name end)
      assert "s" in var_names
      assert "p" in var_names
      assert "o" in var_names
    end

    test "SELECT DISTINCT wraps result in distinct node" do
      {:ok, {:select, props}} = Parser.parse("SELECT DISTINCT ?s WHERE { ?s ?p ?o }")
      pattern = Enum.find_value(props, fn {"pattern", p} -> p; _ -> nil end)

      # Should have distinct wrapper
      assert {:distinct, {:project, _inner, _vars}} = pattern
    end

    test "SELECT REDUCED wraps result in reduced node" do
      {:ok, {:select, props}} = Parser.parse("SELECT REDUCED ?s WHERE { ?s ?p ?o }")
      pattern = Enum.find_value(props, fn {"pattern", p} -> p; _ -> nil end)

      # Should have reduced wrapper
      assert {:reduced, {:project, _inner, _vars}} = pattern
    end

    test "SELECT with expression alias (AS) creates extend node" do
      {:ok, {:select, props}} = Parser.parse("SELECT (?x + 1 AS ?y) WHERE { ?s ?p ?x }")
      pattern = Enum.find_value(props, fn {"pattern", p} -> p; _ -> nil end)

      # Should have extend node for the expression binding
      assert {:project, inner, _vars} = pattern
      assert {:extend, _bgp, {:variable, "y"}, {:add, _, _}} = inner
    end

    test "SELECT with multiple expression aliases" do
      query = "SELECT (?x + 1 AS ?y) (?x * 2 AS ?z) WHERE { ?s ?p ?x }"
      {:ok, {:select, props}} = Parser.parse(query)
      pattern = Enum.find_value(props, fn {"pattern", p} -> p; _ -> nil end)

      # Should project both computed variables
      assert {:project, _inner, vars} = pattern
      var_names = Enum.map(vars, fn {:variable, name} -> name end)
      assert "y" in var_names
      assert "z" in var_names
    end
  end

  # Task 2.1.2.3: CONSTRUCT queries with template patterns
  describe "Task 2.1.2.3: CONSTRUCT template patterns" do
    test "CONSTRUCT returns template as list of triple patterns" do
      query = "CONSTRUCT { ?s <http://example.org/name> ?name } WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name }"
      {:ok, {:construct, props}} = Parser.parse(query)

      template = Enum.find_value(props, fn {"template", t} -> t; _ -> nil end)

      assert is_list(template)
      assert length(template) == 1

      [{:triple, s, p, o}] = template
      assert {:variable, "s"} = s
      assert {:named_node, "http://example.org/name"} = p
      assert {:variable, "name"} = o
    end

    test "CONSTRUCT with multiple template patterns" do
      query = """
      CONSTRUCT {
        ?s <http://example.org/name> ?name .
        ?s <http://example.org/type> <http://example.org/Person>
      } WHERE {
        ?s <http://xmlns.com/foaf/0.1/name> ?name
      }
      """
      {:ok, {:construct, props}} = Parser.parse(query)

      template = Enum.find_value(props, fn {"template", t} -> t; _ -> nil end)
      assert length(template) == 2
    end

    test "CONSTRUCT WHERE shorthand (template matches WHERE pattern)" do
      query = "CONSTRUCT WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name }"
      {:ok, {:construct, props}} = Parser.parse(query)

      template = Enum.find_value(props, fn {"template", t} -> t; _ -> nil end)
      assert is_list(template)
      assert length(template) == 1
    end

    test "CONSTRUCT with blank node in template" do
      query = "CONSTRUCT { ?s <http://example.org/knows> _:b0 } WHERE { ?s ?p ?o }"
      {:ok, {:construct, props}} = Parser.parse(query)

      template = Enum.find_value(props, fn {"template", t} -> t; _ -> nil end)
      [{:triple, _s, _p, o}] = template
      assert {:blank_node, _} = o
    end
  end

  # Task 2.1.2.4: ASK queries for boolean results
  describe "Task 2.1.2.4: ASK queries" do
    test "ASK query has pattern but no projection" do
      {:ok, {:ask, props}} = Parser.parse("ASK WHERE { ?s ?p ?o }")

      pattern = Enum.find_value(props, fn {"pattern", p} -> p; _ -> nil end)
      assert pattern != nil

      # ASK should have BGP pattern, not project
      assert {:bgp, _triples} = pattern
    end

    test "ASK without explicit WHERE keyword" do
      {:ok, {:ask, props}} = Parser.parse("ASK { ?s a <http://example.org/Person> }")

      pattern = Enum.find_value(props, fn {"pattern", p} -> p; _ -> nil end)
      assert {:bgp, _triples} = pattern
    end

    test "ASK with complex pattern" do
      query = """
      ASK WHERE {
        ?s a <http://example.org/Person> .
        ?s <http://xmlns.com/foaf/0.1/name> ?name .
        FILTER(STRLEN(?name) > 5)
      }
      """
      {:ok, {:ask, props}} = Parser.parse(query)

      pattern = Enum.find_value(props, fn {"pattern", p} -> p; _ -> nil end)
      assert {:filter, _expr, _inner} = pattern
    end

    test "ASK with OPTIONAL" do
      query = """
      ASK WHERE {
        ?s a <http://example.org/Person> .
        OPTIONAL { ?s <http://xmlns.com/foaf/0.1/age> ?age }
      }
      """
      {:ok, {:ask, _props}} = Parser.parse(query)
    end
  end

  # Task 2.1.2.5: DESCRIBE queries with IRI expansion
  describe "Task 2.1.2.5: DESCRIBE queries" do
    test "DESCRIBE with single variable" do
      {:ok, {:describe, props}} = Parser.parse("DESCRIBE ?s WHERE { ?s ?p ?o }")

      pattern = Enum.find_value(props, fn {"pattern", p} -> p; _ -> nil end)
      assert pattern != nil
    end

    test "DESCRIBE with single IRI (no WHERE clause)" do
      {:ok, {:describe, props}} = Parser.parse("DESCRIBE <http://example.org/person1>")

      # DESCRIBE with just IRI should still parse
      pattern = Enum.find_value(props, fn {"pattern", p} -> p; _ -> nil end)
      # Pattern may be a table with the single IRI
      assert pattern != nil
    end

    test "DESCRIBE with multiple IRIs" do
      query = "DESCRIBE <http://example.org/person1> <http://example.org/person2>"
      {:ok, {:describe, _props}} = Parser.parse(query)
    end

    test "DESCRIBE with mixed variables and IRIs" do
      query = "DESCRIBE ?s <http://example.org/person1> WHERE { ?s a <http://example.org/Person> }"
      {:ok, {:describe, _props}} = Parser.parse(query)
    end

    test "DESCRIBE * expands to all variables" do
      query = "DESCRIBE * WHERE { ?s ?p ?o }"
      {:ok, {:describe, _props}} = Parser.parse(query)
    end

    test "DESCRIBE with PREFIX for IRI expansion" do
      query = """
      PREFIX ex: <http://example.org/>
      DESCRIBE ex:person1
      """
      {:ok, {:describe, _props}} = Parser.parse(query)
    end
  end

  # ===========================================================================
  # Task 2.1.3: UPDATE Parsing
  # ===========================================================================

  # Task 2.1.3.1: parse_update API
  describe "parse_update/1" do
    test "returns {:ok, ast} for valid UPDATE" do
      assert {:ok, {:update, _props}} =
               Parser.parse_update("INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }")
    end

    test "returns {:error, {:parse_error, msg}} for invalid UPDATE" do
      assert {:error, {:parse_error, msg}} = Parser.parse_update("NOT AN UPDATE")
      assert is_binary(msg)
    end
  end

  describe "parse_update!/1" do
    test "returns AST for valid UPDATE" do
      ast = Parser.parse_update!("INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }")
      assert {:update, _} = ast
    end

    test "raises ArgumentError for invalid UPDATE" do
      assert_raise ArgumentError, ~r/SPARQL UPDATE parse error/, fn ->
        Parser.parse_update!("NOT AN UPDATE")
      end
    end
  end

  describe "update?/1" do
    test "returns true for UPDATE AST" do
      {:ok, ast} = Parser.parse_update("INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }")
      assert Parser.update?(ast)
    end

    test "returns false for QUERY AST" do
      {:ok, ast} = Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      refute Parser.update?(ast)
    end
  end

  describe "get_operations/1" do
    test "extracts operations from UPDATE AST" do
      {:ok, ast} = Parser.parse_update("INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }")
      ops = Parser.get_operations(ast)
      assert length(ops) == 1
    end

    test "returns empty list for non-UPDATE AST" do
      {:ok, ast} = Parser.parse("SELECT ?s WHERE { ?s ?p ?o }")
      assert Parser.get_operations(ast) == []
    end
  end

  # Task 2.1.3.2: INSERT DATA operations
  describe "Task 2.1.3.2: INSERT DATA operations" do
    test "parses INSERT DATA with single triple" do
      query = "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      assert [{:insert_data, quads}] = ops
      assert length(quads) == 1

      [{:quad, s, p, o, g}] = quads
      assert {:named_node, "http://example.org/s"} = s
      assert {:named_node, "http://example.org/p"} = p
      assert {:named_node, "http://example.org/o"} = o
      assert :default_graph = g
    end

    test "parses INSERT DATA with multiple triples" do
      query = """
      INSERT DATA {
        <http://example.org/s1> <http://example.org/p> <http://example.org/o1> .
        <http://example.org/s2> <http://example.org/p> <http://example.org/o2>
      }
      """
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:insert_data, quads}] = ops
      assert length(quads) == 2
    end

    test "parses INSERT DATA with literal value" do
      query = "INSERT DATA { <http://example.org/s> <http://example.org/name> \"John\" }"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:insert_data, [{:quad, _s, _p, o, _g}]}] = ops
      assert {:literal, :simple, "John"} = o
    end

    test "parses INSERT DATA with typed literal" do
      query = "INSERT DATA { <http://example.org/s> <http://example.org/age> \"30\"^^<http://www.w3.org/2001/XMLSchema#integer> }"
      {:ok, {:update, _}} = Parser.parse_update(query)
    end

    test "parses INSERT DATA with named graph" do
      query = "INSERT DATA { GRAPH <http://example.org/graph1> { <http://example.org/s> <http://example.org/p> <http://example.org/o> } }"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:insert_data, [{:quad, _s, _p, _o, g}]}] = ops
      assert {:named_graph, "http://example.org/graph1"} = g
    end
  end

  # Task 2.1.3.3: DELETE DATA operations
  describe "Task 2.1.3.3: DELETE DATA operations" do
    test "parses DELETE DATA with single triple" do
      query = "DELETE DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      assert [{:delete_data, quads}] = ops
      assert length(quads) == 1
    end

    test "parses DELETE DATA with multiple triples" do
      query = """
      DELETE DATA {
        <http://example.org/s1> <http://example.org/p> <http://example.org/o1> .
        <http://example.org/s2> <http://example.org/p> <http://example.org/o2>
      }
      """
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:delete_data, quads}] = ops
      assert length(quads) == 2
    end

    test "parses DELETE DATA with named graph" do
      query = "DELETE DATA { GRAPH <http://example.org/graph1> { <http://example.org/s> <http://example.org/p> <http://example.org/o> } }"
      {:ok, {:update, _}} = Parser.parse_update(query)
    end
  end

  # Task 2.1.3.4: DELETE WHERE / INSERT WHERE operations
  describe "Task 2.1.3.4: DELETE WHERE / INSERT WHERE operations" do
    test "parses DELETE WHERE" do
      query = "DELETE { ?s <http://example.org/p> ?o } WHERE { ?s <http://example.org/p> ?o }"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:delete_insert, op_props}] = ops

      delete = Enum.find_value(op_props, fn {"delete", d} -> d; _ -> nil end)
      insert = Enum.find_value(op_props, fn {"insert", i} -> i; _ -> nil end)
      pattern = Enum.find_value(op_props, fn {"pattern", p} -> p; _ -> nil end)

      assert length(delete) == 1
      assert insert == []
      assert pattern != nil
    end

    test "parses INSERT WHERE" do
      query = "INSERT { ?s <http://example.org/newPred> ?o } WHERE { ?s <http://example.org/oldPred> ?o }"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:delete_insert, op_props}] = ops

      delete = Enum.find_value(op_props, fn {"delete", d} -> d; _ -> nil end)
      insert = Enum.find_value(op_props, fn {"insert", i} -> i; _ -> nil end)

      assert delete == []
      assert length(insert) == 1
    end

    test "parses DELETE INSERT WHERE (modify)" do
      query = """
      DELETE { ?s <http://example.org/oldPred> ?o }
      INSERT { ?s <http://example.org/newPred> ?o }
      WHERE { ?s <http://example.org/oldPred> ?o }
      """
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:delete_insert, op_props}] = ops

      delete = Enum.find_value(op_props, fn {"delete", d} -> d; _ -> nil end)
      insert = Enum.find_value(op_props, fn {"insert", i} -> i; _ -> nil end)

      assert length(delete) == 1
      assert length(insert) == 1
    end

    test "parses DELETE WHERE with complex pattern" do
      query = """
      DELETE { ?s <http://example.org/p> ?o }
      WHERE {
        ?s <http://example.org/p> ?o .
        FILTER(?o > 10)
      }
      """
      {:ok, {:update, _}} = Parser.parse_update(query)
    end
  end

  # Task 2.1.3.5: LOAD and CLEAR operations
  describe "Task 2.1.3.5: LOAD and CLEAR operations" do
    test "parses LOAD" do
      query = "LOAD <http://example.org/data.ttl>"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:load, load_props}] = ops

      silent = Enum.find_value(load_props, fn {"silent", s} -> s; _ -> nil end)
      source = Enum.find_value(load_props, fn {"source", s} -> s; _ -> nil end)

      refute silent
      assert {:named_node, "http://example.org/data.ttl"} = source
    end

    test "parses LOAD SILENT" do
      query = "LOAD SILENT <http://example.org/data.ttl>"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:load, load_props}] = ops

      silent = Enum.find_value(load_props, fn {"silent", s} -> s; _ -> nil end)
      assert silent
    end

    test "parses LOAD INTO GRAPH" do
      query = "LOAD <http://example.org/data.ttl> INTO GRAPH <http://example.org/graph1>"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:load, load_props}] = ops

      dest = Enum.find_value(load_props, fn {"destination", d} -> d; _ -> nil end)
      assert {:named_graph, "http://example.org/graph1"} = dest
    end

    test "parses CLEAR ALL" do
      query = "CLEAR ALL"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:clear, clear_props}] = ops

      silent = Enum.find_value(clear_props, fn {"silent", s} -> s; _ -> nil end)
      graph = Enum.find_value(clear_props, fn {"graph", g} -> g; _ -> nil end)

      refute silent
      assert :all_graphs = graph
    end

    test "parses CLEAR SILENT ALL" do
      query = "CLEAR SILENT ALL"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:clear, clear_props}] = ops

      silent = Enum.find_value(clear_props, fn {"silent", s} -> s; _ -> nil end)
      assert silent
    end

    test "parses CLEAR DEFAULT" do
      query = "CLEAR DEFAULT"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:clear, clear_props}] = ops

      graph = Enum.find_value(clear_props, fn {"graph", g} -> g; _ -> nil end)
      assert :default_graph = graph
    end

    test "parses CLEAR NAMED" do
      query = "CLEAR NAMED"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:clear, clear_props}] = ops

      graph = Enum.find_value(clear_props, fn {"graph", g} -> g; _ -> nil end)
      assert :all_named = graph
    end

    test "parses CLEAR GRAPH" do
      query = "CLEAR GRAPH <http://example.org/graph1>"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:clear, clear_props}] = ops

      graph = Enum.find_value(clear_props, fn {"graph", g} -> g; _ -> nil end)
      assert {:named_graph, "http://example.org/graph1"} = graph
    end

    test "parses DROP GRAPH" do
      query = "DROP GRAPH <http://example.org/graph1>"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:drop, drop_props}] = ops

      graph = Enum.find_value(drop_props, fn {"graph", g} -> g; _ -> nil end)
      assert {:named_graph, "http://example.org/graph1"} = graph
    end

    test "parses CREATE GRAPH" do
      query = "CREATE GRAPH <http://example.org/newgraph>"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:create, create_props}] = ops

      silent = Enum.find_value(create_props, fn {"silent", s} -> s; _ -> nil end)
      graph = Enum.find_value(create_props, fn {"graph", g} -> g; _ -> nil end)

      refute silent
      assert {:named_node, "http://example.org/newgraph"} = graph
    end

    test "parses CREATE SILENT GRAPH" do
      query = "CREATE SILENT GRAPH <http://example.org/newgraph>"
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      [{:create, create_props}] = ops

      silent = Enum.find_value(create_props, fn {"silent", s} -> s; _ -> nil end)
      assert silent
    end
  end

  # Multiple operations in single update
  describe "multiple UPDATE operations" do
    test "parses multiple operations separated by semicolon" do
      query = """
      INSERT DATA { <http://example.org/s1> <http://example.org/p> <http://example.org/o1> } ;
      DELETE DATA { <http://example.org/s2> <http://example.org/p> <http://example.org/o2> }
      """
      {:ok, {:update, props}} = Parser.parse_update(query)

      ops = Enum.find_value(props, fn {"operations", o} -> o; _ -> nil end)
      assert length(ops) == 2

      assert {:insert_data, _} = Enum.at(ops, 0)
      assert {:delete_data, _} = Enum.at(ops, 1)
    end
  end

  # PREFIX support in UPDATE
  describe "UPDATE with PREFIX" do
    test "parses UPDATE with PREFIX declarations" do
      query = """
      PREFIX ex: <http://example.org/>
      INSERT DATA { ex:s ex:p ex:o }
      """
      {:ok, {:update, _}} = Parser.parse_update(query)
    end
  end
end
