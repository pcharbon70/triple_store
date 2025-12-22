defmodule TripleStore.SPARQL.ParserTest do
  @moduledoc """
  Tests for the SPARQL parser NIF and Elixir interface.

  Task 2.1.1: Parser Crate Setup
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
end
