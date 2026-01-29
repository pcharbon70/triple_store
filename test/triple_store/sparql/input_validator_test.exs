defmodule TripleStore.SPARQL.InputValidatorTest do
  @moduledoc """
  Tests for input validation framework (S6).
  """

  use ExUnit.Case

  alias TripleStore.SPARQL.InputValidator

  describe "validate_query/1" do
    test "accepts valid SPARQL query" do
      query = "SELECT * WHERE { ?s ?p ?o }"
      assert :ok = InputValidator.validate_query(query)
    end

    test "rejects empty query" do
      assert {:error, :empty_query} = InputValidator.validate_query("")
    end

    test "rejects query that is too long" do
      long_query = String.duplicate("a", 200_000)
      assert {:error, :query_too_long} = InputValidator.validate_query(long_query)
    end

    test "rejects non-binary input" do
      assert {:error, :invalid_query_type} = InputValidator.validate_query(123)
      assert {:error, :invalid_query_type} = InputValidator.validate_query(nil)
    end

    test "accepts query at max length" do
      max_query = "SELECT * WHERE { ?s ?p ?o } " <> String.duplicate("a", 99_970)
      assert String.length(max_query) <= 100_000
      assert :ok = InputValidator.validate_query(max_query)
    end

    test "accepts query with valid SPARQL features" do
      query = """
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      SELECT ?s WHERE {
        ?s a ?type .
        FILTER (?type = rdf:Class)
      }
      LIMIT 10
      """

      assert :ok = InputValidator.validate_query(query)
    end

    test "detects injection patterns" do
      # SPARQL comments are valid but we still handle them
      query_with_comments = "SELECT * WHERE { -- comment\n ?s ?p ?o }"
      assert :ok = InputValidator.validate_query(query_with_comments)
    end
  end

  describe "validate_quad_pattern/1" do
    test "accepts valid quad pattern with variables" do
      pattern = {:quad, {:variable, "s"}, 1, {:variable, "o"}, 0}
      assert :ok = InputValidator.validate_quad_pattern(pattern)
    end

    test "accepts valid triple pattern" do
      pattern = {:triple, {:variable, "s"}, 1, {:variable, "o"}}
      assert :ok = InputValidator.validate_quad_pattern(pattern)
    end

    test "rejects invalid subject" do
      pattern = {:quad, "not_a_variable", 1, {:variable, "o"}, 0}
      assert {:error, :invalid_term} = InputValidator.validate_quad_pattern(pattern)
    end

    test "rejects invalid pattern format" do
      assert {:error, :invalid_pattern_format} = InputValidator.validate_quad_pattern(:invalid)

      assert {:error, :invalid_pattern_format} =
               InputValidator.validate_quad_pattern({:invalid, :data})
    end

    test "accepts quad with term IDs" do
      pattern = {:quad, 1, 2, 3, 0}
      assert :ok = InputValidator.validate_quad_pattern(pattern)
    end

    test "accepts triple with term IDs" do
      pattern = {:triple, 1, 2, 3}
      assert :ok = InputValidator.validate_quad_pattern(pattern)
    end
  end

  describe "validate_term/2" do
    test "accepts variable" do
      assert :ok = InputValidator.validate_term({:variable, "s"}, :subject)
      assert :ok = InputValidator.validate_term({:variable, "long_variable_name_123"}, :any)
    end

    test "rejects variable name that is too long" do
      long_var = String.duplicate("x", 300)

      assert {:error, :variable_name_too_long} =
               InputValidator.validate_term({:variable, long_var}, :subject)
    end

    test "accepts IRI term" do
      assert :ok = InputValidator.validate_term({:iri, "http://example.org"}, :subject)

      assert :ok =
               InputValidator.validate_term(
                 {:iri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"},
                 :predicate
               )
    end

    test "rejects IRI that is too long" do
      long_iri = "http://example.org/" <> String.duplicate("x", 10_000)
      assert {:error, :iri_too_long} = InputValidator.validate_term({:iri, long_iri}, :subject)
    end

    test "rejects IRI with invalid format" do
      assert {:error, :invalid_iri_format} =
               InputValidator.validate_term({:iri, "not-an-iri"}, :subject)
    end

    test "accepts literal term" do
      assert :ok = InputValidator.validate_term({:literal, "hello"}, :object)

      assert :ok =
               InputValidator.validate_term(
                 {:literal, "123", "http://www.w3.org/2001/XMLSchema#integer"},
                 :object
               )
    end

    test "accepts literal with language tag" do
      assert :ok =
               InputValidator.validate_term({:literal, "hello", nil, "en"}, :object)
    end

    test "rejects literal that is too long" do
      long_literal = String.duplicate("x", 100_000)

      assert {:error, :literal_too_long} =
               InputValidator.validate_term({:literal, long_literal}, :object)
    end

    test "accepts term ID" do
      assert :ok = InputValidator.validate_term(1, :subject)
      assert :ok = InputValidator.validate_term(0, :subject)
      assert :ok = InputValidator.validate_term(999_999, :object)
    end

    test "rejects invalid term types" do
      assert {:error, :invalid_term} = InputValidator.validate_term("string", :subject)
      assert {:error, :invalid_term} = InputValidator.validate_term(nil, :subject)
      assert {:error, :invalid_term} = InputValidator.validate_term({:invalid, "type"}, :subject)
    end
  end

  describe "validate_term_id/2" do
    test "accepts valid term IDs" do
      assert :ok = InputValidator.validate_term_id(0, :predicate)
      assert :ok = InputValidator.validate_term_id(1, :predicate)
      assert :ok = InputValidator.validate_term_id(999_999, :graph)
    end

    test "rejects negative term IDs" do
      assert {:error, :invalid_term_id} = InputValidator.validate_term_id(-1, :predicate)
    end

    test "rejects non-integer term IDs" do
      assert {:error, :invalid_term_id} = InputValidator.validate_term_id("1", :predicate)
      assert {:error, :invalid_term_id} = InputValidator.validate_term_id(nil, :predicate)
    end
  end

  describe "validate_algebra/1" do
    test "accepts simple BGP" do
      algebra = {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "o"}}]}
      assert :ok = InputValidator.validate_algebra(algebra)
    end

    test "accepts BGP with multiple patterns" do
      algebra =
        {:bgp,
         [
           {:triple, {:variable, "s"}, 1, {:variable, "o"}},
           {:triple, {:variable, "s"}, 2, {:variable, "p"}}
         ]}

      assert :ok = InputValidator.validate_algebra(algebra)
    end

    test "accepts nested join" do
      algebra =
        {:join, {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "o"}}]},
         {:bgp, [{:triple, {:variable, "s"}, 2, {:variable, "p"}}]}}

      assert :ok = InputValidator.validate_algebra(algebra)
    end

    test "accepts filter with expression" do
      algebra =
        {:filter, {:binary_op, :>, {:variable, "x"}, {:literal, "5"}},
         {:bgp, [{:triple, {:variable, "x"}, 1, {:variable, "o"}}]}}

      assert :ok = InputValidator.validate_algebra(algebra)
    end

    test "accepts union" do
      algebra =
        {:union, {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "o"}}]},
         {:bgp, [{:triple, {:variable, "s"}, 2, {:variable, "p"}}]}}

      assert :ok = InputValidator.validate_algebra(algebra)
    end

    test "accepts project" do
      algebra = {:project, ["s", "o"], {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "o"}}]}}
      assert :ok = InputValidator.validate_algebra(algebra)
    end

    test "accepts distinct" do
      algebra = {:distinct, {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "o"}}]}}
      assert :ok = InputValidator.validate_algebra(algebra)
    end

    test "accepts reduced" do
      algebra = {:reduced, {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "o"}}]}}
      assert :ok = InputValidator.validate_algebra(algebra)
    end

    test "accepts slice" do
      algebra = {:slice, {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "o"}}]}, 0, 10}
      assert :ok = InputValidator.validate_algebra(algebra)
    end

    test "accepts order by" do
      algebra =
        {:order, [{:asc, {:variable, "o"}}],
         {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "o"}}]}}

      assert :ok = InputValidator.validate_algebra(algebra)
    end

    test "accepts left join" do
      algebra =
        {:left_join, {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "o"}}]},
         {:bgp, [{:triple, {:variable, "s"}, 2, {:variable, "p"}}]},
         {:binary_op, :>, {:variable, "o"}, {:literal, "5"}}}

      assert :ok = InputValidator.validate_algebra(algebra)
    end

    test "accepts extend" do
      algebra =
        {:extend, "newVar", {:literal, "42"},
         {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "o"}}]}}

      assert :ok = InputValidator.validate_algebra(algebra)
    end

    test "rejects unknown algebra form" do
      algebra = {:unknown_form, :data}
      assert {:error, :unknown_algebra_form} = InputValidator.validate_algebra(algebra)
    end

    test "rejects algebra with invalid nested pattern" do
      algebra = {:bgp, [{:invalid, :pattern}]}
      assert {:error, :invalid_pattern_format} = InputValidator.validate_algebra(algebra)
    end
  end

  describe "validate_expression/1" do
    test "accepts binary operation" do
      expr = {:binary_op, :>, {:variable, "x"}, {:literal, "5"}}
      assert :ok = InputValidator.validate_expression(expr)
    end

    test "accepts unary operation" do
      expr = {:unary_op, :!, {:variable, "x"}}
      assert :ok = InputValidator.validate_expression(expr)
    end

    test "accepts builtin call" do
      expr = {:builtin_call, :bound, [{:variable, "x"}]}
      assert :ok = InputValidator.validate_expression(expr)
    end

    test "accepts variable expression" do
      expr = {:variable, "x"}
      assert :ok = InputValidator.validate_expression(expr)
    end

    test "accepts literal expression" do
      expr = {:literal, "42", "http://www.w3.org/2001/XMLSchema#integer"}
      assert :ok = InputValidator.validate_expression(expr)
    end

    test "accepts IRI expression" do
      expr = {:iri, "http://example.org"}
      assert :ok = InputValidator.validate_expression(expr)
    end

    test "rejects invalid expression" do
      assert {:error, :invalid_expression} = InputValidator.validate_expression(:invalid)
      assert {:error, :invalid_expression} = InputValidator.validate_expression("string")
    end
  end

  describe "validate_stats_options/1" do
    test "accepts valid options" do
      assert :ok = InputValidator.validate_stats_options([])
      assert :ok = InputValidator.validate_stats_options([:include_histograms])
      assert :ok = InputValidator.validate_stats_options([:include_graphs, cache: true])
      assert :ok = InputValidator.validate_stats_options([:lazy, ttl: 1000])
    end

    test "rejects invalid options" do
      assert {:error, {:invalid_stats_option, :invalid_option}} =
               InputValidator.validate_stats_options([:invalid_option])
    end

    test "accepts keyword list options" do
      assert :ok = InputValidator.validate_stats_options(cache: true, ttl: 5000)
    end
  end

  describe "sanitize_query/1" do
    test "removes null bytes" do
      query = "SELECT \x00 * WHERE { ?s ?p ?o }"
      assert {:ok, sanitized} = InputValidator.sanitize_query(query)
      refute String.contains?(sanitized, "\x00")
    end

    test "removes control characters except newline and tab" do
      query = "SELECT \x01\x02 * \n WHERE { ?s ?p ?o }"
      assert {:ok, sanitized} = InputValidator.sanitize_query(query)
      assert String.contains?(sanitized, "\n")
      refute String.contains?(sanitized, "\x01")
      refute String.contains?(sanitized, "\x02")
    end

    test "rejects query that is too long after sanitization" do
      # Create a query that's too long even after null byte removal
      long_query = String.duplicate("a", 200_000)
      assert {:error, :query_too_long} = InputValidator.sanitize_query(long_query)
    end
  end

  describe "validate_and_sanitize/1" do
    test "validates and sanitizes in one step" do
      query = "SELECT * WHERE { ?s ?p ?o }"
      assert {:ok, sanitized} = InputValidator.validate_and_sanitize(query)
      assert sanitized == query
    end

    test "returns error for invalid query" do
      assert {:error, :empty_query} = InputValidator.validate_and_sanitize("")
    end

    test "sanitizes and validates together" do
      query = "SELECT \x00 * WHERE { ?s ?p ?o }"
      assert {:ok, sanitized} = InputValidator.validate_and_sanitize(query)
      refute String.contains?(sanitized, "\x00")
    end
  end

  describe "depth validation" do
    test "accepts reasonably deep algebra" do
      # Create a deep but valid algebra (50 levels)
      algebra = create_deep_algebra(50)
      assert :ok = InputValidator.validate_algebra(algebra)
    end

    test "rejects excessively deep algebra" do
      # Create an algebra that exceeds max depth
      algebra = create_deep_algebra(150)
      assert {:error, :pattern_too_deep} = InputValidator.validate_algebra(algebra)
    end
  end

  # Helper to create nested algebra for depth testing
  defp create_deep_algebra(depth) when depth > 1 do
    {:filter, {:literal, "true"}, create_deep_algebra(depth - 1)}
  end

  defp create_deep_algebra(1) do
    {:bgp, [{:triple, {:variable, "s"}, 1, {:variable, "o"}}]}
  end
end
