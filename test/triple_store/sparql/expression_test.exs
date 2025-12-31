defmodule TripleStore.SPARQL.ExpressionTest do
  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.Expression

  @xsd_integer "http://www.w3.org/2001/XMLSchema#integer"
  @xsd_decimal "http://www.w3.org/2001/XMLSchema#decimal"
  @xsd_double "http://www.w3.org/2001/XMLSchema#double"
  @xsd_boolean "http://www.w3.org/2001/XMLSchema#boolean"
  @xsd_string "http://www.w3.org/2001/XMLSchema#string"

  # Helper to create typed literals
  defp int(n), do: {:literal, :typed, Integer.to_string(n), @xsd_integer}
  defp dec(n), do: {:literal, :typed, Float.to_string(n), @xsd_decimal}
  defp bool(true), do: {:literal, :typed, "true", @xsd_boolean}
  defp bool(false), do: {:literal, :typed, "false", @xsd_boolean}
  defp str(s), do: {:literal, :simple, s}
  defp lang_str(s, l), do: {:literal, :lang, s, l}

  describe "evaluate/2 - variables and literals" do
    test "evaluates bound variable" do
      bindings = %{"x" => int(42)}
      assert Expression.evaluate({:variable, "x"}, bindings) == {:ok, int(42)}
    end

    test "returns error for unbound variable" do
      assert Expression.evaluate({:variable, "x"}, %{}) == :error
    end

    test "evaluates literal constants" do
      assert Expression.evaluate(int(10), %{}) == {:ok, int(10)}
      assert Expression.evaluate(str("hello"), %{}) == {:ok, str("hello")}

      assert Expression.evaluate({:named_node, "http://example.org"}, %{}) ==
               {:ok, {:named_node, "http://example.org"}}

      assert Expression.evaluate({:blank_node, "b1"}, %{}) == {:ok, {:blank_node, "b1"}}
    end
  end

  describe "evaluate/2 - arithmetic expressions" do
    test "addition of integers" do
      expr = {:add, int(3), int(5)}
      assert Expression.evaluate(expr, %{}) == {:ok, int(8)}
    end

    test "addition with variables" do
      expr = {:add, {:variable, "x"}, int(5)}
      bindings = %{"x" => int(10)}
      assert Expression.evaluate(expr, bindings) == {:ok, int(15)}
    end

    test "subtraction" do
      expr = {:subtract, int(10), int(3)}
      assert Expression.evaluate(expr, %{}) == {:ok, int(7)}
    end

    test "multiplication" do
      expr = {:multiply, int(4), int(7)}
      assert Expression.evaluate(expr, %{}) == {:ok, int(28)}
    end

    test "division" do
      expr = {:divide, int(10), int(4)}
      {:ok, result} = Expression.evaluate(expr, %{})
      assert result == {:literal, :typed, "2.5", @xsd_decimal}
    end

    test "division by zero returns error" do
      expr = {:divide, int(10), int(0)}
      assert Expression.evaluate(expr, %{}) == :error
    end

    test "unary minus" do
      expr = {:unary_minus, int(5)}
      assert Expression.evaluate(expr, %{}) == {:ok, int(-5)}
    end

    test "type promotion - integer and decimal" do
      expr = {:add, int(1), dec(2.5)}
      {:ok, result} = Expression.evaluate(expr, %{})
      assert elem(result, 3) == @xsd_decimal
    end

    test "arithmetic error on non-numeric" do
      expr = {:add, str("hello"), int(5)}
      assert Expression.evaluate(expr, %{}) == :error
    end
  end

  describe "evaluate/2 - comparison expressions" do
    test "equality of integers" do
      assert Expression.evaluate({:equal, int(5), int(5)}, %{}) == {:ok, bool(true)}
      assert Expression.evaluate({:equal, int(5), int(6)}, %{}) == {:ok, bool(false)}
    end

    test "equality of strings" do
      assert Expression.evaluate({:equal, str("a"), str("a")}, %{}) == {:ok, bool(true)}
      assert Expression.evaluate({:equal, str("a"), str("b")}, %{}) == {:ok, bool(false)}
    end

    test "equality of IRIs" do
      iri = {:named_node, "http://example.org/x"}
      assert Expression.evaluate({:equal, iri, iri}, %{}) == {:ok, bool(true)}
    end

    test "greater than" do
      assert Expression.evaluate({:greater, int(10), int(5)}, %{}) == {:ok, bool(true)}
      assert Expression.evaluate({:greater, int(5), int(10)}, %{}) == {:ok, bool(false)}
      assert Expression.evaluate({:greater, int(5), int(5)}, %{}) == {:ok, bool(false)}
    end

    test "less than" do
      assert Expression.evaluate({:less, int(5), int(10)}, %{}) == {:ok, bool(true)}
      assert Expression.evaluate({:less, int(10), int(5)}, %{}) == {:ok, bool(false)}
    end

    test "greater or equal" do
      assert Expression.evaluate({:greater_or_equal, int(10), int(5)}, %{}) == {:ok, bool(true)}
      assert Expression.evaluate({:greater_or_equal, int(5), int(5)}, %{}) == {:ok, bool(true)}
      assert Expression.evaluate({:greater_or_equal, int(4), int(5)}, %{}) == {:ok, bool(false)}
    end

    test "less or equal" do
      assert Expression.evaluate({:less_or_equal, int(5), int(10)}, %{}) == {:ok, bool(true)}
      assert Expression.evaluate({:less_or_equal, int(5), int(5)}, %{}) == {:ok, bool(true)}
      assert Expression.evaluate({:less_or_equal, int(6), int(5)}, %{}) == {:ok, bool(false)}
    end

    test "string comparison" do
      assert Expression.evaluate({:less, str("apple"), str("banana")}, %{}) == {:ok, bool(true)}
      assert Expression.evaluate({:greater, str("zoo"), str("abc")}, %{}) == {:ok, bool(true)}
    end
  end

  describe "evaluate/2 - logical expressions" do
    test "logical AND" do
      assert Expression.evaluate({:and, bool(true), bool(true)}, %{}) == {:ok, bool(true)}
      assert Expression.evaluate({:and, bool(true), bool(false)}, %{}) == {:ok, bool(false)}
      assert Expression.evaluate({:and, bool(false), bool(true)}, %{}) == {:ok, bool(false)}
      assert Expression.evaluate({:and, bool(false), bool(false)}, %{}) == {:ok, bool(false)}
    end

    test "logical OR" do
      assert Expression.evaluate({:or, bool(true), bool(true)}, %{}) == {:ok, bool(true)}
      assert Expression.evaluate({:or, bool(true), bool(false)}, %{}) == {:ok, bool(true)}
      assert Expression.evaluate({:or, bool(false), bool(true)}, %{}) == {:ok, bool(true)}
      assert Expression.evaluate({:or, bool(false), bool(false)}, %{}) == {:ok, bool(false)}
    end

    test "logical NOT" do
      assert Expression.evaluate({:not, bool(true)}, %{}) == {:ok, bool(false)}
      assert Expression.evaluate({:not, bool(false)}, %{}) == {:ok, bool(true)}
    end

    test "effective boolean value of non-empty string is true" do
      assert Expression.evaluate({:not, str("")}, %{}) == {:ok, bool(true)}
      assert Expression.evaluate({:not, str("hello")}, %{}) == {:ok, bool(false)}
    end

    test "effective boolean value of zero is false" do
      assert Expression.evaluate({:not, int(0)}, %{}) == {:ok, bool(true)}
      assert Expression.evaluate({:not, int(1)}, %{}) == {:ok, bool(false)}
    end

    test "complex logical expression" do
      # (?x > 5) && (?x < 10)
      expr = {:and, {:greater, {:variable, "x"}, int(5)}, {:less, {:variable, "x"}, int(10)}}
      assert Expression.evaluate(expr, %{"x" => int(7)}) == {:ok, bool(true)}
      assert Expression.evaluate(expr, %{"x" => int(3)}) == {:ok, bool(false)}
      assert Expression.evaluate(expr, %{"x" => int(12)}) == {:ok, bool(false)}
    end
  end

  describe "evaluate/2 - BOUND function" do
    test "BOUND returns true for bound variable" do
      expr = {:bound, {:variable, "x"}}
      assert Expression.evaluate(expr, %{"x" => int(42)}) == {:ok, bool(true)}
    end

    test "BOUND returns false for unbound variable" do
      expr = {:bound, {:variable, "x"}}
      assert Expression.evaluate(expr, %{}) == {:ok, bool(false)}
    end
  end

  describe "evaluate/2 - STR function" do
    test "STR on literal returns string" do
      expr = {:function_call, "STR", [int(42)]}
      assert Expression.evaluate(expr, %{}) == {:ok, str("42")}
    end

    test "STR on IRI returns string" do
      expr = {:function_call, "STR", [{:named_node, "http://example.org"}]}
      assert Expression.evaluate(expr, %{}) == {:ok, str("http://example.org")}
    end
  end

  describe "evaluate/2 - LANG function" do
    test "LANG returns language tag" do
      expr = {:function_call, "LANG", [lang_str("hello", "en")]}
      assert Expression.evaluate(expr, %{}) == {:ok, str("en")}
    end

    test "LANG returns empty string for non-language literal" do
      expr = {:function_call, "LANG", [str("hello")]}
      assert Expression.evaluate(expr, %{}) == {:ok, str("")}
    end
  end

  describe "evaluate/2 - DATATYPE function" do
    test "DATATYPE returns type IRI for typed literal" do
      expr = {:function_call, "DATATYPE", [int(42)]}
      assert Expression.evaluate(expr, %{}) == {:ok, {:named_node, @xsd_integer}}
    end

    test "DATATYPE returns xsd:string for simple literal" do
      expr = {:function_call, "DATATYPE", [str("hello")]}
      assert Expression.evaluate(expr, %{}) == {:ok, {:named_node, @xsd_string}}
    end

    test "DATATYPE returns rdf:langString for language literal" do
      expr = {:function_call, "DATATYPE", [lang_str("hello", "en")]}

      assert Expression.evaluate(expr, %{}) ==
               {:ok, {:named_node, "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"}}
    end
  end

  describe "evaluate/2 - type checking functions" do
    test "ISIRI returns true for IRI" do
      expr = {:function_call, "ISIRI", [{:named_node, "http://example.org"}]}
      assert Expression.evaluate(expr, %{}) == {:ok, bool(true)}
    end

    test "ISIRI returns false for non-IRI" do
      expr = {:function_call, "ISIRI", [str("hello")]}
      assert Expression.evaluate(expr, %{}) == {:ok, bool(false)}
    end

    test "ISBLANK returns true for blank node" do
      expr = {:function_call, "ISBLANK", [{:blank_node, "b1"}]}
      assert Expression.evaluate(expr, %{}) == {:ok, bool(true)}
    end

    test "ISLITERAL returns true for literal" do
      expr = {:function_call, "ISLITERAL", [str("hello")]}
      assert Expression.evaluate(expr, %{}) == {:ok, bool(true)}
    end

    test "ISNUMERIC returns true for numeric" do
      expr = {:function_call, "ISNUMERIC", [int(42)]}
      assert Expression.evaluate(expr, %{}) == {:ok, bool(true)}
    end

    test "ISNUMERIC returns false for non-numeric" do
      expr = {:function_call, "ISNUMERIC", [str("hello")]}
      assert Expression.evaluate(expr, %{}) == {:ok, bool(false)}
    end
  end

  describe "evaluate/2 - string functions" do
    test "STRLEN returns string length" do
      expr = {:function_call, "STRLEN", [str("hello")]}
      assert Expression.evaluate(expr, %{}) == {:ok, int(5)}
    end

    test "SUBSTR with start" do
      expr = {:function_call, "SUBSTR", [str("hello"), int(2)]}
      assert Expression.evaluate(expr, %{}) == {:ok, str("ello")}
    end

    test "SUBSTR with start and length" do
      expr = {:function_call, "SUBSTR", [str("hello"), int(2), int(3)]}
      assert Expression.evaluate(expr, %{}) == {:ok, str("ell")}
    end

    test "UCASE returns uppercase" do
      expr = {:function_call, "UCASE", [str("hello")]}
      assert Expression.evaluate(expr, %{}) == {:ok, str("HELLO")}
    end

    test "LCASE returns lowercase" do
      expr = {:function_call, "LCASE", [str("HELLO")]}
      assert Expression.evaluate(expr, %{}) == {:ok, str("hello")}
    end

    test "STRSTARTS" do
      assert Expression.evaluate({:function_call, "STRSTARTS", [str("hello"), str("hel")]}, %{}) ==
               {:ok, bool(true)}

      assert Expression.evaluate({:function_call, "STRSTARTS", [str("hello"), str("ell")]}, %{}) ==
               {:ok, bool(false)}
    end

    test "STRENDS" do
      assert Expression.evaluate({:function_call, "STRENDS", [str("hello"), str("llo")]}, %{}) ==
               {:ok, bool(true)}

      assert Expression.evaluate({:function_call, "STRENDS", [str("hello"), str("hel")]}, %{}) ==
               {:ok, bool(false)}
    end

    test "CONTAINS" do
      assert Expression.evaluate({:function_call, "CONTAINS", [str("hello"), str("ell")]}, %{}) ==
               {:ok, bool(true)}

      assert Expression.evaluate({:function_call, "CONTAINS", [str("hello"), str("xyz")]}, %{}) ==
               {:ok, bool(false)}
    end

    test "STRBEFORE" do
      expr = {:function_call, "STRBEFORE", [str("hello world"), str(" ")]}
      assert Expression.evaluate(expr, %{}) == {:ok, str("hello")}
    end

    test "STRAFTER" do
      expr = {:function_call, "STRAFTER", [str("hello world"), str(" ")]}
      assert Expression.evaluate(expr, %{}) == {:ok, str("world")}
    end

    test "CONCAT" do
      expr = {:function_call, "CONCAT", [str("hello"), str(" "), str("world")]}
      assert Expression.evaluate(expr, %{}) == {:ok, str("hello world")}
    end

    test "ENCODE_FOR_URI" do
      expr = {:function_call, "ENCODE_FOR_URI", [str("hello world")]}
      assert Expression.evaluate(expr, %{}) == {:ok, str("hello%20world")}
    end

    test "LANGMATCHES" do
      assert Expression.evaluate({:function_call, "LANGMATCHES", [str("en"), str("en")]}, %{}) ==
               {:ok, bool(true)}

      assert Expression.evaluate({:function_call, "LANGMATCHES", [str("en-US"), str("en")]}, %{}) ==
               {:ok, bool(true)}

      assert Expression.evaluate({:function_call, "LANGMATCHES", [str("en"), str("*")]}, %{}) ==
               {:ok, bool(true)}

      assert Expression.evaluate({:function_call, "LANGMATCHES", [str(""), str("*")]}, %{}) ==
               {:ok, bool(false)}
    end

    test "REGEX" do
      assert Expression.evaluate({:function_call, "REGEX", [str("hello"), str("ell")]}, %{}) ==
               {:ok, bool(true)}

      assert Expression.evaluate({:function_call, "REGEX", [str("hello"), str("^hel")]}, %{}) ==
               {:ok, bool(true)}

      assert Expression.evaluate({:function_call, "REGEX", [str("hello"), str("xyz")]}, %{}) ==
               {:ok, bool(false)}
    end

    test "REGEX with flags" do
      expr = {:function_call, "REGEX", [str("HELLO"), str("hello"), str("i")]}
      assert Expression.evaluate(expr, %{}) == {:ok, bool(true)}
    end

    test "REPLACE" do
      expr = {:function_call, "REPLACE", [str("hello"), str("l"), str("L")]}
      assert Expression.evaluate(expr, %{}) == {:ok, str("heLLo")}
    end
  end

  describe "evaluate/2 - numeric functions" do
    test "ABS" do
      assert Expression.evaluate({:function_call, "ABS", [int(-5)]}, %{}) == {:ok, int(5)}
      assert Expression.evaluate({:function_call, "ABS", [int(5)]}, %{}) == {:ok, int(5)}
    end

    test "ROUND" do
      assert Expression.evaluate({:function_call, "ROUND", [dec(2.4)]}, %{}) == {:ok, dec(2.0)}
      assert Expression.evaluate({:function_call, "ROUND", [dec(2.6)]}, %{}) == {:ok, dec(3.0)}
    end

    test "CEIL" do
      assert Expression.evaluate({:function_call, "CEIL", [dec(2.1)]}, %{}) == {:ok, dec(3.0)}
      assert Expression.evaluate({:function_call, "CEIL", [dec(2.9)]}, %{}) == {:ok, dec(3.0)}
    end

    test "FLOOR" do
      assert Expression.evaluate({:function_call, "FLOOR", [dec(2.1)]}, %{}) == {:ok, dec(2.0)}
      assert Expression.evaluate({:function_call, "FLOOR", [dec(2.9)]}, %{}) == {:ok, dec(2.0)}
    end

    test "RAND returns a double between 0 and 1" do
      {:ok, {:literal, :typed, val, @xsd_double}} =
        Expression.evaluate({:function_call, "RAND", []}, %{})

      {n, ""} = Float.parse(val)
      assert n >= 0 and n < 1
    end
  end

  describe "evaluate/2 - hash functions" do
    test "MD5" do
      {:ok, {:literal, :simple, hash}} =
        Expression.evaluate({:function_call, "MD5", [str("hello")]}, %{})

      assert String.length(hash) == 32
      assert hash == "5d41402abc4b2a76b9719d911017c592"
    end

    test "SHA1" do
      {:ok, {:literal, :simple, hash}} =
        Expression.evaluate({:function_call, "SHA1", [str("hello")]}, %{})

      assert String.length(hash) == 40
    end

    test "SHA256" do
      {:ok, {:literal, :simple, hash}} =
        Expression.evaluate({:function_call, "SHA256", [str("hello")]}, %{})

      assert String.length(hash) == 64
    end
  end

  describe "evaluate/2 - conditional expressions" do
    test "IF with true condition" do
      expr = {:if_expr, bool(true), int(1), int(2)}
      assert Expression.evaluate(expr, %{}) == {:ok, int(1)}
    end

    test "IF with false condition" do
      expr = {:if_expr, bool(false), int(1), int(2)}
      assert Expression.evaluate(expr, %{}) == {:ok, int(2)}
    end

    test "IF with expression condition" do
      expr = {:if_expr, {:greater, {:variable, "x"}, int(5)}, str("big"), str("small")}
      assert Expression.evaluate(expr, %{"x" => int(10)}) == {:ok, str("big")}
      assert Expression.evaluate(expr, %{"x" => int(3)}) == {:ok, str("small")}
    end

    test "COALESCE returns first non-error" do
      expr = {:coalesce, [{:variable, "x"}, {:variable, "y"}, int(0)]}
      assert Expression.evaluate(expr, %{"y" => int(42)}) == {:ok, int(42)}
      assert Expression.evaluate(expr, %{}) == {:ok, int(0)}
    end

    test "IN expression" do
      expr = {:in_expr, {:variable, "x"}, [int(1), int(2), int(3)]}
      assert Expression.evaluate(expr, %{"x" => int(2)}) == {:ok, bool(true)}
      assert Expression.evaluate(expr, %{"x" => int(5)}) == {:ok, bool(false)}
    end
  end

  describe "evaluate/2 - IRI and BNODE functions" do
    test "IRI from string" do
      expr = {:function_call, "IRI", [str("http://example.org")]}
      assert Expression.evaluate(expr, %{}) == {:ok, {:named_node, "http://example.org"}}
    end

    test "IRI from IRI (passthrough)" do
      iri = {:named_node, "http://example.org"}
      expr = {:function_call, "IRI", [iri]}
      assert Expression.evaluate(expr, %{}) == {:ok, iri}
    end

    test "BNODE generates blank node" do
      {:ok, {:blank_node, id}} = Expression.evaluate({:function_call, "BNODE", []}, %{})
      assert String.starts_with?(id, "b")
    end

    test "BNODE with label" do
      expr = {:function_call, "BNODE", [str("label")]}
      assert Expression.evaluate(expr, %{}) == {:ok, {:blank_node, "blabel"}}
    end
  end

  describe "evaluate/2 - datetime functions" do
    test "NOW returns current datetime" do
      {:ok, {:literal, :typed, value, dt}} =
        Expression.evaluate({:function_call, "NOW", []}, %{})

      assert dt == "http://www.w3.org/2001/XMLSchema#dateTime"
      assert {:ok, _, _} = DateTime.from_iso8601(value)
    end

    test "YEAR extracts year" do
      datetime =
        {:literal, :typed, "2024-03-15T10:30:00Z", "http://www.w3.org/2001/XMLSchema#dateTime"}

      expr = {:function_call, "YEAR", [datetime]}
      assert Expression.evaluate(expr, %{}) == {:ok, int(2024)}
    end

    test "MONTH extracts month" do
      datetime =
        {:literal, :typed, "2024-03-15T10:30:00Z", "http://www.w3.org/2001/XMLSchema#dateTime"}

      expr = {:function_call, "MONTH", [datetime]}
      assert Expression.evaluate(expr, %{}) == {:ok, int(3)}
    end

    test "DAY extracts day" do
      datetime =
        {:literal, :typed, "2024-03-15T10:30:00Z", "http://www.w3.org/2001/XMLSchema#dateTime"}

      expr = {:function_call, "DAY", [datetime]}
      assert Expression.evaluate(expr, %{}) == {:ok, int(15)}
    end

    test "HOURS extracts hours" do
      datetime =
        {:literal, :typed, "2024-03-15T10:30:00Z", "http://www.w3.org/2001/XMLSchema#dateTime"}

      expr = {:function_call, "HOURS", [datetime]}
      assert Expression.evaluate(expr, %{}) == {:ok, int(10)}
    end

    test "MINUTES extracts minutes" do
      datetime =
        {:literal, :typed, "2024-03-15T10:30:00Z", "http://www.w3.org/2001/XMLSchema#dateTime"}

      expr = {:function_call, "MINUTES", [datetime]}
      assert Expression.evaluate(expr, %{}) == {:ok, int(30)}
    end
  end

  describe "evaluate_aggregate/2" do
    test "COUNT over solutions" do
      solutions = [
        %{"x" => int(1)},
        %{"x" => int(2)},
        %{"x" => int(3)}
      ]

      result = Expression.evaluate_aggregate({:count, {:variable, "x"}, false}, solutions)
      assert result == {:ok, int(3)}
    end

    test "COUNT with DISTINCT" do
      solutions = [
        %{"x" => int(1)},
        %{"x" => int(1)},
        %{"x" => int(2)}
      ]

      result = Expression.evaluate_aggregate({:count, {:variable, "x"}, true}, solutions)
      assert result == {:ok, int(2)}
    end

    test "COUNT star" do
      solutions = [%{"x" => int(1)}, %{"y" => int(2)}]
      result = Expression.evaluate_aggregate({:count, :star, false}, solutions)
      assert result == {:ok, int(2)}
    end

    test "SUM" do
      solutions = [
        %{"x" => int(10)},
        %{"x" => int(20)},
        %{"x" => int(30)}
      ]

      result = Expression.evaluate_aggregate({:sum, {:variable, "x"}, false}, solutions)
      assert result == {:ok, int(60)}
    end

    test "SUM of empty returns 0" do
      result = Expression.evaluate_aggregate({:sum, {:variable, "x"}, false}, [])
      assert result == {:ok, int(0)}
    end

    test "AVG" do
      solutions = [
        %{"x" => int(10)},
        %{"x" => int(20)},
        %{"x" => int(30)}
      ]

      {:ok, {:literal, :typed, val, @xsd_decimal}} =
        Expression.evaluate_aggregate({:avg, {:variable, "x"}, false}, solutions)

      {n, ""} = Float.parse(val)
      assert_in_delta n, 20.0, 0.001
    end

    test "MIN" do
      solutions = [
        %{"x" => int(30)},
        %{"x" => int(10)},
        %{"x" => int(20)}
      ]

      result = Expression.evaluate_aggregate({:min, {:variable, "x"}, false}, solutions)
      assert result == {:ok, int(10)}
    end

    test "MAX" do
      solutions = [
        %{"x" => int(10)},
        %{"x" => int(30)},
        %{"x" => int(20)}
      ]

      result = Expression.evaluate_aggregate({:max, {:variable, "x"}, false}, solutions)
      assert result == {:ok, int(30)}
    end

    test "GROUP_CONCAT" do
      solutions = [
        %{"x" => str("a")},
        %{"x" => str("b")},
        %{"x" => str("c")}
      ]

      result =
        Expression.evaluate_aggregate({{:group_concat, ","}, {:variable, "x"}, false}, solutions)

      assert result == {:ok, str("a,b,c")}
    end

    test "GROUP_CONCAT with default separator" do
      solutions = [
        %{"x" => str("a")},
        %{"x" => str("b")}
      ]

      result =
        Expression.evaluate_aggregate({{:group_concat, nil}, {:variable, "x"}, false}, solutions)

      assert result == {:ok, str("a b")}
    end

    test "SAMPLE returns first value" do
      solutions = [
        %{"x" => int(42)},
        %{"x" => int(100)}
      ]

      result = Expression.evaluate_aggregate({:sample, {:variable, "x"}, false}, solutions)
      assert result == {:ok, int(42)}
    end

    test "SAMPLE returns error for empty solutions" do
      result = Expression.evaluate_aggregate({:sample, {:variable, "x"}, false}, [])
      assert result == :error
    end

    test "SUM with mixed numeric types promotes to decimal" do
      solutions = [
        %{"x" => int(10)},
        %{"x" => dec(20.5)},
        %{"x" => int(30)}
      ]

      {:ok, {:literal, :typed, val, type}} =
        Expression.evaluate_aggregate({:sum, {:variable, "x"}, false}, solutions)

      assert type == @xsd_decimal
      {n, ""} = Float.parse(val)
      assert_in_delta n, 60.5, 0.001
    end

    test "SUM with DISTINCT removes duplicates" do
      solutions = [
        %{"x" => int(10)},
        %{"x" => int(10)},
        %{"x" => int(20)}
      ]

      result = Expression.evaluate_aggregate({:sum, {:variable, "x"}, true}, solutions)
      assert result == {:ok, int(30)}
    end

    test "AVG with DISTINCT removes duplicates" do
      solutions = [
        %{"x" => int(10)},
        %{"x" => int(10)},
        %{"x" => int(20)}
      ]

      {:ok, {:literal, :typed, val, @xsd_decimal}} =
        Expression.evaluate_aggregate({:avg, {:variable, "x"}, true}, solutions)

      {n, ""} = Float.parse(val)
      # AVG(DISTINCT 10, 20) = 15
      assert_in_delta n, 15.0, 0.001
    end

    test "AVG of empty returns 0" do
      {:ok, {:literal, :typed, val, @xsd_decimal}} =
        Expression.evaluate_aggregate({:avg, {:variable, "x"}, false}, [])

      assert val == "0"
    end

    test "MIN returns error for empty solutions" do
      result = Expression.evaluate_aggregate({:min, {:variable, "x"}, false}, [])
      assert result == :error
    end

    test "MAX returns error for empty solutions" do
      result = Expression.evaluate_aggregate({:max, {:variable, "x"}, false}, [])
      assert result == :error
    end

    test "GROUP_CONCAT with DISTINCT removes duplicates" do
      solutions = [
        %{"x" => str("a")},
        %{"x" => str("a")},
        %{"x" => str("b")}
      ]

      result =
        Expression.evaluate_aggregate({{:group_concat, ","}, {:variable, "x"}, true}, solutions)

      assert result == {:ok, str("a,b")}
    end

    test "COUNT skips unbound variables" do
      solutions = [
        %{"x" => int(1)},
        # x is unbound
        %{},
        %{"x" => int(3)}
      ]

      result = Expression.evaluate_aggregate({:count, {:variable, "x"}, false}, solutions)
      assert result == {:ok, int(2)}
    end

    test "SUM skips non-numeric values" do
      solutions = [
        %{"x" => int(10)},
        %{"x" => str("not a number")},
        %{"x" => int(20)}
      ]

      result = Expression.evaluate_aggregate({:sum, {:variable, "x"}, false}, solutions)
      assert result == {:ok, int(30)}
    end
  end

  describe "expression_variables/1" do
    test "extracts variables from simple expression" do
      expr = {:add, {:variable, "x"}, {:variable, "y"}}
      vars = Expression.expression_variables(expr)
      assert Enum.sort(vars) == ["x", "y"]
    end

    test "extracts variables from nested expression" do
      expr = {:and, {:greater, {:variable, "x"}, int(5)}, {:less, {:variable, "y"}, int(10)}}
      vars = Expression.expression_variables(expr)
      assert Enum.sort(vars) == ["x", "y"]
    end

    test "returns empty for constant expression" do
      expr = {:add, int(1), int(2)}
      assert Expression.expression_variables(expr) == []
    end

    test "deduplicates variables" do
      expr = {:add, {:variable, "x"}, {:multiply, {:variable, "x"}, int(2)}}
      assert Expression.expression_variables(expr) == ["x"]
    end
  end

  describe "constant?/1" do
    test "returns true for literal" do
      assert Expression.constant?(int(42))
    end

    test "returns true for constant expression" do
      assert Expression.constant?({:add, int(1), int(2)})
    end

    test "returns false for variable" do
      refute Expression.constant?({:variable, "x"})
    end

    test "returns false for expression with variable" do
      refute Expression.constant?({:add, {:variable, "x"}, int(1)})
    end
  end

  describe "expression_type/1" do
    test "identifies variable" do
      assert Expression.expression_type({:variable, "x"}) == :variable
    end

    test "identifies arithmetic" do
      assert Expression.expression_type({:add, int(1), int(2)}) == :arithmetic
      assert Expression.expression_type({:subtract, int(1), int(2)}) == :arithmetic
      assert Expression.expression_type({:multiply, int(1), int(2)}) == :arithmetic
      assert Expression.expression_type({:divide, int(1), int(2)}) == :arithmetic
    end

    test "identifies comparison" do
      assert Expression.expression_type({:equal, int(1), int(2)}) == :comparison
      assert Expression.expression_type({:greater, int(1), int(2)}) == :comparison
    end

    test "identifies logical" do
      assert Expression.expression_type({:and, bool(true), bool(true)}) == :logical
      assert Expression.expression_type({:or, bool(true), bool(false)}) == :logical
      assert Expression.expression_type({:not, bool(true)}) == :logical
    end

    test "identifies function call" do
      assert Expression.expression_type({:function_call, "STR", [int(1)]}) == :function_call
    end

    test "identifies aggregate" do
      assert Expression.expression_type({:count, {:variable, "x"}, false}) == :aggregate
      assert Expression.expression_type({:sum, {:variable, "x"}, false}) == :aggregate
    end
  end

  describe "integration with parser expressions" do
    test "evaluates parsed FILTER expression" do
      # Simulating what comes from the parser: FILTER(?y > 10 && ?y < 20)
      expr =
        {:and, {:greater, {:variable, "y"}, {:literal, :typed, "10", @xsd_integer}},
         {:less, {:variable, "y"}, {:literal, :typed, "20", @xsd_integer}}}

      bindings = %{"y" => int(15)}
      assert Expression.evaluate(expr, bindings) == {:ok, bool(true)}

      bindings = %{"y" => int(25)}
      assert Expression.evaluate(expr, bindings) == {:ok, bool(false)}
    end

    test "evaluates parsed function call expression" do
      # STR(?y) = "test"
      expr = {:equal, {:function_call, "STR", [{:variable, "y"}]}, str("test")}

      bindings = %{"y" => str("test")}
      assert Expression.evaluate(expr, bindings) == {:ok, bool(true)}

      bindings = %{"y" => str("other")}
      assert Expression.evaluate(expr, bindings) == {:ok, bool(false)}
    end

    test "evaluates parsed BIND arithmetic expression" do
      # ?x + ?y AS ?sum
      expr = {:add, {:variable, "x"}, {:variable, "y"}}
      bindings = %{"x" => int(10), "y" => int(32)}
      assert Expression.evaluate(expr, bindings) == {:ok, int(42)}
    end
  end

  describe "edge cases and error handling" do
    test "unknown function returns error" do
      expr = {:function_call, "UNKNOWN_FUNC", [int(1)]}
      result = Expression.evaluate(expr, %{})
      assert result == :error
    end

    test "unknown expression type returns error" do
      assert Expression.evaluate({:unknown_type, 1, 2}, %{}) == :error
    end

    test "EXISTS returns error (requires executor context)" do
      expr = {:exists, {:bgp, []}}
      assert Expression.evaluate(expr, %{}) == :error
    end

    test "division type promotion" do
      # Integer division still returns decimal
      expr = {:divide, int(10), int(3)}
      {:ok, {:literal, :typed, val, @xsd_decimal}} = Expression.evaluate(expr, %{})
      {n, ""} = Float.parse(val)
      assert_in_delta n, 3.333, 0.001
    end
  end

  describe "regex security (ReDoS protection)" do
    test "REGEX rejects patterns with nested quantifiers" do
      # Pattern like (a+)+ can cause catastrophic backtracking
      pattern = {:literal, :simple, "(a+)+"}
      str = {:literal, :simple, "aaaaaaaaaa"}
      expr = {:function_call, "REGEX", [str, pattern]}
      # Should return error due to dangerous pattern
      assert Expression.evaluate(expr, %{}) == :error
    end

    test "REGEX rejects excessively long patterns" do
      # Pattern longer than 1000 chars should be rejected
      pattern = {:literal, :simple, String.duplicate("a", 1001)}
      str = {:literal, :simple, "test"}
      expr = {:function_call, "REGEX", [str, pattern]}
      assert Expression.evaluate(expr, %{}) == :error
    end

    test "REGEX works with safe patterns" do
      pattern = {:literal, :simple, "hello"}
      str = {:literal, :simple, "hello world"}
      expr = {:function_call, "REGEX", [str, pattern]}
      {:ok, {:literal, :typed, "true", _}} = Expression.evaluate(expr, %{})
    end

    test "REPLACE rejects patterns with nested quantifiers" do
      pattern = {:literal, :simple, "(a*)*"}
      str = {:literal, :simple, "aaaa"}
      replacement = {:literal, :simple, "b"}
      expr = {:function_call, "REPLACE", [str, pattern, replacement]}
      assert Expression.evaluate(expr, %{}) == :error
    end
  end
end
