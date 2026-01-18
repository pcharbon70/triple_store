defmodule TripleStore.SPARQL.ErrorHandlerTest do
  @moduledoc """
  Tests for structured error handling (S10).
  """

  use ExUnit.Case

  alias TripleStore.SPARQL.ErrorHandler

  describe "build_error/3" do
    test "builds a syntax error" do
      error = ErrorHandler.build_error(:syntax, "Unexpected token", %{line: 5, column: 10})
      assert error.kind == :syntax
      assert error.message == "Unexpected token"
      assert error.context.line == 5
      assert is_integer(error.timestamp)
    end

    test "builds an execution error" do
      error = ErrorHandler.build_error(:execution, "Join failed", %{step: :join})
      assert error.kind == :execution
      assert error.message == "Join failed"
    end

    test "builds a timeout error" do
      error = ErrorHandler.build_error(:timeout, "Query timed out", %{timeout_ms: 5000})
      assert error.kind == :timeout
      assert error.message == "Query timed out"
    end
  end

  describe "wrap/1" do
    test "wraps an error in a tuple" do
      assert {:error, "test error"} == ErrorHandler.wrap("test error")
    end

    test "wraps nil as error" do
      assert {:error, nil} == ErrorHandler.wrap(nil)
    end
  end

  describe "wrap/2" do
    test "wraps error with context" do
      assert {:error, %{error: "test", context: "value"}} ==
        ErrorHandler.wrap("test", context: "value")
    end

    test "wraps error with multiple context fields" do
      result = ErrorHandler.wrap("test", field: :name, value: "test")
      assert {:error, _} = result
      assert elem(result, 1).field == :name
    end
  end

  describe "error?/1" do
    test "returns true for error tuples" do
      assert ErrorHandler.error?({:error, "test"})
      assert ErrorHandler.error?({:error, %{field: "value"}})
    end

    test "returns false for non-error tuples" do
      refute ErrorHandler.error?({:ok, "value"})
      refute ErrorHandler.error?(:other)
      refute ErrorHandler.error?(nil)
      refute ErrorHandler.error?("string")
    end
  end

  describe "handle_or/2" do
    test "returns value for ok tuples" do
      assert ErrorHandler.handle_or({:ok, 42}, :default) == 42
    end

    test "returns default for error tuples" do
      assert ErrorHandler.handle_or({:error, "failed"}, :default) == :default
    end
  end

  describe "handle_or/3" do
    test "returns value for ok tuples" do
      assert ErrorHandler.handle_or({:ok, 42}, :default, fn _ -> :handled end) == 42
    end

    test "calls handler function for errors" do
      assert ErrorHandler.handle_or({:error, "failed"}, :default, fn error ->
        "handled: #{error}"
      end) == "handled: failed"
    end
  end

  describe "from_exception/1" do
    test "converts exception to error map" do
      exception = RuntimeError.exception("test error")
      error = ErrorHandler.from_exception(exception)

      assert error.message == "test error"
      assert error.type == RuntimeError
      assert error.kind == :unknown
      assert is_list(error.stacktrace)
    end

    test "handles syntax error exception" do
      exception = ErrorHandler.SyntaxError.exception("syntax error")
      error = ErrorHandler.from_exception(exception)

      assert error.kind == :syntax
      assert error.message == "syntax error"
      assert error.type == ErrorHandler.SyntaxError
    end

    test "handles execution error exception" do
      exception = ErrorHandler.ExecutionError.exception("exec error")
      error = ErrorHandler.from_exception(exception)

      assert error.kind == :execution
      assert error.message == "exec error"
    end
  end

  describe "format/1" do
    test "formats error tuple with exception" do
      exception = RuntimeError.exception("test")
      formatted = ErrorHandler.format({:error, exception})

      assert formatted == "RuntimeError: test"
    end

    test "formats error tuple with string" do
      formatted = ErrorHandler.format({:error, "test error"})

      assert formatted == "test error"
    end

    test "formats error tuple with map" do
      formatted = ErrorHandler.format({:error, %{message: "custom error"}})

      assert formatted == "custom error"
    end

    test "formats exception directly" do
      exception = RuntimeError.exception("test")
      formatted = ErrorHandler.format(exception)

      assert formatted == "RuntimeError: test"
    end

    test "formats string directly" do
      formatted = ErrorHandler.format("test error")

      assert formatted == "test error"
    end

    test "inspects unknown types" do
      formatted = ErrorHandler.format(%{field: "value"})

      assert String.contains?(formatted, "%{field")
    end
  end

  describe "custom exceptions" do
    test "SyntaxError has message" do
      exception = ErrorHandler.SyntaxError.exception("syntax error", line: 5)
      assert Exception.message(exception) == "syntax error"
    end

    test "ExecutionError has message" do
      exception = ErrorHandler.ExecutionError.exception("exec error", step: :filter)
      assert Exception.message(exception) == "exec error"
    end

    test "TimeoutError has message and context" do
      exception = ErrorHandler.TimeoutError.exception("timeout", timeout_ms: 5000, query: "SELECT *")
      assert Exception.message(exception) == "timeout"
      assert exception.timeout_ms == 5000
      assert exception.query == "SELECT *"
    end

    test "ValidationError has message and field info" do
      exception = ErrorHandler.ValidationError.exception("invalid value", field: :age, value: -5)
      assert Exception.message(exception) == "invalid value"
    end

    test "ResourceError has message and resource info" do
      exception = ErrorHandler.ResourceError.exception("out of memory", resource: :memory, operation: :query)
      assert Exception.message(exception) == "out of memory"
    end
  end
end
