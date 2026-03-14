#!/usr/bin/env elixir

defmodule TripleStore.CodeDocsValidator do
  @moduledoc false

  def main(argv) do
    files =
      argv
      |> Enum.reject(&(&1 == "--"))
      |> Enum.uniq()

    if files == [] do
      IO.puts("Skipping code-doc validation: no candidate Elixir source files.")
      :ok
    else
      violations =
        files
        |> Enum.flat_map(&validate_file/1)
        |> Enum.sort_by(fn %{file: file, line: line} -> {file, line} end)

      if violations == [] do
        IO.puts("Code-doc validation passed.")
        :ok
      else
        IO.puts("Code-doc validation failed.")

        Enum.each(violations, fn %{file: file, line: line, message: message} ->
          IO.puts("FAIL: #{file}:#{line} #{message}")
        end)

        :error
      end
    end
  end

  defp validate_file(file) do
    with {:ok, source} <- File.read(file),
         {:ok, ast} <- Code.string_to_quoted(source, columns: true, token_metadata: true) do
      {_ast, violations} =
        Macro.prewalk(ast, [], fn
          {:defmodule, meta, [name_ast, [do: body]]} = node, acc ->
            module_name = Macro.to_string(name_ast)
            {node, validate_module(file, module_name, meta, body) ++ acc}

          node, acc ->
            {node, acc}
        end)

      violations
    else
      {:error, {line, error, token}} ->
        [
          %{
            file: file,
            line: line || 1,
            message: "could not parse file (#{error_message(error, token)})"
          }
        ]
    end
  end

  defp validate_module(file, module_name, module_meta, body) do
    expressions = block_to_list(body)
    module_line = module_meta[:line] || 1

    moduledoc_values =
      expressions
      |> Enum.flat_map(fn expr ->
        case module_attribute(expr, :moduledoc) do
          {:ok, value, _line} -> [value]
          :error -> []
        end
      end)

    cond do
      moduledoc_values == [] ->
        [%{file: file, line: module_line, message: "#{module_name} is missing @moduledoc"}]

      Enum.any?(moduledoc_values, &(&1 == false)) ->
        [
          %{
            file: file,
            line: module_line,
            message: "#{module_name} uses @moduledoc false; module docs are required"
          }
        ]

      true ->
        []
    end
  end

  defp module_attribute({:@, _meta, [{name, attr_meta, [value]}]}, expected_name)
       when name == expected_name do
    {:ok, value, attr_meta[:line] || 1}
  end

  defp module_attribute(_, _expected_name), do: :error

  defp block_to_list({:__block__, _meta, expressions}) when is_list(expressions), do: expressions
  defp block_to_list(nil), do: []
  defp block_to_list(expression), do: [expression]

  defp error_message(error, token) do
    message =
      case error do
        :missing_terminator -> "missing terminator"
        :syntax_error -> "syntax error"
        _ -> to_string(error)
      end

    if token do
      "#{message}: #{inspect(token)}"
    else
      message
    end
  end
end

case TripleStore.CodeDocsValidator.main(System.argv()) do
  :ok -> System.halt(0)
  :error -> System.halt(1)
end
