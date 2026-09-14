Application.load(:triple_store)
{:ok, app_modules} = :application.get_key(:triple_store, :modules)

resolve_module = fn parts ->
  requested = Module.concat(parts)
  requested_text = Atom.to_string(requested)

  if String.starts_with?(requested_text, "Elixir.TripleStore") do
    requested
  else
    suffix = "." <> Enum.join(parts, ".")

    case Enum.filter(app_modules, &String.ends_with?(Atom.to_string(&1), suffix)) do
      [match] -> match
      _ -> requested
    end
  end
end

{block_count, failures} =
  "guides/**/*.md"
  |> Path.wildcard()
  |> Enum.sort()
  |> Enum.reduce({0, []}, fn file, {count, failures} ->
    blocks =
      Regex.scan(~r/(?:```|~~~)elixir\n(.*?)(?:```|~~~)/s, File.read!(file),
        capture: :all_but_first
      )

    Enum.reduce(Enum.with_index(blocks, 1), {count, failures}, fn {[code], index},
                                                                  {count, failures} ->
      location = "#{file}: Elixir block #{index}"

      case Code.string_to_quoted(code, file: file) do
        {:ok, ast} ->
          {_ast, missing} =
            Macro.prewalk(ast, [], fn
              {{:., meta, [{:__aliases__, _, parts}, function]}, _, arguments} = node, acc
              when is_atom(function) and is_list(arguments) ->
                module = resolve_module.(parts)
                Code.ensure_loaded(module)

                if module in app_modules and
                     not function_exported?(module, function, length(arguments)) do
                  failure =
                    "#{location}, line #{meta[:line] || 0}: " <>
                      "#{inspect(module)}.#{function}/#{length(arguments)} is not exported"

                  {node, [failure | acc]}
                else
                  {node, acc}
                end

              node, acc ->
                {node, acc}
            end)

          {count + 1, Enum.reverse(missing, failures)}

        {:error, {line, error, token}} ->
          failure = "#{location}, line #{line}: #{error}#{token}"
          {count + 1, [failure | failures]}
      end
    end)
  end)

case Enum.reverse(failures) do
  [] ->
    IO.puts("Guide example validation passed (#{block_count} Elixir blocks).")

  failures ->
    Enum.each(failures, &IO.puts("FAIL: #{&1}"))
    System.halt(1)
end
