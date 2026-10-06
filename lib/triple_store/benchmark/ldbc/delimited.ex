defmodule TripleStore.Benchmark.LDBC.Delimited do
  @moduledoc """
  Small streaming delimited-row decoder with RFC 4180-style quoted fields.

  It supports the single-character separators used by SNB CSV serializers and
  rejects unterminated quoted fields instead of repairing them.
  """

  @doc "Parses one physical record using the requested one-byte separator."
  @spec parse_line(String.t(), String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def parse_line(line, <<delimiter>>) when is_binary(line) do
    line
    |> String.trim_trailing("\n")
    |> String.trim_trailing("\r")
    |> do_parse(delimiter, false, [], [], false)
  end

  def parse_line(_line, _delimiter), do: {:error, :invalid_delimiter}

  defp do_parse(<<>>, _delimiter, false, field, fields, _quoted),
    do: {:ok, Enum.reverse([field |> Enum.reverse() |> IO.iodata_to_binary() | fields])}

  defp do_parse(<<>>, _delimiter, true, _field, _fields, _quoted),
    do: {:error, :unterminated_quoted_field}

  defp do_parse(<<?", ?", rest::binary>>, delimiter, true, field, fields, quoted),
    do: do_parse(rest, delimiter, true, [?" | field], fields, quoted)

  defp do_parse(<<?", rest::binary>>, delimiter, true, field, fields, quoted),
    do: do_parse(rest, delimiter, false, field, fields, quoted)

  defp do_parse(<<?", rest::binary>>, delimiter, false, [], fields, false),
    do: do_parse(rest, delimiter, true, [], fields, true)

  defp do_parse(<<delimiter, rest::binary>>, delimiter, false, field, fields, _quoted) do
    value = field |> Enum.reverse() |> IO.iodata_to_binary()
    do_parse(rest, delimiter, false, [], [value | fields], false)
  end

  defp do_parse(<<character, rest::binary>>, delimiter, quoted?, field, fields, quoted) do
    do_parse(rest, delimiter, quoted?, [character | field], fields, quoted)
  end
end
