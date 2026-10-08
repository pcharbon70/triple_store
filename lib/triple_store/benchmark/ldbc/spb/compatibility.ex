defmodule TripleStore.Benchmark.LDBC.SPB.Compatibility do
  @moduledoc """
  Audited, versioned repairs applied to executable copies of pinned SPB text.

  Source templates remain byte-exact and checksum protected. SPB 2.0.2 contains
  invalid UTF-8 bytes in comments for queries 11 and 12, and the standard query
  20 omits `?` from one `cWork` variable although both vendor variants include
  it. These repairs are reported as benchmark transformations.
  """

  @version "spb-2.0.2-compat-v1"

  @doc "Returns the compatibility transformation version."
  @spec version() :: String.t()
  def version, do: @version

  @doc "Returns the complete, reviewable repair record for an upstream operation."
  @spec disclosures(String.t()) :: [map()]
  def disclosures(upstream_id) do
    utf8 =
      if upstream_id in ["query11", "query12"] do
        [
          %{
            id: :invalid_comment_utf8,
            semantic_change?: false,
            reason: "replace invalid source-encoding bytes in comments before native parsing"
          }
        ]
      else
        []
      end

    typo =
      if upstream_id == "query20" do
        [
          %{
            id: :missing_variable_marker,
            semantic_change?: false,
            reason: "restore ?cWork as present in upstream GraphDB and Virtuoso variants"
          }
        ]
      else
        []
      end

    utf8 ++ typo
  end

  @doc "Builds parser input while leaving the pinned source bytes unchanged."
  @spec executable_text(String.t(), binary()) :: {:ok, String.t()} | {:error, term()}
  def executable_text(upstream_id, source) when is_binary(source) do
    repaired = String.replace_invalid(source, "�")

    case upstream_id do
      "query20" -> repair_query20(repaired)
      _ -> {:ok, repaired}
    end
  end

  defp repair_query20(source) do
    original = "  cWork cwork:dateModified ?dateModified ."
    replacement = "  ?cWork cwork:dateModified ?dateModified ."

    case length(:binary.matches(source, original)) do
      1 -> {:ok, String.replace(source, original, replacement)}
      count -> {:error, {:unexpected_query20_repair_site_count, count}}
    end
  end
end
