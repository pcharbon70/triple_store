defmodule TripleStore.Benchmark.Wikidata.AnswerNormalizerTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Wikidata.AnswerNormalizer

  describe "normalize/2" do
    test "canonicalizes unordered bindings and ignores row order by default" do
      result_a = [
        %{
          "person" => {:named_node, "http://example.org/Q42"},
          "count" => {:literal, :typed, "01", "http://www.w3.org/2001/XMLSchema#integer"}
        },
        %{
          "person" => {:named_node, "http://example.org/Q80"},
          "count" => {:literal, :typed, "2", "http://www.w3.org/2001/XMLSchema#integer"}
        }
      ]

      result_b = Enum.reverse(result_a)

      assert {:ok, normalized_a} =
               AnswerNormalizer.normalize(result_a,
                 execution_variant: :raw,
                 ordering: :unordered,
                 blank_node_policy: :anonymous
               )

      assert {:ok, normalized_b} =
               AnswerNormalizer.normalize(result_b,
                 execution_variant: :raw,
                 ordering: :unordered,
                 blank_node_policy: :anonymous
               )

      assert normalized_a.fingerprint == normalized_b.fingerprint
      assert normalized_a.unordered_fingerprint == normalized_b.unordered_fingerprint
      assert normalized_a.row_count == 2
      assert normalized_a.distinct_row_count == 2
    end

    test "preserves ordered results when ordering matters" do
      results = [
        %{"name" => {:literal, :simple, "Alice"}},
        %{"name" => {:literal, :simple, "Bob"}}
      ]

      assert {:ok, ordered_a} =
               AnswerNormalizer.normalize(results,
                 execution_variant: :raw,
                 ordering: :ordered,
                 blank_node_policy: :anonymous
               )

      assert {:ok, ordered_b} =
               AnswerNormalizer.normalize(Enum.reverse(results),
                 execution_variant: :raw,
                 ordering: :ordered,
                 blank_node_policy: :anonymous
               )

      assert ordered_a.fingerprint != ordered_b.fingerprint
      assert ordered_a.unordered_fingerprint == ordered_b.unordered_fingerprint
    end

    test "provides datatype-relaxed and anonymous-blank-node comparison views" do
      assert {:ok, datatype_a} =
               AnswerNormalizer.normalize(
                 [
                   %{
                     "flag" => {:literal, :typed, "1", "http://www.w3.org/2001/XMLSchema#boolean"}
                   }
                 ],
                 execution_variant: :raw,
                 ordering: :unordered,
                 blank_node_policy: :anonymous
               )

      assert {:ok, datatype_b} =
               AnswerNormalizer.normalize(
                 [
                   %{
                     "flag" =>
                       {:literal, :typed, "true", "http://www.w3.org/2001/XMLSchema#boolean"}
                   }
                 ],
                 execution_variant: :raw,
                 ordering: :unordered,
                 blank_node_policy: :anonymous
               )

      refute datatype_a.fingerprint == datatype_b.fingerprint
      assert datatype_a.datatype_relaxed_fingerprint == datatype_b.datatype_relaxed_fingerprint

      assert {:ok, preserved_a} =
               AnswerNormalizer.normalize(
                 [%{"node" => {:blank_node, "b1"}}],
                 execution_variant: :raw,
                 ordering: :unordered,
                 blank_node_policy: :preserve
               )

      assert {:ok, preserved_b} =
               AnswerNormalizer.normalize(
                 [%{"node" => {:blank_node, "generated-9"}}],
                 execution_variant: :raw,
                 ordering: :unordered,
                 blank_node_policy: :preserve
               )

      refute preserved_a.fingerprint == preserved_b.fingerprint

      assert preserved_a.anonymous_blank_node_fingerprint ==
               preserved_b.anonymous_blank_node_fingerprint
    end

    test "deduplicates rows for distinct-only variants" do
      results = [
        %{"person" => {:named_node, "http://example.org/Q42"}},
        %{"person" => {:named_node, "http://example.org/Q42"}}
      ]

      assert {:ok, normalized} =
               AnswerNormalizer.normalize(results,
                 execution_variant: :distinct_only,
                 ordering: :unordered,
                 blank_node_policy: :anonymous
               )

      assert normalized.row_count == 1
      assert normalized.distinct_row_count == 1
    end
  end
end
