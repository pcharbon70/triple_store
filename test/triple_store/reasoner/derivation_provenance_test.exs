defmodule TripleStore.Reasoner.DerivationProvenanceTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.QuadOperations
  alias TripleStore.Reasoner.DeleteWithReasoningQuad
  alias TripleStore.Reasoner.DerivationProvenance

  @moduletag :integration
  @provenance_cf :derivation_provenance

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "triple_store_derivation_provenance_#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(path)
    {:ok, db} = ErlangAdapter.open(path, schema: :quad)

    on_exit(fn ->
      if Process.alive?(db), do: ErlangAdapter.close(db)
      File.rm_rf(path)
    end)

    {:ok, db: db}
  end

  test "round-trips atom and binary rule identifiers in the compatible format", %{db: db} do
    atom_quad = {1, 10, 20, 30}
    binary_quad = {1, 11, 20, 31}

    tracker =
      DerivationProvenance.new()
      |> DerivationProvenance.record_derivation(atom_quad,
        rule_name: :cax_sco,
        premises: [{1, 10, 21, 32}],
        bindings: %{"x" => {:bound, 10}}
      )
      |> DerivationProvenance.record_derivation(binary_quad,
        rule_name: "prp_trp_transitive_contains_deadbeef",
        premises: [{1, 11, 22, 33}],
        bindings: %{"property" => {:iri, "https://example.test/contains"}},
        metadata: %{graph_id: 1, scope: :local, iteration: 2}
      )

    assert :ok = DerivationProvenance.save(db, tracker)
    assert {:ok, loaded} = DerivationProvenance.load(db, 1)
    assert loaded.derivations == tracker.derivations

    assert {:ok, explanation} =
             DerivationProvenance.explain_inference(loaded, binary_quad, db)

    assert explanation.rule_name == "prp_trp_transitive_contains_deadbeef"
    assert explanation.formatted =~ "Rule: prp_trp_transitive_contains_deadbeef"
  end

  test "loads an existing valid unversioned record without rewriting it", %{db: db} do
    quad = {2, 10, 20, 30}

    derivation = %{
      rule_name: :cax_sco,
      premises: [{2, 11, 21, 31}],
      bindings: %{"x" => {:bound, 10}},
      timestamp: 1_700_000_000_000
    }

    key = provenance_key(quad)
    encoded = :erlang.term_to_binary(derivation)
    :ok = ErlangAdapter.put(db, @provenance_cf, key, encoded)

    assert {:ok, tracker} = DerivationProvenance.load(db, 2)
    assert tracker.derivations[quad] == derivation
    assert {:ok, ^encoded} = ErlangAdapter.get(db, @provenance_cf, key)
  end

  test "rejects malformed and truncated derivation records", %{db: db} do
    key = provenance_key({3, 10, 20, 30})
    :ok = ErlangAdapter.put(db, @provenance_cf, key, <<131>>)

    assert {:error, {:corrupt_provenance, :unsafe_or_invalid_term}} =
             DerivationProvenance.load(db, 3)
  end

  test "rejects unsafe external atoms before constructing a runtime term", %{db: db} do
    key = provenance_key({4, 10, 20, 30})
    atom_name = "untrusted_provenance_atom_#{System.unique_integer([:positive])}"
    unsafe = <<131, 119, byte_size(atom_name), atom_name::binary>>
    :ok = ErlangAdapter.put(db, @provenance_cf, key, unsafe)

    assert {:error, {:corrupt_provenance, :unsafe_or_invalid_term}} =
             DerivationProvenance.load(db, 4)
  end

  test "rejects wrong record shapes and unsupported versions", %{db: db} do
    wrong_shape_key = provenance_key({5, 10, 20, 30})
    wrong_shape = :erlang.term_to_binary(%{rule_name: :cax_sco})
    :ok = ErlangAdapter.put(db, @provenance_cf, wrong_shape_key, wrong_shape)

    assert {:error, {:corrupt_provenance, :missing_required_fields}} =
             DerivationProvenance.load(db, 5)

    versioned_key = provenance_key({6, 10, 20, 30})
    versioned = :erlang.term_to_binary(%{version: 2})
    :ok = ErlangAdapter.put(db, @provenance_cf, versioned_key, versioned)

    assert {:error, {:corrupt_provenance, {:unsupported_version, 2}}} =
             DerivationProvenance.load(db, 6)
  end

  test "rejects malformed fact keys", %{db: db} do
    malformed_key = <<7::64-big>>
    :ok = ErlangAdapter.put(db, @provenance_cf, malformed_key, valid_derivation())

    assert {:error, {:corrupt_provenance, :invalid_fact_key}} =
             DerivationProvenance.load(db, 7)
  end

  test "clear_graph preserves bytes when validation fails", %{db: db} do
    key = provenance_key({8, 10, 20, 30})
    corrupt = :erlang.term_to_binary(%{premises: :invalid})
    :ok = ErlangAdapter.put(db, @provenance_cf, key, corrupt)

    assert {:error, {:corrupt_provenance, :missing_required_fields}} =
             DerivationProvenance.clear_graph(db, 8)

    assert {:ok, ^corrupt} = ErlangAdapter.get(db, @provenance_cf, key)
  end

  test "save rejects invalid in-memory records before writing", %{db: db} do
    quad = {9, 10, 20, 30}

    tracker = %DerivationProvenance{
      derivations: %{
        quad => %{rule_name: "", premises: [], bindings: %{}, timestamp: 1}
      },
      count: 1
    }

    assert {:error, {:invalid_provenance, {:invalid_rule_name, ""}}} =
             DerivationProvenance.save(db, tracker)

    assert :not_found = ErlangAdapter.get(db, @provenance_cf, provenance_key(quad))
  end

  test "delete with reasoning fails before changing explicit data when provenance is corrupt", %{
    db: db
  } do
    {graph, subject, predicate, object} = id_quad = {10, 20, 30, 40}
    explicit_quad = {subject, predicate, object, graph}
    corrupt_key = provenance_key({graph, 21, 31, 41})

    assert :ok = QuadOperations.insert_quad(db, explicit_quad)
    assert :ok = ErlangAdapter.put(db, @provenance_cf, corrupt_key, <<131>>)

    assert {:error, {:corrupt_provenance, :unsafe_or_invalid_term}} =
             DeleteWithReasoningQuad.delete_quads_with_reasoning(db, [id_quad], [],
               graph_id: graph,
               emit_telemetry: false
             )

    assert QuadOperations.quad_exists?(db, explicit_quad)
  end

  defp provenance_key({g, s, p, o}), do: <<g::64-big, s::64-big, p::64-big, o::64-big>>

  defp valid_derivation do
    :erlang.term_to_binary(%{
      rule_name: :cax_sco,
      premises: [],
      bindings: %{},
      timestamp: 1
    })
  end
end
