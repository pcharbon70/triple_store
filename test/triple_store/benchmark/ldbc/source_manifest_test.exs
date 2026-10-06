defmodule TripleStore.Benchmark.LDBC.SourceManifestTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LDBC.SourceManifest

  test "loads immutable, licensed source pins for all benchmark families" do
    assert {:ok, sources} = SourceManifest.load()
    assert length(sources) == 8

    assert MapSet.new(Enum.map(sources, & &1.benchmark)) ==
             MapSet.new([:spb, :snb_bi, :snb_interactive])

    assert Enum.all?(sources, fn source ->
             source.release not in ["main", "master", "HEAD", "latest"] and
               source.checksum == %{algorithm: :git_sha1, value: source.commit} and
               source.license == "Apache-2.0" and source.notice == "NOTICE.txt"
           end)

    assert {:ok, deep_delete} =
             SourceManifest.fetch(sources, "snb-interactive-v2-driver-30a73a28")

    assert deep_delete.readiness == :work_in_progress
    assert deep_delete.profiles == ["snb-interactive-deep-delete-development-30a73a28"]
  end

  test "rejects moving refs, mismatched checksums, missing notices, and duplicate IDs" do
    valid = valid_source()

    invalid = [
      valid,
      %{
        valid
        | release: "main",
          checksum: %{algorithm: :git_sha1, value: String.duplicate("b", 40)}
      },
      %{valid | id: "missing-notice", notice: ""}
    ]

    assert {:error, errors} = SourceManifest.validate(invalid)
    assert Enum.any?(errors, &match?({"example", :id, "is duplicated"}, &1))
    assert Enum.any?(errors, &match?({"example", :release, _}, &1))
    assert Enum.any?(errors, &match?({"example", :checksum, _}, &1))
    assert Enum.any?(errors, &match?({"missing-notice", :notice, _}, &1))
  end

  test "reports missing external artifacts without attempting acquisition" do
    missing = Path.join(System.tmp_dir!(), "ldbc-missing-#{System.unique_integer([:positive])}")

    assert {:error, {:external_asset_missing, "example", ^missing}} =
             SourceManifest.require_external_asset(valid_source(), missing)
  end

  defp valid_source do
    commit = String.duplicate("a", 40)

    %{
      id: "example",
      benchmark: :spb,
      profiles: ["example-smoke"],
      roles: [:driver],
      repository: "https://github.com/example/project",
      release: "v1.0.0",
      commit: commit,
      checksum: %{algorithm: :git_sha1, value: commit},
      license: "Apache-2.0",
      notice: "NOTICE.txt",
      runtimes: ["Java 8"],
      platforms: ["Linux"],
      readiness: :stable,
      asset_policy: :generate_or_download
    }
  end
end
