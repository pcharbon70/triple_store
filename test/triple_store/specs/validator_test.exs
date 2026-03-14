defmodule TripleStore.Specs.ValidatorTest do
  use ExUnit.Case, async: true

  alias TripleStore.Specs.Validator

  test "current specs pass governance and conformance validation" do
    spec_root = Path.expand("specs", File.cwd!())

    assert {:ok, report} = Validator.validate(spec_root: spec_root, mode: :all)
    assert report.errors == []
    assert report.stats.requirements > 0
    assert report.stats.acceptances > 0
    assert report.stats.scenarios > 0
  end

  test "broken scenario references are reported" do
    temp_root = temp_repo_root()
    File.cp_r!(Path.expand("specs", File.cwd!()), Path.join(temp_root, "specs"))

    matrix_path = Path.join([temp_root, "specs", "conformance", "spec_conformance_matrix.md"])

    matrix_path
    |> File.read!()
    |> String.replace("`SCN-012`, `SCN-013`, `SCN-014`", "`SCN-012`, `SCN-013`, `SCN-999`")
    |> then(&File.write!(matrix_path, &1))

    assert {:error, report} =
             Validator.validate(spec_root: Path.join(temp_root, "specs"), mode: :conformance)

    assert Enum.any?(report.errors, &String.contains?(&1, "SCN-999"))
  end

  defp temp_repo_root do
    root =
      Path.join(
        System.tmp_dir!(),
        "triple_store_specs_validator_#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(root)
    File.mkdir_p!(root)
    root
  end
end
