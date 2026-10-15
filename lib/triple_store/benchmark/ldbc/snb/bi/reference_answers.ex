defmodule TripleStore.Benchmark.LDBC.SNB.BI.ReferenceAnswers do
  @moduledoc """
  Loads provenance-bound SNB BI reference answers and compares them canonically.

  Every answer is tied to an exact catalog operation and variant, dataset and
  mapping checksum, parameter row, operation source, and reference-engine
  version. Embedded smoke answers are checksummed JSON. Large answer bundles
  remain external, addressed by an immutable URI and checksum, and must be
  supplied explicitly before a run can be validated.
  """

  alias TripleStore.Benchmark.LDBC.{Correctness, Result}
  alias TripleStore.Benchmark.LDBC.SNB.BI.{ResultContract, Workload}

  @manifest_schema_version 2
  @payload_schema_version 1
  @bundle_schema_version 1
  @layout_version "snb-bi-reference-answers-v1"

  @doc "Returns the checked-in reference-answer root."
  @spec default_root() :: Path.t()
  def default_root do
    case :code.priv_dir(:triple_store) do
      {:error, _reason} ->
        Path.expand("../../../../../../priv/benchmarks/ldbc/snb-bi/reference", __DIR__)

      priv_dir ->
        Path.join(to_string(priv_dir), "benchmarks/ldbc/snb-bi/reference")
    end
  end

  @doc "Returns the validated reference-answer inventory without loading answer payloads."
  @spec inventory(keyword()) :: {:ok, [map()]} | {:error, term()}
  def inventory(opts \\ []) do
    root = Keyword.get(opts, :root, default_root())

    with {:ok, manifest} <- read_manifest(root),
         :ok <- validate_manifest(manifest) do
      entries =
        manifest.answers
        |> Enum.map(fn entry ->
          %{
            operation_id: entry.operation_id,
            variant: entry.variant,
            parameter_row_index: entry.parameter.row_index,
            purpose: entry.purpose,
            artifact: entry.artifact,
            reference_source: entry.reference_source
          }
        end)
        |> Enum.sort_by(&{&1.operation_id, &1.variant, &1.parameter_row_index})

      {:ok, entries}
    end
  end

  @doc "Loads one reference answer after validating every active-run identity."
  @spec load(map(), non_neg_integer(), map(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def load(definition, parameter_row_index, dataset, parameters, opts \\ [])
      when is_map(definition) and is_integer(parameter_row_index) and parameter_row_index >= 0 do
    root = Keyword.get(opts, :root, default_root())

    with {:ok, manifest} <- read_manifest(root),
         :ok <- validate_manifest(manifest),
         {:ok, entry} <- find_entry(manifest, definition, parameter_row_index),
         :ok <- validate_identity(manifest, entry, definition, dataset, parameters, opts),
         {:ok, payload, checksum} <- read_artifact(root, entry, parameters),
         {:ok, result} <- materialize_payload(definition, payload) do
      {:ok,
       %{
         result: result,
         identity: Map.take(entry, [:operation_id, :variant, :parameter, :dataset]),
         purpose: entry.purpose,
         reference_source: entry.reference_source,
         operation_source: entry.operation_source,
         content_checksum: checksum
       }}
    end
  end

  @doc "Loads the expected answer and compares an actual BI result through `Correctness`."
  @spec compare(map(), non_neg_integer(), Result.t() | map(), map(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def compare(definition, parameter_row_index, actual, dataset, parameters, opts \\ []) do
    with {:ok, answer} <- load(definition, parameter_row_index, dataset, parameters, opts),
         :ok <- validate_answer_purpose(answer.purpose),
         :ok <- validate_divergence(opts, answer.reference_source.version),
         {:ok, actual_result} <- ResultContract.to_result(definition, actual) do
      comparison_opts =
        Keyword.take(opts, [:full_comparison_limit, :numeric_tolerance, :accepted_divergence])

      comparison = Correctness.compare(answer.result, actual_result, comparison_opts)

      {:ok,
       Map.put(comparison, :reference_answer, %{
         content_checksum: answer.content_checksum,
         parameter: answer.identity.parameter,
         reference_source: answer.reference_source
       })}
    end
  end

  defp validate_answer_purpose(:accepted_answer), do: :ok

  defp validate_answer_purpose(purpose),
    do: {:error, {:reference_answer_not_accepted, purpose}}

  defp read_manifest(root) do
    path = Path.join(root, "manifest.exs")

    if File.regular?(path) do
      {manifest, _binding} = Code.eval_file(path)
      {:ok, manifest}
    else
      {:error, {:reference_answer_manifest_missing, path}}
    end
  rescue
    error -> {:error, {:invalid_reference_answer_manifest, Exception.message(error)}}
  end

  defp validate_manifest(%{
         schema_version: @manifest_schema_version,
         layout_version: @layout_version,
         catalog_id: catalog_id,
         answers: answers
       })
       when is_binary(catalog_id) and is_list(answers) do
    errors =
      answers
      |> Enum.with_index()
      |> Enum.flat_map(fn {entry, index} -> entry_errors(entry, index) end)
      |> Kernel.++(duplicate_errors(answers))

    if errors == [], do: :ok, else: {:error, {:invalid_reference_answer_manifest, errors}}
  end

  defp validate_manifest(%{schema_version: version}) when version != @manifest_schema_version,
    do: {:error, {:unsupported_reference_answer_schema, version}}

  defp validate_manifest(_manifest), do: {:error, :invalid_reference_answer_manifest}

  defp entry_errors(entry, index) when is_map(entry) do
    []
    |> require(non_empty?(entry[:operation_id]), {index, :operation_id})
    |> require(non_empty?(entry[:variant]), {index, :variant})
    |> require(entry[:purpose] in [:accepted_answer, :contract_fixture], {index, :purpose})
    |> require(valid_parameter?(entry[:parameter]), {index, :parameter})
    |> require(valid_dataset?(entry[:dataset]), {index, :dataset})
    |> require(valid_source?(entry[:operation_source]), {index, :operation_source})
    |> require(valid_source?(entry[:reference_source]), {index, :reference_source})
    |> require(valid_artifact?(entry[:artifact]), {index, :artifact})
  end

  defp entry_errors(_entry, index), do: [{index, :entry}]

  defp duplicate_errors(answers) do
    answers
    |> Enum.filter(&is_map/1)
    |> Enum.frequencies_by(fn entry ->
      parameter_index =
        case entry[:parameter] do
          %{row_index: index} -> index
          _parameter -> nil
        end

      {entry[:operation_id], entry[:variant], parameter_index}
    end)
    |> Enum.flat_map(fn
      {identity, count} when count > 1 -> [{:duplicate_reference_answer, identity}]
      _entry -> []
    end)
  end

  defp find_entry(manifest, definition, parameter_row_index) do
    matches =
      Enum.filter(manifest.answers, fn entry ->
        entry.operation_id == definition.catalog_id and entry.variant == definition.variant and
          entry.parameter.row_index == parameter_row_index
      end)

    case matches do
      [entry] ->
        {:ok, entry}

      [] ->
        {:error,
         {:reference_answer_not_found, definition.catalog_id, definition.variant,
          parameter_row_index}}
    end
  end

  defp validate_identity(manifest, entry, definition, dataset, parameters, opts) do
    reference_source = Keyword.get(opts, :reference_source)
    transformation = Map.get(dataset, :transformation, %{})
    dataset_checksum = Map.get(transformation, :output_checksum)
    mapping_version = Map.get(transformation, :mapping_version)
    parameter_row = parameter_row(parameters, entry.parameter)

    errors =
      []
      |> require(manifest.catalog_id == Workload.catalog_id(), :catalog)
      |> require(entry.operation_id == definition.catalog_id, :operation)
      |> require(entry.variant == definition.variant, :variant)
      |> require(entry.dataset.id == Map.get(dataset, :dataset_id), :dataset_id)
      |> require(entry.dataset.checksum == dataset_checksum, :dataset_checksum)
      |> require(entry.dataset.mapping_version == mapping_version, :mapping_version)
      |> require(Map.get(parameters, :dataset_checksum) == dataset_checksum, :parameter_dataset)
      |> require(entry.parameter.source_id == Map.get(parameters, :source_id), :parameter_source)
      |> require(
        entry.parameter.generator_pin == Map.get(parameters, :generator_pin),
        :parameter_generator
      )
      |> require(match?({:ok, _row}, parameter_row), :parameter_row)
      |> require(
        parameter_checksum_matches?(parameter_row, entry.parameter.checksum),
        :parameter_checksum
      )
      |> require(entry.operation_source == operation_source(definition), :operation_source)
      |> require(
        not is_nil(reference_source) and entry.reference_source == reference_source,
        :reference_source
      )

    if errors == [], do: :ok, else: {:error, {:invalid_reference_answer, errors}}
  end

  defp parameter_row(parameters, parameter) do
    with sequences when is_map(sequences) <- Map.get(parameters, :sequences),
         rows when is_list(rows) <- Map.get(sequences, parameter.sequence),
         row when not is_nil(row) <- Enum.at(rows, parameter.row_index) do
      {:ok, row}
    else
      _other -> :error
    end
  end

  defp parameter_checksum_matches?({:ok, row}, expected), do: term_checksum(row) == expected
  defp parameter_checksum_matches?(:error, _expected), do: false

  defp operation_source(definition) do
    definition.source
    |> Map.take([:source_id, :path, :commit, :reference])
  end

  defp read_artifact(
         root,
         %{artifact: %{kind: :embedded, path: relative, checksum: expected}},
         _parameters
       ) do
    with {:ok, path} <- resolve_embedded_path(root, relative),
         {:ok, binary} <- read_answer_file(path),
         actual <- binary_checksum(binary),
         :ok <- verify_checksum(expected, actual),
         {:ok, payload} <- decode_payload(binary) do
      {:ok, payload, actual}
    end
  end

  defp read_artifact(
         root,
         %{
           artifact: %{
             kind: :embedded_bundle,
             path: relative,
             checksum: expected_bundle,
             answer_checksum: expected_answer
           }
         } = entry,
         parameters
       ) do
    with {:ok, path} <- resolve_embedded_path(root, relative),
         {:ok, binary} <- read_answer_file(path),
         actual_bundle <- binary_checksum(binary),
         :ok <- verify_checksum(expected_bundle, actual_bundle),
         {:ok, payload, actual_answer} <- decode_bundle_answer(binary, entry, parameters),
         :ok <- verify_checksum(expected_answer, actual_answer) do
      {:ok, payload, actual_answer}
    end
  end

  defp read_artifact(_root, %{artifact: %{kind: :external} = artifact}, _parameters),
    do: {:error, {:external_reference_answer_required, artifact}}

  defp resolve_embedded_path(root, relative) do
    expanded_root = Path.expand(root)
    path = Path.expand(relative, expanded_root)

    if path != expanded_root and String.starts_with?(path, expanded_root <> "/") do
      {:ok, path}
    else
      {:error, {:invalid_reference_answer_path, relative}}
    end
  end

  defp read_answer_file(path) do
    case File.read(path) do
      {:ok, binary} -> {:ok, binary}
      {:error, :enoent} -> {:error, {:reference_answer_missing, path}}
      {:error, reason} -> {:error, {:reference_answer_read_failed, path, reason}}
    end
  end

  defp verify_checksum(checksum, checksum), do: :ok

  defp verify_checksum(expected, actual),
    do: {:error, {:reference_answer_checksum_mismatch, expected, actual}}

  defp decode_payload(binary) do
    case Jason.decode(binary) do
      {:ok, %{"schema_version" => @payload_schema_version, "rows" => rows} = payload}
      when is_list(rows) and map_size(payload) == 2 ->
        {:ok, payload}

      {:ok, _payload} ->
        {:error, {:malformed_reference_answer, :invalid_payload}}

      {:error, reason} ->
        {:error, {:malformed_reference_answer, Exception.message(reason)}}
    end
  end

  defp decode_bundle_answer(binary, entry, parameters) do
    with {:ok, bundle} <- Jason.decode(binary),
         {:ok, answers} <- bundle_answers(bundle),
         {:ok, answer} <- select_bundle_answer(answers, entry),
         :ok <- validate_bundle_parameters(answer, entry, parameters),
         {:ok, rows} <- bundle_rows(answer),
         actual_checksum <- canonical_checksum(rows),
         :ok <- verify_bundle_rows_checksum(answer, actual_checksum) do
      {:ok, %{"rows" => rows}, actual_checksum}
    else
      {:error, {:malformed_reference_answer_bundle, _reason} = error} ->
        {:error, error}

      {:error, %Jason.DecodeError{} = error} ->
        {:error, {:malformed_reference_answer_bundle, Exception.message(error)}}
    end
  end

  defp bundle_answers(%{"schema_version" => @bundle_schema_version, "answers" => answers})
       when is_list(answers),
       do: {:ok, answers}

  defp bundle_answers(_bundle),
    do: {:error, {:malformed_reference_answer_bundle, :invalid_payload}}

  defp select_bundle_answer(answers, entry) do
    matches =
      Enum.filter(answers, fn answer ->
        answer["operation_id"] == entry.operation_id and answer["variant"] == entry.variant and
          answer["parameter_row_index"] == entry.parameter.row_index
      end)

    case matches do
      [answer] -> {:ok, answer}
      [] -> {:error, {:malformed_reference_answer_bundle, :answer_not_found}}
      _answers -> {:error, {:malformed_reference_answer_bundle, :duplicate_answer}}
    end
  end

  defp validate_bundle_parameters(answer, entry, parameters) do
    case parameter_row(parameters, entry.parameter) do
      {:ok, row} ->
        if answer["parameters"] == row,
          do: :ok,
          else: {:error, {:malformed_reference_answer_bundle, :parameter_mismatch}}

      :error ->
        {:error, {:malformed_reference_answer_bundle, :parameter_mismatch}}
    end
  end

  defp bundle_rows(%{"rows" => rows}) when is_list(rows), do: {:ok, rows}
  defp bundle_rows(_answer), do: {:error, {:malformed_reference_answer_bundle, :invalid_rows}}

  defp verify_bundle_rows_checksum(%{"rows_checksum" => checksum}, checksum), do: :ok

  defp verify_bundle_rows_checksum(_answer, _actual),
    do: {:error, {:malformed_reference_answer_bundle, :rows_checksum_mismatch}}

  defp materialize_payload(definition, %{"rows" => rows}) do
    case ResultContract.decode_reference(definition, rows) do
      {:ok, %Result{} = result} -> {:ok, result}
      {:error, reason} -> {:error, {:malformed_reference_answer, reason}}
    end
  end

  defp validate_divergence(opts, reference_version) do
    case Keyword.fetch(opts, :accepted_divergence) do
      {:ok, divergence} -> Correctness.validate_divergence(divergence, reference_version)
      :error -> :ok
    end
  end

  defp valid_parameter?(%{
         sequence: sequence,
         row_index: index,
         checksum: checksum,
         source_id: source_id,
         generator_pin: generator_pin
       }) do
    non_empty?(sequence) and is_integer(index) and index >= 0 and checksum?(checksum) and
      non_empty?(source_id) and non_empty?(generator_pin)
  end

  defp valid_parameter?(_parameter), do: false

  defp valid_dataset?(%{id: id, checksum: checksum, mapping_version: mapping_version}),
    do: non_empty?(id) and checksum?(checksum) and non_empty?(mapping_version)

  defp valid_dataset?(_dataset), do: false

  defp valid_source?(source) when is_map(source),
    do: map_size(source) > 0 and Enum.all?(source, fn {_key, value} -> non_empty?(value) end)

  defp valid_source?(_source), do: false

  defp valid_artifact?(%{kind: :embedded, path: path, checksum: checksum}),
    do: non_empty?(path) and checksum?(checksum)

  defp valid_artifact?(%{
         kind: :embedded_bundle,
         path: path,
         checksum: checksum,
         answer_checksum: answer_checksum
       }),
       do: non_empty?(path) and checksum?(checksum) and checksum?(answer_checksum)

  defp valid_artifact?(%{kind: :external, uri: uri, checksum: checksum}),
    do: non_empty?(uri) and checksum?(checksum)

  defp valid_artifact?(_artifact), do: false

  defp term_checksum(term),
    do: term |> :erlang.term_to_binary([:deterministic]) |> binary_checksum()

  defp binary_checksum(binary),
    do: "sha256:" <> (:crypto.hash(:sha256, binary) |> Base.encode16(case: :lower))

  defp canonical_checksum(value), do: value |> canonical_json() |> binary_checksum()

  defp canonical_json(value) when is_list(value),
    do: "[" <> Enum.map_join(value, ",", &canonical_json/1) <> "]"

  defp canonical_json(value) when is_map(value) do
    encoded =
      value
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map_join(",", fn {key, nested} ->
        Jason.encode!(key) <> ":" <> canonical_json(nested)
      end)

    "{" <> encoded <> "}"
  end

  defp canonical_json(value), do: Jason.encode!(value)

  defp checksum?("sha256:" <> digest),
    do: byte_size(digest) == 64 and String.match?(digest, ~r/\A[0-9a-f]+\z/)

  defp checksum?(_checksum), do: false
  defp non_empty?(value), do: is_binary(value) and value != ""

  defp require(errors, true, _error), do: errors
  defp require(errors, false, error), do: errors ++ [error]
end
