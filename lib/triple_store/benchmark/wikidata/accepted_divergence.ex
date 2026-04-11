defmodule TripleStore.Benchmark.Wikidata.AcceptedDivergence do
  @moduledoc """
  Accepted-divergence records for benchmark correctness comparisons.

  These records provide the benchmark-facing API for acknowledging known
  divergences without suppressing unrelated failures.
  """

  @enforce_keys [:benchmark_id, :classification, :notes]
  defstruct benchmark_id: nil,
            execution_variant: nil,
            classification: nil,
            reference_fingerprint: nil,
            actual_fingerprint: nil,
            notes: nil,
            accepted_on: nil,
            expires_at: nil

  @type t :: %__MODULE__{
          benchmark_id: String.t(),
          execution_variant: atom() | nil,
          classification: atom(),
          reference_fingerprint: String.t() | nil,
          actual_fingerprint: String.t() | nil,
          notes: String.t(),
          accepted_on: Date.t() | nil,
          expires_at: Date.t() | nil
        }

  @doc """
  Builds a validated accepted-divergence record.
  """
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Enum.into(attrs, %{})

    record = %__MODULE__{
      benchmark_id: Map.get(attrs, :benchmark_id),
      execution_variant: Map.get(attrs, :execution_variant),
      classification: Map.get(attrs, :classification),
      reference_fingerprint: Map.get(attrs, :reference_fingerprint),
      actual_fingerprint: Map.get(attrs, :actual_fingerprint),
      notes: Map.get(attrs, :notes),
      accepted_on: Map.get(attrs, :accepted_on),
      expires_at: Map.get(attrs, :expires_at)
    }

    validate(record)
  end

  @doc """
  Returns true when an accepted divergence applies to a correctness summary.
  """
  @spec matches?(t(), map()) :: boolean()
  def matches?(%__MODULE__{} = record, comparison_summary) when is_map(comparison_summary) do
    same_benchmark? = record.benchmark_id == Map.get(comparison_summary, :benchmark_id)

    same_variant? =
      is_nil(record.execution_variant) or
        record.execution_variant == Map.get(comparison_summary, :execution_variant)

    same_classification? =
      record.classification == Map.get(comparison_summary, :classification)

    same_reference? =
      is_nil(record.reference_fingerprint) or
        record.reference_fingerprint == Map.get(comparison_summary, :reference_fingerprint)

    same_actual? =
      is_nil(record.actual_fingerprint) or
        record.actual_fingerprint == Map.get(comparison_summary, :answer_fingerprint)

    not_expired? =
      case record.expires_at do
        %Date{} = expires_at -> Date.compare(expires_at, Date.utc_today()) in [:gt, :eq]
        nil -> true
        _ -> true
      end

    same_benchmark? and same_variant? and same_classification? and same_reference? and
      same_actual? and not_expired?
  end

  @doc """
  Persists accepted divergence records in Erlang term format.
  """
  @spec write(Path.t(), [t()]) :: :ok | {:error, term()}
  def write(path, records) when is_binary(path) and is_list(records) do
    serializable =
      Enum.map(records, fn record ->
        record
        |> Map.from_struct()
        |> Map.update(:accepted_on, nil, &serialize_date/1)
        |> Map.update(:expires_at, nil, &serialize_date/1)
      end)

    File.write(path, :erlang.term_to_binary(serializable))
  end

  @doc """
  Persists accepted divergence records as pretty JSON.
  """
  @spec write_json(Path.t(), [t()]) :: :ok | {:error, term()}
  def write_json(path, records) when is_binary(path) and is_list(records) do
    records
    |> Enum.map(fn record ->
      record
      |> Map.from_struct()
      |> Map.update(:accepted_on, nil, &serialize_date/1)
      |> Map.update(:expires_at, nil, &serialize_date/1)
      |> Enum.map(fn {key, value} -> {to_string(key), value} end)
      |> Enum.into(%{})
    end)
    |> Jason.encode!(pretty: true)
    |> then(&File.write(path, &1))
  end

  @doc """
  Loads accepted divergence records from Erlang term format.
  """
  @spec load(Path.t()) :: {:ok, [t()]} | {:error, term()}
  def load(path) when is_binary(path) do
    with {:ok, binary} <- File.read(path),
         records when is_list(records) <- :erlang.binary_to_term(binary, [:safe]),
         {:ok, validated} <- validate_many(records) do
      {:ok, validated}
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_accepted_divergence_file}
    end
  end

  @doc """
  Loads accepted divergence records from JSON.
  """
  @spec load_json(Path.t()) :: {:ok, [t()]} | {:error, term()}
  def load_json(path) when is_binary(path) do
    with {:ok, json} <- File.read(path),
         {:ok, records} when is_list(records) <- Jason.decode(json),
         {:ok, validated} <- validate_many_json(records) do
      {:ok, validated}
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_accepted_divergence_json}
    end
  end

  defp validate(%__MODULE__{} = record) do
    cond do
      not is_binary(record.benchmark_id) or record.benchmark_id == "" ->
        {:error, :invalid_benchmark_id}

      not is_atom(record.classification) ->
        {:error, :invalid_classification}

      not is_binary(record.notes) or record.notes == "" ->
        {:error, :invalid_notes}

      not is_nil(record.execution_variant) and
          record.execution_variant not in [:raw, :count_only, :distinct_only] ->
        {:error, :invalid_execution_variant}

      true ->
        {:ok, record}
    end
  end

  defp validate_many(records) do
    records
    |> Enum.reduce_while({:ok, []}, fn attrs, {:ok, acc} ->
      attrs =
        attrs
        |> deserialize_date(:accepted_on)
        |> deserialize_date(:expires_at)

      case new(attrs) do
        {:ok, record} -> {:cont, {:ok, [record | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, records} -> {:ok, Enum.reverse(records)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp serialize_date(%Date{} = date), do: Date.to_iso8601(date)
  defp serialize_date(other), do: other

  defp deserialize_date(attrs, field) when is_map(attrs) do
    case Map.get(attrs, field) do
      value when is_binary(value) ->
        case Date.from_iso8601(value) do
          {:ok, date} -> Map.put(attrs, field, date)
          _ -> attrs
        end

      _ ->
        attrs
    end
  end

  defp validate_many_json(records) do
    records
    |> Enum.reduce_while({:ok, []}, fn attrs, {:ok, acc} ->
      attrs =
        attrs
        |> Enum.map(fn {key, value} -> {String.to_existing_atom(key), value} end)
        |> Enum.into(%{})
        |> deserialize_date(:accepted_on)
        |> deserialize_date(:expires_at)

      case new(attrs) do
        {:ok, record} -> {:cont, {:ok, [record | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, records} -> {:ok, Enum.reverse(records)}
      {:error, reason} -> {:error, reason}
    end
  end
end
