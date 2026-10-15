defmodule TripleStore.Benchmark.LDBC.SNB.BI.StateIdentity do
  @moduledoc """
  Immutable identity for one persisted SNB BI update state.

  The identity binds dataset and mapping provenance to a named checkpoint, the
  last applied update batch, its source checksum, and a checksum derived from
  canonical persisted RDF evidence. The same value is carried through
  checkpoints, protocol receipts, correctness records, and run artifacts.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.StateProbe
  alias TripleStore.Benchmark.LDBC.StoreFixture

  @fields [
    :dataset_checksum,
    :mapping_version,
    :checkpoint_name,
    :last_applied_batch,
    :update_stream_checksum,
    :state_checksum
  ]

  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          dataset_checksum: String.t(),
          mapping_version: String.t(),
          checkpoint_name: atom() | String.t(),
          last_applied_batch: non_neg_integer(),
          update_stream_checksum: String.t() | nil,
          state_checksum: String.t()
        }

  @doc "Builds and validates a complete immutable state identity."
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    identity = struct(__MODULE__, Map.new(attrs))

    with :ok <- validate(identity), do: {:ok, identity}
  rescue
    KeyError -> {:error, {:invalid_state_identity, :missing_field}}
  end

  @doc "Captures an identity from canonical persisted evidence in an open fixture."
  @spec capture(StoreFixture.t(), atom() | String.t(), keyword()) ::
          {:ok, t()} | {:error, term()}
  def capture(fixture, checkpoint_name, opts \\ [])

  def capture(%StoreFixture{store: nil}, _checkpoint_name, _opts),
    do: {:error, :store_closed}

  def capture(%StoreFixture{} = fixture, checkpoint_name, opts) do
    with {:ok, snapshot} <- StateProbe.capture(fixture.store) do
      new(%{
        dataset_checksum: fixture.manifest.transformation.output_checksum,
        mapping_version: fixture.manifest.transformation.mapping_version,
        checkpoint_name: checkpoint_name,
        last_applied_batch: Keyword.get(opts, :last_applied_batch, 0),
        update_stream_checksum: Keyword.get(opts, :update_stream_checksum),
        state_checksum: snapshot.state_checksum
      })
    end
  end

  @doc "Validates all identity fields and checksum formats."
  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = identity) do
    validations = [
      {:dataset_checksum, checksum?(identity.dataset_checksum)},
      {:mapping_version, non_empty?(identity.mapping_version)},
      {:checkpoint_name, checkpoint_name?(identity.checkpoint_name)},
      {:last_applied_batch, valid_batch?(identity.last_applied_batch)},
      {:update_stream_checksum, valid_update_checksum?(identity)},
      {:state_checksum, checksum?(identity.state_checksum)}
    ]

    case Enum.find(validations, fn {_field, valid?} -> not valid? end) do
      nil -> :ok
      {field, false} -> invalid(field)
    end
  end

  def validate(_identity), do: {:error, {:invalid_state_identity, :not_a_state_identity}}

  @doc "Verifies that an identity belongs to a dataset manifest."
  @spec verify_manifest(t(), map()) :: :ok | {:error, term()}
  def verify_manifest(%__MODULE__{} = identity, manifest) when is_map(manifest) do
    transformation = Map.get(manifest, :transformation, %{})

    expected = %{
      dataset_checksum: Map.get(transformation, :output_checksum),
      mapping_version: Map.get(transformation, :mapping_version)
    }

    compare_fields(identity, expected, [:dataset_checksum, :mapping_version])
  end

  @doc "Verifies an identity against the fixture's persisted canonical state."
  @spec verify_persisted(t(), StoreFixture.t()) :: :ok | {:error, term()}
  def verify_persisted(%__MODULE__{} = identity, %StoreFixture{} = fixture) do
    with :ok <- verify_manifest(identity, fixture.manifest),
         {:ok, actual} <-
           capture(fixture, identity.checkpoint_name,
             last_applied_batch: identity.last_applied_batch,
             update_stream_checksum: identity.update_stream_checksum
           ) do
      compare(identity, actual)
    end
  end

  @doc "Advances an identity from one successful update receipt."
  @spec advance(t(), pos_integer(), map()) :: {:ok, t()} | {:error, term()}
  def advance(%__MODULE__{} = current, batch, receipt)
      when is_integer(batch) and batch > 0 and is_map(receipt) do
    with {:ok, update_checksum} <- fetch_checksum(receipt, :checksum),
         {:ok, state_checksum} <- fetch_checksum(receipt, :state_checksum),
         {:ok, next} <-
           new(%{
             dataset_checksum: current.dataset_checksum,
             mapping_version: current.mapping_version,
             checkpoint_name: current.checkpoint_name,
             last_applied_batch: batch,
             update_stream_checksum: update_checksum,
             state_checksum: state_checksum
           }),
         :ok <- compare_reported(receipt, next) do
      {:ok, next}
    end
  end

  def advance(%__MODULE__{}, _batch, _receipt),
    do: {:error, {:invalid_state_identity, :update_receipt}}

  @doc "Compares two identities field by field and reports the first mismatch."
  @spec compare(t(), t()) :: :ok | {:error, term()}
  def compare(%__MODULE__{} = expected, %__MODULE__{} = actual) do
    compare_fields(expected, actual, @fields)
  end

  def compare(_expected, _actual),
    do: {:error, {:invalid_state_identity, :not_a_state_identity}}

  defp compare_reported(%{state_identity: reported}, expected), do: compare(expected, reported)
  defp compare_reported(_receipt, _expected), do: :ok

  defp compare_fields(expected, actual, fields) do
    case Enum.find(fields, &(Map.get(expected, &1) != Map.get(actual, &1))) do
      nil ->
        :ok

      field ->
        {:error,
         {:state_identity_mismatch, field, Map.get(expected, field), Map.get(actual, field)}}
    end
  end

  defp fetch_checksum(map, field) do
    case Map.fetch(map, field) do
      {:ok, value} when is_binary(value) ->
        if checksum?(value),
          do: {:ok, value},
          else: {:error, {:invalid_state_identity, field}}

      _other ->
        {:error, {:invalid_state_identity, field}}
    end
  end

  defp invalid(field), do: {:error, {:invalid_state_identity, field}}
  defp non_empty?(value), do: is_binary(value) and value != ""
  defp checkpoint_name?(value) when is_atom(value), do: not is_nil(value)
  defp checkpoint_name?(value), do: non_empty?(value)
  defp valid_batch?(batch), do: is_integer(batch) and batch >= 0

  defp valid_update_checksum?(%__MODULE__{last_applied_batch: 0, update_stream_checksum: value}),
    do: is_nil(value)

  defp valid_update_checksum?(%__MODULE__{
         last_applied_batch: batch,
         update_stream_checksum: value
       })
       when is_integer(batch) and batch > 0,
       do: checksum?(value)

  defp valid_update_checksum?(_identity), do: false

  defp checksum?("sha256:" <> digest) when byte_size(digest) == 64,
    do: String.match?(digest, ~r/\A[0-9a-f]{64}\z/)

  defp checksum?(_value), do: false
end
