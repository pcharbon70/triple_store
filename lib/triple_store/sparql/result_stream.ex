defmodule TripleStore.SPARQL.ResultStream do
  @moduledoc """
  Result streaming for large SPARQL query results (S9).

  Provides efficient streaming of large result sets:
  - Batches results to avoid memory bloat
  - Supports Elixir Stream protocol
  - Configurable batch size
  - Lazy evaluation
  - Compatible with Enum functions

  ## Examples

      # Create a stream from an enumerable
      stream = ResultStream.new(large_list, batch_size: 100)

      # Consume in batches
      Enum.each(stream, fn batch ->
        process_batch(batch)
      end)
  """

  defstruct [:enumerable, :batch_size, :transform, :state]

  @type t :: %__MODULE__{
          enumerable: Enumerable.t(),
          batch_size: pos_integer(),
          transform: (term() -> term()) | nil,
          state: :running | :done
        }

  @default_batch_size 1000

  @doc """
  Creates a new result stream.

  ## Options
    - `:batch_size` - Number of items per batch (default: 1000)
    - `:transform` - Optional function to transform each batch
  """
  @spec new(Enumerable.t(), keyword()) :: t()
  def new(enumerable, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
    transform = Keyword.get(opts, :transform)

    %__MODULE__{
      enumerable: enumerable,
      batch_size: batch_size,
      transform: transform,
      state: :running
    }
  end

  @doc """
  Streams an enumerable in batches.

  Returns a stream that yields batches of the specified size.
  """
  @spec stream_batches(Enumerable.t(), keyword()) :: Enumerable.t()
  def stream_batches(enumerable, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
    transform = Keyword.get(opts, :transform)

    enumerable
    |> Stream.chunk_every(batch_size)
    |> maybe_transform(transform)
  end

  @doc """
  Streams results from a RocksDB fold operation.

  Efficiently streams results from database operations.
  """
  @spec from_db_fold((-> Enumerable.t()), keyword()) :: Enumerable.t()
  def from_db_fold(fold_fn, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    Stream.resource(
      fn ->
        # Start the fold
        fold_fn.()
      end,
      fn state ->
        case advance_state(state, batch_size) do
          {:done, _} -> {:halt, state}
          {:more, batch, new_state} -> {[batch], new_state}
        end
      end,
      fn _state -> :ok end
    )
  end

  @doc """
  Counts items in a stream efficiently.
  """
  @spec count(t() | Enumerable.t()) :: non_neg_integer()
  def count(%__MODULE__{enumerable: enumerable}), do: Enum.count(enumerable)
  def count(enumerable), do: Enum.count(enumerable)

  @doc """
  Takes the first n items from a stream.
  """
  @spec take(t() | Enumerable.t(), non_neg_integer()) :: list()
  def take(%__MODULE__{enumerable: enumerable}, n), do: Enum.take(enumerable, n)
  def take(enumerable, n), do: Enum.take(enumerable, n)

  @doc """
  Creates a stream that limits results and emits telemetry.
  """
  @spec limited_stream(Enumerable.t(), pos_integer(), keyword()) :: Enumerable.t()
  def limited_stream(enumerable, limit, opts \\ []) do
    transform = Keyword.get(opts, :transform)

    enumerable
    |> Stream.take(limit)
    |> maybe_transform(transform)
  end

  @doc """
  Applies a function to each element in the stream.
  """
  @spec each(t() | Enumerable.t(), (term() -> any())) :: :ok
  def each(%__MODULE__{enumerable: enumerable}, fun) do
    enumerable
    |> Enum.each(fun)

    :ok
  end

  def each(enumerable, fun) do
    Enum.each(enumerable, fun)
    :ok
  end

  @doc """
  Reduces a stream into a single value.
  """
  @spec reduce(t() | Enumerable.t(), any(), (any(), any() -> any())) :: any()
  def reduce(%__MODULE__{enumerable: enumerable}, acc, fun), do: Enum.reduce(enumerable, acc, fun)
  def reduce(enumerable, acc, fun), do: Enum.reduce(enumerable, acc, fun)

  @doc """
  Maps a function over a stream.
  """
  @spec map(t() | Enumerable.t(), (term() -> any())) :: Enumerable.t()
  def map(%__MODULE__{enumerable: enumerable}, fun), do: Stream.map(enumerable, fun)
  def map(enumerable, fun), do: Stream.map(enumerable, fun)

  @doc """
  Filters a stream.
  """
  @spec filter(t() | Enumerable.t(), (term() -> boolean())) :: Enumerable.t()
  def filter(%__MODULE__{enumerable: enumerable}, fun), do: Stream.filter(enumerable, fun)
  def filter(enumerable, fun), do: Stream.filter(enumerable, fun)

  @doc """
  Converts a stream to a list (with memory safety for large streams).
  """
  @spec to_list(t() | Enumerable.t(), keyword()) :: list()
  def to_list(stream_or_enumerable, opts \\ [])

  def to_list(%__MODULE__{enumerable: enumerable}, opts) do
    max_items = Keyword.get(opts, :max_items)

    base =
      if max_items do
        Stream.take(enumerable, max_items)
      else
        enumerable
      end

    Enum.to_list(base)
  end

  def to_list(enumerable, opts) do
    max_items = Keyword.get(opts, :max_items)

    if max_items do
      enumerable
      |> Stream.take(max_items)
      |> Enum.to_list()
    else
      Enum.to_list(enumerable)
    end
  end

  @doc """
  Paginates a stream.
  """
  @spec paginate(Enumerable.t(), pos_integer(), pos_integer()) :: %{
          items: list(),
          total: non_neg_integer(),
          page: pos_integer(),
          page_size: pos_integer()
        }
  def paginate(enumerable, page, page_size \\ @default_batch_size) do
    offset = (page - 1) * page_size

    items =
      enumerable
      |> Stream.drop(offset)
      |> Stream.take(page_size)
      |> Enum.to_list()

    total = Enum.count(enumerable)

    %{
      items: items,
      total: total,
      page: page,
      page_size: page_size,
      total_pages: ceil(total / page_size)
    }
  end

  # Implement Enumerable protocol for ResultStream
  defimpl Enumerable, for: __MODULE__ do
    def reduce(_stream, {:halt, acc}, _fun), do: {:halted, acc}

    def reduce(%{enumerable: _enumerable} = stream, {:suspend, acc}, fun),
      do: {:suspended, acc, &reduce(stream, &1, fun)}

    def reduce(%{enumerable: enumerable}, {:cont, acc}, fun) do
      Enumerable.reduce(enumerable, {:cont, acc}, fun)
    end

    def count(%{enumerable: enumerable}), do: Enumerable.count(enumerable)

    def member?(%{enumerable: enumerable}, element) when is_list(enumerable) do
      element in enumerable
    end

    def member?(%{enumerable: enumerable}, element) do
      # For non-list enumerables, do linear search
      # First convert to list to avoid infinite recursion
      list = Enum.to_list(enumerable)
      element in list
    end

    def slice(%{enumerable: enumerable}), do: Enumerable.slice(enumerable)
  end

  # Private helpers

  defp advance_state(:done, _batch_size), do: :done

  defp advance_state(state, batch_size) do
    case Enum.split(state, batch_size) do
      {[], _} -> {:done, state}
      {batch, remaining} -> {:more, batch, remaining}
    end
  end

  defp maybe_transform(stream, nil), do: stream
  defp maybe_transform(stream, transform), do: Stream.map(stream, transform)
end
