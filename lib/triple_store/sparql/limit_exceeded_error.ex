defmodule TripleStore.SPARQL.LimitExceededError do
  @moduledoc """
  Exception raised when a SPARQL query exceeds configured safety limits.

  This error is raised to prevent denial-of-service attacks or accidental
  resource exhaustion from queries that produce too many results.

  ## Fields

  - `:message` - Human-readable error message
  - `:limit` - The limit that was exceeded
  - `:operation` - The operation that exceeded the limit (:distinct, :order_by, :describe)

  ## Examples

      try do
        Executor.distinct(huge_stream) |> Enum.to_list()
      rescue
        e in TripleStore.SPARQL.LimitExceededError ->
          Logger.warning("Query exceeded limit: \#{e.message}")
          {:error, {:limit_exceeded, e.operation, e.limit}}
      end

  """

  defexception [:message, :limit, :operation]

  @impl true
  def exception(opts) do
    message = Keyword.fetch!(opts, :message)
    limit = Keyword.fetch!(opts, :limit)
    operation = Keyword.fetch!(opts, :operation)

    %__MODULE__{
      message: message,
      limit: limit,
      operation: operation
    }
  end
end
