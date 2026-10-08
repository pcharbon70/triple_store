defmodule TripleStore.Benchmark.LDBC.SNB.BI.Analytics do
  @moduledoc """
  Execution boundary for SNB BI analytical operations.

  Implementations are registered explicitly by operation number. Unknown or
  unfinished translations fail closed instead of emitting empty benchmark
  answers that could be mistaken for validated results.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.ResultContract

  @doc "Runs a registered engine handler and enforces the operation's typed result contract."
  @spec execute(map(), map(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def execute(definition, context, parameters, opts \\ []) do
    handlers = Map.get(context, :operation_handlers, %{})

    case Map.fetch(handlers, {definition.number, definition.variant}) do
      {:ok, handler} when is_function(handler, 3) ->
        with {:ok, rows} <- handler.(context, parameters, opts),
             {:ok, result} <- ResultContract.materialize(definition, rows) do
          {:ok, result}
        end

      _other ->
        {:error, {:bi_operation_not_implemented, definition.number, definition.variant}}
    end
  end
end
