defmodule TripleStore.Benchmark.LDBC.SNB.BI.Analytics do
  @moduledoc """
  Execution boundary for SNB BI analytical operations.

  Implementations are registered explicitly by operation number. Unknown or
  unfinished translations fail closed instead of emitting empty benchmark
  answers that could be mistaken for validated results.
  """

  alias TripleStore.Benchmark.LDBC.SNB.BI.{HandlerRegistry, ResultContract}

  @doc "Runs a registered engine handler and enforces the operation's typed result contract."
  @spec execute(map(), map(), map(), keyword()) ::
          {:ok, TripleStore.Benchmark.LDBC.Result.t()} | {:error, term()}
  def execute(definition, context, parameters, opts \\ []) do
    handlers = Map.get(context, :operation_handlers, %{})

    case Map.fetch(handlers, {definition.number, definition.variant}) do
      {:ok, handler} when is_function(handler, 3) ->
        with {:ok, rows} <- handler.(context, parameters, opts) do
          ResultContract.materialize(definition, rows)
        end

      :error ->
        with {:ok, entry} <- HandlerRegistry.fetch(definition),
             {:ok, rows} <- entry.adapter.execute(definition, context, parameters, opts) do
          ResultContract.materialize(definition, rows)
        end

      {:ok, _invalid_handler} ->
        {:error, {:invalid_bi_handler, definition.number, definition.variant}}
    end
  end
end
