defmodule TripleStore.Benchmark.LDBC.SNB.BI.Analytics do
  @moduledoc """
  Execution boundary for SNB BI analytical operations.

  Implementations are registered explicitly by operation number. Unknown or
  unfinished translations fail closed instead of emitting empty benchmark
  answers that could be mistaken for validated results.
  """

  @doc "Rejects an operation until its exact analytical implementation is registered."
  @spec execute(term(), term(), keyword()) :: {:error, term()}
  def execute(definition, _context, _opts \\ []) do
    {:error, {:bi_operation_not_implemented, definition.number, definition.variant}}
  end
end
