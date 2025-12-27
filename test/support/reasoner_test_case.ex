# credo:disable-for-this-file Credo.Check.Readability.FunctionNames
defmodule TripleStore.ReasonerTestCase do
  @moduledoc """
  ExUnit case template for OWL 2 RL reasoning integration tests.

  This template provides:
  - Import of shared test helpers from `TripleStore.Test.ReasonerHelpers`
  - Standard module tag for integration tests
  - Common aliases for reasoning modules

  ## Usage

      defmodule MyReasonerTest do
        use TripleStore.ReasonerTestCase

        test "example test" do
          facts = MapSet.new([
            {ex_iri("alice"), rdf_type(), ex_iri("Person")}
          ])
          all_facts = materialize(facts)
          assert has_triple?(all_facts, {ex_iri("alice"), rdf_type(), ex_iri("Person")})
        end
      end

  ## Available Helpers

  All functions from `TripleStore.Test.ReasonerHelpers` are imported:

  ### IRI Builders
  - `ex_iri/1` - Example namespace IRIs
  - `ub_iri/1` - LUBM university benchmark IRIs

  ### Vocabulary
  - `rdf_type/0`
  - `rdfs_subClassOf/0`, `rdfs_subPropertyOf/0`, `rdfs_domain/0`, `rdfs_range/0`
  - `owl_TransitiveProperty/0`, `owl_SymmetricProperty/0`, `owl_FunctionalProperty/0`
  - `owl_InverseFunctionalProperty/0`, `owl_inverseOf/0`, `owl_sameAs/0`, `owl_Nothing/0`

  ### Query Helpers
  - `query/2` - Pattern matching query
  - `select_types/2` - Find types of a subject
  - `select_objects/3` - Find objects for subject+predicate
  - `select_subjects/3` - Find subjects for predicate+object
  - `has_triple?/2` - Check triple existence

  ### Materialization
  - `materialize/1,2` - Run materialization
  - `materialize_with_stats/1,2` - Run materialization with statistics
  - `compute_derived/2` - Compute derived facts
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      import TripleStore.Test.ReasonerHelpers

      alias TripleStore.Reasoner.{
        SemiNaive,
        ReasoningProfile,
        ReasoningConfig,
        ReasoningMode,
        PatternMatcher,
        Incremental,
        DeleteWithReasoning,
        ReasoningStatus,
        TBoxCache
      }

      @moduletag :integration
    end
  end

  setup _tags do
    :ok
  end
end
