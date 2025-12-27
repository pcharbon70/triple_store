defmodule TripleStore.Benchmark.LUBMQueries do
  @moduledoc """
  LUBM (Lehigh University Benchmark) query templates.

  Implements the 14 standard LUBM benchmark queries with varying complexity.
  These queries test different aspects of RDF store performance:

  - Simple lookups (Q1, Q6, Q14)
  - Complex joins (Q2, Q7, Q8, Q9)
  - Inference requirements (Q1, Q4, Q5, Q10, Q11, Q12, Q13)
  - Large result sets (Q3, Q4)
  - Property paths and transitivity (Q11)

  ## Usage

      # Get all queries
      queries = TripleStore.Benchmark.LUBMQueries.all()

      # Get a specific query
      {:ok, query} = TripleStore.Benchmark.LUBMQueries.get(:q1)

      # Get parameterized query
      {:ok, query} = TripleStore.Benchmark.LUBMQueries.get(:q1, uni: 1, dept: 0)

  ## Query Descriptions

  | Query | Description | Complexity |
  |-------|-------------|------------|
  | Q1    | GraduateStudents taking specific course | Simple, inference |
  | Q2    | GraduateStudents in specific department | Join, inference |
  | Q3    | Publications of specific faculty | Simple lookup |
  | Q4    | Professors in specific department | Inference |
  | Q5    | Members of specific department | Inference |
  | Q6    | All students | Large result |
  | Q7    | Students and courses taught by faculty | Complex join |
  | Q8    | Students and their departments | Join |
  | Q9    | Faculty, students, and courses | Complex join |
  | Q10   | Students taking course by advisor | Inference |
  | Q11   | Research groups in suborganization | Transitivity |
  | Q12   | Department heads | Inference |
  | Q13   | Alumni of university | Inference |
  | Q14   | Undergraduate students | Simple |

  """

  @lubm_ns "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#"
  @rdf_ns "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

  @type query_id :: :q1 | :q2 | :q3 | :q4 | :q5 | :q6 | :q7 | :q8 | :q9 | :q10 | :q11 | :q12 | :q13 | :q14
  @type query_params :: keyword()
  @type query_template :: %{
          id: query_id(),
          name: String.t(),
          description: String.t(),
          sparql: String.t(),
          params: [atom()],
          complexity: :simple | :medium | :complex,
          requires_inference: boolean(),
          expected_result_factor: float() | :varies
        }

  @doc """
  Returns all LUBM query templates.
  """
  @spec all() :: [query_template()]
  def all do
    [
      query_1(),
      query_2(),
      query_3(),
      query_4(),
      query_5(),
      query_6(),
      query_7(),
      query_8(),
      query_9(),
      query_10(),
      query_11(),
      query_12(),
      query_13(),
      query_14()
    ]
  end

  @doc """
  Returns a specific query template by ID.
  """
  @spec get(query_id()) :: {:ok, query_template()} | {:error, :not_found}
  def get(id) when is_atom(id) do
    case Enum.find(all(), fn q -> q.id == id end) do
      nil -> {:error, :not_found}
      query -> {:ok, query}
    end
  end

  @doc """
  Returns a specific query with parameters substituted.

  ## Parameters

  Common parameters:
  - `:uni` - University ID (default: 1)
  - `:dept` - Department ID (default: 0)
  - `:course` - Course ID (default: 1)
  - `:faculty` - Faculty ID (default: 1)

  ## Examples

      {:ok, query} = LUBMQueries.get(:q1, uni: 1, dept: 0, course: 1)
      # Returns query with GraduateCourse1 at Department0.University1

  """
  @spec get(query_id(), query_params()) :: {:ok, query_template()} | {:error, :not_found}
  def get(id, params) when is_atom(id) and is_list(params) do
    case get(id) do
      {:ok, query} ->
        substituted_sparql = substitute_params(query.sparql, params)
        {:ok, %{query | sparql: substituted_sparql}}

      error ->
        error
    end
  end

  @doc """
  Returns the LUBM namespace.
  """
  @spec namespace() :: String.t()
  def namespace, do: @lubm_ns

  @doc """
  Estimates expected result count for a query given scale factor.

  The result count depends on the scale factor (number of universities)
  and the specific query. Some queries return fixed counts while others
  scale with data size.
  """
  @spec estimate_results(query_id(), pos_integer()) :: pos_integer() | :varies
  def estimate_results(id, scale) do
    case get(id) do
      {:ok, query} ->
        case query.expected_result_factor do
          :varies -> :varies
          factor when is_number(factor) -> trunc(factor * scale)
        end

      {:error, _} ->
        :varies
    end
  end

  # ===========================================================================
  # Query Definitions
  # ===========================================================================

  defp query_1 do
    %{
      id: :q1,
      name: "Q1: Graduate students taking course",
      description: "Find graduate students who take a specific course",
      sparql: """
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX ub: <#{@lubm_ns}>
      SELECT ?x
      WHERE {
        ?x rdf:type ub:GraduateStudent .
        ?x ub:takesCourse <http://www.Department{dept}.University{uni}.edu/GraduateCourse{course}> .
      }
      """,
      params: [:uni, :dept, :course],
      complexity: :simple,
      requires_inference: true,
      expected_result_factor: 4.0
    }
  end

  defp query_2 do
    %{
      id: :q2,
      name: "Q2: Graduate students and university",
      description: "Find graduate students in a department and their university",
      sparql: """
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX ub: <#{@lubm_ns}>
      SELECT ?x ?y ?z
      WHERE {
        ?x rdf:type ub:GraduateStudent .
        ?y rdf:type ub:University .
        ?z rdf:type ub:Department .
        ?x ub:memberOf ?z .
        ?z ub:subOrganizationOf ?y .
        ?x ub:undergraduateDegreeFrom ?y .
      }
      """,
      params: [],
      complexity: :complex,
      requires_inference: true,
      expected_result_factor: 0.0
    }
  end

  defp query_3 do
    %{
      id: :q3,
      name: "Q3: Publications by faculty",
      description: "Find publications authored by a specific faculty member",
      sparql: """
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX ub: <#{@lubm_ns}>
      SELECT ?x
      WHERE {
        ?x rdf:type ub:Publication .
        ?x ub:publicationAuthor <http://www.Department{dept}.University{uni}.edu/AssistantProfessor{faculty}> .
      }
      """,
      params: [:uni, :dept, :faculty],
      complexity: :simple,
      requires_inference: false,
      expected_result_factor: 7.5
    }
  end

  defp query_4 do
    %{
      id: :q4,
      name: "Q4: Professors in department",
      description: "Find all professors in a specific department with their details",
      sparql: """
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX ub: <#{@lubm_ns}>
      SELECT ?x ?y1 ?y2 ?y3
      WHERE {
        ?x rdf:type ub:Professor .
        ?x ub:worksFor <http://www.Department{dept}.University{uni}.edu> .
        ?x ub:name ?y1 .
        ?x ub:emailAddress ?y2 .
        ?x ub:telephone ?y3 .
      }
      """,
      params: [:uni, :dept],
      complexity: :medium,
      requires_inference: true,
      expected_result_factor: 15.0
    }
  end

  defp query_5 do
    %{
      id: :q5,
      name: "Q5: Members of department",
      description: "Find all persons who are members of a specific department",
      sparql: """
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX ub: <#{@lubm_ns}>
      SELECT ?x
      WHERE {
        ?x rdf:type ub:Person .
        ?x ub:memberOf <http://www.Department{dept}.University{uni}.edu> .
      }
      """,
      params: [:uni, :dept],
      complexity: :medium,
      requires_inference: true,
      expected_result_factor: 180.0
    }
  end

  defp query_6 do
    %{
      id: :q6,
      name: "Q6: All students",
      description: "Find all students in the dataset",
      sparql: """
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX ub: <#{@lubm_ns}>
      SELECT ?x
      WHERE {
        ?x rdf:type ub:Student .
      }
      """,
      params: [],
      complexity: :simple,
      requires_inference: true,
      expected_result_factor: 5000.0
    }
  end

  defp query_7 do
    %{
      id: :q7,
      name: "Q7: Students and courses by faculty",
      description: "Find students taking courses taught by a specific faculty",
      sparql: """
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX ub: <#{@lubm_ns}>
      SELECT ?x ?y
      WHERE {
        ?x rdf:type ub:Student .
        ?y rdf:type ub:Course .
        <http://www.Department{dept}.University{uni}.edu/AssociateProfessor{faculty}> ub:teacherOf ?y .
        ?x ub:takesCourse ?y .
      }
      """,
      params: [:uni, :dept, :faculty],
      complexity: :complex,
      requires_inference: true,
      expected_result_factor: 20.0
    }
  end

  defp query_8 do
    %{
      id: :q8,
      name: "Q8: Students and departments",
      description: "Find students, their departments, and emails",
      sparql: """
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX ub: <#{@lubm_ns}>
      SELECT ?x ?y ?z
      WHERE {
        ?x rdf:type ub:Student .
        ?y rdf:type ub:Department .
        ?x ub:memberOf ?y .
        ?y ub:subOrganizationOf <http://www.University{uni}.edu> .
        ?x ub:emailAddress ?z .
      }
      """,
      params: [:uni],
      complexity: :medium,
      requires_inference: true,
      expected_result_factor: 3500.0
    }
  end

  defp query_9 do
    %{
      id: :q9,
      name: "Q9: Faculty, students, courses",
      description: "Find faculty teaching courses taken by students with advisors",
      sparql: """
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX ub: <#{@lubm_ns}>
      SELECT ?x ?y ?z
      WHERE {
        ?x rdf:type ub:Student .
        ?y rdf:type ub:Faculty .
        ?z rdf:type ub:Course .
        ?x ub:advisor ?y .
        ?y ub:teacherOf ?z .
        ?x ub:takesCourse ?z .
      }
      """,
      params: [],
      complexity: :complex,
      requires_inference: true,
      expected_result_factor: 15.0
    }
  end

  defp query_10 do
    %{
      id: :q10,
      name: "Q10: Students taking course by advisor",
      description: "Find students who take a course taught by their advisor",
      sparql: """
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX ub: <#{@lubm_ns}>
      SELECT ?x
      WHERE {
        ?x rdf:type ub:Student .
        ?x ub:takesCourse <http://www.Department{dept}.University{uni}.edu/GraduateCourse{course}> .
      }
      """,
      params: [:uni, :dept, :course],
      complexity: :simple,
      requires_inference: true,
      expected_result_factor: 4.0
    }
  end

  defp query_11 do
    %{
      id: :q11,
      name: "Q11: Research groups in suborganization",
      description: "Find research groups that are suborganizations of a university",
      sparql: """
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX ub: <#{@lubm_ns}>
      SELECT ?x
      WHERE {
        ?x rdf:type ub:ResearchGroup .
        ?x ub:subOrganizationOf <http://www.University{uni}.edu> .
      }
      """,
      params: [:uni],
      complexity: :medium,
      requires_inference: true,
      expected_result_factor: 40.0
    }
  end

  defp query_12 do
    %{
      id: :q12,
      name: "Q12: Department heads",
      description: "Find professors who head departments",
      sparql: """
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX ub: <#{@lubm_ns}>
      SELECT ?x ?y
      WHERE {
        ?x rdf:type ub:Chair .
        ?y rdf:type ub:Department .
        ?x ub:worksFor ?y .
        ?y ub:subOrganizationOf <http://www.University{uni}.edu> .
      }
      """,
      params: [:uni],
      complexity: :medium,
      requires_inference: true,
      expected_result_factor: 0.0
    }
  end

  defp query_13 do
    %{
      id: :q13,
      name: "Q13: Alumni of university",
      description: "Find alumni who have degrees from a specific university",
      sparql: """
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX ub: <#{@lubm_ns}>
      SELECT ?x
      WHERE {
        ?x rdf:type ub:Person .
        ?x ub:hasAlumnus <http://www.University{uni}.edu> .
      }
      """,
      params: [:uni],
      complexity: :simple,
      requires_inference: true,
      expected_result_factor: 0.0
    }
  end

  defp query_14 do
    %{
      id: :q14,
      name: "Q14: Undergraduate students",
      description: "Find all undergraduate students",
      sparql: """
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX ub: <#{@lubm_ns}>
      SELECT ?x
      WHERE {
        ?x rdf:type ub:UndergraduateStudent .
      }
      """,
      params: [],
      complexity: :simple,
      requires_inference: false,
      expected_result_factor: 3000.0
    }
  end

  # ===========================================================================
  # Parameter Substitution
  # ===========================================================================

  defp substitute_params(sparql, params) do
    defaults = [uni: 1, dept: 0, course: 1, faculty: 1]
    merged = Keyword.merge(defaults, params)

    Enum.reduce(merged, sparql, fn {key, value}, acc ->
      String.replace(acc, "{#{key}}", to_string(value))
    end)
  end
end
