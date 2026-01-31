defmodule TripleStore.Benchmark.GMarkQueries do
  @moduledoc """
  gMark query templates for the Bib (bibliographical) scenario.

  Implements SPARQL queries organized by selectivity class as defined
  in the gMark research paper (Bagan et al., 2015).

  ## Selectivity Classes

  gMark categorizes queries by their asymptotic result growth with graph size:

  | Selectivity | α Value | Description | Example |
  |-------------|---------|-------------|---------|
  | Constant | α ≈ 0 | Results don't grow with graph size | Cities with conferences |
  | Linear | α ≈ 1 | Results grow proportionally to nodes | Papers by author |
  | Quadratic | α ≈ 2 | Results grow quadratically | Transitive closure of co-authorship |

  ## Usage

      # Get all queries
      queries = TripleStore.Benchmark.GMarkQueries.all()

      # Get a specific query
      {:ok, query} = TripleStore.Benchmark.GMarkQueries.get(:c1)

      # Get queries by selectivity class
      constant_queries = TripleStore.Benchmark.GMarkQueries.by_selectivity(:constant)

      # Get parameterized query
      {:ok, query} = TripleStore.Benchmark.GMarkQueries.get(:l1, researcher: "Researcher1")

  ## Query Descriptions

  ### Constant Queries (α ≈ 0)
  - C1: Cities hosting conferences (fixed number of cities)
  - C2: Journal publishers (fixed relative to journals)
  - C3: Research affiliations (limited set)

  ### Linear Queries (α ≈ 1)
  - L1: Papers by a researcher (proportional to papers)
  - L2: Papers in a conference (proportional to papers)
  - L3: Papers extended to a journal (proportional to papers)
  - L4: Researchers with papers in a specific year
  - L5: Conferences by year

  ### Quadratic Queries (α ≈ 2)
  - Q1: Co-authorship pairs (Cartesian-like product)
  - Q2: Papers published in same conference
  - Q3: Researcher co-authorship transitive closure (recursive)

  """

  @gmark "http://gmark.example.org/"
  @xsd "http://www.w3.org/2001/XMLSchema#"
  @foaf "http://xmlns.com/foaf/0.1/"
  @dc "http://purl.org/dc/elements/1.1/"
  @dcterms "http://purl.org/dc/terms/"
  @prism "http://prismstandard.org/namespaces/basic/2.0/"

  @type query_id :: :c1 | :c2 | :c3 | :l1 | :l2 | :l3 | :l4 | :l5 | :q1 | :q2 | :q3
  @type query_params :: keyword()
  @type selectivity_class :: :constant | :linear | :quadratic
  @type query_template :: %{
          id: query_id(),
          name: String.t(),
          description: String.t(),
          sparql: String.t(),
          params: [atom()],
          selectivity: selectivity_class(),
          complexity: :simple | :medium | :complex
        }

  @doc """
  Returns all gMark query templates.
  """
  @spec all() :: [query_template()]
  def all do
    [
      # Constant queries
      query_c1(),
      query_c2(),
      query_c3(),
      # Linear queries
      query_l1(),
      query_l2(),
      query_l3(),
      query_l4(),
      query_l5(),
      # Quadratic queries
      query_q1(),
      query_q2(),
      query_q3()
    ]
  end

  @doc """
  Returns all queries in a specific selectivity class.
  """
  @spec by_selectivity(selectivity_class()) :: [query_template()]
  def by_selectivity(selectivity) do
    Enum.filter(all(), fn q -> q.selectivity == selectivity end)
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
  - `:researcher` - Researcher ID (e.g., "Researcher1")
  - `:conference` - Conference ID (e.g., "Conference1")
  - `:journal` - Journal ID (e.g., "Journal1")
  - `:city` - City ID (e.g., "City1")
  - `:year` - Publication year (e.g., "2020")

  ## Examples

      {:ok, query} = GMarkQueries.get(:l1, researcher: "Researcher42")

  If no parameters are provided, default values are used (e.g., "Researcher1", "Conference1").

  """
  @spec get(query_id(), query_params()) :: {:ok, query_template()} | {:error, :not_found}
  def get(id, params) when is_atom(id) and is_list(params) do
    case get(id) do
      {:ok, query} ->
        # Provide default values for common parameters when not specified
        params_with_defaults = add_default_params(params, query.params)
        substituted_sparql = substitute_params(query.sparql, params_with_defaults)
        {:ok, %{query | sparql: substituted_sparql}}

      error ->
        error
    end
  end

  # Add default parameter values for benchmarking
  defp add_default_params(params, required_params) do
    defaults = %{
      researcher: "1",
      conference: "1",
      journal: "1",
      city: "1",
      year: "2020"
    }

    Enum.reduce(required_params, params, fn param, acc ->
      if Keyword.has_key?(acc, param) do
        acc
      else
        Keyword.put(acc, param, Map.get(defaults, param))
      end
    end)
  end

  @doc """
  Returns the gMark vocabulary namespace.
  """
  @spec namespace() :: String.t()
  def namespace, do: @gmark

  # ===========================================================================
  # Constant Queries (α ≈ 0)
  # Results don't grow with graph size
  # ===========================================================================

  defp query_c1 do
    %{
      id: :c1,
      name: "C1: Cities hosting conferences",
      description: "Find cities that host conferences (constant because cities are fixed)",
      sparql: """
      PREFIX gmark: <#{@gmark}>

      SELECT DISTINCT ?city ?cityId WHERE {
        ?conference a gmark:Conference .
        ?conference gmark:heldIn ?city .
        ?city a gmark:City .
        ?city gmark:cityId ?cityId .
      }
      ORDER BY ?cityId
      """,
      params: [],
      selectivity: :constant,
      complexity: :simple
    }
  end

  defp query_c2 do
    %{
      id: :c2,
      name: "C2: Journal publishers",
      description: "Find journal publishers (constant relative to journal count)",
      sparql: """
      PREFIX gmark: <#{@gmark}>
      PREFIX prism: <#{@prism}>

      SELECT DISTINCT ?journal ?publisher WHERE {
        ?journal a gmark:Journal .
        ?journal gmark:publisher ?publisher .
      }
      ORDER BY ?journal
      """,
      params: [],
      selectivity: :constant,
      complexity: :simple
    }
  end

  defp query_c3 do
    %{
      id: :c3,
      name: "C3: Research affiliations",
      description: "Find unique research affiliations (limited set)",
      sparql: """
      PREFIX gmark: <#{@gmark}>
      PREFIX foaf: <#{@foaf}>

      SELECT DISTINCT ?affiliation WHERE {
        ?researcher a gmark:Researcher .
        ?researcher gmark:affiliation ?affiliation .
      }
      ORDER BY ?affiliation
      """,
      params: [],
      selectivity: :constant,
      complexity: :simple
    }
  end

  # ===========================================================================
  # Linear Queries (α ≈ 1)
  # Results grow proportionally to graph size
  # ===========================================================================

  defp query_l1 do
    %{
      id: :l1,
      name: "L1: Papers by researcher",
      description: "Find all papers authored by a specific researcher",
      sparql: """
      PREFIX gmark: <#{@gmark}>
      PREFIX dc: <#{@dc}>

      SELECT ?paper ?title WHERE {
        ?paper a gmark:Paper .
        ?paper gmark:authors <%researcher%> .
        ?paper dc:title ?title .
      }
      ORDER BY ?paper
      """,
      params: [:researcher],
      selectivity: :linear,
      complexity: :simple
    }
  end

  defp query_l2 do
    %{
      id: :l2,
      name: "L2: Papers in conference",
      description: "Find all papers published in a specific conference",
      sparql: """
      PREFIX gmark: <#{@gmark}>
      PREFIX dc: <#{@dc}>

      SELECT ?paper ?title WHERE {
        ?paper a gmark:Paper .
        ?paper gmark:publishedIn <%conference%> .
        ?paper dc:title ?title .
      }
      ORDER BY ?paper
      """,
      params: [:conference],
      selectivity: :linear,
      complexity: :simple
    }
  end

  defp query_l3 do
    %{
      id: :l3,
      name: "L3: Papers extended to journal",
      description: "Find all papers extended to a specific journal",
      sparql: """
      PREFIX gmark: <#{@gmark}>
      PREFIX dc: <#{@dc}>

      SELECT ?paper ?title WHERE {
        ?paper a gmark:Paper .
        ?paper gmark:extendedTo <%journal%> .
        ?paper dc:title ?title .
      }
      ORDER BY ?paper
      """,
      params: [:journal],
      selectivity: :linear,
      complexity: :simple
    }
  end

  defp query_l4 do
    %{
      id: :l4,
      name: "L4: Researcher papers by year",
      description: "Find papers by a researcher in a specific year",
      sparql: """
      PREFIX gmark: <#{@gmark}>
      PREFIX dc: <#{@dc}>
      PREFIX dcterms: <#{@dcterms}>
      PREFIX xsd: <#{@xsd}>

      SELECT ?paper ?title WHERE {
        ?paper a gmark:Paper .
        ?paper gmark:authors <%researcher%> .
        ?paper dcterms:issued <%year%>^^xsd:gYear .
        ?paper dc:title ?title .
      }
      ORDER BY ?paper
      """,
      params: [:researcher, :year],
      selectivity: :linear,
      complexity: :medium
    }
  end

  defp query_l5 do
    %{
      id: :l5,
      name: "L5: Conferences by year",
      description: "Find conferences held in a specific year",
      sparql: """
      PREFIX gmark: <#{@gmark}>
      PREFIX dcterms: <#{@dcterms}>
      PREFIX xsd: <#{@xsd}>

      SELECT ?conference ?name WHERE {
        ?conference a gmark:Conference .
        ?conference dcterms:issued <%year%>^^xsd:gYear .
        ?conference gmark:name ?name .
      }
      ORDER BY ?conference
      """,
      params: [:year],
      selectivity: :linear,
      complexity: :simple
    }
  end

  # ===========================================================================
  # Quadratic Queries (α ≈ 2)
  # Results grow quadratically with graph size (Cartesian products)
  # ===========================================================================

  defp query_q1 do
    %{
      id: :q1,
      name: "Q1: Co-authorship pairs",
      description: "Find pairs of researchers who co-authored papers",
      sparql: """
      PREFIX gmark: <#{@gmark}>

      SELECT DISTINCT ?researcher1 ?researcher2 WHERE {
        ?paper a gmark:Paper .
        ?paper gmark:authors ?researcher1 .
        ?paper gmark:authors ?researcher2 .
        FILTER(?researcher1 < ?researcher2)
      }
      ORDER BY ?researcher1 ?researcher2
      """,
      params: [],
      selectivity: :quadratic,
      complexity: :medium
    }
  end

  defp query_q2 do
    %{
      id: :q2,
      name: "Q2: Papers in same conference",
      description: "Find pairs of papers published in the same conference",
      sparql: """
      PREFIX gmark: <#{@gmark}>

      SELECT DISTINCT ?paper1 ?paper2 ?conference WHERE {
        ?paper1 a gmark:Paper .
        ?paper1 gmark:publishedIn ?conference .
        ?paper2 a gmark:Paper .
        ?paper2 gmark:publishedIn ?conference .
        FILTER(?paper1 < ?paper2)
      }
      ORDER BY ?paper1 ?paper2
      """,
      params: [],
      selectivity: :quadratic,
      complexity: :medium
    }
  end

  defp query_q3 do
    %{
      id: :q3,
      name: "Q3: Co-authorship transitive closure",
      description: "Find all researchers connected via co-authorship paths (recursive)",
      sparql: """
      PREFIX gmark: <#{@gmark}>

      SELECT DISTINCT ?researcher1 ?researcher2 WHERE {
        ?researcher1 a gmark:Researcher .
        ?researcher2 a gmark:Researcher .
        ?researcher1 (^(gmark:authors/gmark:Paper/gmark:authors))+ ?researcher2 .
        FILTER(?researcher1 != ?researcher2)
      }
      ORDER BY ?researcher1 ?researcher2
      """,
      params: [],
      selectivity: :quadratic,
      complexity: :complex
    }
  end

  # ===========================================================================
  # Parameter Substitution
  # ===========================================================================

  defp substitute_params(sparql, params) do
    # Parameters that are literals (quoted) vs IRIs (angle bracketed)
    literal_params = [:year]

    Enum.reduce(params, sparql, fn {key, value}, acc ->
      placeholder = "<%#{key}%>"

      substituted_value =
        cond do
          # Literal parameters (year) - wrap in quotes
          key in literal_params ->
            "\"#{value}\""

          # Full URIs provided - just wrap in angle brackets
          String.starts_with?(value, ["http://", "https://"]) ->
            "<#{value}>"

          # Value already contains the capitalized type name (e.g., "Researcher42")
          # Check if value starts with the capitalized key name
          value =~ ~r/^#{String.capitalize(to_string(key))}\d+/ ->
            "<http://gmark.example.org/#{value}>"

          # Entity IRIs (researcher, conference, journal, city) - construct IRI
          true ->
            iri = "http://gmark.example.org/#{String.capitalize(to_string(key))}#{value}"
            "<#{iri}>"
        end

      String.replace(acc, placeholder, substituted_value)
    end)
  end
end
