defmodule TripleStore.Benchmark.GMark do
  @moduledoc """
  gMark: Schema-driven generation of graphs and queries.

  This is a simplified Elixir implementation of the gMark benchmark
  (based on the research paper by Bagan et al., 2015).

  ## Default Scenario: "Bib" (Bibliographical Database)

  The default schema simulates a bibliographical database with:
  - Researchers (authors of papers)
  - Papers (research publications)
  - Journals (academic journals)
  - Conferences (conference venues)
  - Cities (conference locations)

  ## Scale Factor

  The scale factor determines the number of researchers.
  gMark scale factor 1 generates approximately 10,000 triples.

  - Scale 1: ~10K triples (1000 researchers)
  - Scale 10: ~100K triples (10000 researchers)
  - Scale 100: ~1M triples (100000 researchers)

  ## Usage

      # Generate data for scale factor 1 (~10K triples)
      graph = TripleStore.Benchmark.GMark.generate(1)

      # Generate with seed for reproducibility
      graph = TripleStore.Benchmark.GMark.generate(1, seed: 12345)

      # Generate as stream for large datasets
      stream = TripleStore.Benchmark.GMark.stream(10)

  ## Degree Distributions

  gMark uses three types of degree distributions for edge generation:
  - **Uniform**: Random within [min, max] range
  - **Gaussian**: Normal distribution with μ (mean) and σ (stddev)
  - **Zipfian**: Power-law distribution (few hubs, many low-degree nodes)

  ## Schema Constraints (Bib scenario)

  From the gMark paper:

  | Source Type | Predicate      | Target Type | In-Distribution | Out-Distribution |
  |-------------|----------------|-------------|-----------------|------------------|
  | Researcher  | (inverse auth) | Paper       | Gaussian        | Zipfian          |
  | Paper       | authors        | Researcher  | Gaussian        | Zipfian (inverse)|
  | Paper       | publishedIn    | Conference  | Gaussian        | Uniform [1,1]    |
  | Paper       | extendedTo     | Journal     | Gaussian        | Uniform [0,1]    |
  | Conference  | heldIn         | City        | Zipfian         | Uniform [1,1]    |

  ## Node Type Distribution

  - Researcher: 50% of nodes (proportional to scale)
  - Paper: 30% of nodes (proportional to scale)
  - Journal: 10% of nodes (proportional to scale)
  - Conference: 10% of nodes (proportional to scale)
  - City: Fixed count (100, doesn't scale)

  """

  # gMark namespaces
  @gmark "http://gmark.example.org/"
  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @xsd "http://www.w3.org/2001/XMLSchema#"
  @foaf "http://xmlns.com/foaf/0.1/"
  @dc "http://purl.org/dc/elements/1.1/"
  @dcterms "http://purl.org/dc/terms/"
  @prism "http://prismstandard.org/namespaces/basic/2.0/"

  # Node type proportions at scale factor 1
  # Total nodes at scale 1 = 1000 + 600 + 200 + 200 + 100 = 2100
  @researchers_scale1 1000  # 50% of nodes
  @papers_scale1 600        # 30% of nodes
  @journals_scale1 200      # 10% of nodes
  @conferences_scale1 200   # 10% of nodes
  @cities 100               # Fixed count (doesn't scale)

  # Average attributes per entity type
  @researcher_attrs 3       # name, homepage, email
  @paper_attrs 5            # title, abstract, year, pages, keywords
  @journal_attrs 3          # name, publisher, issn
  @conference_attrs 3       # name, year, location

  # Average edges per entity
  @avg_authors_per_paper 2.5
  @journal_extension_rate 0.5  # 50% of papers extended to journal

  @typedoc "Generator options"
  @type opts :: [
          seed: integer(),
          stream: boolean()
        ]

  @doc """
  Generates gMark benchmark data as an RDF.Graph.

  ## Arguments

  - `scale_factor` - Scale factor (1 = ~10K triples)

  ## Options

  - `:seed` - Random seed for reproducible generation (default: based on scale_factor)

  ## Returns

  An `RDF.Graph` containing the generated triples.

  ## Examples

      graph = TripleStore.Benchmark.GMark.generate(1)
      RDF.Graph.triple_count(graph)
      # => ~10000

  """
  @spec generate(pos_integer(), opts()) :: RDF.Graph.t()
  def generate(scale_factor, opts \\ []) when scale_factor > 0 do
    seed = Keyword.get(opts, :seed, scale_factor * 42)
    :rand.seed(:exsss, {seed, seed * 2, seed * 3})

    state = init_state(scale_factor)
    triples = generate_all(state)

    RDF.Graph.new(triples)
  end

  @doc """
  Generates gMark benchmark data as a stream of triples.

  Useful for large scale factors where holding all triples in memory
  is not feasible.

  ## Arguments

  - `scale_factor` - Scale factor

  ## Options

  - `:seed` - Random seed for reproducible generation

  ## Returns

  A stream of `{subject, predicate, object}` triples.

  ## Examples

      stream = TripleStore.Benchmark.GMark.stream(10)
      Enum.take(stream, 1000)

  """
  @spec stream(pos_integer(), opts()) :: Enumerable.t()
  def stream(scale_factor, opts \\ []) when scale_factor > 0 do
    seed = Keyword.get(opts, :seed, scale_factor * 42)

    Stream.resource(
      fn ->
        :rand.seed(:exsss, {seed, seed * 2, seed * 3})
        state = init_state(scale_factor)
        {:cities, state, 1}
      end,
      fn
        {:cities, state, n} when n <= state.num_cities ->
          triples = generate_city(n, state)
          {triples, {:cities, state, n + 1}}

        {:cities, state, _} ->
          {[], {:journals, state, 1}}

        {:journals, state, n} when n <= state.num_journals ->
          triples = generate_journal(n, state)
          {triples, {:journals, state, n + 1}}

        {:journals, state, _} ->
          {[], {:conferences, state, 1}}

        {:conferences, state, n} when n <= state.num_conferences ->
          triples = generate_conference(n, state)
          {triples, {:conferences, state, n + 1}}

        {:conferences, state, _} ->
          {[], {:researchers, state, 1}}

        {:researchers, state, n} when n <= state.num_researchers ->
          triples = generate_researcher(n, state)
          {triples, {:researchers, state, n + 1}}

        {:researchers, state, _} ->
          {[], {:papers, state, 1}}

        {:papers, state, n} when n <= state.num_papers ->
          triples = generate_paper(n, state)
          {triples, {:papers, state, n + 1}}

        {:papers, _state, _} ->
          {:halt, nil}
      end,
      fn _ -> :ok end
    )
  end

  @doc """
  Returns the estimated triple count for a given scale factor.

  ## Examples

      TripleStore.Benchmark.GMark.estimate_triple_count(1)
      # => ~10000

  """
  @spec estimate_triple_count(pos_integer()) :: pos_integer()
  def estimate_triple_count(scale_factor) do
    # Node attribute triples
    researcher_triples = state_num_researchers(scale_factor) * @researcher_attrs
    paper_triples = state_num_papers(scale_factor) * @paper_attrs
    journal_triples = state_num_journals(scale_factor) * @journal_attrs
    conference_triples = state_num_conferences(scale_factor) * @conference_attrs
    city_triples = @cities * 2

    # Edge triples
    authors_edges = trunc(state_num_papers(scale_factor) * @avg_authors_per_paper)
    published_in_edges = state_num_papers(scale_factor)
    extended_to_edges = trunc(state_num_papers(scale_factor) * @journal_extension_rate)
    held_in_edges = state_num_conferences(scale_factor)

    # Type triples
    type_triples =
      state_num_researchers(scale_factor) + state_num_papers(scale_factor) +
        state_num_journals(scale_factor) + state_num_conferences(scale_factor) + @cities

    researcher_triples + paper_triples + journal_triples + conference_triples +
      city_triples + authors_edges + published_in_edges + extended_to_edges +
      held_in_edges + type_triples
  end

  @doc """
  Returns the gMark vocabulary namespace.
  """
  @spec namespace() :: String.t()
  def namespace, do: @gmark

  # ===========================================================================
  # Private: State Initialization
  # ===========================================================================

  defp init_state(scale_factor) do
    %{
      scale_factor: scale_factor,
      num_researchers: state_num_researchers(scale_factor),
      num_papers: state_num_papers(scale_factor),
      num_journals: state_num_journals(scale_factor),
      num_conferences: state_num_conferences(scale_factor),
      num_cities: @cities,
      # Track paper IDs for authorship edges
      paper_authors_ets: :ets.new(:paper_authors, [:bag])
    }
  end

  defp state_num_researchers(scale), do: @researchers_scale1 * scale
  defp state_num_papers(scale), do: @papers_scale1 * scale
  defp state_num_journals(scale), do: @journals_scale1 * scale
  defp state_num_conferences(scale), do: @conferences_scale1 * scale

  # ===========================================================================
  # Private: Generation Functions
  # ===========================================================================

  defp generate_all(state) do
    # Generate all entities
    city_triples = Enum.flat_map(1..state.num_cities, &generate_city(&1, state))
    journal_triples = Enum.flat_map(1..state.num_journals, &generate_journal(&1, state))
    conference_triples = Enum.flat_map(1..state.num_conferences, &generate_conference(&1, state))
    researcher_triples = Enum.flat_map(1..state.num_researchers, &generate_researcher(&1, state))
    paper_triples = Enum.flat_map(1..state.num_papers, &generate_paper(&1, state))

    # Generate edge triples (authorship relationships)
    authors_edges = generate_authors_edges(state)

    city_triples ++ journal_triples ++ conference_triples ++
      researcher_triples ++ paper_triples ++ authors_edges
  end

  # ===========================================================================
  # City Entities (Fixed count, doesn't scale)
  # ===========================================================================

  defp generate_city(city_id, _state) do
    city_uri = city_uri(city_id)

    base = [
      {city_uri, rdf_type(), gmark("City")},
      {city_uri, gmark("cityId"), RDF.literal(city_id)}
    ]

    # Add optional attributes probabilistically
    attrs =
      [
        maybe_add(city_uri, gmark("name"), RDF.literal("City#{city_id}"), 0.9),
        maybe_add(city_uri, gmark("country"), RDF.literal("Country#{rem(city_id, 10) + 1}"), 0.8)
      ]
      |> Enum.filter(&(&1 != nil))

    base ++ attrs
  end

  # ===========================================================================
  # Journal Entities (Scale with scale factor)
  # ===========================================================================

  defp generate_journal(journal_id, _state) do
    journal_uri = journal_uri(journal_id)

    base = [
      {journal_uri, rdf_type(), gmark("Journal")},
      {journal_uri, gmark("journalId"), RDF.literal(journal_id)}
    ]

    # Add attributes with realistic probabilities
    attrs =
      [
        maybe_add(journal_uri, gmark("name"), RDF.literal("Journal#{journal_id}"), 0.95),
        maybe_add(journal_uri, gmark("publisher"), RDF.literal("Publisher#{rem(journal_id, 20) + 1}"), 0.9),
        maybe_add(journal_uri, prism("issn"), generate_issn(journal_id), 0.8),
        maybe_add(journal_uri, dcterms("issued"), RDF.literal(random_year(1980, 2024), datatype: xsd("gYear")), 0.7)
      ]
      |> Enum.filter(&(&1 != nil))

    base ++ attrs
  end

  # ===========================================================================
  # Conference Entities (Scale with scale factor)
  # ===========================================================================

  defp generate_conference(conf_id, state) do
    conf_uri = conference_uri(conf_id)
    city_id = rem(conf_id - 1, state.num_cities) + 1

    base = [
      {conf_uri, rdf_type(), gmark("Conference")},
      {conf_uri, gmark("conferenceId"), RDF.literal(conf_id)},
      {conf_uri, gmark("heldIn"), city_uri(city_id)}
    ]

    # Add attributes with realistic probabilities
    attrs =
      [
        maybe_add(conf_uri, gmark("name"), RDF.literal("Conference#{conf_id}"), 0.95),
        maybe_add(conf_uri, dcterms("issued"), RDF.literal(random_year(2010, 2024), datatype: xsd("gYear")), 0.9),
        maybe_add(conf_uri, gmark("edition"), RDF.literal(rem(conf_id, 15) + 1), 0.7)
      ]
      |> Enum.filter(&(&1 != nil))

    base ++ attrs
  end

  # ===========================================================================
  # Researcher Entities (Scale with scale factor)
  # ===========================================================================

  defp generate_researcher(researcher_id, _state) do
    researcher_uri = researcher_uri(researcher_id)

    base = [
      {researcher_uri, rdf_type(), gmark("Researcher")},
      {researcher_uri, gmark("researcherId"), RDF.literal(researcher_id)}
    ]

    # Add attributes with realistic probabilities
    attrs =
      [
        maybe_add(researcher_uri, foaf("name"), RDF.literal("Researcher#{researcher_id}"), 0.95),
        maybe_add(researcher_uri, foaf("homepage"), RDF.literal("http://example.org/~r#{researcher_id}"), 0.6),
        maybe_add(researcher_uri, foaf("mbox"), RDF.literal("r#{researcher_id}@example.org"), 0.7),
        maybe_add(researcher_uri, gmark("affiliation"), RDF.literal("University#{rem(researcher_id, 50) + 1}"), 0.5)
      ]
      |> Enum.filter(&(&1 != nil))

    base ++ attrs
  end

  # ===========================================================================
  # Paper Entities (Scale with scale factor)
  # ===========================================================================

  defp generate_paper(paper_id, state) do
    paper_uri = paper_uri(paper_id)
    conference_id = rem(paper_id - 1, state.num_conferences) + 1

    base = [
      {paper_uri, rdf_type(), gmark("Paper")},
      {paper_uri, gmark("paperId"), RDF.literal(paper_id)},
      {paper_uri, gmark("publishedIn"), conference_uri(conference_id)}
    ]

    # Generate authorship edges (Zipfian-like distribution)
    # Some papers have many authors, most have few
    num_authors = zipfian_sample(6, 2.5) # 1-6 authors, s=2.5 for zipfian
    author_ids = sample_researchers(state.num_researchers, num_authors)

    authors_edges =
      for author_id <- author_ids do
        {paper_uri, gmark("authors"), researcher_uri(author_id)}
      end

    # Add optional journal extension (50% of papers)
    journal_edges =
      if :rand.uniform() < @journal_extension_rate do
        journal_id = rem(paper_id - 1, state.num_journals) + 1
        [{paper_uri, gmark("extendedTo"), journal_uri(journal_id)}]
      else
        []
      end

    # Add attributes with realistic probabilities
    attrs =
      [
        maybe_add(paper_uri, dc("title"), RDF.literal("Paper#{paper_id} Title"), 0.95),
        maybe_add(paper_uri, dcterms("abstract"), RDF.literal("Abstract for paper #{paper_id}"), 0.7),
        maybe_add(paper_uri, dcterms("issued"), RDF.literal(random_year(2015, 2024), datatype: xsd("gYear")), 0.9),
        maybe_add(paper_uri, gmark("pages"), RDF.literal("#{random_int(1, 50)}-#{random_int(51, 500)}"), 0.6),
        maybe_add(paper_uri, dc("subject"), RDF.literal("Keyword#{rem(paper_id, 30) + 1}"), 0.5)
      ]
      |> Enum.filter(&(&1 != nil))

    base ++ attrs ++ authors_edges ++ journal_edges
  end

  # ===========================================================================
  # Edge Generation Functions
  # ===========================================================================

  defp generate_authors_edges(_state) do
    # Authorship edges are generated inline with papers
    # This function is kept for consistency with the pattern
    []
  end

  # ===========================================================================
  # Degree Distribution Helpers
  # ===========================================================================

  # Zipfian distribution sample (power law)
  # Returns a value in [1, n] with parameter s
  defp zipfian_sample(n, s) when is_integer(n) and n > 0 and is_number(s) do
    u = :rand.uniform()
    k = :math.pow(u, -1 / (s - 1))
    min(trunc(k), n)
  end

  # Sample unique researcher IDs
  defp sample_researchers(max_researchers, count) do
    1..max_researchers
    |> Enum.shuffle()
    |> Enum.take(count)
  end

  # ===========================================================================
  # Utility Functions
  # ===========================================================================

  # Conditionally add a triple based on probability
  defp maybe_add(subject, predicate, object, probability) do
    if :rand.uniform() < probability do
      {subject, predicate, object}
    else
      nil
    end
  end

  # Random integer in range
  defp random_int(min, max), do: :rand.uniform(max - min + 1) + min - 1

  # Random year in range
  defp random_year(min, max), do: random_int(min, max)

  # Generate fake ISSN
  defp generate_issn(journal_id) do
    checksum = rem(journal_id * 7 + 3, 10)
    RDF.literal("#{String.pad_leading(to_string(journal_id), 4, "0")}-#{checksum}7")
  end

  # ===========================================================================
  # URI Constructors
  # ===========================================================================

  defp gmark(local), do: RDF.iri(@gmark <> local)
  defp rdf_type, do: RDF.iri(@rdf <> "type")

  defp city_uri(id), do: gmark("City#{id}")
  defp journal_uri(id), do: gmark("Journal#{id}")
  defp conference_uri(id), do: gmark("Conference#{id}")
  defp researcher_uri(id), do: gmark("Researcher#{id}")
  defp paper_uri(id), do: gmark("Paper#{id}")

  # Namespace helpers for common vocabularies
  defp foaf(local), do: RDF.iri(@foaf <> local)
  defp dc(local), do: RDF.iri(@dc <> local)
  defp dcterms(local), do: RDF.iri(@dcterms <> local)
  defp prism(local), do: RDF.iri(@prism <> local)
  defp xsd(local), do: RDF.iri(@xsd <> local)
end
