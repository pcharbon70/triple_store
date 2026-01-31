defmodule TripleStore.Benchmark.WatDivQueries do
  @moduledoc """
  WatDiv (Waterloo SPARQL Diversity Test) query templates.

  Implements the 20 standard WatDiv benchmark queries testing diverse
  SPARQL query patterns over heterogeneous RDF data.

  ## Usage

      # Get all queries
      queries = TripleStore.Benchmark.WatDivQueries.all()

      # Get a specific query
      {:ok, query} = TripleStore.Benchmark.WatDivQueries.get(:l1)

      # Get parameterized query
      {:ok, query} = TripleStore.Benchmark.WatDivQueries.get(:l1, v1: "Website1")

  ## Query Categories

  WatDiv queries are organized into 4 categories based on their graph pattern shape:

  | Category | Queries | Description |
  |----------|---------|-------------|
  | Linear (L) | L1-L5 | Queries following linear paths through the graph |
  | Star (S) | S1-S7 | Queries centered on a single entity with many relationships |
  | Snowflake (F) | F1-F5 | Queries with branching patterns from multiple entities |
  | Complex (C) | C1-C5 | Queries combining multiple features |

  ## Query Descriptions

  ### Linear Queries (L1-L5)
  - L1: User likes content with caption
  - L2: Users who like a product with nationality
  - L3: User likes and subscribes
  - L4: Content tagged with topic with caption
  - L5: Person with job title and nationality

  ### Star Queries (S1-S7)
  - S1: Offer with all properties
  - S2: User by location, nationality, gender, role
  - S3: Product by type with caption, genre, publisher
  - S4: Person by age with name and artist connection
  - S5: Product by type with description, keywords, language
  - S6: Musical work with conductor and genre
  - S7: Product liked by user

  ### Snowflake Queries (F1-F5)
  - F1: Movie with genre tagged with topic
  - F2: Product with homepage and genre
  - F3: Product purchase by genre
  - F4: Product with offer, likes, and language
  - F5: Offer with product title and type

  ### Complex Queries (C1-C5)
  - C1: Review with actor and language
  - C2: Purchase flow with offers and reviews
  - C3: User with all profile attributes
  - C4: (Not in basic test set)
  - C5: (Not in basic test set)

  """

  @wsdbm "http://db.uwaterloo.ca/~galuc/wsdbm/"
  @sorg "http://schema.org/"
  @mo "http://purl.org/ontology/mo/"
  @rev "http://purl.org/stuff/rev#"
  @og "http://ogp.me/ns#"
  @dc "http://purl.org/dc/terms/"
  @foaf "http://xmlns.com/foaf/"
  @gr "http://purl.org/goodrelations/"
  @gn "http://www.geonames.org/ontology#"
  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

  @type query_id ::
          :l1
          | :l2
          | :l3
          | :l4
          | :l5
          | :s1
          | :s2
          | :s3
          | :s4
          | :s5
          | :s6
          | :s7
          | :f1
          | :f2
          | :f3
          | :f4
          | :f5
          | :c1
          | :c2
          | :c3
  @type query_params :: keyword()
  @type query_category :: :linear | :star | :snowflake | :complex
  @type query_template :: %{
          id: query_id(),
          name: String.t(),
          description: String.t(),
          sparql: String.t(),
          params: [atom()],
          category: query_category(),
          complexity: :simple | :medium | :complex
        }

  @doc """
  Returns all WatDiv query templates.
  """
  @spec all() :: [query_template()]
  def all do
    [
      # Linear queries
      query_l1(),
      query_l2(),
      query_l3(),
      query_l4(),
      query_l5(),
      # Star queries
      query_s1(),
      query_s2(),
      query_s3(),
      query_s4(),
      query_s5(),
      query_s6(),
      query_s7(),
      # Snowflake queries
      query_f1(),
      query_f2(),
      query_f3(),
      query_f4(),
      query_f5(),
      # Complex queries
      query_c1(),
      query_c2(),
      query_c3()
    ]
  end

  @doc """
  Returns all queries in a specific category.
  """
  @spec by_category(query_category()) :: [query_template()]
  def by_category(category) do
    Enum.filter(all(), fn q -> q.category == category end)
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
  - `:v0`, `:v1`, `:v2`, etc. - Entity ID placeholders

  ## Examples

      {:ok, query} = WatDivQueries.get(:l1, v1: "Website1")

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
  Returns the WatDiv vocabulary namespace.
  """
  @spec namespace() :: String.t()
  def namespace, do: @wsdbm

  # ===========================================================================
  # Linear Queries (L1-L5)
  # ===========================================================================

  defp query_l1 do
    %{
      id: :l1,
      name: "L1: User likes content with caption",
      description: "Find users who like content with captions",
      sparql: """
      PREFIX wsdbm: <#{@wsdbm}>
      PREFIX sorg: <#{@sorg}>
      SELECT ?v0 ?v2 ?v3 WHERE {
        ?v0 wsdbm:subscribes <%v1%> .
        ?v2 sorg:caption ?v3 .
        ?v0 wsdbm:likes ?v2 .
      }
      """,
      params: [:v1],
      category: :linear,
      complexity: :simple
    }
  end

  defp query_l2 do
    %{
      id: :l2,
      name: "L2: Users who like a product with nationality",
      description: "Find users who like a specific product and have nationality",
      sparql: """
      PREFIX wsdbm: <#{@wsdbm}>
      PREFIX gn: <#{@gn}>
      PREFIX sorg: <#{@sorg}>
      SELECT ?v1 ?v2 WHERE {
        <%v0%> gn:parentCountry ?v1 .
        ?v2 wsdbm:likes wsdbm:Product0 .
        ?v2 sorg:nationality ?v1 .
      }
      """,
      params: [:v0],
      category: :linear,
      complexity: :simple
    }
  end

  defp query_l3 do
    %{
      id: :l3,
      name: "L3: User likes and subscribes",
      description: "Find users who like content and subscribe to website",
      sparql: """
      PREFIX wsdbm: <#{@wsdbm}>
      SELECT ?v0 ?v1 WHERE {
        ?v0 wsdbm:likes ?v1 .
        ?v0 wsdbm:subscribes <%v2%> .
      }
      """,
      params: [:v2],
      category: :linear,
      complexity: :simple
    }
  end

  defp query_l4 do
    %{
      id: :l4,
      name: "L4: Content tagged with topic",
      description: "Find content tagged with a specific topic",
      sparql: """
      PREFIX og: <#{@og}>
      PREFIX sorg: <#{@sorg}>
      SELECT ?v0 ?v2 WHERE {
        ?v0 og:tag <%v1%> .
        ?v0 sorg:caption ?v2 .
      }
      """,
      params: [:v1],
      category: :linear,
      complexity: :simple
    }
  end

  defp query_l5 do
    %{
      id: :l5,
      name: "L5: Person with job title and nationality",
      description: "Find people with job titles from a specific country",
      sparql: """
      PREFIX wsdbm: <#{@wsdbm}>
      PREFIX sorg: <#{@sorg}>
      PREFIX gn: <#{@gn}>
      SELECT ?v0 ?v1 ?v3 WHERE {
        ?v0 sorg:jobTitle ?v1 .
        <%v2%> gn:parentCountry ?v3 .
        ?v0 sorg:nationality ?v3 .
      }
      """,
      params: [:v2],
      category: :linear,
      complexity: :simple
    }
  end

  # ===========================================================================
  # Star Queries (S1-S7)
  # ===========================================================================

  defp query_s1 do
    %{
      id: :s1,
      name: "S1: Offer with all properties",
      description: "Find offers with all their properties",
      sparql: """
      PREFIX gr: <#{@gr}>
      PREFIX sorg: <#{@sorg}>
      PREFIX wsdbm: <#{@wsdbm}>
      SELECT ?v0 ?v1 ?v3 ?v4 ?v5 ?v6 ?v7 ?v8 ?v9 WHERE {
        ?v0 gr:includes ?v1 .
        <%v2%> gr:offers ?v0 .
        ?v0 gr:price ?v3 .
        ?v0 gr:serialNumber ?v4 .
        ?v0 gr:validFrom ?v5 .
        ?v0 gr:validThrough ?v6 .
        ?v0 sorg:eligibleQuantity ?v7 .
        ?v0 sorg:eligibleRegion ?v8 .
        ?v0 sorg:priceValidUntil ?v9 .
      }
      """,
      params: [:v2],
      category: :star,
      complexity: :medium
    }
  end

  defp query_s2 do
    %{
      id: :s2,
      name: "S2: User by location, nationality, gender, role",
      description: "Find users with specific location, nationality, gender and role",
      sparql: """
      PREFIX dc: <#{@dc}>
      PREFIX sorg: <#{@sorg}>
      PREFIX wsdbm: <#{@wsdbm}>
      PREFIX rdf: <#{@rdf}>
      SELECT ?v0 ?v1 ?v3 WHERE {
        ?v0 dc:Location ?v1 .
        ?v0 sorg:nationality <%v2%> .
        ?v0 wsdbm:gender ?v3 .
        ?v0 rdf:type wsdbm:Role2 .
      }
      """,
      params: [:v2],
      category: :star,
      complexity: :medium
    }
  end

  defp query_s3 do
    %{
      id: :s3,
      name: "S3: Product by type with caption, genre, publisher",
      description: "Find products of a specific type with caption, genre, and publisher",
      sparql: """
      PREFIX wsdbm: <#{@wsdbm}>
      PREFIX sorg: <#{@sorg}>
      PREFIX rdf: <#{@rdf}>
      SELECT ?v0 ?v2 ?v3 ?v4 WHERE {
        ?v0 rdf:type <%v1%> .
        ?v0 sorg:caption ?v2 .
        ?v0 wsdbm:hasGenre ?v3 .
        ?v0 sorg:publisher ?v4 .
      }
      """,
      params: [:v1],
      category: :star,
      complexity: :medium
    }
  end

  defp query_s4 do
    %{
      id: :s4,
      name: "S4: Person by age with name and artist connection",
      description: "Find persons by age with family name and artist connection",
      sparql: """
      PREFIX foaf: <#{@foaf}>
      PREFIX sorg: <#{@sorg}>
      PREFIX mo: <#{@mo}>
      PREFIX wsdbm: <#{@wsdbm}>
      PREFIX rdf: <#{@rdf}>
      SELECT ?v0 ?v2 ?v3 WHERE {
        ?v0 foaf:age <%v1%> .
        ?v0 foaf:familyName ?v2 .
        ?v3 mo:artist ?v0 .
        ?v0 sorg:nationality wsdbm:Country1 .
      }
      """,
      params: [:v1],
      category: :star,
      complexity: :medium
    }
  end

  defp query_s5 do
    %{
      id: :s5,
      name: "S5: Product by type with description, keywords, language",
      description: "Find products of a specific type with description and keywords",
      sparql: """
      PREFIX sorg: <#{@sorg}>
      PREFIX rdf: <#{@rdf}>
      PREFIX wsdbm: <#{@wsdbm}>
      SELECT ?v0 ?v2 ?v3 WHERE {
        ?v0 rdf:type <%v1%> .
        ?v0 sorg:description ?v2 .
        ?v0 sorg:keywords ?v3 .
        ?v0 sorg:language wsdbm:Language0 .
      }
      """,
      params: [:v1],
      category: :star,
      complexity: :medium
    }
  end

  defp query_s6 do
    %{
      id: :s6,
      name: "S6: Musical work with conductor and genre",
      description: "Find musical works with conductor and genre",
      sparql: """
      PREFIX mo: <#{@mo}>
      PREFIX wsdbm: <#{@wsdbm}>
      PREFIX rdf: <#{@rdf}>
      SELECT ?v0 ?v1 ?v2 WHERE {
        ?v0 mo:conductor ?v1 .
        ?v0 rdf:type ?v2 .
        ?v0 wsdbm:hasGenre <%v3%> .
      }
      """,
      params: [:v3],
      category: :star,
      complexity: :medium
    }
  end

  defp query_s7 do
    %{
      id: :s7,
      name: "S7: Product liked by user",
      description: "Find products that are liked by a specific user",
      sparql: """
      PREFIX wsdbm: <#{@wsdbm}>
      PREFIX sorg: <#{@sorg}>
      PREFIX rdf: <#{@rdf}>
      SELECT ?v0 ?v1 ?v2 WHERE {
        ?v0 rdf:type ?v1 .
        ?v0 sorg:text ?v2 .
        <%v3%> wsdbm:likes ?v0 .
      }
      """,
      params: [:v3],
      category: :star,
      complexity: :simple
    }
  end

  # ===========================================================================
  # Snowflake Queries (F1-F5)
  # ===========================================================================

  defp query_f1 do
    %{
      id: :f1,
      name: "F1: Movie with genre tagged with topic",
      description: "Find movies with genre that is tagged with a topic",
      sparql: """
      PREFIX og: <#{@og}>
      PREFIX wsdbm: <#{@wsdbm}>
      PREFIX sorg: <#{@sorg}>
      PREFIX rdf: <#{@rdf}>
      SELECT ?v0 ?v2 ?v3 ?v4 ?v5 WHERE {
        ?v0 og:tag <%v1%> .
        ?v0 rdf:type ?v2 .
        ?v3 sorg:trailer ?v4 .
        ?v3 sorg:keywords ?v5 .
        ?v3 wsdbm:hasGenre ?v0 .
        ?v3 rdf:type wsdbm:ProductCategory2 .
      }
      """,
      params: [:v1],
      category: :snowflake,
      complexity: :medium
    }
  end

  defp query_f2 do
    %{
      id: :f2,
      name: "F2: Product with homepage and genre",
      description: "Find products with homepage URL and genre",
      sparql: """
      PREFIX foaf: <#{@foaf}>
      PREFIX og: <#{@og}>
      PREFIX wsdbm: <#{@wsdbm}>
      PREFIX sorg: <#{@sorg}>
      PREFIX rdf: <#{@rdf}>
      SELECT ?v0 ?v1 ?v2 ?v4 ?v5 ?v6 ?v7 WHERE {
        ?v0 foaf:homepage ?v1 .
        ?v0 og:title ?v2 .
        ?v0 rdf:type ?v3 .
        ?v0 sorg:caption ?v4 .
        ?v0 sorg:description ?v5 .
        ?v1 sorg:url ?v6 .
        ?v1 wsdbm:hits ?v7 .
        ?v0 wsdbm:hasGenre <%v8%> .
      }
      """,
      params: [:v8],
      category: :snowflake,
      complexity: :complex
    }
  end

  defp query_f3 do
    %{
      id: :f3,
      name: "F3: Product purchase by genre",
      description: "Find product purchases by genre",
      sparql: """
      PREFIX sorg: <#{@sorg}>
      PREFIX wsdbm: <#{@wsdbm}>
      SELECT ?v0 ?v1 ?v2 ?v4 ?v5 ?v6 WHERE {
        ?v0 sorg:contentRating ?v1 .
        ?v0 sorg:contentSize ?v2 .
        ?v0 wsdbm:hasGenre <%v3%> .
        ?v4 wsdbm:makesPurchase ?v5 .
        ?v5 wsdbm:purchaseDate ?v6 .
        ?v5 wsdbm:purchaseFor ?v0 .
      }
      """,
      params: [:v3],
      category: :snowflake,
      complexity: :complex
    }
  end

  defp query_f4 do
    %{
      id: :f4,
      name: "F4: Product with offer, likes, and language",
      description: "Find products with offers, likes, and language",
      sparql: """
      PREFIX foaf: <#{@foaf}>
      PREFIX og: <#{@og}>
      PREFIX gr: <#{@gr}>
      PREFIX sorg: <#{@sorg}>
      PREFIX wsdbm: <#{@wsdbm}>
      SELECT ?v0 ?v1 ?v2 ?v4 ?v5 ?v6 ?v7 ?v8 WHERE {
        ?v0 foaf:homepage ?v1 .
        ?v2 gr:includes ?v0 .
        ?v0 og:tag <%v3%> .
        ?v0 sorg:description ?v4 .
        ?v0 sorg:contentSize ?v8 .
        ?v1 sorg:url ?v5 .
        ?v1 wsdbm:hits ?v6 .
        ?v1 sorg:language wsdbm:Language0 .
        ?v7 wsdbm:likes ?v0 .
      }
      """,
      params: [:v3],
      category: :snowflake,
      complexity: :complex
    }
  end

  defp query_f5 do
    %{
      id: :f5,
      name: "F5: Offer with product title and type",
      description: "Find offers with product title and type",
      sparql: """
      PREFIX gr: <#{@gr}>
      PREFIX sorg: <#{@sorg}>
      PREFIX og: <#{@og}>
      PREFIX wsdbm: <#{@wsdbm}>
      PREFIX rdf: <#{@rdf}>
      SELECT ?v0 ?v1 ?v3 ?v4 ?v5 ?v6 WHERE {
        ?v0 gr:includes ?v1 .
        <%v2%> gr:offers ?v0 .
        ?v0 gr:price ?v3 .
        ?v0 gr:validThrough ?v4 .
        ?v1 og:title ?v5 .
        ?v1 rdf:type ?v6 .
      }
      """,
      params: [:v2],
      category: :snowflake,
      complexity: :medium
    }
  end

  # ===========================================================================
  # Complex Queries (C1-C3)
  # ===========================================================================

  defp query_c1 do
    %{
      id: :c1,
      name: "C1: Review with actor and language",
      description: "Find reviews with reviewer and actor language",
      sparql: """
      PREFIX sorg: <#{@sorg}>
      PREFIX rev: <#{@rev}>
      SELECT ?v0 ?v4 ?v6 ?v7 WHERE {
        ?v0 sorg:caption ?v1 .
        ?v0 sorg:text ?v2 .
        ?v0 sorg:contentRating ?v3 .
        ?v0 rev:hasReview ?v4 .
        ?v4 rev:title ?v5 .
        ?v4 rev:reviewer ?v6 .
        ?v7 sorg:actor ?v6 .
        ?v7 sorg:language ?v8 .
      }
      """,
      params: [],
      category: :complex,
      complexity: :complex
    }
  end

  defp query_c2 do
    %{
      id: :c2,
      name: "C2: Purchase flow with offers and reviews",
      description: "Complex purchase flow with offers and reviews",
      sparql: """
      PREFIX sorg: <#{@sorg}>
      PREFIX gr: <#{@gr}>
      PREFIX foaf: <#{@foaf}>
      PREFIX wsdbm: <#{@wsdbm}>
      PREFIX rev: <#{@rev}>
      SELECT ?v0 ?v3 ?v4 ?v8 WHERE {
        ?v0 sorg:legalName ?v1 .
        ?v0 gr:offers ?v2 .
        ?v2 sorg:eligibleRegion wsdbm:Country5 .
        ?v2 gr:includes ?v3 .
        ?v4 sorg:jobTitle ?v5 .
        ?v4 foaf:homepage ?v6 .
        ?v4 wsdbm:makesPurchase ?v7 .
        ?v7 wsdbm:purchaseFor ?v3 .
        ?v3 rev:hasReview ?v8 .
        ?v8 rev:totalVotes ?v9 .
      }
      """,
      params: [],
      category: :complex,
      complexity: :complex
    }
  end

  defp query_c3 do
    %{
      id: :c3,
      name: "C3: User with all profile attributes",
      description: "Find users with all profile attributes",
      sparql: """
      PREFIX wsdbm: <#{@wsdbm}>
      PREFIX dc: <#{@dc}>
      PREFIX foaf: <#{@foaf}>
      SELECT ?v0 WHERE {
        ?v0 wsdbm:likes ?v1 .
        ?v0 wsdbm:friendOf ?v2 .
        ?v0 dc:Location ?v3 .
        ?v0 foaf:age ?v4 .
        ?v0 wsdbm:gender ?v5 .
        ?v0 foaf:givenName ?v6 .
      }
      """,
      params: [],
      category: :complex,
      complexity: :medium
    }
  end

  # ===========================================================================
  # Parameter Substitution
  # ===========================================================================

  defp substitute_params(sparql, params) do
    defaults = [
      v0: "User0",
      v1: "Website0",
      v2: "City0",
      v3: "Topic0",
      v4: "Retailer0",
      v8: "SubGenre0"
    ]

    merged = Keyword.merge(defaults, params)

    Enum.reduce(merged, sparql, fn {key, value}, acc ->
      String.replace(acc, "<%#{key}%>", "<#{@wsdbm}#{value}>")
    end)
  end
end
