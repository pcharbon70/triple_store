defmodule TripleStore.Benchmark.BSBMQueries do
  @moduledoc """
  BSBM (Berlin SPARQL Benchmark) query templates.

  Implements the 12 standard BSBM benchmark queries simulating e-commerce
  operations. These queries model realistic business intelligence scenarios:

  - Product search (Q1, Q2, Q3, Q4, Q5)
  - Product details (Q6)
  - Offers and reviews (Q7, Q8)
  - Updates (Q9, Q10) - not included as they require UPDATE support
  - Analytics (Q11, Q12)

  ## Usage

      # Get all queries
      queries = TripleStore.Benchmark.BSBMQueries.all()

      # Get a specific query
      {:ok, query} = TripleStore.Benchmark.BSBMQueries.get(:q1)

      # Get parameterized query
      {:ok, query} = TripleStore.Benchmark.BSBMQueries.get(:q1, product_type: 1)

  ## Query Descriptions

  | Query | Description | Operation Type |
  |-------|-------------|----------------|
  | Q1    | Product type lookup with features | Search |
  | Q2    | Product details for type | Search |
  | Q3    | Product features filtered | Search |
  | Q4    | Product features with UNION | Search |
  | Q5    | Product by label (text match) | Search |
  | Q6    | Product details page | Lookup |
  | Q7    | Product with offers | Join |
  | Q8    | Product reviews | Join |
  | Q9    | Describe product | Describe |
  | Q10   | Offers for product | Analytics |
  | Q11   | Offers with conditions | Analytics |
  | Q12   | Export product data | Export |

  """

  @bsbm_ns "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/"
  @bsbm_inst "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/"
  @rdf_ns "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs_ns "http://www.w3.org/2000/01/rdf-schema#"
  @dc_ns "http://purl.org/dc/elements/1.1/"

  @type query_id :: :q1 | :q2 | :q3 | :q4 | :q5 | :q6 | :q7 | :q8 | :q9 | :q10 | :q11 | :q12
  @type query_params :: keyword()
  @type query_template :: %{
          id: query_id(),
          name: String.t(),
          description: String.t(),
          sparql: String.t(),
          params: [atom()],
          complexity: :simple | :medium | :complex,
          operation_type: :search | :lookup | :join | :analytics | :describe | :export,
          expected_result_factor: float() | :varies
        }

  @doc """
  Returns all BSBM query templates.
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
      query_12()
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
  - `:product_type` - Product type ID (default: 1)
  - `:product` - Product ID (default: 1)
  - `:feature1` - First feature ID (default: 1)
  - `:feature2` - Second feature ID (default: 2)
  - `:min_price` - Minimum price (default: 50)
  - `:max_price` - Maximum price (default: 500)

  ## Examples

      {:ok, query} = BSBMQueries.get(:q1, product_type: 5, feature1: 3)

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
  Returns the BSBM vocabulary namespace.
  """
  @spec namespace() :: String.t()
  def namespace, do: @bsbm_ns

  @doc """
  Returns the BSBM instances namespace.
  """
  @spec instances_namespace() :: String.t()
  def instances_namespace, do: @bsbm_inst

  @doc """
  Estimates expected result count for a query given number of products.

  The result count depends on the number of products and the specific query.
  """
  @spec estimate_results(query_id(), pos_integer()) :: pos_integer() | :varies
  def estimate_results(id, num_products) do
    case get(id) do
      {:ok, query} ->
        case query.expected_result_factor do
          :varies -> :varies
          factor when is_number(factor) -> trunc(factor * num_products / 100)
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
      name: "Q1: Product type lookup with features",
      description: "Find products of a type with specific features and numeric properties",
      sparql: """
      PREFIX bsbm: <#{@bsbm_ns}>
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX rdfs: <#{@rdfs_ns}>
      SELECT ?product ?label
      WHERE {
        ?product rdf:type <#{@bsbm_inst}ProductType{product_type}> .
        ?product rdfs:label ?label .
        ?product bsbm:productFeature <#{@bsbm_inst}ProductFeature{product_type}_{feature1}> .
        ?product bsbm:productPropertyNumeric1 ?value1 .
        FILTER (?value1 > {min_value})
      }
      ORDER BY ?label
      LIMIT 10
      """,
      params: [:product_type, :feature1, :min_value],
      complexity: :medium,
      operation_type: :search,
      expected_result_factor: 5.0
    }
  end

  defp query_2 do
    %{
      id: :q2,
      name: "Q2: Product details for type",
      description: "Get all details of products of a specific type",
      sparql: """
      PREFIX bsbm: <#{@bsbm_ns}>
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX rdfs: <#{@rdfs_ns}>
      PREFIX dc: <#{@dc_ns}>
      SELECT ?product ?label ?comment ?producer ?propertyTextual1 ?propertyTextual2 ?propertyNumeric1 ?propertyNumeric2
      WHERE {
        ?product rdf:type <#{@bsbm_inst}ProductType{product_type}> .
        ?product rdfs:label ?label .
        ?product rdfs:comment ?comment .
        ?product bsbm:producer ?producer .
        ?product bsbm:productPropertyTextual1 ?propertyTextual1 .
        ?product bsbm:productPropertyTextual2 ?propertyTextual2 .
        ?product bsbm:productPropertyNumeric1 ?propertyNumeric1 .
        ?product bsbm:productPropertyNumeric2 ?propertyNumeric2 .
      }
      """,
      params: [:product_type],
      complexity: :simple,
      operation_type: :search,
      expected_result_factor: 10.0
    }
  end

  defp query_3 do
    %{
      id: :q3,
      name: "Q3: Product features filtered",
      description: "Find products with features and numeric property filters",
      sparql: """
      PREFIX bsbm: <#{@bsbm_ns}>
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX rdfs: <#{@rdfs_ns}>
      SELECT ?product ?label
      WHERE {
        ?product rdf:type <#{@bsbm_inst}ProductType{product_type}> .
        ?product rdfs:label ?label .
        ?product bsbm:productFeature <#{@bsbm_inst}ProductFeature{product_type}_{feature1}> .
        ?product bsbm:productPropertyNumeric1 ?p1 .
        FILTER (?p1 > {min_value} && ?p1 < {max_value})
      }
      ORDER BY ?label
      LIMIT 10
      """,
      params: [:product_type, :feature1, :min_value, :max_value],
      complexity: :medium,
      operation_type: :search,
      expected_result_factor: 3.0
    }
  end

  defp query_4 do
    %{
      id: :q4,
      name: "Q4: Product features with UNION",
      description: "Find products with either of two features using UNION",
      sparql: """
      PREFIX bsbm: <#{@bsbm_ns}>
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX rdfs: <#{@rdfs_ns}>
      SELECT ?product ?label
      WHERE {
        ?product rdf:type <#{@bsbm_inst}ProductType{product_type}> .
        ?product rdfs:label ?label .
        {
          ?product bsbm:productFeature <#{@bsbm_inst}ProductFeature{product_type}_{feature1}> .
        } UNION {
          ?product bsbm:productFeature <#{@bsbm_inst}ProductFeature{product_type}_{feature2}> .
        }
      }
      ORDER BY ?label
      LIMIT 10
      """,
      params: [:product_type, :feature1, :feature2],
      complexity: :complex,
      operation_type: :search,
      expected_result_factor: 8.0
    }
  end

  defp query_5 do
    %{
      id: :q5,
      name: "Q5: Product by label",
      description: "Find a product by its label (simulates text search)",
      sparql: """
      PREFIX bsbm: <#{@bsbm_ns}>
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX rdfs: <#{@rdfs_ns}>
      SELECT ?product
      WHERE {
        ?product rdfs:label "Product{product}"^^xsd:string .
      }
      """,
      params: [:product],
      complexity: :simple,
      operation_type: :search,
      expected_result_factor: 1.0
    }
  end

  defp query_6 do
    %{
      id: :q6,
      name: "Q6: Product details page",
      description: "Get all information about a specific product",
      sparql: """
      PREFIX bsbm: <#{@bsbm_ns}>
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX rdfs: <#{@rdfs_ns}>
      PREFIX dc: <#{@dc_ns}>
      SELECT ?product ?label ?comment ?producer ?propertyTextual1 ?propertyTextual2 ?propertyNumeric1 ?propertyNumeric2
      WHERE {
        BIND(<#{@bsbm_inst}dataFromProducer/Product{product}> AS ?product)
        ?product rdfs:label ?label .
        ?product rdfs:comment ?comment .
        ?product bsbm:producer ?producer .
        ?product bsbm:productPropertyTextual1 ?propertyTextual1 .
        ?product bsbm:productPropertyTextual2 ?propertyTextual2 .
        ?product bsbm:productPropertyNumeric1 ?propertyNumeric1 .
        ?product bsbm:productPropertyNumeric2 ?propertyNumeric2 .
      }
      """,
      params: [:product],
      complexity: :simple,
      operation_type: :lookup,
      expected_result_factor: 1.0
    }
  end

  defp query_7 do
    %{
      id: :q7,
      name: "Q7: Product with offers",
      description: "Find products with their offers and vendor information",
      sparql: """
      PREFIX bsbm: <#{@bsbm_ns}>
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX rdfs: <#{@rdfs_ns}>
      SELECT ?product ?offer ?price ?vendor
      WHERE {
        ?product rdf:type bsbm:Product .
        ?offer bsbm:product ?product .
        ?offer bsbm:price ?price .
        ?offer bsbm:vendor ?vendor .
        FILTER (?price >= {min_price} && ?price <= {max_price})
      }
      ORDER BY ?price
      LIMIT 20
      """,
      params: [:min_price, :max_price],
      complexity: :medium,
      operation_type: :join,
      expected_result_factor: 50.0
    }
  end

  defp query_8 do
    %{
      id: :q8,
      name: "Q8: Product reviews",
      description: "Find reviews for a specific product",
      sparql: """
      PREFIX bsbm: <#{@bsbm_ns}>
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX rdfs: <#{@rdfs_ns}>
      SELECT ?review ?reviewer ?rating1 ?rating2 ?reviewDate
      WHERE {
        ?review bsbm:reviewFor <#{@bsbm_inst}dataFromProducer/Product{product}> .
        ?review bsbm:reviewer ?reviewer .
        ?review bsbm:rating1 ?rating1 .
        ?review bsbm:rating2 ?rating2 .
        ?review bsbm:reviewDate ?reviewDate .
      }
      ORDER BY DESC(?reviewDate)
      """,
      params: [:product],
      complexity: :medium,
      operation_type: :join,
      expected_result_factor: 6.0
    }
  end

  defp query_9 do
    %{
      id: :q9,
      name: "Q9: Describe product",
      description: "Get all triples about a product (DESCRIBE-like)",
      sparql: """
      PREFIX bsbm: <#{@bsbm_ns}>
      SELECT ?p ?o
      WHERE {
        <#{@bsbm_inst}dataFromProducer/Product{product}> ?p ?o .
      }
      """,
      params: [:product],
      complexity: :simple,
      operation_type: :describe,
      expected_result_factor: 12.0
    }
  end

  defp query_10 do
    %{
      id: :q10,
      name: "Q10: Offers for product",
      description: "Find all offers for a specific product with details",
      sparql: """
      PREFIX bsbm: <#{@bsbm_ns}>
      PREFIX rdf: <#{@rdf_ns}>
      SELECT ?offer ?vendor ?price ?validFrom ?validTo
      WHERE {
        ?offer bsbm:product <#{@bsbm_inst}dataFromProducer/Product{product}> .
        ?offer bsbm:vendor ?vendor .
        ?offer bsbm:price ?price .
        ?offer bsbm:validFrom ?validFrom .
        ?offer bsbm:validTo ?validTo .
      }
      ORDER BY ?price
      """,
      params: [:product],
      complexity: :medium,
      operation_type: :analytics,
      expected_result_factor: 12.0
    }
  end

  defp query_11 do
    %{
      id: :q11,
      name: "Q11: Offers with conditions",
      description: "Find offers within a price range from specific countries",
      sparql: """
      PREFIX bsbm: <#{@bsbm_ns}>
      PREFIX rdf: <#{@rdf_ns}>
      SELECT ?offer ?product ?vendor ?price
      WHERE {
        ?offer rdf:type bsbm:Offer .
        ?offer bsbm:product ?product .
        ?offer bsbm:vendor ?vendor .
        ?offer bsbm:price ?price .
        ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries\#{country}> .
        FILTER (?price >= {min_price} && ?price <= {max_price})
      }
      ORDER BY ?price
      LIMIT 10
      """,
      params: [:country, :min_price, :max_price],
      complexity: :complex,
      operation_type: :analytics,
      expected_result_factor: 10.0
    }
  end

  defp query_12 do
    %{
      id: :q12,
      name: "Q12: Export product data",
      description: "Export all data about products of a specific type",
      sparql: """
      PREFIX bsbm: <#{@bsbm_ns}>
      PREFIX rdf: <#{@rdf_ns}>
      PREFIX rdfs: <#{@rdfs_ns}>
      CONSTRUCT {
        ?product rdf:type ?type .
        ?product rdfs:label ?label .
        ?product bsbm:producer ?producer .
        ?product bsbm:productPropertyNumeric1 ?num1 .
        ?product bsbm:productPropertyNumeric2 ?num2 .
      }
      WHERE {
        ?product rdf:type <#{@bsbm_inst}ProductType{product_type}> .
        ?product rdf:type ?type .
        ?product rdfs:label ?label .
        ?product bsbm:producer ?producer .
        ?product bsbm:productPropertyNumeric1 ?num1 .
        ?product bsbm:productPropertyNumeric2 ?num2 .
      }
      """,
      params: [:product_type],
      complexity: :complex,
      operation_type: :export,
      expected_result_factor: 50.0
    }
  end

  # ===========================================================================
  # Parameter Substitution
  # ===========================================================================

  defp substitute_params(sparql, params) do
    defaults = [
      product_type: 1,
      product: 1,
      feature1: 1,
      feature2: 2,
      min_value: 100,
      max_value: 1000,
      min_price: 50,
      max_price: 500,
      country: "#US"
    ]

    merged = Keyword.merge(defaults, params)

    Enum.reduce(merged, sparql, fn {key, value}, acc ->
      String.replace(acc, "{#{key}}", to_string(value))
    end)
  end
end
