defmodule TripleStore.Benchmark.BSBM do
  @moduledoc """
  BSBM (Berlin SPARQL Benchmark) data generator.

  Generates synthetic e-commerce data for benchmarking RDF stores.
  The data simulates an e-commerce scenario with products, vendors,
  offers, reviews, and producers.

  ## Scale Factor

  The scale factor determines the number of products to generate.
  Related entities (vendors, offers, reviews) scale proportionally.

  - Scale 100: ~50K triples (100 products)
  - Scale 1000: ~500K triples (1000 products)
  - Scale 10000: ~5M triples (10000 products)

  ## Usage

      # Generate data for 1000 products
      graph = TripleStore.Benchmark.BSBM.generate(1000)

      # Generate with seed for reproducibility
      graph = TripleStore.Benchmark.BSBM.generate(1000, seed: 12345)

      # Generate as stream for large datasets
      stream = TripleStore.Benchmark.BSBM.stream(100000)

  ## Ontology

  The BSBM ontology defines:
  - Product, ProductType, ProductFeature
  - Producer, Vendor, Offer
  - Review, ReviewSite, Person

  """

  @bsbm_ns "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/"
  @bsbm_inst "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/"
  @rdf_ns "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs_ns "http://www.w3.org/2000/01/rdf-schema#"
  @dc_ns "http://purl.org/dc/elements/1.1/"

  # Generation parameters
  @product_types 10
  @features_per_type 5..10
  @producers_ratio 0.02
  @vendors_ratio 0.03
  @offers_per_product 5..20
  @reviews_per_product 3..10
  @review_sites 5

  @typedoc "Generator options"
  @type opts :: [
          seed: integer(),
          stream: boolean()
        ]

  @doc """
  Generates BSBM benchmark data as an RDF.Graph.

  ## Arguments

  - `num_products` - Number of products to generate (scale factor)

  ## Options

  - `:seed` - Random seed for reproducible generation (default: based on num_products)

  ## Returns

  An `RDF.Graph` containing the generated triples.

  ## Examples

      graph = TripleStore.Benchmark.BSBM.generate(1000)
      RDF.Graph.triple_count(graph)
      # => ~500000

  """
  @spec generate(pos_integer(), opts()) :: RDF.Graph.t()
  def generate(num_products, opts \\ []) when num_products > 0 do
    seed = Keyword.get(opts, :seed, num_products * 42)
    :rand.seed(:exsss, {seed, seed * 2, seed * 3})

    state = init_state(num_products)
    triples = generate_all(state)

    RDF.Graph.new(triples)
  end

  @doc """
  Generates BSBM benchmark data as a stream of triples.

  Useful for large scale factors where holding all triples in memory
  is not feasible.

  ## Arguments

  - `num_products` - Number of products to generate

  ## Options

  - `:seed` - Random seed for reproducible generation

  ## Returns

  A stream of `{subject, predicate, object}` triples.

  ## Examples

      stream = TripleStore.Benchmark.BSBM.stream(100000)
      Enum.take(stream, 1000)

  """
  @spec stream(pos_integer(), opts()) :: Enumerable.t()
  def stream(num_products, opts \\ []) when num_products > 0 do
    seed = Keyword.get(opts, :seed, num_products * 42)

    Stream.resource(
      fn ->
        :rand.seed(:exsss, {seed, seed * 2, seed * 3})
        state = init_state(num_products)
        {:schema, state}
      end,
      fn
        {:schema, state} ->
          schema_triples = generate_schema(state)
          {schema_triples, {:producers, state, 1}}

        {:producers, state, n} when n <= state.num_producers ->
          triples = generate_producer(n)
          {triples, {:producers, state, n + 1}}

        {:producers, state, _} ->
          {[], {:vendors, state, 1}}

        {:vendors, state, n} when n <= state.num_vendors ->
          triples = generate_vendor(n)
          {triples, {:vendors, state, n + 1}}

        {:vendors, state, _} ->
          {[], {:products, state, 1}}

        {:products, state, n} when n <= state.num_products ->
          triples = generate_product(n, state)
          {triples, {:products, state, n + 1}}

        {:products, _state, _} ->
          {:halt, nil}
      end,
      fn _ -> :ok end
    )
  end

  @doc """
  Returns the estimated triple count for a given number of products.

  ## Examples

      TripleStore.Benchmark.BSBM.estimate_triple_count(1000)
      # => ~500000

  """
  @spec estimate_triple_count(pos_integer()) :: pos_integer()
  def estimate_triple_count(num_products) do
    num_producers = max(1, trunc(num_products * @producers_ratio))
    num_vendors = max(1, trunc(num_products * @vendors_ratio))
    avg_offers = div(Enum.min(@offers_per_product) + Enum.max(@offers_per_product), 2)
    avg_reviews = div(Enum.min(@reviews_per_product) + Enum.max(@reviews_per_product), 2)

    # Triples per entity
    schema_triples = @product_types * 3 + @product_types * 7
    producer_triples = num_producers * 4
    vendor_triples = num_vendors * 5
    product_triples = num_products * 10
    offer_triples = num_products * avg_offers * 6
    review_triples = num_products * avg_reviews * 8

    schema_triples + producer_triples + vendor_triples + product_triples + offer_triples + review_triples
  end

  @doc """
  Returns the BSBM vocabulary namespace.
  """
  @spec namespace() :: String.t()
  def namespace, do: @bsbm_ns

  # ===========================================================================
  # Private: State Initialization
  # ===========================================================================

  defp init_state(num_products) do
    %{
      num_products: num_products,
      num_producers: max(1, trunc(num_products * @producers_ratio)),
      num_vendors: max(1, trunc(num_products * @vendors_ratio)),
      num_product_types: @product_types,
      num_review_sites: @review_sites
    }
  end

  # ===========================================================================
  # Private: Generation Functions
  # ===========================================================================

  defp generate_all(state) do
    schema_triples = generate_schema(state)
    producer_triples = Enum.flat_map(1..state.num_producers, &generate_producer/1)
    vendor_triples = Enum.flat_map(1..state.num_vendors, &generate_vendor/1)
    product_triples = Enum.flat_map(1..state.num_products, &generate_product(&1, state))

    schema_triples ++ producer_triples ++ vendor_triples ++ product_triples
  end

  defp generate_schema(state) do
    # Generate product types
    type_triples =
      Enum.flat_map(1..state.num_product_types, fn type_id ->
        type_uri = product_type_uri(type_id)

        [
          {type_uri, rdf_type(), bsbm("ProductType")},
          {type_uri, rdfs("label"), RDF.literal("ProductType#{type_id}")},
          {type_uri, rdfs("comment"), RDF.literal("Product type number #{type_id}")}
        ]
      end)

    # Generate product features
    feature_triples =
      Enum.flat_map(1..state.num_product_types, fn type_id ->
        num_features = random_in_range(@features_per_type)

        Enum.flat_map(1..num_features, fn feat_id ->
          feat_uri = product_feature_uri(type_id, feat_id)

          [
            {feat_uri, rdf_type(), bsbm("ProductFeature")},
            {feat_uri, rdfs("label"), RDF.literal("Feature#{type_id}_#{feat_id}")},
            {feat_uri, bsbm("publishDate"), date_literal(random_date())}
          ]
        end)
      end)

    # Generate review sites
    site_triples =
      Enum.flat_map(1..state.num_review_sites, fn site_id ->
        site_uri = review_site_uri(site_id)

        [
          {site_uri, rdf_type(), bsbm("ReviewSite")},
          {site_uri, rdfs("label"), RDF.literal("ReviewSite#{site_id}")}
        ]
      end)

    type_triples ++ feature_triples ++ site_triples
  end

  defp generate_producer(producer_id) do
    producer_uri = producer_uri(producer_id)
    country = random_country()

    [
      {producer_uri, rdf_type(), bsbm("Producer")},
      {producer_uri, rdfs("label"), RDF.literal("Producer#{producer_id}")},
      {producer_uri, bsbm("country"), RDF.iri("http://downlode.org/rdf/iso-3166/countries##{country}")},
      {producer_uri, dc("publisher"), producer_uri}
    ]
  end

  defp generate_vendor(vendor_id) do
    vendor_uri = vendor_uri(vendor_id)
    country = random_country()

    [
      {vendor_uri, rdf_type(), bsbm("Vendor")},
      {vendor_uri, rdfs("label"), RDF.literal("Vendor#{vendor_id}")},
      {vendor_uri, bsbm("country"), RDF.iri("http://downlode.org/rdf/iso-3166/countries##{country}")},
      {vendor_uri, bsbm("homepage"), RDF.literal("http://www.vendor#{vendor_id}.com")},
      {vendor_uri, dc("publisher"), vendor_uri}
    ]
  end

  defp generate_product(product_id, state) do
    product_uri = product_uri(product_id)
    type_id = rem(product_id - 1, state.num_product_types) + 1
    producer_id = rem(product_id - 1, state.num_producers) + 1

    base_triples = [
      {product_uri, rdf_type(), bsbm("Product")},
      {product_uri, rdf_type(), product_type_uri(type_id)},
      {product_uri, rdfs("label"), RDF.literal("Product#{product_id}")},
      {product_uri, rdfs("comment"), RDF.literal("Product description for product #{product_id}")},
      {product_uri, bsbm("producer"), producer_uri(producer_id)},
      {product_uri, bsbm("productPropertyNumeric1"), RDF.literal(random_range(1, 2000))},
      {product_uri, bsbm("productPropertyNumeric2"), RDF.literal(random_range(1, 2000))},
      {product_uri, bsbm("productPropertyTextual1"), RDF.literal("textual1_#{product_id}")},
      {product_uri, bsbm("productPropertyTextual2"), RDF.literal("textual2_#{product_id}")},
      {product_uri, dc("publisher"), producer_uri(producer_id)}
    ]

    # Add product features
    num_features = random_in_range(@features_per_type)

    feature_triples =
      Enum.map(1..min(num_features, 3), fn feat_id ->
        {product_uri, bsbm("productFeature"), product_feature_uri(type_id, feat_id)}
      end)

    # Generate offers for this product
    num_offers = random_in_range(@offers_per_product)
    offer_triples = Enum.flat_map(1..num_offers, &generate_offer(product_id, &1, state))

    # Generate reviews for this product
    num_reviews = random_in_range(@reviews_per_product)
    review_triples = Enum.flat_map(1..num_reviews, &generate_review(product_id, &1, state))

    base_triples ++ feature_triples ++ offer_triples ++ review_triples
  end

  defp generate_offer(product_id, offer_id, state) do
    offer_uri = offer_uri(product_id, offer_id)
    vendor_id = rem(offer_id - 1, state.num_vendors) + 1
    product_uri = product_uri(product_id)
    price = random_range(10, 10_000) / 100.0

    [
      {offer_uri, rdf_type(), bsbm("Offer")},
      {offer_uri, bsbm("product"), product_uri},
      {offer_uri, bsbm("vendor"), vendor_uri(vendor_id)},
      {offer_uri, bsbm("price"), RDF.literal(price)},
      {offer_uri, bsbm("validFrom"), date_literal(random_date())},
      {offer_uri, bsbm("validTo"), date_literal(random_date())}
    ]
  end

  defp generate_review(product_id, review_id, state) do
    review_uri = review_uri(product_id, review_id)
    product_uri = product_uri(product_id)
    reviewer_uri = reviewer_uri(product_id, review_id)
    site_id = rem(review_id - 1, state.num_review_sites) + 1
    rating = random_range(1, 10)

    [
      {review_uri, rdf_type(), bsbm("Review")},
      {review_uri, bsbm("reviewFor"), product_uri},
      {review_uri, bsbm("reviewer"), reviewer_uri},
      {review_uri, bsbm("reviewDate"), date_literal(random_date())},
      {review_uri, bsbm("rating1"), RDF.literal(rating)},
      {review_uri, bsbm("rating2"), RDF.literal(random_range(1, 10))},
      {review_uri, dc("publisher"), review_site_uri(site_id)},
      {reviewer_uri, rdf_type(), bsbm("Person")}
    ]
  end

  # ===========================================================================
  # Private: URI Generators
  # ===========================================================================

  defp product_uri(id), do: RDF.iri(@bsbm_inst <> "dataFromProducer/Product#{id}")
  defp product_type_uri(id), do: RDF.iri(@bsbm_inst <> "ProductType#{id}")
  defp product_feature_uri(type_id, feat_id), do: RDF.iri(@bsbm_inst <> "ProductFeature#{type_id}_#{feat_id}")
  defp producer_uri(id), do: RDF.iri(@bsbm_inst <> "dataFromProducer/Producer#{id}")
  defp vendor_uri(id), do: RDF.iri(@bsbm_inst <> "dataFromVendor/Vendor#{id}")
  defp offer_uri(prod_id, offer_id), do: RDF.iri(@bsbm_inst <> "dataFromVendor/Offer#{prod_id}_#{offer_id}")
  defp review_uri(prod_id, rev_id), do: RDF.iri(@bsbm_inst <> "dataFromRatingSite/Review#{prod_id}_#{rev_id}")
  defp reviewer_uri(prod_id, rev_id), do: RDF.iri(@bsbm_inst <> "dataFromRatingSite/Reviewer#{prod_id}_#{rev_id}")
  defp review_site_uri(id), do: RDF.iri(@bsbm_inst <> "dataFromRatingSite/RatingSite#{id}")

  # ===========================================================================
  # Private: Helpers
  # ===========================================================================

  defp bsbm(local_name), do: RDF.iri(@bsbm_ns <> local_name)
  defp rdf_type, do: RDF.iri(@rdf_ns <> "type")
  defp rdfs(local_name), do: RDF.iri(@rdfs_ns <> local_name)
  defp dc(local_name), do: RDF.iri(@dc_ns <> local_name)

  defp random_range(min, max) when min <= max do
    min + :rand.uniform(max - min + 1) - 1
  end

  defp random_in_range(range) do
    random_range(Enum.min(range), Enum.max(range))
  end

  defp random_date do
    year = random_range(2020, 2025)
    month = random_range(1, 12)
    day = random_range(1, 28)
    Date.new!(year, month, day)
  end

  defp date_literal(date) do
    RDF.literal(Date.to_iso8601(date), datatype: RDF.iri("http://www.w3.org/2001/XMLSchema#date"))
  end

  @countries ["US", "DE", "FR", "GB", "JP", "CN", "IN", "BR", "CA", "AU"]

  defp random_country do
    Enum.at(@countries, :rand.uniform(length(@countries)) - 1)
  end
end
