defmodule TripleStore.Benchmark.WatDiv do
  @moduledoc """
  WatDiv (Waterloo SPARQL Diversity Test) data generator.

  Generates synthetic heterogeneous RDF datasets for benchmarking RDF stores.
  The data simulates an e-commerce scenario with users, products, reviews,
  offers, purchases, and social relationships.

  ## Scale Factor

  The scale factor determines the number of triples generated.
  WatDiv scale factor 1 generates approximately 100,000 triples.

  - Scale 1: ~100K triples
  - Scale 10: ~1M triples
  - Scale 100: ~10M triples

  ## Usage

      # Generate data for scale factor 1 (~100K triples)
      graph = TripleStore.Benchmark.WatDiv.generate(1)

      # Generate with seed for reproducibility
      graph = TripleStore.Benchmark.WatDiv.generate(1, seed: 12345)

      # Generate as stream for large datasets
      stream = TripleStore.Benchmark.WatDiv.stream(10)

  ## WatDiv Characteristics

  Unlike BSBM and LUBM, WatDiv uses:
  - **Heterogeneous structure**: Same entity types don't always have same attributes
  - **Probabilistic attributes**: Attributes appear with specific probabilities
  - **Correlated attributes**: Attributes grouped using pgroup construct

  This creates more realistic and challenging query patterns for testing.

  ## Ontology

  The WatDiv ontology defines:
  - User (social network users)
  - Product (goods for sale)
  - Review (product reviews)
  - Seller/Retailer (product sellers)
  - Offer (seller offers)
  - Purchase (purchase transactions)
  - Website (retailer websites)
  - Genre, SubGenre (product categorization)
  - City, Country, Language (metadata)

  """

  # WatDiv namespaces
  @wsdbm "http://db.uwaterloo.ca/~galuc/wsdbm/"
  @sorg "http://schema.org/"
  @mo "http://purl.org/ontology/mo/"
  @dc "http://purl.org/dc/terms/"
  @foaf "http://xmlns.com/foaf/"
  @gn "http://www.geonames.org/ontology#"
  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @xsd "http://www.w3.org/2001/XMLSchema#"

  # Entity counts at scale factor 1
  @num_users 1000
  @num_purchases 1500
  @num_offers 900
  @num_topics 250
  @num_products 250
  @num_cities 240
  @num_subgenres 145
  @num_websites 50
  @num_languages 25
  @num_countries 25
  @num_genres 21
  @num_product_categories 15
  @num_retailers 12
  @num_age_groups 9
  @num_roles 3
  @num_genders 2

  @typedoc "Generator options"
  @type opts :: [
          seed: integer(),
          stream: boolean()
        ]

  @doc """
  Generates WatDiv benchmark data as an RDF.Graph.

  ## Arguments

  - `scale_factor` - Scale factor (1 = ~100K triples)

  ## Options

  - `:seed` - Random seed for reproducible generation (default: based on scale_factor)

  ## Returns

  An `RDF.Graph` containing the generated triples.

  ## Examples

      graph = TripleStore.Benchmark.WatDiv.generate(1)
      RDF.Graph.triple_count(graph)
      # => ~100000

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
  Generates WatDiv benchmark data as a stream of triples.

  Useful for large scale factors where holding all triples in memory
  is not feasible.

  ## Arguments

  - `scale_factor` - Scale factor

  ## Options

  - `:seed` - Random seed for reproducible generation

  ## Returns

  A stream of `{subject, predicate, object}` triples.

  ## Examples

      stream = TripleStore.Benchmark.WatDiv.stream(10)
      Enum.take(stream, 1000)

  """
  @spec stream(pos_integer(), opts()) :: Enumerable.t()
  def stream(scale_factor, opts \\ []) when scale_factor > 0 do
    seed = Keyword.get(opts, :seed, scale_factor * 42)

    Stream.resource(
      fn ->
        :rand.seed(:exsss, {seed, seed * 2, seed * 3})
        state = init_state(scale_factor)
        {:genres, state, 1}
      end,
      fn
        {:genres, state, n} when n <= state.num_genres ->
          triples = generate_genre(n, state)
          {triples, {:genres, state, n + 1}}

        {:genres, state, _} ->
          {[], {:subgenres, state, 1}}

        {:subgenres, state, n} when n <= state.num_subgenres ->
          triples = generate_subgenre(n, state)
          {triples, {:subgenres, state, n + 1}}

        {:subgenres, state, _} ->
          {[], {:product_categories, state, 1}}

        {:product_categories, state, n} when n <= state.num_product_categories ->
          triples = generate_product_category(n, state)
          {triples, {:product_categories, state, n + 1}}

        {:product_categories, state, _} ->
          {[], {:cities, state, 1}}

        {:cities, state, n} when n <= state.num_cities ->
          triples = generate_city(n, state)
          {triples, {:cities, state, n + 1}}

        {:cities, state, _} ->
          {[], {:countries, state, 1}}

        {:countries, state, n} when n <= state.num_countries ->
          triples = generate_country(n)
          {triples, {:countries, state, n + 1}}

        {:countries, state, _} ->
          {[], {:languages, state, 1}}

        {:languages, state, n} when n <= state.num_languages ->
          triples = generate_language(n)
          {triples, {:languages, state, n + 1}}

        {:languages, state, _} ->
          {[], {:topics, state, 1}}

        {:topics, state, n} when n <= state.num_topics ->
          triples = generate_topic(n, state)
          {triples, {:topics, state, n + 1}}

        {:topics, state, _} ->
          {[], {:genders, state, 1}}

        {:genders, state, n} when n <= state.num_genders ->
          triples = generate_gender(n)
          {triples, {:genders, state, n + 1}}

        {:genders, state, _} ->
          {[], {:age_groups, state, 1}}

        {:age_groups, state, n} when n <= state.num_age_groups ->
          triples = generate_age_group(n)
          {triples, {:age_groups, state, n + 1}}

        {:age_groups, state, _} ->
          {[], {:roles, state, 1}}

        {:roles, state, n} when n <= state.num_roles ->
          triples = generate_role(n)
          {triples, {:roles, state, n + 1}}

        {:roles, state, _} ->
          {[], {:retailers, state, 1}}

        {:retailers, state, n} when n <= state.num_retailers ->
          triples = generate_retailer(n, state)
          {triples, {:retailers, state, n + 1}}

        {:retailers, state, _} ->
          {[], {:websites, state, 1}}

        {:websites, state, n} when n <= state.num_websites ->
          triples = generate_website(n, state)
          {triples, {:websites, state, n + 1}}

        {:websites, state, _} ->
          {[], {:products, state, 1}}

        {:products, state, n} when n <= state.num_products ->
          triples = generate_product(n, state)
          {triples, {:products, state, n + 1}}

        {:products, state, _} ->
          {[], {:users, state, 1}}

        {:users, state, n} when n <= state.num_users ->
          triples = generate_user(n, state)
          {triples, {:users, state, n + 1}}

        {:users, state, _} ->
          {[], {:offers, state, 1}}

        {:offers, state, n} when n <= state.num_offers ->
          triples = generate_offer(n, state)
          {triples, {:offers, state, n + 1}}

        {:offers, state, _} ->
          {[], {:purchases, state, 1}}

        {:purchases, state, n} when n <= state.num_purchases ->
          triples = generate_purchase(n, state)
          {triples, {:purchases, state, n + 1}}

        {:purchases, _state, _} ->
          {:halt, nil}
      end,
      fn _ -> :ok end
    )
  end

  @doc """
  Returns the estimated triple count for a given scale factor.

  ## Examples

      TripleStore.Benchmark.WatDiv.estimate_triple_count(1)
      # => ~100000

  """
  @spec estimate_triple_count(pos_integer()) :: pos_integer()
  def estimate_triple_count(scale_factor) do
    # Base counts at scale factor 1 (~38K based on actual generation)
    base_triples = 40_000
    trunc(base_triples * scale_factor)
  end

  @doc """
  Returns the WatDiv vocabulary namespace.
  """
  @spec namespace() :: String.t()
  def namespace, do: @wsdbm

  # ===========================================================================
  # Private: State Initialization
  # ===========================================================================

  defp init_state(scale_factor) do
    %{
      scale_factor: scale_factor,
      num_users: @num_users * scale_factor,
      num_purchases: @num_purchases * scale_factor,
      num_offers: @num_offers * scale_factor,
      num_topics: @num_topics,
      num_products: @num_products * scale_factor,
      num_cities: @num_cities,
      num_subgenres: @num_subgenres * scale_factor,
      num_websites: @num_websites * scale_factor,
      num_languages: @num_languages,
      num_countries: @num_countries,
      num_genres: @num_genres * scale_factor,
      num_product_categories: @num_product_categories,
      num_retailers: @num_retailers,
      num_age_groups: @num_age_groups,
      num_roles: @num_roles,
      num_genders: @num_genders,
      # Keep track of friendship IDs to avoid duplicates
      friendship_ids: :ets.new(:friendships, [:set]),
      like_ids: :ets.new(:likes, [:set])
    }
  end

  # ===========================================================================
  # Private: Generation Functions
  # ===========================================================================

  defp generate_all(state) do
    # Static metadata (doesn't scale much)
    genre_triples = Enum.flat_map(1..state.num_genres, &generate_genre(&1, state))
    subgenre_triples = Enum.flat_map(1..state.num_subgenres, &generate_subgenre(&1, state))

    category_triples =
      Enum.flat_map(1..state.num_product_categories, &generate_product_category(&1, state))

    city_triples = Enum.flat_map(1..state.num_cities, &generate_city(&1, state))
    country_triples = Enum.flat_map(1..state.num_countries, &generate_country/1)
    language_triples = Enum.flat_map(1..state.num_languages, &generate_language/1)
    topic_triples = Enum.flat_map(1..state.num_topics, &generate_topic(&1, state))
    gender_triples = Enum.flat_map(1..state.num_genders, &generate_gender/1)
    age_group_triples = Enum.flat_map(1..state.num_age_groups, &generate_age_group/1)
    role_triples = Enum.flat_map(1..state.num_roles, &generate_role/1)

    # Retailers and websites
    retailer_triples = Enum.flat_map(1..state.num_retailers, &generate_retailer(&1, state))
    website_triples = Enum.flat_map(1..state.num_websites, &generate_website(&1, state))

    # Main entities (scale with scale factor)
    product_triples = Enum.flat_map(1..state.num_products, &generate_product(&1, state))
    user_triples = Enum.flat_map(1..state.num_users, &generate_user(&1, state))
    offer_triples = Enum.flat_map(1..state.num_offers, &generate_offer(&1, state))
    purchase_triples = Enum.flat_map(1..state.num_purchases, &generate_purchase(&1, state))

    genre_triples ++
      subgenre_triples ++
      category_triples ++
      city_triples ++
      country_triples ++
      language_triples ++
      topic_triples ++
      gender_triples ++
      age_group_triples ++
      role_triples ++
      retailer_triples ++
      website_triples ++
      product_triples ++ user_triples ++ offer_triples ++ purchase_triples
  end

  # ===========================================================================
  # Static Metadata Entities
  # ===========================================================================

  defp generate_genre(genre_id, _state) do
    genre_uri = genre_uri(genre_id)

    [
      {genre_uri, rdf_type(), sorg("Genre")},
      {genre_uri, sorg("title"), RDF.literal("Genre#{genre_id}")}
    ]
  end

  defp generate_subgenre(subgenre_id, state) do
    subgenre_uri = subgenre_uri(subgenre_id)
    genre_id = rem(subgenre_id - 1, state.num_genres) + 1

    [
      {subgenre_uri, rdf_type(), sorg("Genre")},
      {subgenre_uri, sorg("title"), RDF.literal("SubGenre#{subgenre_id}")},
      {subgenre_uri, sorg("genre"), genre_uri(genre_id)}
    ]
  end

  defp generate_product_category(cat_id, _state) do
    cat_uri = product_category_uri(cat_id)

    base = [
      {cat_uri, rdf_type(), sorg("ProductCategory")},
      {cat_uri, sorg("title"), RDF.literal("ProductCategory#{cat_id}")}
    ]

    # Add category-specific attributes
    category_specific =
      case cat_id do
        # Book category
        1 ->
          [
            {cat_uri, sorg("isbn"), RDF.literal("978-#{cat_id}-#{random_int(1000, 9999)}")}
          ]

        # Movie category
        2 ->
          [
            {cat_uri, sorg("duration"), RDF.literal("PT#{random_int(80, 180)}M")}
          ]

        _ ->
          []
      end

    base ++ category_specific
  end

  defp generate_city(city_id, state) do
    city_uri = city_uri(city_id)
    country_id = rem(city_id - 1, state.num_countries) + 1

    [
      {city_uri, rdf_type(), gn("City")},
      {city_uri, sorg("name"), RDF.literal("City#{city_id}")},
      {city_uri, gn("parentCountry"), country_uri(country_id)}
    ]
  end

  defp generate_country(country_id) do
    country_uri = country_uri(country_id)

    [
      {country_uri, rdf_type(), sorg("Country")},
      {country_uri, sorg("name"), RDF.literal("Country#{country_id}")}
    ]
  end

  defp generate_language(lang_id) do
    lang_uri = language_uri(lang_id)

    [
      {lang_uri, rdf_type(), sorg("Language")},
      {lang_uri, sorg("name"), RDF.literal("Language#{lang_id}")}
    ]
  end

  defp generate_topic(topic_id, _state) do
    topic_uri = topic_uri(topic_id)

    [
      {topic_uri, rdf_type(), sorg("Topic")},
      {topic_uri, dc("title"), RDF.literal("Topic#{topic_id}")}
    ]
  end

  defp generate_gender(gender_id) do
    gender_uri = gender_uri(gender_id)

    names = ["Male", "Female", "Other"]
    name = Enum.at(names, gender_id - 1) || "Gender#{gender_id}"

    [
      {gender_uri, rdf_type(), sorg("Gender")},
      {gender_uri, sorg("name"), RDF.literal(name)}
    ]
  end

  defp generate_age_group(age_id) do
    age_uri = age_group_uri(age_id)

    [
      {age_uri, rdf_type(), sorg("PeopleAudience")},
      {age_uri, sorg("suggestedMinAge"), RDF.literal(age_id * 10)},
      {age_uri, sorg("suggestedMaxAge"), RDF.literal((age_id + 1) * 10)}
    ]
  end

  defp generate_role(role_id) do
    role_uri = role_uri(role_id)

    roles = ["User", "Reviewer", "Moderator"]
    name = Enum.at(roles, role_id - 1) || "Role#{role_id}"

    [
      {role_uri, rdf_type(), sorg("Role")},
      {role_uri, sorg("name"), RDF.literal(name)}
    ]
  end

  # ===========================================================================
  # Retailers and Websites
  # ===========================================================================

  defp generate_retailer(retailer_id, state) do
    retailer_uri = retailer_uri(retailer_id)
    country_id = rem(retailer_id - 1, state.num_countries) + 1

    [
      {retailer_uri, rdf_type(), wsdbm("Retailer")},
      {retailer_uri, wsdbm("country"), country_uri(country_id)}
    ]
  end

  defp generate_website(website_id, state) do
    website_uri = website_uri(website_id)
    _retailer_id = rem(website_id - 1, state.num_retailers) + 1

    [
      {website_uri, rdf_type(), sorg("WebSite")},
      {website_uri, sorg("url"), RDF.literal("http://retailer#{website_id}.com")},
      {website_uri, sorg("name"), RDF.literal("Retailer#{website_id} Website")}
    ]
  end

  # ===========================================================================
  # Products
  # ===========================================================================

  defp generate_product(product_id, state) do
    product_uri = product_uri(product_id)
    cat_id = rem(product_id - 1, state.num_product_categories) + 1
    subgenre_id = rem(product_id - 1, state.num_subgenres) + 1

    base_triples = [
      {product_uri, rdf_type(), wsdbm("ProductCategory#{cat_id}")},
      {product_uri, dc("title"), RDF.literal("Product#{product_id}")},
      {product_uri, sorg("description"), RDF.literal("Description for product #{product_id}")}
    ]

    # Add genre/subgenre with probability
    genre_triples =
      if :rand.uniform() < 0.8 do
        [{product_uri, sorg("genre"), subgenre_uri(subgenre_id)}]
      else
        []
      end

    # Add publisher with probability
    publisher_triples =
      if :rand.uniform() < 0.5 do
        retailer_id = rem(product_id - 1, state.num_retailers) + 1
        [{product_uri, dc("publisher"), retailer_uri(retailer_id)}]
      else
        []
      end

    # Add language with probability
    language_triples =
      if :rand.uniform() < 0.3 do
        lang_id = rem(product_id - 1, state.num_languages) + 1
        [{product_uri, dc("language"), language_uri(lang_id)}]
      else
        []
      end

    # Add category-specific attributes
    category_triples = generate_product_category_attributes(product_uri, cat_id, product_id)

    base_triples ++ genre_triples ++ publisher_triples ++ language_triples ++ category_triples
  end

  defp generate_product_category_attributes(product_uri, cat_id, _product_id) do
    case cat_id do
      # Book - ProductCategory1
      1 ->
        [
          {product_uri, sorg("isbn"), RDF.literal("978-#{cat_id}-#{random_int(1000, 9999)}")},
          maybe_add(
            product_uri,
            sorg("bookEdition"),
            RDF.literal("Edition#{random_int(1, 5)}"),
            0.5
          ),
          maybe_add(product_uri, sorg("numberOfPages"), RDF.literal(random_int(100, 1000)), 0.25)
        ]

      # Movie - ProductCategory2
      2 ->
        [
          {product_uri, mo("imdb"), RDF.literal("tt#{random_int(100_000, 999_999)}")},
          maybe_add(product_uri, sorg("duration"), RDF.literal("PT#{random_int(80, 180)}M"), 0.9)
        ]

      # Other categories
      _ ->
        [
          maybe_add(product_uri, sorg("weight"), RDF.literal("#{random_int(100, 5000)} g"), 0.3)
        ]
    end
    |> Enum.filter(fn
      nil -> false
      _ -> true
    end)
  end

  # ===========================================================================
  # Users
  # ===========================================================================

  defp generate_user(user_id, state) do
    user_uri = user_uri(user_id)
    city_id = rem(user_id - 1, state.num_cities) + 1
    gender_id = rem(user_id - 1, state.num_genders) + 1
    age_id = rem(user_id - 1, state.num_age_groups) + 1

    base_triples = [
      {user_uri, rdf_type(), wsdbm("User")},
      {user_uri, sorg("name"), RDF.literal("User#{user_id}")},
      {user_uri, sorg("email"), RDF.literal("user#{user_id}@example.com")},
      {user_uri, wsdbm("gender"), gender_uri(gender_id)},
      {user_uri, wsdbm("ageGroup"), age_group_uri(age_id)},
      {user_uri, wsdbm("livesIn"), city_uri(city_id)}
    ]

    # Add homepage with probability
    homepage_triples =
      if :rand.uniform() < 0.3 do
        [{user_uri, foaf("homepage"), RDF.literal("http://user#{user_id}.com")}]
      else
        []
      end

    # Add interests (topics)
    num_interests = random_int(1, 5)

    interest_triples =
      Enum.map(1..num_interests, fn _ ->
        topic_id = :rand.uniform(state.num_topics)
        {user_uri, dc("interest"), topic_uri(topic_id)}
      end)

    # Add friends (friendship relationships)
    num_friends = random_int(1, 10)

    friend_triples =
      Enum.map(1..num_friends, fn _ ->
        friend_id = random_int(1, state.num_users)
        friend_uri = user_uri(friend_id)

        # Create friendship URI (lower ID first to avoid duplicates)
        friendship_id =
          if user_id < friend_id, do: "#{user_id}-#{friend_id}", else: "#{friend_id}-#{user_id}"

        if user_id != friend_id and
             not :ets.member(state.friendship_ids, friendship_id) do
          :ets.insert(state.friendship_ids, {friendship_id, true})
          {user_uri, wsdbm("friendOf"), friend_uri}
        else
          nil
        end
      end)
      |> Enum.filter(fn x -> x != nil end)

    base_triples ++ homepage_triples ++ interest_triples ++ friend_triples
  end

  # ===========================================================================
  # Offers
  # ===========================================================================

  defp generate_offer(offer_id, state) do
    offer_uri = offer_uri(offer_id)
    product_id = rem(offer_id - 1, state.num_products) + 1
    retailer_id = rem(offer_id - 1, state.num_retailers) + 1
    website_id = rem(offer_id - 1, state.num_websites) + 1

    price = random_int(100, 10_000) / 100.0
    valid_from = random_date()
    valid_to = Date.add(valid_from, random_int(30, 365))

    delivery_days = random_int(1, 30)
    delivery_date = Date.add(valid_from, delivery_days)

    [
      {offer_uri, rdf_type(), wsdbm("Offer")},
      {offer_uri, wsdbm("product"), product_uri(product_id)},
      {offer_uri, wsdbm("retailer"), retailer_uri(retailer_id)},
      {offer_uri, wsdbm("website"), website_uri(website_id)},
      {offer_uri, sorg("price"), RDF.literal(price)},
      {offer_uri, sorg("priceCurrency"), RDF.literal("USD")},
      {offer_uri, sorg("validFrom"), date_literal(valid_from)},
      {offer_uri, sorg("validThrough"), date_literal(valid_to)},
      {offer_uri, wsdbm("deliveryDays"), RDF.literal(delivery_days)},
      {offer_uri, wsdbm("deliveryDate"), date_literal(delivery_date)},
      {offer_uri, wsdbm("eligibleRegion"),
       country_uri(rem(offer_id - 1, state.num_countries) + 1)},
      {offer_uri, wsdbm("availability"),
       RDF.literal(if :rand.uniform() < 0.8, do: "InStock", else: "OutOfStock")}
    ]
  end

  # ===========================================================================
  # Purchases
  # ===========================================================================

  defp generate_purchase(purchase_id, state) do
    purchase_uri = purchase_uri(purchase_id)
    user_id = rem(purchase_id - 1, state.num_users) + 1
    product_id = rem(purchase_id - 1, state.num_products) + 1

    purchase_date = random_date()

    base_triples = [
      {purchase_uri, rdf_type(), wsdbm("Purchase")},
      {purchase_uri, wsdbm("purchaseDate"), date_literal(purchase_date)}
    ]

    # Add buyer
    buyer_triples = [
      {purchase_uri, wsdbm("buyer"), user_uri(user_id)}
    ]

    # Add product likes
    num_likes = random_int(1, 5)

    like_triples =
      Enum.map(1..num_likes, fn _ ->
        like_user_id = random_int(1, state.num_users)
        _like_uri = like_uri(purchase_id, like_user_id)

        if like_user_id != user_id do
          {user_uri(like_user_id), wsdbm("likes"), product_uri(product_id)}
        else
          nil
        end
      end)
      |> Enum.filter(fn x -> x != nil end)

    base_triples ++ buyer_triples ++ like_triples
  end

  # ===========================================================================
  # URI Generators
  # ===========================================================================

  defp user_uri(id), do: RDF.iri(@wsdbm <> "User#{id}")
  defp product_uri(id), do: RDF.iri(@wsdbm <> "Product#{id}")
  defp offer_uri(id), do: RDF.iri(@wsdbm <> "Offer#{id}")
  defp purchase_uri(id), do: RDF.iri(@wsdbm <> "Purchase#{id}")
  defp like_uri(purchase_id, user_id), do: RDF.iri(@wsdbm <> "Like#{purchase_id}_#{user_id}")

  defp retailer_uri(id), do: RDF.iri(@wsdbm <> "Retailer#{id}")
  defp website_uri(id), do: RDF.iri(@wsdbm <> "Website#{id}")

  defp genre_uri(id), do: RDF.iri(@wsdbm <> "Genre#{id}")
  defp subgenre_uri(id), do: RDF.iri(@wsdbm <> "SubGenre#{id}")
  defp product_category_uri(id), do: RDF.iri(@wsdbm <> "ProductCategory#{id}")
  defp topic_uri(id), do: RDF.iri(@wsdbm <> "Topic#{id}")

  defp city_uri(id), do: RDF.iri(@wsdbm <> "City#{id}")
  defp country_uri(id), do: RDF.iri(@wsdbm <> "Country#{id}")
  defp language_uri(id), do: RDF.iri(@wsdbm <> "Language#{id}")

  defp gender_uri(id), do: RDF.iri(@wsdbm <> "Gender#{id}")
  defp age_group_uri(id), do: RDF.iri(@wsdbm <> "AgeGroup#{id}")
  defp role_uri(id), do: RDF.iri(@wsdbm <> "Role#{id}")

  # Namespace helpers
  defp wsdbm(local_name), do: RDF.iri(@wsdbm <> local_name)
  defp sorg(local_name), do: RDF.iri(@sorg <> local_name)
  defp mo(local_name), do: RDF.iri(@mo <> local_name)
  defp dc(local_name), do: RDF.iri(@dc <> local_name)
  defp foaf(local_name), do: RDF.iri(@foaf <> local_name)
  defp gn(local_name), do: RDF.iri(@gn <> local_name)
  defp rdf_type, do: RDF.iri(@rdf <> "type")

  # ===========================================================================
  # Private: Helpers
  # ===========================================================================

  defp random_int(min, max) when min <= max do
    min + :rand.uniform(max - min + 1) - 1
  end

  defp random_date do
    year = random_int(2020, 2025)
    month = random_int(1, 12)
    day = random_int(1, 28)
    Date.new!(year, month, day)
  end

  defp date_literal(date) do
    RDF.literal(Date.to_iso8601(date), datatype: RDF.iri(@xsd <> "date"))
  end

  # Add a triple only with given probability
  defp maybe_add(subject, predicate, object, probability) do
    if :rand.uniform() < probability do
      {subject, predicate, object}
    else
      nil
    end
  end
end
