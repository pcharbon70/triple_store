defmodule TripleStore.Benchmark.LDBC.SNBBIGraphAdaptersTest do
  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Benchmark.LDBC.SNB.Mapping

  alias TripleStore.Benchmark.LDBC.SNB.BI.{
    Analytics,
    GraphAdapterRegistry,
    IndexAccess,
    Workload
  }

  alias TripleStore.QuadOperations

  test "registry covers every graph variant without promoting its capability" do
    assert GraphAdapterRegistry.keys() == [
             {10, "a"},
             {10, "b"},
             {15, "a"},
             {15, "b"},
             {19, "a"},
             {19, "b"},
             {20, "a"},
             {20, "b"},
             {20, "m"}
           ]

    assert {:ok, definitions} = Workload.load()

    for {number, variant} <- GraphAdapterRegistry.keys() do
      definition = Enum.find(definitions, &(&1.number == number and &1.variant == variant))
      assert {:ok, %{adapter: adapter}} = GraphAdapterRegistry.fetch(definition)
      assert {:module, ^adapter} = Code.ensure_loaded(adapter)
      assert function_exported?(adapter, :execute, 4)
    end
  end

  test "index scans observe cancellation and deadlines during enumeration", %{test: test} do
    with_store(test, fn store ->
      insert_entities(store, [{"Person", 1, %{}}, {"Person", 2, %{}}])
      insert_relationships(store, [{"knows", "Person", 1, "Person", 2, %{}}])

      cancel_key = {__MODULE__, :cancel_checks}
      Process.put(cancel_key, 0)

      cancelled? = fn ->
        checks = Process.get(cancel_key, 0)
        Process.put(cancel_key, checks + 1)
        checks >= 1
      end

      assert {:ok, cancelled_access} =
               store
               |> trusted_context()
               |> Map.put(:cancelled?, cancelled?)
               |> IndexAccess.new(timeout: 1_000)

      assert {:ok, person} = IndexAccess.entity(cancelled_access, "Person", 1)
      assert {:error, :cancelled} = IndexAccess.objects(cancelled_access, person, "knows")

      timeout_key = {__MODULE__, :timeout_checks}
      Process.put(timeout_key, 0)

      delay_after_open = fn ->
        checks = Process.get(timeout_key, 0)
        Process.put(timeout_key, checks + 1)
        if checks >= 1, do: Process.sleep(120)
        false
      end

      assert {:ok, timed_access} =
               store
               |> trusted_context()
               |> Map.put(:cancelled?, delay_after_open)
               |> IndexAccess.new(timeout: 100)

      assert {:error, :timeout} = IndexAccess.objects(timed_access, person, "knows")
    end)
  end

  test "BI 10 applies path bounds, country and tag-class filters, and distinct message counts",
       %{test: test} do
    with_store(test, fn store ->
      insert_entities(store, [
        {"Person", 1, %{}},
        {"Person", 2, %{}},
        {"Person", 3, %{}},
        {"Place", 10, %{"name" => "Toronto", "type" => "City"}},
        {"Place", 11, %{"name" => "Canada", "type" => "Country"}},
        {"TagClass", 20, %{"name" => "Technology"}},
        {"Tag", 21, %{"name" => "alpha"}},
        {"Tag", 22, %{"name" => "beta"}},
        {"Post", 30, %{}},
        {"Post", 31, %{}}
      ])

      insert_relationships(store, [
        {"knows", "Person", 1, "Person", 2, %{}},
        {"knows", "Person", 2, "Person", 3, %{}},
        {"isLocatedIn", "Person", 3, "Place", 10, %{}},
        {"isPartOf", "Place", 10, "Place", 11, %{}},
        {"hasCreator", "Post", 30, "Person", 3, %{}},
        {"hasCreator", "Post", 31, "Person", 3, %{}},
        {"hasTag", "Post", 30, "Tag", 21, %{}},
        {"hasTag", "Post", 30, "Tag", 22, %{}},
        {"hasTag", "Post", 31, "Tag", 21, %{}},
        {"hasType", "Tag", 21, "TagClass", 20, %{}}
      ])

      definition = definition!(10, "a")
      handler_id = "graph-adapter-#{System.unique_integer([:positive])}"

      :ok =
        :telemetry.attach(
          handler_id,
          [:triple_store, :benchmark, :ldbc, :snb_bi, :graph_operation, :stop],
          &__MODULE__.handle_graph_event/4,
          self()
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      parameters = %{
        "personId" => 1,
        "country" => "Canada",
        "tagClass" => "Technology",
        "minPathDistance" => 2,
        "maxPathDistance" => 2
      }

      assert {:ok, result} = Analytics.execute(definition, trusted_context(store), parameters)
      assert result.rows == [[3, "alpha", 2], [3, "beta", 1]]

      assert_receive {:graph_evidence, _event, %{row_count: 2}, metadata}
      assert metadata.operation == definition.id
      assert metadata.explain.operation == definition.id

      assert {:error, {:memory_bound_exceeded, 1}} =
               Analytics.execute(definition, trusted_context(store), parameters, max_visited: 1)

      assert {:error, {:neighbour_lookup_failed, _vertex, {:intermediate_bound_exceeded, 1}}} =
               Analytics.execute(definition, trusted_context(store), parameters,
                 max_intermediate: 1
               )

      assert {:error, _authorization_reason} =
               Analytics.execute(
                 definition,
                 Map.drop(store, [:path, :schema, :transaction]),
                 parameters
               )
    end)
  end

  test "BI 15 returns the exact fractional cost and the disconnected sentinel", %{test: test} do
    with_store(test, fn store ->
      build_weighted_fixture(store)
      definition = definition!(15, "a")

      assert {:ok, result} =
               Analytics.execute(definition, trusted_context(store), %{
                 "person1Id" => 1,
                 "person2Id" => 3,
                 "startDate" => "2012-01-01",
                 "endDate" => "2012-12-31"
               })

      assert result.rows == [[0.5]]

      assert {:ok, disconnected} =
               Analytics.execute(definition, trusted_context(store), %{
                 "person1Id" => 1,
                 "person2Id" => 4,
                 "startDate" => "2012-01-01",
                 "endDate" => "2012-12-31"
               })

      assert disconnected.rows == [[-1.0]]
    end)
  end

  test "BI 19 derives interaction weights and returns all globally cheapest endpoint ties",
       %{test: test} do
    with_store(test, fn store ->
      build_weighted_fixture(store)

      insert_entities(store, [
        {"Place", 50, %{"name" => "A", "type" => "City"}},
        {"Place", 51, %{"name" => "B", "type" => "City"}},
        {"Place", 52, %{"name" => "Not a city", "type" => "Country"}}
      ])

      insert_relationships(store, [
        {"isLocatedIn", "Person", 1, "Place", 50, %{}},
        {"isLocatedIn", "Person", 4, "Place", 50, %{}},
        {"isLocatedIn", "Person", 3, "Place", 51, %{}},
        {"isLocatedIn", "Forum", 40, "Place", 50, %{}},
        {"knows", "Person", 4, "Person", 3, %{}},
        {"knows", "Forum", 40, "Person", 3, %{}}
      ])

      add_reply_interactions(store, "Person", 1, 3, 300)
      add_reply_interactions(store, "Person", 4, 3, 303)
      add_reply_interactions(store, "Forum", 40, 3, 306)

      assert {:ok, result} =
               Analytics.execute(definition!(19, "b"), trusted_context(store), %{
                 "city1Id" => 50,
                 "city2Id" => 51
               })

      assert result.rows == [[1, 3, 38], [4, 3, 38]]

      assert {:error, {:invalid_city_reference, 52}} =
               Analytics.execute(definition!(19, "b"), trusted_context(store), %{
                 "city1Id" => 52,
                 "city2Id" => 51
               })
    end)
  end

  test "BI 20 derives study-year weights, keeps global ties, and rejects missing weights",
       %{test: test} do
    with_store(test, fn store ->
      insert_entities(store, [
        {"Person", 1, %{}},
        {"Person", 2, %{}},
        {"Person", 3, %{}},
        {"Organisation", 60, %{"name" => "Acme", "type" => "Company"}},
        {"Organisation", 61, %{"name" => "University", "type" => "University"}}
      ])

      insert_relationships(store, [
        {"knows", "Person", 1, "Person", 3, %{}},
        {"knows", "Person", 2, "Person", 3, %{}},
        {"workAt", "Person", 1, "Organisation", 60, %{"workFrom" => 2010}},
        {"workAt", "Person", 2, "Organisation", 60, %{"workFrom" => 2011}},
        {"studyAt", "Person", 1, "Organisation", 61, %{"classYear" => 2001}},
        {"studyAt", "Person", 2, "Organisation", 61, %{"classYear" => 2001}},
        {"studyAt", "Person", 3, "Organisation", 61, %{"classYear" => 2002}},
        {"workAt", "Organisation", 61, "Organisation", 60, %{"workFrom" => 2012}},
        {"knows", "Organisation", 61, "Person", 3, %{}},
        {"studyAt", "Organisation", 61, "Organisation", 61, %{"classYear" => 2002}}
      ])

      definition = definition!(20, "m")

      assert {:ok, result} =
               Analytics.execute(definition, trusted_context(store), %{
                 "company" => "Acme",
                 "person2Id" => 3
               })

      assert result.rows == [[1, 2], [2, 2]]

      insert_entities(store, [{"Person", 5, %{}}])

      insert_relationships(store, [
        {"knows", "Person", 5, "Person", 3, %{}},
        {"workAt", "Person", 5, "Organisation", 60, %{"workFrom" => 2012}},
        {"studyAt", "Person", 5, "Organisation", 61, %{}}
      ])

      assert {:error,
              {:neighbour_lookup_failed, _vertex,
               {:missing_edge_weight, :study_at, _person, _university}}} =
               Analytics.execute(definition, trusted_context(store), %{
                 "company" => "Acme",
                 "person2Id" => 3
               })
    end)
  end

  defp build_weighted_fixture(store) do
    insert_entities(
      store,
      Enum.map(1..4, &{"Person", &1, %{}}) ++
        [{"Forum", 40, %{"creationDate" => "2012-06-01T00:00:00Z"}}] ++
        for(id <- 100..105, do: {"Post", id, %{}}) ++
        for(id <- 200..205, do: {"Comment", id, %{}})
    )

    insert_relationships(store, [
      {"knows", "Person", 1, "Person", 2, %{}},
      {"knows", "Person", 2, "Person", 3, %{}},
      {"knows", "Person", 1, "Person", 3, %{}}
    ])

    interactions =
      Enum.flat_map([{1, 2, 100, 200}, {2, 3, 103, 203}], fn {left, right, post, comment} ->
        Enum.flat_map(0..2, fn offset ->
          post_id = post + offset
          comment_id = comment + offset

          [
            {"hasCreator", "Post", post_id, "Person", left, %{}},
            {"hasCreator", "Comment", comment_id, "Person", right, %{}},
            {"replyOf", "Comment", comment_id, "Post", post_id, %{}},
            {"containerOf", "Forum", 40, "Post", post_id, %{}}
          ]
        end)
      end)

    insert_relationships(store, interactions)
  end

  @doc false
  def handle_graph_event(event, measurements, metadata, pid) do
    send(pid, {:graph_evidence, event, measurements, metadata})
  end

  defp add_reply_interactions(store, left_type, left_id, right_id, first_post) do
    posts = for id <- first_post..(first_post + 2), do: {"Post", id, %{}}
    comments = for id <- (first_post + 100)..(first_post + 102), do: {"Comment", id, %{}}
    insert_entities(store, posts ++ comments)

    relationships =
      Enum.flat_map(0..2, fn offset ->
        post = first_post + offset
        comment = first_post + 100 + offset

        [
          {"hasCreator", "Post", post, left_type, left_id, %{}},
          {"hasCreator", "Comment", comment, "Person", right_id, %{}},
          {"replyOf", "Comment", comment, "Post", post, %{}}
        ]
      end)

    insert_relationships(store, relationships)
  end

  defp definition!(number, variant) do
    {:ok, definitions} = Workload.load()
    {:ok, definition} = Workload.fetch(definitions, number, variant)
    definition
  end

  defp trusted_context(store) do
    store
    |> Map.take([:db, :dict_manager])
    |> Map.put(:permit_all, true)
  end

  defp insert_entities(store, entities) do
    quads =
      Enum.flat_map(entities, fn {type, id, properties} ->
        {:ok, mapped} =
          Mapping.entity_quads(
            type,
            Map.put(properties, "id", to_string(id)),
            Mapping.graph_iri(:snb_bi, :initial)
          )

        mapped
      end)

    insert_quads(store, quads)
  end

  defp insert_relationships(store, relationships) do
    quads =
      Enum.flat_map(relationships, fn {type, from_type, from, to_type, to, properties} ->
        {:ok, mapped} =
          Mapping.relationship_quads(
            type,
            from_type,
            from,
            to_type,
            to,
            properties,
            Mapping.graph_iri(:snb_bi, :initial)
          )

        mapped
      end)

    insert_quads(store, quads)
  end

  defp insert_quads(store, quads) do
    assert {:ok, encoded} = Adapter.from_rdf_quads(store.dict_manager, quads)
    assert :ok = QuadOperations.insert_quads(store.db, encoded, [])
  end

  defp with_store(test, fun) do
    path =
      Path.join(
        System.tmp_dir!(),
        "snb_bi_graph_adapter_#{:erlang.phash2(test)}_#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(path)
    assert {:ok, store} = TripleStore.open(path, schema: :quad)

    try do
      fun.(store)
    after
      TripleStore.close(store)
      File.rm_rf!(path)
    end
  end
end
