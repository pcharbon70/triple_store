defmodule TripleStore.Benchmark.LDBC.SNB.BI.UpdateSchema do
  @moduledoc """
  Versioned validation and mapped-RDF translation for SNB BI update records.

  The catalog is tied to the pinned BI generator and RDF mapping. Records are
  rejected before mutation when their type, fields, identity, or within-batch
  dependency order is ambiguous. Delete translation derives an exact cleanup
  set from the current explicit graph and fails when the target cannot be
  proven to exist.
  """

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Benchmark.LDBC.SNB.Mapping
  alias TripleStore.QuadIndex

  @schema_version 1
  @catalog_version "v1.0.3"
  @generator_source "snb-bi-datagen-0.5.1"
  @generator_version "v0.5.1"
  @generator_pin "2459f4e45834c78902a50511fc64a05c48dd4029"
  @rdf_type RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
  @rdf_subject RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#subject")
  @rdf_predicate RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate")
  @rdf_object RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#object")
  @ontology "https://ldbcouncil.org/snb/ontology/"

  @type record :: %{sequence: pos_integer(), type: String.t(), fields: map()}

  @doc "Loads and verifies the immutable update schema catalog."
  @spec catalog() :: {:ok, map()} | {:error, term()}
  def catalog do
    path =
      Path.join([
        to_string(:code.priv_dir(:triple_store)),
        "benchmarks",
        "ldbc",
        "snb-bi",
        "updates",
        "manifest.exs"
      ])

    with {catalog, _binding} <- Code.eval_file(path),
         :ok <- validate_catalog(catalog) do
      {:ok, catalog}
    else
      {:error, _} = error -> error
    end
  rescue
    error -> {:error, {:invalid_update_schema_catalog, Exception.message(error)}}
  end

  @doc "Returns one deterministic example for every mandatory record type."
  @spec fixture_records() :: [record()]
  def fixture_records do
    {:ok, value} = catalog()

    value.fixture_records
    |> Enum.with_index(1)
    |> Enum.map(fn {record, sequence} -> Map.put(record, :sequence, sequence) end)
  end

  @doc "Proves that the checked-in examples cover every catalog record type."
  @spec verify_fixture_coverage() :: :ok | {:error, term()}
  def verify_fixture_coverage do
    with {:ok, value} <- catalog() do
      expected = MapSet.new(value.record_types, & &1.id)
      actual = MapSet.new(value.fixture_records, & &1.type)

      if expected == actual,
        do: :ok,
        else: {:error, {:incomplete_update_fixture_coverage, MapSet.difference(expected, actual)}}
    end
  end

  @doc "Validates one record independently of stream ordering."
  @spec validate_record(map()) :: :ok | {:error, term()}
  def validate_record(record), do: validate_records([record])

  @doc "Validates a complete ordered microbatch before translation."
  @spec validate_records([map()]) :: :ok | {:error, term()}
  def validate_records(records) when is_list(records),
    do: validate_record_source(fn -> records end)

  def validate_records(_records), do: {:error, :invalid_update_records}

  @doc "Validates a replayable record stream without retaining every record."
  @spec validate_stream((-> Enumerable.t())) :: :ok | {:error, term()}
  def validate_stream(record_source) when is_function(record_source, 0),
    do: validate_record_source(record_source)

  def validate_stream(_record_source), do: {:error, :invalid_update_records}

  @doc "Returns the pinned schema for one canonical update record type."
  @spec record_schema(String.t()) :: {:ok, map()} | {:error, term()}
  def record_schema(type) when is_binary(type) do
    with {:ok, value} <- catalog(),
         %{} = schema <- Enum.find(value.record_types, &(&1.id == type)) do
      {:ok, schema}
    else
      nil -> {:error, {:unknown_update_type, type}}
      {:error, _} = error -> error
    end
  end

  def record_schema(type), do: {:error, {:unknown_update_type, type}}

  @doc "Normalizes delimited source values according to the pinned field catalog."
  @spec normalize_source_fields(String.t(), map()) :: {:ok, map()} | {:error, term()}
  def normalize_source_fields(type, raw_fields) when is_binary(type) and is_map(raw_fields) do
    with {:ok, schema} <- record_schema(type),
         :ok <- validate_source_extras(schema, raw_fields),
         {:ok, fields} <- normalize_schema_fields(schema, raw_fields),
         {:cont, :ok} <- validate_fields(0, fields, schema) do
      {:ok, fields}
    else
      {:halt, {:error, reason}} -> {:error, reason}
      {:error, _} = error -> error
    end
  end

  def normalize_source_fields(_type, _raw_fields), do: {:error, :invalid_update_fields}

  defp validate_record_source(record_source) do
    case catalog() do
      {:ok, value} ->
        schemas = Map.new(value.record_types, &{&1.id, &1})

        with {:ok, insert_positions} <- validate_first_pass(record_source.(), schemas) do
          validate_dependency_order(record_source.(), schemas, insert_positions)
        end

      {:error, _} = error ->
        error
    end
  end

  @doc "Translates validated records into ordered canonical RDF quad mutations."
  @spec translate_records(TripleStore.store() | nil, [record()]) ::
          {:ok, [%{operation: :insert | :delete, quads: [RDF.Quad.t()], sequence: pos_integer()}]}
          | {:error, term()}
  def translate_records(store, records) do
    with :ok <- validate_records(records),
         {:ok, catalog} <- catalog(),
         {:ok, initial} <- current_quads(store),
         schemas = Map.new(catalog.record_types, &{&1.id, &1}),
         {:ok, translated, _state} <-
           translate_in_order(records, initial, [], schemas, not is_nil(store)) do
      {:ok, translated}
    end
  end

  defp validate_catalog(%{
         schema_version: @schema_version,
         operation_catalog_version: @catalog_version,
         generator: %{
           source_id: @generator_source,
           version: @generator_version,
           pin: @generator_pin
         },
         mapping_version: mapping,
         record_types: types,
         fixture_records: fixtures
       })
       when is_list(types) and is_list(fixtures) do
    ids = Enum.map(types, & &1.id)

    expected =
      for operation <- ~w(insert delete), number <- 1..8, do: "#{operation}-#{pad(number)}"

    if mapping == Mapping.version() and Enum.sort(ids) == Enum.sort(expected) and
         length(Enum.uniq(ids)) == 16,
       do: :ok,
       else: {:error, :incomplete_update_schema_catalog}
  end

  defp validate_catalog(_catalog), do: {:error, :update_schema_pin_mismatch}

  defp validate_shape(
         %{sequence: sequence, type: type, fields: fields},
         :ok,
         schemas
       )
       when is_integer(sequence) and sequence > 0 and is_binary(type) and is_map(fields) do
    case Map.fetch(schemas, type) do
      {:ok, schema} -> validate_fields(sequence, fields, schema)
      :error -> {:halt, {:error, {:unknown_update_type, type}}}
    end
  end

  defp validate_shape(record, :ok, _schemas),
    do: {:halt, {:error, {:invalid_update_record, record}}}

  defp validate_fields(sequence, fields, schema) do
    allowed = MapSet.new(schema.required_fields ++ schema.optional_fields)
    actual = MapSet.new(Map.keys(fields))
    unknown = actual |> MapSet.difference(allowed) |> MapSet.to_list() |> Enum.sort()
    missing = allowed_fields_missing(fields, schema.required_fields)

    cond do
      unknown != [] ->
        {:halt, {:error, {:unknown_update_fields, sequence, unknown}}}

      missing != [] ->
        {:halt, {:error, {:missing_update_fields, sequence, missing}}}

      invalid = invalid_field(fields, schema.field_types) ->
        {:halt, {:error, {:invalid_update_field, sequence, invalid}}}

      schema.id == "insert-07" ->
        validate_reply_target(sequence, fields)

      true ->
        {:cont, :ok}
    end
  end

  defp validate_reply_target(sequence, fields) do
    targets = Enum.count(~w(replyToPostId replyToCommentId), &present?(Map.get(fields, &1)))
    if targets == 1, do: {:cont, :ok}, else: {:halt, {:error, {:invalid_reply_target, sequence}}}
  end

  defp allowed_fields_missing(fields, required) do
    required
    |> Enum.reject(&Map.has_key?(fields, &1))
    |> Enum.sort()
  end

  defp invalid_field(fields, field_types) do
    Enum.find_value(field_types, &invalid_field_name(fields, &1))
  end

  defp invalid_field_name(fields, {field, type}) do
    case Map.fetch(fields, field) do
      {:ok, value} -> if valid_field_value?(value, type), do: nil, else: field
      :error -> nil
    end
  end

  defp valid_field_value?(value, :identifier),
    do: (is_integer(value) and value >= 0) or non_negative_integer_string?(value)

  defp valid_field_value?(value, :string), do: is_binary(value)
  defp valid_field_value?(value, :date), do: valid_iso_date?(value)

  defp valid_field_value?(value, type) when type in [:integer, :timestamp],
    do: is_integer(value) or integer_string?(value)

  defp valid_field_value?(values, {:list, item_type}) when is_list(values),
    do: Enum.all?(values, &valid_field_value?(&1, item_type))

  defp valid_field_value?(
         %{} = pair,
         {:pair, identity_field, property_field}
       ) do
    Map.keys(pair) |> Enum.sort() == Enum.sort([identity_field, property_field]) and
      valid_field_value?(Map.get(pair, identity_field), :identifier) and
      valid_field_value?(Map.get(pair, property_field), :integer)
  end

  defp valid_field_value?(_value, _type), do: false

  defp integer_string?(value) when is_binary(value) do
    case Integer.parse(value) do
      {_integer, ""} -> true
      _other -> false
    end
  end

  defp integer_string?(_value), do: false

  defp non_negative_integer_string?(value) do
    integer_string?(value) and String.to_integer(value) >= 0
  end

  defp valid_iso_date?(value) when is_binary(value),
    do: match?({:ok, _date}, Date.from_iso8601(value))

  defp valid_iso_date?(_value), do: false

  defp validate_source_extras(schema, raw_fields) do
    allowed = MapSet.new(schema.required_fields ++ schema.optional_fields ++ ~w(sequence type))

    extras =
      raw_fields
      |> Enum.filter(fn {field, value} ->
        not MapSet.member?(allowed, field) and value not in [nil, ""]
      end)
      |> Enum.map(&elem(&1, 0))
      |> Enum.sort()

    if extras == [], do: :ok, else: {:error, {:unknown_update_source_fields, extras}}
  end

  defp normalize_schema_fields(schema, raw_fields) do
    fields = schema.required_fields ++ schema.optional_fields

    Enum.reduce_while(fields, {:ok, %{}}, fn field, acc ->
      normalize_schema_field(field, acc, schema, raw_fields)
    end)
  end

  defp normalize_schema_field(field, {:ok, normalized}, schema, raw_fields) do
    value = Map.get(raw_fields, field)

    if field in schema.optional_fields and value in [nil, ""] do
      {:cont, {:ok, normalized}}
    else
      normalize_and_put_field(normalized, field, value, schema.field_types)
    end
  end

  defp normalize_and_put_field(normalized, field, value, field_types) do
    case normalize_source_value(value, Map.fetch!(field_types, field)) do
      {:ok, normalized_value} -> {:cont, {:ok, Map.put(normalized, field, normalized_value)}}
      :error -> {:halt, {:error, {:invalid_update_source_field, field, value}}}
    end
  end

  defp normalize_source_value(value, :identifier)
       when is_binary(value) and value != "",
       do: {:ok, value}

  defp normalize_source_value(value, type) when type in [:string, :date] and is_binary(value),
    do: {:ok, value}

  defp normalize_source_value(value, type) when type in [:integer, :timestamp] do
    case value do
      integer when is_integer(integer) -> {:ok, integer}
      binary when is_binary(binary) -> parse_integer(binary)
      _other -> :error
    end
  end

  defp normalize_source_value(nil, {:list, _item_type}), do: {:ok, []}
  defp normalize_source_value("", {:list, _item_type}), do: {:ok, []}

  defp normalize_source_value(value, {:list, {:pair, identity_field, property_field}})
       when is_binary(value) do
    value
    |> String.split(";", trim: true)
    |> Enum.reduce_while({:ok, []}, fn entry, {:ok, pairs} ->
      case normalize_pair(entry, identity_field, property_field) do
        {:ok, pair} -> {:cont, {:ok, [pair | pairs]}}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, pairs} -> {:ok, Enum.reverse(pairs)}
      :error -> :error
    end
  end

  defp normalize_source_value(value, {:list, item_type}) when is_binary(value) do
    values = String.split(value, ";", trim: true)

    if Enum.all?(values, &valid_field_value?(&1, item_type)),
      do: {:ok, values},
      else: :error
  end

  defp normalize_source_value(value, type) do
    if valid_field_value?(value, type), do: {:ok, value}, else: :error
  end

  defp normalize_pair(entry, identity_field, property_field) do
    case String.split(entry, ":", parts: 2) do
      [identity, property] when identity != "" ->
        with {:ok, property} <- parse_integer(property) do
          {:ok, %{identity_field => identity, property_field => property}}
        end

      _other ->
        :error
    end
  end

  defp parse_integer(value) do
    case Integer.parse(value) do
      {integer, ""} -> {:ok, integer}
      _other -> :error
    end
  end

  defp present?(value), do: value not in [nil, ""]

  defp validate_dependency_order(records, schemas, insert_positions) do
    records
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {record, index}, :ok ->
      schema = Map.fetch!(schemas, record.type)

      schema.dependencies
      |> dependency_identities(record.fields)
      |> Enum.find_value(&future_dependency_error(&1, insert_positions, index, record.sequence))
      |> case do
        nil -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp validate_first_pass(records, schemas) do
    initial = %{previous: 0, seen: MapSet.new(), insert_positions: %{}}

    records
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, initial}, fn {record, index}, {:ok, acc} ->
      with {:cont, :ok} <- validate_shape(record, :ok, schemas),
           :ok <- validate_next_sequence(record, acc.previous),
           schema <- Map.fetch!(schemas, record.type),
           {:ok, seen} <- put_identity(acc.seen, schema, record) do
        next = %{
          previous: record.sequence,
          seen: seen,
          insert_positions: put_insert_position(acc.insert_positions, schema, record, index)
        }

        {:cont, {:ok, next}}
      else
        {:halt, {:error, _} = error} -> {:halt, error}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, acc.insert_positions}
      {:error, _} = error -> error
    end
  end

  defp validate_next_sequence(%{sequence: sequence}, previous) when sequence > previous, do: :ok

  defp validate_next_sequence(record, _previous),
    do: {:error, {:invalid_or_reordered_update, record}}

  defp put_identity(seen, schema, record) do
    identity = Enum.map(schema.identity, &to_string(Map.fetch!(record.fields, &1)))
    key = {schema.operation, record.type, identity}

    if MapSet.member?(seen, key),
      do: {:error, {:duplicate_update_identity, record.type, identity}},
      else: {:ok, MapSet.put(seen, key)}
  end

  defp put_insert_position(positions, %{operation: :insert} = schema, record, index) do
    case entity_identity(schema, record.fields) do
      nil -> positions
      identity -> Map.put(positions, identity, {index, record.sequence})
    end
  end

  defp put_insert_position(positions, _schema, _record, _index), do: positions

  defp future_dependency_error(identity, positions, index, record_sequence) do
    case Map.get(positions, identity) do
      {future, dependency_sequence} when future > index ->
        {:error,
         {:out_of_order_update_dependency, record_sequence, identity, dependency_sequence}}

      _ ->
        nil
    end
  end

  defp entity_identity(%{effect: {:entity, type}, identity: [field]}, fields),
    do: {type, to_string(Map.fetch!(fields, field))}

  defp entity_identity(_schema, _fields), do: nil

  defp dependency_identities(dependencies, fields) do
    Enum.flat_map(dependencies, fn {field, type} ->
      fields |> Map.get(field) |> dependency_values() |> Enum.map(&{type, to_string(&1)})
    end)
  end

  defp dependency_values(nil), do: []

  defp dependency_values(values) when is_list(values) do
    Enum.map(values, fn
      %{"organisationId" => id} -> id
      value -> value
    end)
  end

  defp dependency_values(value), do: [value]

  defp translate_in_order([], state, translated, _schemas, _verify_dependencies?),
    do: {:ok, Enum.reverse(translated), state}

  defp translate_in_order([record | rest], state, translated, schemas, verify_dependencies?) do
    schema = Map.fetch!(schemas, record.type)

    with :ok <- verify_dependencies(record, schema, state, verify_dependencies?),
         {:ok, mutation} <- translate(record, state) do
      next_state = apply_to_state(state, mutation)
      translate_in_order(rest, next_state, [mutation | translated], schemas, verify_dependencies?)
    end
  end

  defp verify_dependencies(_record, _schema, _state, false), do: :ok

  defp verify_dependencies(record, schema, state, true) do
    case Enum.find(dependency_identities(schema.dependencies, record.fields), fn {type, id} ->
           not dependency_exists?(state, type, id)
         end) do
      nil -> :ok
      identity -> {:error, {:missing_update_dependency, record.sequence, identity}}
    end
  end

  defp dependency_exists?(state, type, id) do
    entity = Mapping.entity_iri(type, id)
    class = ontology(type)
    Enum.any?(state, &match?({^entity, @rdf_type, ^class, _graph}, &1))
  end

  defp translate(%{type: "insert-01", fields: fields} = record, _state) do
    graph = update_graph()

    row = %{
      "id" => fields["personId"],
      "firstName" => fields["personFirstName"],
      "lastName" => fields["personLastName"],
      "gender" => fields["gender"],
      "birthday" => fields["birthday"],
      "creationDate" => fields["creationDate"],
      "locationIP" => fields["locationIP"],
      "browserUsed" => fields["browserUsed"],
      "languages" => join_array(fields["languages"]),
      "email" => join_array(fields["emails"])
    }

    with {:ok, entity} <- Mapping.entity_quads("Person", row, graph),
         {:ok, relationships} <- person_relationships(fields, graph) do
      mutation(record, :insert, entity ++ relationships)
    end
  end

  defp translate(%{type: "insert-02", fields: fields} = record, _state),
    do:
      relationship_mutation(
        record,
        :insert,
        "likes",
        "Person",
        fields["personId"],
        "Post",
        fields["postId"],
        %{"creationDate" => fields["creationDate"]}
      )

  defp translate(%{type: "insert-03", fields: fields} = record, _state),
    do:
      relationship_mutation(
        record,
        :insert,
        "likes",
        "Person",
        fields["personId"],
        "Comment",
        fields["commentId"],
        %{"creationDate" => fields["creationDate"]}
      )

  defp translate(%{type: "insert-04", fields: fields} = record, _state) do
    graph = update_graph()

    row = %{
      "id" => fields["forumId"],
      "title" => fields["forumTitle"],
      "creationDate" => fields["creationDate"]
    }

    with {:ok, entity} <- Mapping.entity_quads("Forum", row, graph),
         {:ok, moderator} <-
           relationship(
             "hasModerator",
             "Forum",
             fields["forumId"],
             "Person",
             fields["moderatorId"],
             %{},
             graph
           ),
         {:ok, tags} <-
           relationships("hasTag", "Forum", fields["forumId"], "Tag", fields["tagIds"], graph) do
      mutation(record, :insert, entity ++ moderator ++ tags)
    end
  end

  defp translate(%{type: "insert-05", fields: fields} = record, _state),
    do:
      relationship_mutation(
        record,
        :insert,
        "hasMember",
        "Forum",
        fields["forumId"],
        "Person",
        fields["personId"],
        %{"joinDate" => fields["creationDate"]}
      )

  defp translate(%{type: "insert-06", fields: fields} = record, _state) do
    graph = update_graph()

    row =
      Map.take(fields, ~w(imageFile creationDate locationIP browserUsed language content length))
      |> Map.put("id", fields["postId"])

    with {:ok, entity} <- Mapping.entity_quads("Post", row, graph),
         {:ok, creator} <-
           relationship(
             "hasCreator",
             "Post",
             fields["postId"],
             "Person",
             fields["authorPersonId"],
             %{},
             graph
           ),
         {:ok, forum} <-
           relationship(
             "containerOf",
             "Forum",
             fields["forumId"],
             "Post",
             fields["postId"],
             %{},
             graph
           ),
         {:ok, country} <-
           relationship(
             "isLocatedIn",
             "Post",
             fields["postId"],
             "Place",
             fields["countryId"],
             %{},
             graph
           ),
         {:ok, tags} <-
           relationships("hasTag", "Post", fields["postId"], "Tag", fields["tagIds"], graph) do
      mutation(record, :insert, entity ++ creator ++ forum ++ country ++ tags)
    end
  end

  defp translate(%{type: "insert-07", fields: fields} = record, _state) do
    graph = update_graph()

    row =
      Map.take(fields, ~w(creationDate locationIP browserUsed content length))
      |> Map.put("id", fields["commentId"])

    {parent_type, parent_id} =
      if present?(fields["replyToPostId"]),
        do: {"Post", fields["replyToPostId"]},
        else: {"Comment", fields["replyToCommentId"]}

    with {:ok, entity} <- Mapping.entity_quads("Comment", row, graph),
         {:ok, creator} <-
           relationship(
             "hasCreator",
             "Comment",
             fields["commentId"],
             "Person",
             fields["authorPersonId"],
             %{},
             graph
           ),
         {:ok, country} <-
           relationship(
             "isLocatedIn",
             "Comment",
             fields["commentId"],
             "Place",
             fields["countryId"],
             %{},
             graph
           ),
         {:ok, reply} <-
           relationship(
             "replyOf",
             "Comment",
             fields["commentId"],
             parent_type,
             parent_id,
             %{},
             graph
           ),
         {:ok, tags} <-
           relationships("hasTag", "Comment", fields["commentId"], "Tag", fields["tagIds"], graph) do
      mutation(record, :insert, entity ++ creator ++ country ++ reply ++ tags)
    end
  end

  defp translate(%{type: "insert-08", fields: fields} = record, _state),
    do:
      relationship_mutation(
        record,
        :insert,
        "knows",
        "Person",
        fields["person1Id"],
        "Person",
        fields["person2Id"],
        %{"creationDate" => fields["creationDate"]}
      )

  defp translate(%{type: type, fields: fields} = record, state)
       when type in ~w(delete-02 delete-03 delete-05 delete-08) do
    {relationship_type, from_type, from_id, to_type, to_id} =
      delete_relationship_spec(type, fields)

    case relationship_cleanup(state, relationship_type, from_type, from_id, to_type, to_id) do
      [] -> {:error, {:unprovable_delete_cleanup, record.sequence, type}}
      quads -> mutation(record, :delete, quads)
    end
  end

  defp translate(%{type: type, fields: fields} = record, state)
       when type in ~w(delete-01 delete-04 delete-06 delete-07) do
    {entity_type, id} = delete_entity_spec(type, fields)
    root = Mapping.entity_iri(entity_type, id)

    with true <- entity_exists?(state, root),
         targets <- deep_targets(type, root, state),
         quads when quads != [] <- cleanup_closure(state, targets) do
      mutation(record, :delete, quads)
    else
      _ -> {:error, {:unprovable_delete_cleanup, record.sequence, type}}
    end
  end

  defp mutation(record, operation, quads),
    do:
      {:ok,
       %{
         sequence: record.sequence,
         type: record.type,
         operation: operation,
         quads: Enum.uniq(quads)
       }}

  defp person_relationships(fields, graph) do
    with {:ok, city} <-
           relationship(
             "isLocatedIn",
             "Person",
             fields["personId"],
             "Place",
             fields["cityId"],
             %{},
             graph
           ),
         {:ok, tags} <-
           relationships(
             "hasInterest",
             "Person",
             fields["personId"],
             "Tag",
             fields["tagIds"],
             graph
           ),
         {:ok, studies} <-
           pair_relationships(
             "studyAt",
             "Person",
             fields["personId"],
             fields["studyAt"],
             "classYear",
             graph
           ),
         {:ok, works} <-
           pair_relationships(
             "workAt",
             "Person",
             fields["personId"],
             fields["workAt"],
             "workFrom",
             graph
           ) do
      {:ok, city ++ tags ++ studies ++ works}
    end
  end

  defp pair_relationships(type, from_type, from_id, pairs, property, graph) do
    Enum.reduce_while(pairs, {:ok, []}, fn %{"organisationId" => id} = pair, {:ok, quads} ->
      case relationship(
             type,
             from_type,
             from_id,
             "Organisation",
             id,
             %{property => pair[property]},
             graph
           ) do
        {:ok, mapped} -> {:cont, {:ok, quads ++ mapped}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp relationships(type, from_type, from_id, to_type, ids, graph) do
    Enum.reduce_while(ids, {:ok, []}, fn id, {:ok, quads} ->
      case relationship(type, from_type, from_id, to_type, id, %{}, graph) do
        {:ok, mapped} -> {:cont, {:ok, quads ++ mapped}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp relationship_mutation(
         record,
         operation,
         type,
         from_type,
         from_id,
         to_type,
         to_id,
         properties
       ) do
    case relationship(type, from_type, from_id, to_type, to_id, properties, update_graph()) do
      {:ok, quads} -> mutation(record, operation, quads)
      {:error, _} = error -> error
    end
  end

  defp relationship(type, from_type, from_id, to_type, to_id, properties, graph),
    do: Mapping.relationship_quads(type, from_type, from_id, to_type, to_id, properties, graph)

  defp current_quads(nil), do: {:ok, []}

  defp current_quads(%{db: db}) do
    internal =
      db
      |> ErlangAdapter.prefix_stream(:gspo, <<>>)
      |> Enum.map(fn {key, _value} -> QuadIndex.key_to_quad(:gspo, key) end)

    Adapter.to_rdf_quads(db, internal)
  rescue
    error -> {:error, {:update_state_scan_failed, Exception.message(error)}}
  end

  defp apply_to_state(state, %{operation: :insert, quads: quads}), do: Enum.uniq(state ++ quads)
  defp apply_to_state(state, %{operation: :delete, quads: quads}), do: state -- quads

  defp relationship_cleanup(state, type, from_type, from_id, to_type, to_id) do
    from = Mapping.entity_iri(from_type, from_id)
    to = Mapping.entity_iri(to_type, to_id)
    predicate = ontology(type)
    direct = Enum.filter(state, fn {s, p, o, _g} -> s == from and p == predicate and o == to end)

    resources =
      state
      |> subjects(@rdf_subject, from)
      |> Enum.filter(&relationship_resource?(state, &1, predicate, to))
      |> MapSet.new()

    direct ++ Enum.filter(state, fn {s, _p, _o, _g} -> MapSet.member?(resources, s) end)
  end

  defp relationship_resource?(state, resource, predicate, object) do
    has_statement?(state, resource, @rdf_predicate, predicate) and
      has_statement?(state, resource, @rdf_object, object)
  end

  defp has_statement?(state, subject, predicate, object),
    do: Enum.any?(state, &match?({^subject, ^predicate, ^object, _graph}, &1))

  defp deep_targets("delete-06", post, state), do: descendant_messages(state, MapSet.new([post]))

  defp deep_targets("delete-07", comment, state),
    do: descendant_messages(state, MapSet.new([comment]))

  defp deep_targets("delete-04", forum, state) do
    posts = objects(state, forum, ontology("containerOf")) |> MapSet.new()
    descendant_messages(state, MapSet.put(posts, forum))
  end

  defp deep_targets("delete-01", person, state) do
    messages = subjects(state, ontology("hasCreator"), person) |> MapSet.new()
    moderated = subjects(state, ontology("hasModerator"), person)

    personal_forums =
      Enum.filter(moderated, fn forum ->
        state
        |> objects(forum, ontology("type"))
        |> Enum.any?(fn object -> literal_value(object) in ["Wall", "Album"] end)
      end)

    messages
    |> MapSet.union(MapSet.new(personal_forums))
    |> MapSet.put(person)
    |> descendant_messages(state)
  end

  defp descendant_messages(targets, state) when is_struct(targets, MapSet),
    do: descendant_messages(state, targets)

  defp descendant_messages(state, targets) do
    expanded =
      Enum.reduce(state, targets, fn {subject, predicate, object, _graph}, acc ->
        if predicate == ontology("replyOf") and MapSet.member?(acc, object),
          do: MapSet.put(acc, subject),
          else: acc
      end)

    if MapSet.size(expanded) == MapSet.size(targets),
      do: expanded,
      else: descendant_messages(state, expanded)
  end

  defp cleanup_closure(state, targets) do
    affected_quads =
      Enum.filter(state, fn {s, _p, o, _g} ->
        MapSet.member?(targets, s) or MapSet.member?(targets, o)
      end)

    relationship_resources =
      affected_quads
      |> Enum.flat_map(&relationship_resources_for_edge(state, &1))
      |> MapSet.new()

    Enum.uniq(
      affected_quads ++
        Enum.filter(state, fn {s, _p, _o, _g} -> MapSet.member?(relationship_resources, s) end)
    )
  end

  defp relationship_resources_for_edge(state, {subject, predicate, object, graph}) do
    state
    |> Enum.filter(&match?({_resource, @rdf_subject, ^subject, ^graph}, &1))
    |> Enum.map(&elem(&1, 0))
    |> Enum.filter(fn resource ->
      has_statement_in_graph?(state, resource, @rdf_predicate, predicate, graph) and
        has_statement_in_graph?(state, resource, @rdf_object, object, graph)
    end)
  end

  defp has_statement_in_graph?(state, subject, predicate, object, graph),
    do: Enum.any?(state, &match?({^subject, ^predicate, ^object, ^graph}, &1))

  defp entity_exists?(state, entity),
    do: Enum.any?(state, fn {s, p, _o, _g} -> s == entity and p == @rdf_type end)

  defp objects(state, subject, predicate),
    do: for({^subject, ^predicate, object, _graph} <- state, do: object)

  defp subjects(state, predicate, object),
    do: for({subject, ^predicate, ^object, _graph} <- state, do: subject)

  defp literal_value(%RDF.Literal{} = literal), do: RDF.Literal.value(literal) |> to_string()
  defp literal_value(_term), do: nil

  defp delete_relationship_spec("delete-02", f),
    do: {"likes", "Person", f["personId"], "Post", f["postId"]}

  defp delete_relationship_spec("delete-03", f),
    do: {"likes", "Person", f["personId"], "Comment", f["commentId"]}

  defp delete_relationship_spec("delete-05", f),
    do: {"hasMember", "Forum", f["forumId"], "Person", f["personId"]}

  defp delete_relationship_spec("delete-08", f),
    do: {"knows", "Person", f["person1Id"], "Person", f["person2Id"]}

  defp delete_entity_spec("delete-01", fields), do: {"Person", fields["personId"]}
  defp delete_entity_spec("delete-04", fields), do: {"Forum", fields["forumId"]}
  defp delete_entity_spec("delete-06", fields), do: {"Post", fields["postId"]}
  defp delete_entity_spec("delete-07", fields), do: {"Comment", fields["commentId"]}

  defp join_array(values) when is_list(values), do: Enum.join(values, ";")
  defp join_array(value), do: value
  defp update_graph, do: Mapping.graph_iri(:snb_bi, :updates)
  defp ontology(name), do: RDF.iri(@ontology <> name)
  defp pad(number), do: number |> Integer.to_string() |> String.pad_leading(2, "0")
end
