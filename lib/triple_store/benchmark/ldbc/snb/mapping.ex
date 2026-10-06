defmodule TripleStore.Benchmark.LDBC.SNB.Mapping do
  @moduledoc """
  Canonical versioned RDF mapping shared by SNB BI and Interactive.

  Entity identities are stable IRIs derived from source type and ID. Scalar and
  array properties use typed literals. Relationships always emit their direct
  RDF edge; relationships with properties additionally emit a deterministic RDF
  reification resource so edge data is not lost.
  """

  @version "snb-rdf-v1"
  @base "https://ldbcouncil.org/snb/"
  @ontology @base <> "ontology/"
  @rdf_type RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
  @rdf_subject RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#subject")
  @rdf_predicate RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate")
  @rdf_object RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#object")

  @entity_types ~w(Person Forum Post Comment Organisation Place Tag TagClass)
  @message_types ~w(Post Comment)
  @relationship_types ~w(
    containerOf hasCreator hasInterest hasMember hasModerator hasTag hasType
    isLocatedIn isPartOf isSubclassOf knows likes replyOf studyAt workAt
  )

  @property_types %{
    "birthday" => :date,
    "browserUsed" => :string,
    "classYear" => :integer,
    "content" => :string,
    "creationDate" => :datetime,
    "deletionDate" => :datetime,
    "email" => {:array, :string},
    "firstName" => :string,
    "gender" => :string,
    "imageFile" => :string,
    "joinDate" => :datetime,
    "language" => :string,
    "languages" => {:array, :string},
    "lastName" => :string,
    "length" => :integer,
    "locationIP" => :string,
    "name" => :string,
    "speaks" => {:array, :string},
    "title" => :string,
    "type" => :string,
    "url" => :string,
    "workFrom" => :integer
  }

  @doc "Returns the immutable mapping version embedded in dataset manifests."
  @spec version() :: String.t()
  def version, do: @version

  @doc "Returns the complete concrete SNB entity type set."
  @spec entity_types() :: [String.t()]
  def entity_types, do: @entity_types

  @doc "Returns the relationship vocabulary supported by the mapping."
  @spec relationship_types() :: [String.t()]
  def relationship_types, do: @relationship_types

  @doc "Builds a stable entity IRI independent of dictionary allocation and load order."
  @spec entity_iri(String.t(), term()) :: RDF.IRI.t()
  def entity_iri(type, id) when type in @entity_types do
    RDF.iri(@base <> "entity/#{type}/#{encode_segment(id)}")
  end

  @doc "Returns the fixed graph IRI for a suite and dataset component."
  @spec graph_iri(:snb_bi | :snb_interactive, :initial | :updates) :: RDF.IRI.t()
  def graph_iri(suite, component)
      when suite in [:snb_bi, :snb_interactive] and component in [:initial, :updates] do
    RDF.iri(@base <> "graph/#{suite}/#{component}")
  end

  @doc "Maps one SNB entity row to canonical quads."
  @spec entity_quads(String.t(), map(), RDF.IRI.t()) ::
          {:ok, [RDF.Quad.t()]} | {:error, term()}
  def entity_quads(type, row, %RDF.IRI{} = graph) when type in @entity_types and is_map(row) do
    with {:ok, id} <- fetch_present(row, "id"),
         {:ok, property_quads} <- property_quads(entity_iri(type, id), row, graph) do
      subject = entity_iri(type, id)

      class_quads =
        [{subject, @rdf_type, RDF.iri(@ontology <> type), graph}] ++
          if type in @message_types do
            [{subject, @rdf_type, RDF.iri(@ontology <> "Message"), graph}]
          else
            []
          end

      {:ok, class_quads ++ property_quads}
    end
  end

  def entity_quads(type, _row, _graph), do: {:error, {:unknown_entity_type, type}}

  @doc "Maps one relationship row, retaining relationship properties by reification."
  @spec relationship_quads(String.t(), String.t(), term(), String.t(), term(), map(), RDF.IRI.t()) ::
          {:ok, [RDF.Quad.t()]} | {:error, term()}
  def relationship_quads(type, from_type, from_id, to_type, to_id, properties, graph)
      when type in @relationship_types and from_type in @entity_types and
             to_type in @entity_types and is_map(properties) do
    subject = entity_iri(from_type, from_id)
    object = entity_iri(to_type, to_id)
    predicate = RDF.iri(@ontology <> type)
    edge = {subject, predicate, object, graph}
    present_properties = Enum.reject(properties, fn {_key, value} -> missing?(value) end)

    if present_properties == [] do
      {:ok, [edge]}
    else
      relationship = relationship_iri(type, from_type, from_id, to_type, to_id)

      with {:ok, property_values} <-
             property_quads(relationship, Map.new(present_properties), graph) do
        {:ok,
         [
           edge,
           {relationship, @rdf_type, RDF.iri(@ontology <> "Relationship"), graph},
           {relationship, @rdf_subject, subject, graph},
           {relationship, @rdf_predicate, predicate, graph},
           {relationship, @rdf_object, object, graph}
           | property_values
         ]}
      end
    end
  end

  def relationship_quads(type, _from_type, _from_id, _to_type, _to_id, _properties, _graph),
    do: {:error, {:invalid_relationship, type}}

  defp property_quads(subject, row, graph) do
    row
    |> Enum.reject(fn {key, value} -> key == "id" or missing?(value) end)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.reduce_while({:ok, []}, fn {property, value}, {:ok, quads} ->
      case Map.fetch(@property_types, property) do
        {:ok, type} ->
          predicate = RDF.iri(@ontology <> property)
          values = if match?({:array, _}, type), do: split_array(value), else: [value]
          scalar_type = if match?({:array, _}, type), do: elem(type, 1), else: type

          case map_values(values, scalar_type) do
            {:ok, objects} ->
              mapped = Enum.map(objects, &{subject, predicate, &1, graph})
              {:cont, {:ok, quads ++ mapped}}

            {:error, _} = error ->
              {:halt, error}
          end

        :error ->
          {:halt, {:error, {:unknown_property, property}}}
      end
    end)
  end

  defp map_values(values, type) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, mapped} ->
      case literal(value, type) do
        {:ok, object} -> {:cont, {:ok, mapped ++ [object]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp literal(value, :string), do: {:ok, RDF.literal(to_string(value))}

  defp literal(value, :integer) do
    case Integer.parse(to_string(value)) do
      {integer, ""} -> {:ok, RDF.literal(integer)}
      _ -> {:error, {:invalid_integer, value}}
    end
  end

  defp literal(value, :date) do
    case Date.from_iso8601(to_string(value)) do
      {:ok, _date} ->
        {:ok,
         RDF.literal(to_string(value),
           datatype: RDF.iri("http://www.w3.org/2001/XMLSchema#date")
         )}

      {:error, _reason} ->
        {:error, {:invalid_date, value}}
    end
  end

  defp literal(value, :datetime) do
    with {:ok, iso8601} <- datetime_iso8601(value) do
      {:ok,
       RDF.literal(iso8601,
         datatype: RDF.iri("http://www.w3.org/2001/XMLSchema#dateTime")
       )}
    end
  end

  defp datetime_iso8601(value) when is_integer(value) do
    case DateTime.from_unix(value, :millisecond) do
      {:ok, datetime} -> {:ok, DateTime.to_iso8601(datetime)}
      {:error, reason} -> {:error, {:invalid_datetime, value, reason}}
    end
  end

  defp datetime_iso8601(value) do
    string = to_string(value)

    case Integer.parse(string) do
      {milliseconds, ""} ->
        datetime_iso8601(milliseconds)

      _ ->
        case DateTime.from_iso8601(string) do
          {:ok, datetime, _offset} -> {:ok, DateTime.to_iso8601(datetime)}
          {:error, reason} -> {:error, {:invalid_datetime, value, reason}}
        end
    end
  end

  defp relationship_iri(type, from_type, from_id, to_type, to_id) do
    identity = Enum.join([type, from_type, from_id, to_type, to_id], "\u0000")
    digest = :crypto.hash(:sha256, identity) |> Base.url_encode64(padding: false)
    RDF.iri(@base <> "relationship/#{type}/#{digest}")
  end

  defp fetch_present(row, key) do
    case Map.fetch(row, key) do
      {:ok, value} when value not in [nil, ""] -> {:ok, value}
      _ -> {:error, {:missing_property, key}}
    end
  end

  defp split_array(value), do: value |> to_string() |> String.split(";", trim: true)
  defp missing?(value), do: value in [nil, ""]

  defp encode_segment(id) do
    value = to_string(id)
    if String.match?(value, ~r/\A[A-Za-z0-9._~-]+\z/), do: value, else: URI.encode(value)
  end
end
