defmodule TripleStore.Reasoner.Namespaces do
  @moduledoc """
  Shared namespace constants for OWL 2 RL reasoning.

  This module centralizes all namespace URIs used across the reasoner
  subsystem to avoid duplication and ensure consistency.

  ## Namespaces

  - RDF: `http://www.w3.org/1999/02/22-rdf-syntax-ns#`
  - RDFS: `http://www.w3.org/2000/01/rdf-schema#`
  - OWL: `http://www.w3.org/2002/07/owl#`
  - XSD: `http://www.w3.org/2001/XMLSchema#`

  ## Usage

      alias TripleStore.Reasoner.Namespaces

      # Get namespace prefixes
      Namespaces.rdf()   # => "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
      Namespaces.rdfs()  # => "http://www.w3.org/2000/01/rdf-schema#"

      # Build full IRIs
      Namespaces.rdf("type")  # => "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
  """

  # ============================================================================
  # Namespace URIs
  # ============================================================================

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"
  @xsd "http://www.w3.org/2001/XMLSchema#"

  # ============================================================================
  # Namespace Accessors
  # ============================================================================

  @doc "Returns the RDF namespace prefix."
  @spec rdf() :: String.t()
  def rdf, do: @rdf

  @doc "Returns a full RDF IRI for the given local name."
  @spec rdf(String.t()) :: String.t()
  def rdf(local_name), do: @rdf <> local_name

  @doc "Returns the RDFS namespace prefix."
  @spec rdfs() :: String.t()
  def rdfs, do: @rdfs

  @doc "Returns a full RDFS IRI for the given local name."
  @spec rdfs(String.t()) :: String.t()
  def rdfs(local_name), do: @rdfs <> local_name

  @doc "Returns the OWL namespace prefix."
  @spec owl() :: String.t()
  def owl, do: @owl

  @doc "Returns a full OWL IRI for the given local name."
  @spec owl(String.t()) :: String.t()
  def owl(local_name), do: @owl <> local_name

  @doc "Returns the XSD namespace prefix."
  @spec xsd() :: String.t()
  def xsd, do: @xsd

  @doc "Returns a full XSD IRI for the given local name."
  @spec xsd(String.t()) :: String.t()
  def xsd(local_name), do: @xsd <> local_name

  # ============================================================================
  # Common IRIs
  # ============================================================================

  @doc "Returns the rdf:type IRI."
  @spec rdf_type() :: String.t()
  def rdf_type, do: @rdf <> "type"

  @doc "Returns the rdfs:subClassOf IRI."
  @spec rdfs_sub_class_of() :: String.t()
  def rdfs_sub_class_of, do: @rdfs <> "subClassOf"

  @doc "Returns the rdfs:subPropertyOf IRI."
  @spec rdfs_sub_property_of() :: String.t()
  def rdfs_sub_property_of, do: @rdfs <> "subPropertyOf"

  @doc "Returns the rdfs:domain IRI."
  @spec rdfs_domain() :: String.t()
  def rdfs_domain, do: @rdfs <> "domain"

  @doc "Returns the rdfs:range IRI."
  @spec rdfs_range() :: String.t()
  def rdfs_range, do: @rdfs <> "range"

  @doc "Returns the owl:sameAs IRI."
  @spec owl_same_as() :: String.t()
  def owl_same_as, do: @owl <> "sameAs"

  @doc "Returns the owl:TransitiveProperty IRI."
  @spec owl_transitive_property() :: String.t()
  def owl_transitive_property, do: @owl <> "TransitiveProperty"

  @doc "Returns the owl:SymmetricProperty IRI."
  @spec owl_symmetric_property() :: String.t()
  def owl_symmetric_property, do: @owl <> "SymmetricProperty"

  @doc "Returns the owl:inverseOf IRI."
  @spec owl_inverse_of() :: String.t()
  def owl_inverse_of, do: @owl <> "inverseOf"

  @doc "Returns the owl:FunctionalProperty IRI."
  @spec owl_functional_property() :: String.t()
  def owl_functional_property, do: @owl <> "FunctionalProperty"

  @doc "Returns the owl:InverseFunctionalProperty IRI."
  @spec owl_inverse_functional_property() :: String.t()
  def owl_inverse_functional_property, do: @owl <> "InverseFunctionalProperty"

  @doc "Returns the owl:hasValue IRI."
  @spec owl_has_value() :: String.t()
  def owl_has_value, do: @owl <> "hasValue"

  @doc "Returns the owl:onProperty IRI."
  @spec owl_on_property() :: String.t()
  def owl_on_property, do: @owl <> "onProperty"

  @doc "Returns the owl:someValuesFrom IRI."
  @spec owl_some_values_from() :: String.t()
  def owl_some_values_from, do: @owl <> "someValuesFrom"

  @doc "Returns the owl:allValuesFrom IRI."
  @spec owl_all_values_from() :: String.t()
  def owl_all_values_from, do: @owl <> "allValuesFrom"

  @doc "Returns the owl:Thing IRI."
  @spec owl_thing() :: String.t()
  def owl_thing, do: @owl <> "Thing"

  # ============================================================================
  # IRI Utilities
  # ============================================================================

  @doc """
  Extracts the local name from an IRI.

  Works with both hash-based and slash-based IRIs.

  ## Examples

      iex> Namespaces.extract_local_name("http://example.org/Person")
      "Person"

      iex> Namespaces.extract_local_name("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
      "type"
  """
  @spec extract_local_name(String.t()) :: String.t()
  def extract_local_name(iri) when is_binary(iri) do
    cond do
      String.contains?(iri, "#") ->
        iri |> String.split("#") |> List.last()

      String.contains?(iri, "/") ->
        iri |> String.split("/") |> List.last()

      true ->
        iri
    end
  end

  @doc """
  Validates that an IRI doesn't contain SPARQL injection characters.

  Returns `{:ok, iri}` if the IRI is safe, or `{:error, reason}` if it contains
  potentially dangerous characters.

  ## Dangerous Characters

  - `>` - Can close an IRI and inject SPARQL
  - `}` - Can close a pattern and inject SPARQL
  - `;` - Can separate statements
  - `{` - Can start new patterns
  - Newlines - Can break query structure

  ## Examples

      iex> Namespaces.validate_iri("http://example.org/Person")
      {:ok, "http://example.org/Person"}

      iex> Namespaces.validate_iri("http://example.org/Person>; DROP")
      {:error, :invalid_iri_characters}
  """
  @spec validate_iri(String.t()) :: {:ok, String.t()} | {:error, :invalid_iri_characters}
  def validate_iri(iri) when is_binary(iri) do
    dangerous_chars = [">", "}", ";", "{", "\n", "\r"]

    if Enum.any?(dangerous_chars, &String.contains?(iri, &1)) do
      {:error, :invalid_iri_characters}
    else
      {:ok, iri}
    end
  end

  @doc """
  Validates an IRI, raising an error if invalid.
  """
  @spec validate_iri!(String.t()) :: String.t()
  def validate_iri!(iri) do
    case validate_iri(iri) do
      {:ok, iri} ->
        iri

      {:error, :invalid_iri_characters} ->
        raise ArgumentError, "IRI contains invalid characters: #{inspect(iri)}"
    end
  end

  @doc """
  Checks if a string is a valid IRI without dangerous characters.
  """
  @spec valid_iri?(String.t()) :: boolean()
  def valid_iri?(iri) when is_binary(iri) do
    case validate_iri(iri) do
      {:ok, _} -> true
      {:error, _} -> false
    end
  end
end
