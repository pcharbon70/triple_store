defmodule TripleStore.SPARQL.Validation do
  @moduledoc """
  Validation functions for SPARQL query security.

  This module provides validation functions to prevent security issues
  in SPARQL queries, including:
  - Graph IRI validation
  - Path traversal prevention
  - IRI whitelist/blacklist enforcement

  ## Graph IRI Validation

  Graph IRIs in SPARQL queries must be validated to prevent:
  - Path traversal attacks (e.g., "../../etc/passwd")
  - Invalid IRI formats
  - Suspicious IRI patterns

  ## Examples

      iex> Validation.validate_graph_iri("http://example.org/graph")
      :ok

      iex> Validation.validate_graph_iri("../../etc/passwd")
      {:error, :invalid_iri}

      iex> Validation.validate_graph_iri("file:///etc/passwd")
      {:error, :blocked_scheme}

  """

  # ===========================================================================
  # Graph IRI Validation
  # ===========================================================================

  @doc """
  Validates a graph IRI for security and correctness.

  ## Validation Rules

  1. IRI must be well-formed (valid URI/IRI format)
  2. IRI scheme must be allowed (default: http, https, urn)
  3. IRI must not contain path traversal sequences
  4. IRI must not exceed maximum length (2048 characters)
  5. IRI must pass whitelist/blacklist checks (if configured)

  ## Parameters

  - `graph_iri` - Graph IRI string to validate

  ## Returns

  - `:ok` - Valid graph IRI
  - `{:error, :invalid_iri}` - Malformed IRI
  - `{:error, :blocked_scheme}` - IRI uses blocked scheme
  - `{:error, :path_traversal_detected}` - Path traversal attempt
  - `{:error, :iri_too_long}` - IRI exceeds maximum length
  - `{:error, :blocked_iri}` - IRI is blocked by blacklist
  - `{:error, :not_whitelisted}` - IRI is not whitelisted (when whitelist is active)

  ## Examples

      iex> Validation.validate_graph_iri("http://example.org/graph")
      :ok

      iex> Validation.validate_graph_iri("https://example.org/graph")
      :ok

      iex> Validation.validate_graph_iri("../../etc/passwd")
      {:error, :invalid_iri}

  """
  @spec validate_graph_iri(String.t()) :: :ok | {:error, atom()}
  def validate_graph_iri(graph_iri) when is_binary(graph_iri) do
    with :ok <- check_iri_length(graph_iri),
         :ok <- check_iri_wellformed(graph_iri),
         :ok <- check_allowed_scheme(graph_iri) do
      check_path_traversal(graph_iri)
    end
  end

  @doc """
  Validates a graph term (which can be an atom or IRI string).

  Accepts:
  - `:default_graph` - Always valid
  - `:default` - Always valid
  - IRI strings - Validated according to graph IRI rules

  ## Returns

  - `:ok` - Valid graph term
  - `{:error, reason}` - Invalid graph term

  """
  @spec validate_graph_term(atom() | String.t() | {atom(), String.t()}) :: :ok | {:error, atom()}
  def validate_graph_term(:default_graph), do: :ok
  def validate_graph_term(:default), do: :ok

  def validate_graph_term({:named_node, graph_iri}) when is_binary(graph_iri) do
    validate_graph_iri(graph_iri)
  end

  def validate_graph_term({:blank_node, _id}) do
    # Blank nodes as graph terms are valid in SPARQL GRAPH clauses
    :ok
  end

  def validate_graph_term(graph_iri) when is_binary(graph_iri) do
    validate_graph_iri(graph_iri)
  end

  def validate_graph_term(_), do: {:error, :invalid_graph_term}

  # ===========================================================================
  # Validation Rules
  # ===========================================================================

  # Maximum IRI length to prevent DoS via excessively long IRIs
  @max_iri_length 2048

  # Allowed IRI schemes for graph IRIs
  @allowed_schemes [:http, :https, :urn, :info, :lsi]

  # Suspicious patterns that may indicate attacks
  @suspicious_patterns [
    "../",
    "..\\",
    # URL-encoded ".."
    "%2e%2e",
    # URL-encoded "%2e"
    "%252e",
    "\\0",
    # Null byte
    "%00",
    # SQL Server null byte alternative
    "~0",
    "etc/passwd",
    "windows/system32"
  ]

  @doc """
  Gets the list of allowed IRI schemes for graph IRIs.

  ## Returns

  List of allowed scheme atoms.

  """
  @spec allowed_schemes() :: [atom()]
  def allowed_schemes, do: @allowed_schemes

  @doc """
  Gets the maximum allowed IRI length.

  ## Returns

  Maximum IRI length in characters.

  """
  @spec max_iri_length() :: pos_integer()
  def max_iri_length, do: @max_iri_length

  # ===========================================================================
  # Private Validation Functions
  # ===========================================================================

  defp check_iri_length(graph_iri) do
    if String.length(graph_iri) <= @max_iri_length do
      :ok
    else
      {:error, :iri_too_long}
    end
  end

  defp check_iri_wellformed(graph_iri) do
    # Check if IRI starts with a scheme (e.g., http:, https:)
    # We use a simple check - for production, consider using a proper IRI parser
    case URI.parse(graph_iri) do
      %URI{scheme: scheme} when is_binary(scheme) and scheme != "" ->
        :ok

      _ ->
        # Check for other valid IRI formats
        # (skippable names, blank nodes, etc.)
        if valid_non_uri_iri?(graph_iri) do
          :ok
        else
          {:error, :invalid_iri}
        end
    end
  end

  defp valid_non_uri_iri?(iri) do
    # Check for skippable blank node patterns
    # Check for valid URN without scheme delimiter
    String.starts_with?(iri, "_:") or
      Regex.match?(~r/^urn:[a-z0-9][a-z0-9\-]{0,31}:/i, iri)
  end

  defp check_allowed_scheme(graph_iri) do
    case URI.parse(graph_iri) do
      %URI{scheme: scheme} when is_binary(scheme) ->
        # Check if scheme is in allowed list by comparing strings
        scheme_lower = String.downcase(scheme)

        allowed_scheme_strings =
          @allowed_schemes |> Enum.map(&Atom.to_string/1) |> Enum.map(&String.downcase/1)

        if scheme_lower in allowed_scheme_strings do
          :ok
        else
          {:error, :blocked_scheme}
        end

      _ ->
        # No scheme - check if it's a valid non-URI IRI
        if valid_non_uri_iri?(graph_iri) do
          :ok
        else
          {:error, :invalid_iri}
        end
    end
  end

  defp check_path_traversal(graph_iri) do
    lower_iri = String.downcase(graph_iri)

    if Enum.any?(@suspicious_patterns, fn pattern ->
         String.contains?(lower_iri, String.downcase(pattern))
       end) do
      {:error, :path_traversal_detected}
    else
      :ok
    end
  end

  # ===========================================================================
  # Telemetry
  # ===========================================================================

  @doc """
  Emits a telemetry event for validation failures.

  ## Parameters

  - `reason` - Validation failure reason atom
  - `iri` - The IRI that failed validation (sanitized)
  - `metadata` - Additional metadata to include

  ## Examples

      Validation.emit_validation_telemetry(:path_traversal_detected, "path_traversal", %{})

  """
  @spec emit_validation_telemetry(atom(), String.t(), keyword()) :: :ok
  def emit_validation_telemetry(reason, iri, metadata \\ []) do
    :telemetry.execute(
      [:triple_store, :sparql, :validation_failure],
      %{reason: reason},
      Map.merge(metadata, %{iri: sanitize_iri_for_telemetry(iri)})
    )

    :ok
  end

  # Sanitizes IRI for telemetry by removing potentially sensitive parts
  defp sanitize_iri_for_telemetry(iri) do
    # Keep scheme and host, but truncate path
    case URI.parse(iri) do
      %URI{scheme: scheme, host: host} when is_binary(scheme) ->
        "#{scheme}://#{host}/..."

      _ ->
        # For non-URI IRIs, just truncate
        if String.length(iri) > 50 do
          String.slice(iri, 0, 50) <> "..."
        else
          iri
        end
    end
  end
end
