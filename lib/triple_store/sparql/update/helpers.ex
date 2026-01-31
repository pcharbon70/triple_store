defmodule TripleStore.SPARQL.Update.Helpers do
  @moduledoc """
  Helper functions for SPARQL UPDATE operations.

  This module contains common utilities used across UPDATE operations:
  - Authorization checking
  - AST term conversion to RDF
  - Graph term normalization
  - Cache invalidation
  - Term ID lookups
  """

  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.StringToId
  alias TripleStore.Query.Cache, as: QueryCache
  alias TripleStore.SPARQL.Authorization

  # ===========================================================================
  # Authorization Helpers
  # ===========================================================================

  @doc """
  Gets the user from the context, returns :public if not present.
  """
  @spec get_user(map()) :: map() | :public
  def get_user(ctx) do
    Map.get(ctx, :user, :public)
  end

  @doc """
  Checks write authorization on a graph term.
  """
  @spec check_write_authorization(map(), term()) :: :ok | {:error, :unauthorized}
  def check_write_authorization(ctx, graph_term) do
    user = get_user(ctx)

    if user == :public do
      :ok
    else
      graph_iri = graph_term_to_iri_string(graph_term)

      case Authorization.can_write?(ctx, graph_iri, user) do
        {:ok, true} -> :ok
        {:ok, false} -> {:error, :unauthorized}
        {:error, _} -> {:error, :unauthorized}
      end
    end
  end

  @doc """
  Checks admin authorization on a graph term.

  Users with :admin role have global admin access to all graphs.
  Other users must have graph-level admin permission.
  """
  @spec check_admin_authorization(map(), term()) :: :ok | {:error, :unauthorized}
  def check_admin_authorization(ctx, graph_term) do
    user = get_user(ctx)

    if user == :public do
      :ok
    else
      # First, check if user has admin role (global admin access)
      user_roles = Map.get(user, :roles, [])

      if :admin in user_roles do
        :ok
      else
        # If not global admin, check graph-level admin permission
        graph_iri = graph_term_to_iri_string(graph_term)

        case Authorization.can_admin?(ctx, graph_iri, user) do
          {:ok, true} -> :ok
          {:ok, false} -> {:error, :unauthorized}
          {:error, _} -> {:error, :unauthorized}
        end
      end
    end
  end

  @doc """
  Checks read authorization on a graph term.
  """
  @spec check_read_authorization(map(), term()) :: :ok | {:error, :unauthorized}
  def check_read_authorization(ctx, graph_term) do
    user = get_user(ctx)

    if user == :public do
      :ok
    else
      graph_iri = graph_term_to_iri_string(graph_term)

      case Authorization.can_read?(ctx, graph_iri, user) do
        {:ok, true} -> :ok
        {:ok, false} -> {:error, :unauthorized}
        {:error, _} -> {:error, :unauthorized}
      end
    end
  end

  @doc """
  Checks authorization on multiple graph terms.

  Users with :admin role have global access for all permission types.
  """
  @spec check_multi_graph_authorization(map(), [term()], atom()) :: :ok | {:error, :unauthorized}
  def check_multi_graph_authorization(ctx, graph_terms, permission_type) do
    user = get_user(ctx)

    if user == :public do
      :ok
    else
      # Check if user has admin role (global access)
      user_roles = Map.get(user, :roles, [])

      if :admin in user_roles do
        :ok
      else
        results =
          Enum.map(graph_terms, fn graph_term ->
            graph_iri = graph_term_to_iri_string(graph_term)

            if is_nil(graph_iri) or graph_iri == "" do
              :ok
            else
              case permission_type do
                :write ->
                  case Authorization.can_write?(ctx, graph_iri, user) do
                    {:ok, true} -> :ok
                    _ -> {:error, :unauthorized}
                  end

                :admin ->
                  case Authorization.can_admin?(ctx, graph_iri, user) do
                    {:ok, true} -> :ok
                    _ -> {:error, :unauthorized}
                  end

                :read ->
                  case Authorization.can_read?(ctx, graph_iri, user) do
                    {:ok, true} -> :ok
                    _ -> {:error, :unauthorized}
                  end
              end
            end
          end)

        if Enum.all?(results, &(&1 == :ok)) do
          :ok
        else
          {:error, :unauthorized}
        end
      end
    end
  end

  # ===========================================================================
  # Graph Term Conversion
  # ===========================================================================

  @doc """
  Converts AST graph term to RDF.IRI or :default atom.
  """
  @spec ast_term_to_rdf_graph(term()) :: RDF.IRI.t() | :default | {:error, :invalid_graph_term}
  def ast_term_to_rdf_graph(:default), do: :default
  def ast_term_to_rdf_graph(:default_graph), do: :default
  def ast_term_to_rdf_graph({:named_node, iri}), do: RDF.iri(iri)
  def ast_term_to_rdf_graph({:named_graph, iri}), do: RDF.iri(iri)
  def ast_term_to_rdf_graph({:iri, iri}), do: RDF.iri(iri)
  def ast_term_to_rdf_graph(graph_iri) when is_binary(graph_iri), do: RDF.iri(graph_iri)
  def ast_term_to_rdf_graph(_other), do: {:error, :invalid_graph_term}

  @doc """
  Converts AST graph term from ast_graph_to_rdf to RDF.IRI or :default.
  """
  @spec ast_graph_to_rdf(term()) :: RDF.IRI.t() | :default
  def ast_graph_to_rdf(:default), do: :default
  def ast_graph_to_rdf(:default_graph), do: :default
  def ast_graph_to_rdf({:named_node, iri}), do: RDF.iri(iri)
  def ast_graph_to_rdf({:named_graph, iri}), do: RDF.iri(iri)
  def ast_graph_to_rdf({:iri, iri}), do: RDF.iri(iri)
  def ast_graph_to_rdf(graph_iri) when is_binary(graph_iri), do: RDF.iri(graph_iri)

  @doc """
  Normalizes graph terms to a consistent format.
  """
  @spec normalize_graph_term(term()) :: RDF.IRI.t() | :default
  def normalize_graph_term(:default), do: :default
  def normalize_graph_term(:default_graph), do: :default
  def normalize_graph_term({:named_node, iri}), do: RDF.iri(iri)
  def normalize_graph_term({:named_graph, iri}), do: RDF.iri(iri)
  def normalize_graph_term(%RDF.IRI{} = iri), do: iri
  def normalize_graph_term(iri) when is_binary(iri), do: RDF.iri(iri)
  def normalize_graph_term(_other), do: :default

  @doc """
  Converts graph term to IRI string for authorization checks.
  """
  @spec graph_term_to_iri_string(term()) :: String.t() | nil
  def graph_term_to_iri_string(:default), do: nil
  def graph_term_to_iri_string(:default_graph), do: nil
  def graph_term_to_iri_string(%RDF.IRI{value: value}), do: value
  def graph_term_to_iri_string({:named_node, iri}), do: iri
  def graph_term_to_iri_string({:named_graph, iri}), do: iri
  def graph_term_to_iri_string(iri) when is_binary(iri), do: iri
  def graph_term_to_iri_string(_), do: nil

  @doc """
  Extracts graph terms from a list of quads.
  """
  @spec extract_graphs_from_quads([term()]) :: [term()]
  def extract_graphs_from_quads(quads) do
    quads
    |> Enum.map(fn
      {:quad, _s, _p, _o, g} -> g
      {:triple, _s, _p, _o} -> :default
      {_s, _p, _o} -> :default
      {_s, _p, _o, g} -> g
      _ -> :default
    end)
    |> Enum.uniq()
  end

  @doc """
  Extracts graph terms from template patterns.
  """
  @spec extract_graphs_from_templates([term()]) :: [term()]
  def extract_graphs_from_templates(templates) do
    templates
    |> Enum.flat_map(fn
      {:quad, _s, _p, _o, g} when not is_tuple(g) or elem(g, 0) != :variable -> [g]
      {:triple, _s, _p, _o} -> [:default]
      {s, p, o} when is_tuple(s) or is_tuple(p) or is_tuple(o) -> []
      {_s, _p, _o} -> [:default]
      {_s, _p, _o, :default_graph} -> [:default]
      {_s, _p, _o, :default} -> [:default]
      {_s, _p, _o, g} when not is_tuple(g) -> [g]
      _ -> []
    end)
    |> Enum.uniq()
  end

  # ===========================================================================
  # AST Term Conversion
  # ===========================================================================

  @doc """
  Converts AST term to RDF term (only for ground terms).
  """
  @spec ast_to_rdf(term()) :: RDF.IRI.t() | RDF.BlankNode.t() | RDF.Literal.t()
  def ast_to_rdf({:named_node, iri}), do: RDF.iri(iri)
  def ast_to_rdf({:blank_node, id}), do: RDF.bnode(id)
  def ast_to_rdf({:literal, :simple, value}), do: RDF.literal(value)
  def ast_to_rdf({:literal, :lang, value, lang}), do: RDF.literal(value, language: lang)

  def ast_to_rdf({:literal, :language_tagged, value, lang}),
    do: RDF.literal(value, language: lang)

  def ast_to_rdf({:literal, :typed, value, datatype}) do
    RDF.literal(value, datatype: RDF.iri(datatype))
  end

  def ast_to_rdf({:variable, _name}) do
    raise ArgumentError, "Variables not allowed in ground terms"
  end

  def ast_to_rdf(term), do: term

  # ===========================================================================
  # Cache Invalidation
  # ===========================================================================

  @doc """
  Invalidates the query cache if it's running.
  """
  @spec invalidate_cache_if_running() :: :ok
  def invalidate_cache_if_running do
    if cache_running?() do
      QueryCache.invalidate()
    else
      :ok
    end
  end

  @doc """
  Checks if the query cache is running.
  """
  @spec cache_running?() :: boolean()
  def cache_running? do
    case Process.whereis(TripleStore.Query.Cache) do
      nil -> false
      pid when is_pid(pid) -> Process.alive?(pid)
    end
  end

  # ===========================================================================
  # Term ID Lookups
  # ===========================================================================

  @doc """
  Looks up term ID - uses inline encoding for numeric types, dictionary for others.
  """
  @spec lookup_term_id(reference(), RDF.Literal.t()) ::
          {:ok, Dictionary.term_id()} | :not_found | {:error, term()}
  def lookup_term_id(db, %RDF.Literal{} = literal) do
    if Dictionary.inline_encodable?(literal) do
      encode_inline_literal(literal)
    else
      StringToId.lookup_id(db, literal)
    end
  end

  @spec lookup_term_id(reference(), term()) ::
          {:ok, Dictionary.term_id()} | :not_found | {:error, term()}
  def lookup_term_id(db, term) do
    StringToId.lookup_id(db, term)
  end

  @doc """
  Encodes inline-encodable literals directly.
  """
  @spec encode_inline_literal(RDF.Literal.t()) :: {:ok, Dictionary.term_id()} | {:error, term()}
  def encode_inline_literal(%RDF.Literal{literal: %RDF.XSD.Integer{value: value}})
      when is_integer(value) do
    {:ok, Dictionary.encode_integer(value)}
  end

  def encode_inline_literal(%RDF.Literal{literal: %RDF.XSD.Decimal{value: %Decimal{} = value}}) do
    {:ok, Dictionary.encode_decimal(value)}
  end

  def encode_inline_literal(%RDF.Literal{literal: %RDF.XSD.DateTime{value: %DateTime{} = value}}) do
    {:ok, Dictionary.encode_datetime(value)}
  end

  def encode_inline_literal(_literal) do
    {:error, :not_inline_encodable}
  end

  @doc """
  Gets a property value from a list that may have string or atom keys.
  """
  @spec get_prop(keyword(), term(), term()) :: term()
  def get_prop(props, key, default \\ nil) do
    case List.keyfind(props, key, 0) do
      {^key, value} ->
        value

      nil ->
        atom_key = String.to_atom(key)
        Keyword.get(props, atom_key, default)
    end
  end
end
