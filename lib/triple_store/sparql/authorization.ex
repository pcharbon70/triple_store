defmodule TripleStore.SPARQL.Authorization do
  @moduledoc """
  Graph-level access control for SPARQL queries with named graphs.

  This module implements an ACL (Access Control List) system for managing
  permissions on named graphs. It supports:
  - User-based permissions
  - Role-based permissions
  - Public/private graphs
  - Graph ownership

  ## Permission Model

  Permissions are represented as atoms:
  - `:read` - Can read data from the graph
  - `:write` - Can modify data in the graph
  - `:admin` - Full control including granting permissions to others
  - `:owner` - Graph owner (implicit admin permission)

  ## User Representation

  Users are represented as maps with at least an `:id` field:
  `%{id: "user123", roles: [:admin, :editor], name: "Alice"}`

  For public/unauthenticated access, use `:public` atom.

  ## ACL Storage

  ACLs are stored in the RocksDB database under the `acl` column family
  with keys like `acl:graph:GRAPH_ID:KEY` where KEY can be:
  - `__public__` - Public access permissions
  - `user:USER_ID` - User-specific permissions
  - `role:ROLE_NAME` - Role-based permissions
  - `owner:USER_ID` - Graph ownership

  ## Examples

      # Check if user can read a graph
      {:ok, true} = Authorization.can_read?(ctx, "http://example.org/private", user)

      # Grant permission to user
      :ok = Authorization.grant(ctx, "http://example.org/graph", user_id, :read)

      # List graphs accessible to user
      {:ok, [graph1, graph2]} = Authorization.list_accessible_graphs(ctx, user)

  """

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.QuadIndex
  alias TripleStore.QuadOperations

  require Logger

  @telemetry_event_prefix [:triple_store, :sparql, :authorization]

  @typedoc "Execution context containing database and dictionary references"
  @type context :: %{
          optional(:db) => pid() | term(),
          optional(:dict_manager) => pid()
        }

  @typedoc "User identifier - either an atom for special users or a string ID"
  @type user_id :: :public | String.t()

  @typedoc "User object with id and optional roles"
  @type user :: %{
            optional(:id) => user_id(),
            optional(:roles) => [atom()],
            optional(:name) => String.t(),
            optional(atom()) => term()
          }

  @typedoc "Permission type"
  @type permission :: :read | :write | :admin | :owner

  @typedoc "ACL entry"
  @type acl_entry :: %{(String.t() | atom()) => [permission()]}

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Checks if a user has read access to a graph.

  ## Parameters

  - ctx - Execution context with :db and :dict_manager keys
  - graph_iri - Graph IRI string
  - user - User object or :public atom

  ## Returns

  - `{:ok, true}` if user can read the graph
  - `{:ok, false}` if user cannot read the graph
  - `{:error, reason}` on database error

  ## Examples

      iex> Authorization.can_read?(ctx, "http://example.org/graph1", user)
      {:ok, true}

      iex> Authorization.can_read?(ctx, "http://example.org/private", :public)
      {:ok, false}

  """
  @spec can_read?(context(), String.t(), user() | :public) :: {:ok, boolean} | {:error, term()}
  def can_read?(ctx, graph_iri, user_or_public) do
    check_permission(ctx, graph_iri, user_or_public, :read)
  end

  @doc """
  Checks if a user has write access to a graph.

  ## Parameters

  - ctx - Execution context with :db and :dict_manager keys
  - graph_iri - Graph IRI string
  - user - User object

  ## Returns

  - `{:ok, true}` if user can write to the graph
  - `{:ok, false}` if user cannot write to the graph
  - `{:error, reason}` on database error

  """
  @spec can_write?(context(), String.t(), user()) :: {:ok, boolean} | {:error, term()}
  def can_write?(ctx, graph_iri, user) do
    check_permission(ctx, graph_iri, user, :write)
  end

  @doc """
  Checks if a user has admin access to a graph.

  Admin access includes the ability to grant permissions to other users.

  ## Parameters

  - ctx - Execution context with :db and :dict_manager keys
  - graph_iri - Graph IRI string
  - user - User object

  ## Returns

  - `{:ok, true}` if user is admin of the graph
  - `{:ok, false}` if user is not admin
  - `{:error, reason}` on database error

  """
  @spec can_admin?(context(), String.t(), user()) :: {:ok, boolean} | {:error, term()}
  def can_admin?(ctx, graph_iri, user) do
    check_permission(ctx, graph_iri, user, :admin)
  end

  @doc """
  Checks if a user can access a graph term (for use during query execution).

  ## Parameters

  - ctx - Execution context with :db and :dict_manager keys
  - graph_term - Graph term (can be :default_graph, %RDF.IRI{}, %RDF.BlankNode{}, or {:named_node, iri})
  - user_or_public - User object or :public atom
  - permission - Permission type (default: :read)

  ## Returns

  - `{:ok, true}` if user has the permission
  - `{:ok, false}` if user does not have the permission
  - `{:error, reason}` on database error

  ## Examples

      iex> Authorization.can_access_graph?(ctx, %RDF.IRI{value: "http://example.org/g1"}, user, :read)
      {:ok, true}

      iex> Authorization.can_access_graph?(ctx, :default_graph, :public, :read)
      {:ok, true}

  """
  @spec can_access_graph?(context(), term(), user() | :public, permission()) ::
          {:ok, boolean} | {:error, term()}
  def can_access_graph?(ctx, graph_term, user_or_public, permission \\ :read) do
    db = ctx[:db]
    dict_manager = ctx[:dict_manager]

    check_permission_for_term(db, dict_manager, graph_term, user_or_public, permission)
  end

  @doc """
  Grants a permission to a user on a graph.

  ## Parameters

  - ctx - Execution context with :db and :dict_manager keys
  - graph_iri - Graph IRI string
  - user_id - User ID string
  - permission - Permission atom (:read, :write, :admin)

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      iex> Authorization.grant(ctx, "http://example.org/graph1", "user123", :read)
      :ok

  """
  @spec grant(context(), String.t(), String.t(), permission()) :: :ok | {:error, term()}
  def grant(ctx, graph_iri, user_id, permission) do
    db = ctx[:db]
    dict_manager = ctx[:dict_manager]

    with {:ok, graph_id} <- graph_name_to_id(dict_manager, graph_iri),
         {:ok, _} <- put_acl_entry(db, graph_id, "user:#{user_id}", permission) do
      :ok
    end
  end

  @doc """
  Revokes a permission from a user on a graph.

  ## Parameters

  - ctx - Execution context with :db and :dict_manager keys
  - graph_iri - Graph IRI string
  - user_id - User ID string
  - permission - Permission atom to revoke

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure

  """
  @spec revoke(context(), String.t(), String.t(), permission()) :: :ok | {:error, term()}
  def revoke(ctx, graph_iri, user_id, permission) do
    db = ctx[:db]
    dict_manager = ctx[:dict_manager]

    with {:ok, graph_id} <- graph_name_to_id(dict_manager, graph_iri),
         {:ok, _} <- remove_acl_entry(db, graph_id, "user:#{user_id}", permission) do
      :ok
    end
  end

  @doc """
  Grants a role-based permission on a graph.

  ## Parameters

  - ctx - Execution context
  - graph_iri - Graph IRI string
  - role - Role atom (e.g., :admin, :editor)
  - permission - Permission atom to grant

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure

  """
  @spec grant_role(context(), String.t(), atom(), permission()) :: :ok | {:error, term()}
  def grant_role(ctx, graph_iri, role, permission) do
    db = ctx[:db]
    dict_manager = ctx[:dict_manager]

    with {:ok, graph_id} <- graph_name_to_id(dict_manager, graph_iri),
         {:ok, _} <- put_acl_entry(db, graph_id, "role:#{role}", permission) do
      :ok
    end
  end

  @doc """
  Sets a graph as publicly readable.

  ## Parameters

  - ctx - Execution context
  - graph_iri - Graph IRI string

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure

  """
  @spec set_public(context(), String.t()) :: :ok | {:error, term()}
  def set_public(ctx, graph_iri) do
    db = ctx[:db]
    dict_manager = ctx[:dict_manager]

    with {:ok, graph_id} <- graph_name_to_id(dict_manager, graph_iri),
         {:ok, _} <- put_acl_entry(db, graph_id, "__public__", :read) do
      :ok
    end
  end

  @doc """
  Removes public access from a graph.

  ## Parameters

  - ctx - Execution context
  - graph_iri - Graph IRI string

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure

  """
  @spec remove_public(context(), String.t()) :: :ok | {:error, term()}
  def remove_public(ctx, graph_iri) do
    db = ctx[:db]
    dict_manager = ctx[:dict_manager]

    with {:ok, graph_id} <- graph_name_to_id(dict_manager, graph_iri),
         {:ok, _} <- remove_acl_entry(db, graph_id, "__public__", :read) do
      :ok
    end
  end

  @doc """
  Lists all graphs accessible to a user with a given permission.

  ## Parameters

  - ctx - Execution context with :db and :dict_manager keys
  - user - User object or :public atom
  - permission - Permission to check (default: :read)
  - opts - Options:
    - `:include_default` - Whether to include default graph (default: true)

  ## Returns

  - `{:ok, [graph_iri]}` - List of accessible graph IRI strings
  - `{:error, reason}` - On database error

  ## Examples

      iex> Authorization.list_accessible_graphs(ctx, user, :read)
      {:ok, ["http://example.org/public", "http://example.org/user1"]}

      iex> Authorization.list_accessible_graphs(ctx, :public, :read)
      {:ok, ["http://example.org/public"]}

  """
  @spec list_accessible_graphs(context(), user() | :public, permission(), keyword()) ::
          {:ok, [String.t()]} | {:error, term()}
  def list_accessible_graphs(ctx, user_or_public, permission \\ :read, opts \\ []) do
    db = ctx[:db]
    dict_manager = ctx[:dict_manager]
    include_default = Keyword.get(opts, :include_default, true)

    # Get all graphs from the database
    case QuadOperations.list_graphs(db, include_default: include_default) do
      {:ok, all_graphs} ->
        # Filter by permission
        accessible =
          Enum.filter(all_graphs, fn graph_term ->
            case check_permission_for_term(db, dict_manager, graph_term, user_or_public, permission) do
              {:ok, true} -> true
              {:ok, false} -> false
              {:error, _} -> false
            end
          end)

        graph_iris =
          Enum.map(accessible, fn
            :default -> nil  # Don't include default in IRIs
            %RDF.IRI{value: iri} -> iri
            %RDF.BlankNode{value: id} -> "_:#{id}"
            {:named_node, iri} -> iri
            {:blank_node, id} -> "_:#{id}"
            _ -> nil
          end)
          |> Enum.reject(&is_nil/1)

        {:ok, graph_iris}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Sets the owner of a graph.

  The owner has implicit admin permissions on the graph.

  ## Parameters

  - ctx - Execution context
  - graph_iri - Graph IRI string
  - user_id - User ID to set as owner

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure

  """
  @spec set_owner(context(), String.t(), String.t()) :: :ok | {:error, term()}
  def set_owner(ctx, graph_iri, user_id) do
    db = ctx[:db]
    dict_manager = ctx[:dict_manager]

    with {:ok, graph_id} <- graph_name_to_id(dict_manager, graph_iri),
         {:ok, _} <- put_acl_entry(db, graph_id, "owner:#{user_id}", :owner) do
      :ok
    end
  end

  @doc """
  Gets the owner of a graph.

  ## Parameters

  - ctx - Execution context
  - graph_iri - Graph IRI string

  ## Returns

  - `{:ok, user_id}` - Owner's user ID
  - `{:ok, nil}` - Graph has no owner
  - `{:error, reason}` - On database error

  """
  @spec get_owner(context(), String.t()) :: {:ok, String.t() | nil} | {:error, term()}
  def get_owner(ctx, graph_iri) do
    db = ctx[:db]
    dict_manager = ctx[:dict_manager]

    with {:ok, graph_id} <- graph_name_to_id(dict_manager, graph_iri) do
      # List all ACL entries for this graph to find the owner
      # We need to scan because we don't know the user_id in advance
      acl_prefix = "acl:graph:#{graph_id}:"

      result =
        NIF.fold(db, :acl, acl_prefix, nil, fn {_k, v}, acc ->
          # If we already found the owner, skip
          if acc != nil do
            {:halt, acc}
          else
            try do
              entry = :erlang.binary_to_term(v)
              # Find owner entry
              owner_key =
                Enum.find(entry, fn {k, _v} ->
                  String.starts_with?(to_string(k), "owner:")
                end)

              case owner_key do
                {key, [:owner]} ->
                  {:halt, String.replace_prefix(to_string(key), "owner:", "")}

                _ ->
                  {:cont, nil}
              end
            rescue
              _ -> {:cont, nil}
            end
          end
        end)

      # Unwrap the fold result (it might be wrapped in {:halt, ...})
      owner_id =
        case result do
          {:halt, val} -> val
          val -> val
        end

      {:ok, owner_id}
    end
  end

  # ===========================================================================
  # Private Functions
  # ===========================================================================

  defp check_permission(ctx, graph_iri, user_or_public, permission) do
    db = ctx[:db]
    dict_manager = ctx[:dict_manager]

    # Convert graph IRI to term for lookup
    graph_term = {:named_node, graph_iri}

    check_permission_for_term(db, dict_manager, graph_term, user_or_public, permission)
  end

  defp check_permission_for_term(db, dict_manager, graph_term, user_or_public, permission) do
    # Special case: default graph is always readable
    if graph_term in [:default, :default_graph] and permission == :read do
      {:ok, true}
    else
      # Check if graph exists and get permissions
      with {:ok, graph_id} <- term_to_graph_id(db, dict_manager, graph_term),
           {:ok, acl_entry} <- get_acl_entry(db, graph_id, "__public__"),
           {:ok, has_perm} <- check_public_permission(acl_entry, permission) do
        {:ok, has_perm}
      else
        {:error, _} ->
          # If public check fails or graph not found, check user/role permissions
          check_user_permission(db, dict_manager, graph_term, user_or_public, permission)

        other ->
          other
      end
    end
  end

  defp check_public_permission(acl_entry, permission) do
    permissions = Map.get(acl_entry, "__public__", [])
    {:ok, permission in permissions}
  end

  defp check_user_permission(_db, _dict_manager, graph_term, :public, permission) do
    # Public access - check if graph is publicly readable
    # This is already checked above via __public__ ACL
    # If we reach here, public access failed
    graph_iri = extract_graph_iri(graph_term)
    emit_auth_denied_telemetry(graph_iri, :public, permission, :no_public_access)
    {:ok, false}
  end

  defp check_user_permission(db, dict_manager, graph_term, user, permission) do
    user_id = user[:id]
    user_roles = Map.get(user, :roles, [])

    with {:ok, graph_id} <- term_to_graph_id(db, dict_manager, graph_term) do
      # First, check if user is the owner
      case get_acl_entry(db, graph_id, "owner:#{user_id}") do
        {:ok, _} ->
          # Owner has all permissions
          {:ok, true}

        {:error, :not_found} ->
          # Not owner, check direct user permissions
          case get_acl_entry(db, graph_id, "user:#{user_id}") do
            {:ok, acl_entry} ->
              permissions = Map.get(acl_entry, "user:#{user_id}", [])
              if permission in permissions do
                {:ok, true}
              else
                # Check role-based permissions
                check_role_permissions(db, graph_id, graph_term, user, user_roles, permission)
              end

            {:error, :not_found} ->
              # No direct permissions, check roles
              check_role_permissions(db, graph_id, graph_term, user, user_roles, permission)

            {:error, reason} ->
              {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp check_role_permissions(db, graph_id, graph_term, user, user_roles, permission) do
    # Check each role the user has
    results =
      Enum.map(user_roles, fn role ->
        role_key = "role:#{role}"

        case get_acl_entry(db, graph_id, role_key) do
          {:ok, acl_entry} ->
            permissions = Map.get(acl_entry, role_key, [])
            permission in permissions

          {:error, :not_found} ->
            false

          {:error, _} ->
            false
        end
      end)

    # User has permission if any role grants it
    has_permission = Enum.any?(results, & &1)

    if not has_permission do
      # Emit telemetry for role-based permission denial
      graph_iri = extract_graph_iri(graph_term)
      emit_auth_denied_telemetry(graph_iri, user, permission, :no_role_permission)
    end

    {:ok, has_permission}
  end

  # ===========================================================================
  # ACL Storage Operations
  # ===========================================================================

  @acl_cf :acl

  defp put_acl_entry(db, graph_id, key, permission) do
    acl_key = encode_acl_key(graph_id, key)

    # Get existing ACL entry for this key
    current_entry =
      case NIF.get(db, @acl_cf, acl_key) do
        {:ok, <<>>} -> %{}
        {:ok, binary} when is_binary(binary) -> :erlang.binary_to_term(binary)
        :not_found -> %{}
        {:error, _} -> %{}
      end

    # Add permission to the entry
    updated_entry =
      Map.update(current_entry, key, [permission], fn perms ->
        Enum.uniq([permission | perms])
      end)

    # Store back
    encoded = :erlang.term_to_binary(updated_entry)
    NIF.put(db, @acl_cf, acl_key, encoded)
  end

  defp remove_acl_entry(db, graph_id, key, permission) do
    acl_key = encode_acl_key(graph_id, key)

    case NIF.get(db, @acl_cf, acl_key) do
      {:ok, <<>>} ->
        {:error, :not_found}

      {:ok, binary} when is_binary(binary) ->
        current_entry = :erlang.binary_to_term(binary)

        case Map.get(current_entry, key) do
          nil ->
            {:error, :not_found}

          [_] ->
            # Remove the only permission, delete the key
            updated_entry = Map.delete(current_entry, key)
            encoded = :erlang.term_to_binary(updated_entry)
            NIF.put(db, @acl_cf, acl_key, encoded)

          permissions ->
            # Remove this permission, keep others
            updated_permissions = List.delete(permissions, permission)
            updated_entry = Map.put(current_entry, key, updated_permissions)
            encoded = :erlang.term_to_binary(updated_entry)
            NIF.put(db, @acl_cf, acl_key, encoded)
        end

      :not_found ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_acl_entry(db, graph_id, key) do
    acl_key = encode_acl_key(graph_id, key)

    case NIF.get(db, @acl_cf, acl_key) do
      {:ok, <<>>} ->
        {:error, :not_found}

      {:ok, binary} when is_binary(binary) ->
        entry = :erlang.binary_to_term(binary)
        {:ok, entry}

      :not_found ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp encode_acl_key(graph_id, key) do
    "acl:graph:#{graph_id}:#{key}"
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp term_to_graph_id(_db, _dict_manager, :default_graph) do
    {:ok, QuadIndex.default_graph_id()}
  end

  defp term_to_graph_id(_db, _dict_manager, :default) do
    {:ok, QuadIndex.default_graph_id()}
  end

  defp term_to_graph_id(_db, dict_manager, %RDF.IRI{} = iri) do
    # Convert RDF.IRI to ID via Adapter
    case Adapter.term_to_id(dict_manager, iri) do
      {:ok, id} -> {:ok, id}
      {:error, _} = error -> error
    end
  end

  defp term_to_graph_id(_db, dict_manager, {:named_node, iri}) do
    # Convert string IRI to RDF.IRI, then get ID
    rdf_iri = RDF.iri(iri)
    case Adapter.from_rdf_iri(dict_manager, rdf_iri) do
      {:ok, id} -> {:ok, id}
      {:error, _} = error -> error
    end
  end

  defp term_to_graph_id(_db, _dict_manager, %RDF.BlankNode{}) do
    {:error, :blank_node_not_supported}
  end

  defp term_to_graph_id(_db, _dict_manager, term) do
    {:error, {:invalid_graph_term, term}}
  end

  defp graph_name_to_id(dict_manager, graph_iri) do
    # Convert string IRI to RDF.IRI, then get ID
    rdf_iri = RDF.iri(graph_iri)
    case Adapter.from_rdf_iri(dict_manager, rdf_iri) do
      {:ok, id} -> {:ok, id}
      {:error, _} = error -> error
    end
  end

  # Helper to extract graph IRI from various term formats for telemetry
  defp extract_graph_iri(%RDF.IRI{value: value}), do: value
  defp extract_graph_iri({:named_node, iri}), do: iri
  defp extract_graph_iri(:default), do: :default
  defp extract_graph_iri(:default_graph), do: :default_graph
  defp extract_graph_iri(other), do: other

  # ===========================================================================
  # Telemetry Functions
  # ===========================================================================

  @doc """
  Emits a telemetry event for authorization denial.

  ## Telemetry Event

  Event: `[:triple_store, :sparql, :authorization, :denied]`

  Measurements:
  - `%{system_time: integer()}` - Timestamp of the denial

  Metadata:
  - `%{graph: String.t() | atom, user: String.t() | :public | nil, permission: atom(), reason: atom()}`

  ## Parameters

  - graph_iri - The graph IRI being accessed
  - user - The user object or :public
  - permission - The permission requested (:read, :write, :admin)
  - reason - The reason for denial (:no_public_access, :not_owner, :no_user_permission, :no_role_permission)

  """
  @spec emit_auth_denied_telemetry(String.t() | atom, user() | :public, atom(), atom()) :: :ok
  defp emit_auth_denied_telemetry(graph_iri, user_or_public, permission, reason) do
    user_id =
      case user_or_public do
        :public -> :public
        user when is_map(user) -> Map.get(user, :id, :unknown)
        _ -> :unknown
      end

    :telemetry.execute(
      @telemetry_event_prefix ++ [:denied],
      %{system_time: System.system_time()},
      %{
        graph: graph_iri,
        user: user_id,
        permission: permission,
        reason: reason
      }
    )

    :ok
  end
end
