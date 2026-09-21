defmodule TripleStore.SPARQL.Authorization do
  @moduledoc """
  Graph-level access control for SPARQL queries with named graphs.

  This module implements an ACL (Access Control List) system for managing
  permissions on named graphs. It supports:
  - User-based permissions
  - Role-based permissions
  - Public/private graphs
  - Graph ownership
  - Permit-all mode for open-access scenarios

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

  ## Permit-All Mode (Open Access)

  For scenarios where authorization is not needed (public datasets, internal tools,
  development environments), permit-all mode bypasses ACL checks entirely:

  ```elixir
  # Enable permit-all mode
  Authorization.set_permit_all(true)

  # All authorization checks return true
  {:ok, true} = Authorization.can_read?(ctx, "any-graph", :public)

  # Disable permit-all mode (use ACLs)
  Authorization.set_permit_all(false)
  ```

  Permit-all mode is **process-global** and affects all authorization checks
  in the calling process. This allows different parts of an application to
  have different authorization policies.

  ## ACL Storage

  ACLs are stored in the RocksDB database under the `acl` column family
  with keys like `acl:graph:GRAPH_ID:KEY` where KEY can be:
  - `__public__` - Public access permissions
  - `user:USER_ID` - User-specific permissions
  - `role:ROLE_NAME` - Role-based permissions
  - `owner:USER_ID` - Graph ownership

  The supported on-disk ACL format is the existing unversioned, single-entry
  map `%{principal_binary => permissions}`. Empty maps written by older
  versions are treated as absent entries. Principal names are binaries and
  permissions come from the finite `:read`, `:write`, `:admin`, and `:owner`
  vocabulary. Reads use safe Erlang-term decoding and reject malformed,
  incompatible, or principal-mismatched records as `{:corrupt_acl, reason}`.
  Authorization and mutation calls propagate those errors instead of treating
  unreadable policy as empty or permissive state.

  ## Examples

      # Check if user can read a graph
      {:ok, true} = Authorization.can_read?(ctx, "http://example.org/private", user)

      # Grant permission to user
      :ok = Authorization.grant(ctx, "http://example.org/graph", user_id, :read)

      # List graphs accessible to user
      {:ok, [graph1, graph2]} = Authorization.list_accessible_graphs(ctx, user)

      # Enable permit-all for open-access scenarios
      Authorization.set_permit_all(true)

  """

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.QuadIndex
  alias TripleStore.QuadOperations

  require Logger

  @telemetry_event_prefix [:triple_store, :sparql, :authorization]

  # Module attributes for permit-all mode
  @permit_all_key :triple_store_authorization_permit_all
  @default_permit_all false

  @typedoc "Execution context containing database and dictionary references"
  @type context :: %{
          optional(:db) => pid() | term(),
          optional(:dict_manager) => pid(),
          optional(:permit_all) => boolean()
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
  @type acl_entry :: %{optional(String.t()) => [permission()]}

  # ===========================================================================
  # Permit-All Mode (Open Access)
  # ===========================================================================

  @doc """
  Checks if permit-all mode is enabled for the current process.

  When permit-all mode is enabled, authorization checks return `true` for
  all graphs and users, bypassing ACL lookups.

  ## Returns

  - `true` if permit-all mode is enabled
  - `false` if ACL-based authorization is active

  ## Examples

      iex> Authorization.permit_all?()
      false

      iex> Authorization.set_permit_all(true)
      :ok

      iex> Authorization.permit_all?()
      true

  """
  @spec permit_all?() :: boolean()
  def permit_all? do
    case Process.get(@permit_all_key) do
      nil -> @default_permit_all
      value -> value
    end
  end

  @doc """
  Sets permit-all mode for the current process.

  When enabled, all authorization checks bypass ACL lookups and return `true`.
  This is useful for:
  - Public datasets with open access
  - Development and testing environments
  - Internal tools that don't need user-based authorization
  - Migrated triple stores that don't have ACLs configured

  ## Parameters

  - `enabled` - `true` to enable permit-all mode, `false` to use ACLs

  ## Returns

  - `:ok`

  ## Examples

      # Enable for open-access scenario
      Authorization.set_permit_all(true)

      # Disable to use ACLs
      Authorization.set_permit_all(false)

  """
  @spec set_permit_all(boolean()) :: :ok
  def set_permit_all(enabled) when is_boolean(enabled) do
    Process.put(@permit_all_key, enabled)
    :ok
  end

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

    check_permission_for_term(ctx, db, dict_manager, graph_term, user_or_public, permission)
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

    with {:ok, graph_id} <- graph_name_to_id(dict_manager, graph_iri) do
      put_acl_entry(db, graph_id, "user:#{user_id}", permission)
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

    with {:ok, graph_id} <- graph_name_to_id(dict_manager, graph_iri) do
      remove_acl_entry(db, graph_id, "user:#{user_id}", permission)
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

    with {:ok, graph_id} <- graph_name_to_id(dict_manager, graph_iri) do
      put_acl_entry(db, graph_id, "role:#{role}", permission)
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

    with {:ok, graph_id} <- graph_name_to_id(dict_manager, graph_iri) do
      put_acl_entry(db, graph_id, "__public__", :read)
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

    with {:ok, graph_id} <- graph_name_to_id(dict_manager, graph_iri) do
      remove_acl_entry(db, graph_id, "__public__", :read)
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
    with {:ok, all_graphs} <- QuadOperations.list_graphs(db, include_default: include_default),
         {:ok, accessible} <-
           filter_accessible_graphs(
             all_graphs,
             ctx,
             db,
             dict_manager,
             user_or_public,
             permission
           ) do
      graph_iris =
        Enum.map(accessible, fn
          # Don't include default in IRIs
          :default -> nil
          %RDF.IRI{value: iri} -> iri
          %RDF.BlankNode{value: id} -> "_:#{id}"
          {:named_node, iri} -> iri
          {:blank_node, id} -> "_:#{id}"
          _ -> nil
        end)
        |> Enum.reject(&is_nil/1)

      {:ok, graph_iris}
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

    with {:ok, graph_id} <- graph_name_to_id(dict_manager, graph_iri) do
      put_acl_entry(db, graph_id, "owner:#{user_id}", :owner)
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

      ErlangAdapter.fold(db, :acl, acl_prefix, {:ok, nil}, fn
        _record, {:error, _reason} = error -> error
        {key, value}, {:ok, owner_id} -> collect_owner(key, value, acl_prefix, owner_id)
      end)
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

    check_permission_for_term(ctx, db, dict_manager, graph_term, user_or_public, permission)
  end

  defp check_permission_for_term(ctx, db, dict_manager, graph_term, user_or_public, permission) do
    cond do
      ctx[:permit_all] == true or permit_all?() ->
        {:ok, true}

      graph_term in [:default, :default_graph] and permission == :read ->
        {:ok, true}

      true ->
        check_graph_permission(db, dict_manager, graph_term, user_or_public, permission)
    end
  end

  defp check_graph_permission(db, dict_manager, graph_term, user_or_public, permission) do
    with {:ok, graph_id} <- term_to_graph_id(db, dict_manager, graph_term) do
      check_public_or_user_permission(
        db,
        dict_manager,
        graph_id,
        graph_term,
        user_or_public,
        permission
      )
    end
  end

  defp check_public_or_user_permission(
         db,
         dict_manager,
         graph_id,
         graph_term,
         user_or_public,
         permission
       ) do
    case get_acl_entry(db, graph_id, "__public__") do
      {:ok, acl_entry} ->
        continue_after_public_check(
          check_public_permission(acl_entry, permission),
          db,
          dict_manager,
          graph_term,
          user_or_public,
          permission
        )

      {:error, :not_found} ->
        check_user_permission(db, dict_manager, graph_term, user_or_public, permission)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp continue_after_public_check({:ok, true}, _db, _manager, _graph, _user, _permission),
    do: {:ok, true}

  defp continue_after_public_check(
         {:ok, false},
         db,
         dict_manager,
         graph_term,
         user_or_public,
         permission
       ),
       do: check_user_permission(db, dict_manager, graph_term, user_or_public, permission)

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
          check_direct_or_role_permissions(
            db,
            graph_id,
            graph_term,
            user,
            user_roles,
            user_id,
            permission
          )

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp filter_accessible_graphs(
         graphs,
         ctx,
         db,
         dict_manager,
         user_or_public,
         permission
       ) do
    graphs
    |> Enum.reduce_while({:ok, []}, fn graph_term, {:ok, acc} ->
      case check_permission_for_term(
             ctx,
             db,
             dict_manager,
             graph_term,
             user_or_public,
             permission
           ) do
        {:ok, true} -> {:cont, {:ok, [graph_term | acc]}}
        {:ok, false} -> {:cont, {:ok, acc}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, accessible} -> {:ok, Enum.reverse(accessible)}
      {:error, _reason} = error -> error
    end
  end

  defp check_direct_or_role_permissions(
         db,
         graph_id,
         graph_term,
         user,
         user_roles,
         user_id,
         permission
       ) do
    case get_acl_entry(db, graph_id, "user:#{user_id}") do
      {:ok, acl_entry} ->
        acl_entry
        |> Map.get("user:#{user_id}", [])
        |> direct_permission_result(db, graph_id, graph_term, user, user_roles, permission)

      {:error, :not_found} ->
        check_role_permissions(db, graph_id, graph_term, user, user_roles, permission)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp direct_permission_result(
         permissions,
         db,
         graph_id,
         graph_term,
         user,
         user_roles,
         permission
       ) do
    if permission in permissions do
      {:ok, true}
    else
      check_role_permissions(db, graph_id, graph_term, user, user_roles, permission)
    end
  end

  defp check_role_permissions(db, graph_id, graph_term, user, user_roles, permission) do
    # Check each role the user has
    result =
      Enum.reduce_while(user_roles, {:ok, false}, fn role, {:ok, false} ->
        reduce_role_permission(db, graph_id, role, permission)
      end)

    case result do
      {:ok, true} ->
        {:ok, true}

      {:ok, false} ->
        graph_iri = extract_graph_iri(graph_term)
        emit_auth_denied_telemetry(graph_iri, user, permission, :no_role_permission)
        {:ok, false}

      {:error, _reason} = error ->
        error
    end
  end

  defp reduce_role_permission(db, graph_id, role, permission) do
    role_key = "role:#{role}"

    case get_acl_entry(db, graph_id, role_key) do
      {:ok, acl_entry} ->
        if permission in Map.get(acl_entry, role_key, []),
          do: {:halt, {:ok, true}},
          else: {:cont, {:ok, false}}

      {:error, :not_found} ->
        {:cont, {:ok, false}}

      {:error, reason} ->
        {:halt, {:error, reason}}
    end
  end

  # ===========================================================================
  # ACL Storage Operations
  # ===========================================================================

  @acl_cf :acl
  @permissions [:read, :write, :admin, :owner]

  defp put_acl_entry(db, graph_id, key, permission) do
    acl_key = encode_acl_key(graph_id, key)

    with :ok <- validate_permission_for_principal(key, permission),
         {:ok, current_entry} <- read_acl_entry_for_mutation(db, acl_key, key) do
      updated_entry =
        Map.update(current_entry, key, [permission], fn perms ->
          Enum.uniq([permission | perms])
        end)

      ErlangAdapter.put(db, @acl_cf, acl_key, :erlang.term_to_binary(updated_entry))
    end
  end

  defp remove_acl_entry(db, graph_id, key, permission) do
    acl_key = encode_acl_key(graph_id, key)

    case ErlangAdapter.get(db, @acl_cf, acl_key) do
      {:ok, <<>>} ->
        {:error, {:corrupt_acl, :empty_value}}

      {:ok, binary} when is_binary(binary) ->
        remove_acl_permission(db, acl_key, key, permission, binary)

      :not_found ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp remove_acl_permission(db, acl_key, key, permission, binary) do
    with :ok <- validate_permission_for_principal(key, permission),
         {:ok, current_entry} <- decode_acl_entry(binary, key) do
      update_acl_after_removal(db, acl_key, key, permission, current_entry)
    end
  end

  defp update_acl_after_removal(db, acl_key, key, permission, current_entry) do
    case Map.get(current_entry, key) do
      nil ->
        {:error, :not_found}

      [^permission] ->
        ErlangAdapter.delete(db, @acl_cf, acl_key)

      permissions ->
        persist_reduced_permissions(db, acl_key, key, permission, permissions, current_entry)
    end
  end

  defp persist_reduced_permissions(db, acl_key, key, permission, permissions, current_entry) do
    updated_permissions = List.delete(permissions, permission)

    if updated_permissions == permissions do
      {:error, :not_found}
    else
      updated_entry = Map.put(current_entry, key, updated_permissions)
      ErlangAdapter.put(db, @acl_cf, acl_key, :erlang.term_to_binary(updated_entry))
    end
  end

  defp get_acl_entry(db, graph_id, key) do
    acl_key = encode_acl_key(graph_id, key)

    case ErlangAdapter.get(db, @acl_cf, acl_key) do
      {:ok, <<>>} ->
        {:error, {:corrupt_acl, :empty_value}}

      {:ok, binary} when is_binary(binary) ->
        case decode_acl_entry(binary, key) do
          {:ok, entry} when map_size(entry) == 0 -> {:error, :not_found}
          result -> result
        end

      :not_found ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp encode_acl_key(graph_id, key) do
    "acl:graph:#{graph_id}:#{key}"
  end

  defp read_acl_entry_for_mutation(db, acl_key, principal) do
    case ErlangAdapter.get(db, @acl_cf, acl_key) do
      {:ok, <<>>} -> {:error, {:corrupt_acl, :empty_value}}
      {:ok, binary} when is_binary(binary) -> decode_acl_entry(binary, principal)
      :not_found -> {:ok, %{}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_acl_entry(binary, expected_principal) do
    binary
    |> :erlang.binary_to_term([:safe])
    |> validate_acl_entry(expected_principal)
  rescue
    ArgumentError -> {:error, {:corrupt_acl, :unsafe_or_invalid_term}}
  end

  defp validate_acl_entry(entry, _expected_principal) when entry == %{}, do: {:ok, entry}

  defp validate_acl_entry(%{version: version}, _expected_principal),
    do: {:error, {:corrupt_acl, {:unsupported_version, version}}}

  defp validate_acl_entry(%{"version" => version}, _expected_principal),
    do: {:error, {:corrupt_acl, {:unsupported_version, version}}}

  defp validate_acl_entry(entry, expected_principal)
       when is_map(entry) and map_size(entry) == 1 do
    [{principal, permissions}] = Map.to_list(entry)

    with :ok <- validate_acl_principal(principal),
         :ok <- validate_expected_principal(principal, expected_principal),
         :ok <- validate_persisted_permissions(principal, permissions) do
      {:ok, entry}
    else
      {:error, reason} -> {:error, {:corrupt_acl, reason}}
    end
  end

  defp validate_acl_entry(_entry, _expected_principal),
    do: {:error, {:corrupt_acl, :invalid_entry_shape}}

  defp validate_acl_principal("__public__"), do: :ok

  defp validate_acl_principal(principal) when is_binary(principal) do
    if Enum.any?(["user:", "role:", "owner:"], fn prefix ->
         String.starts_with?(principal, prefix) and byte_size(principal) > byte_size(prefix)
       end) do
      :ok
    else
      {:error, {:invalid_principal, principal}}
    end
  end

  defp validate_acl_principal(principal), do: {:error, {:invalid_principal, principal}}

  defp validate_expected_principal(principal, principal), do: :ok

  defp validate_expected_principal(principal, expected),
    do: {:error, {:principal_mismatch, expected, principal}}

  defp validate_persisted_permissions(principal, permissions)
       when is_list(permissions) and permissions != [] do
    with :ok <- validate_unique_permissions(permissions),
         :ok <- validate_known_permissions(permissions) do
      validate_principal_permissions(principal, permissions)
    end
  end

  defp validate_persisted_permissions(_principal, permissions),
    do: {:error, {:invalid_permissions, permissions}}

  defp validate_unique_permissions(permissions) do
    if Enum.uniq(permissions) == permissions, do: :ok, else: {:error, :duplicate_permissions}
  end

  defp validate_known_permissions(permissions) do
    if Enum.all?(permissions, &(&1 in @permissions)),
      do: :ok,
      else: {:error, {:invalid_permissions, permissions}}
  end

  defp validate_principal_permissions("__public__", [:read]), do: :ok

  defp validate_principal_permissions("__public__", permissions),
    do: {:error, {:invalid_public_permissions, permissions}}

  defp validate_principal_permissions("owner:" <> _owner, [:owner]), do: :ok

  defp validate_principal_permissions("owner:" <> _owner, permissions),
    do: {:error, {:invalid_owner_permissions, permissions}}

  defp validate_principal_permissions(principal, permissions) do
    if :owner in permissions,
      do: {:error, {:invalid_owner_permission, principal}},
      else: :ok
  end

  defp validate_permission_for_principal("__public__", :read), do: :ok
  defp validate_permission_for_principal("owner:" <> owner, :owner) when owner != "", do: :ok

  defp validate_permission_for_principal(principal, permission)
       when permission in [:read, :write, :admin] do
    if String.starts_with?(principal, "user:") or String.starts_with?(principal, "role:") do
      :ok
    else
      {:error, {:invalid_acl_permission, principal, permission}}
    end
  end

  defp validate_permission_for_principal(principal, permission),
    do: {:error, {:invalid_acl_permission, principal, permission}}

  defp collect_owner(key, value, acl_prefix, owner_id) do
    if String.starts_with?(key, acl_prefix) do
      principal = String.replace_prefix(key, acl_prefix, "")

      case decode_acl_entry(value, principal) do
        {:ok, %{^principal => [:owner]}} ->
          {:ok, owner_id || String.replace_prefix(principal, "owner:", "")}

        {:ok, _entry} ->
          {:ok, owner_id}

        {:error, _reason} = error ->
          error
      end
    else
      {:error, {:corrupt_acl, {:key_outside_graph_prefix, key}}}
    end
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

  # Emits a telemetry event for authorization denial.
  #
  # Telemetry Event: `[:triple_store, :sparql, :authorization, :denied]`
  #
  # Measurements:
  # - `%{system_time: integer()}` - Timestamp of the denial
  #
  # Metadata:
  # - `%{graph: String.t() | atom, user: String.t() | :public | nil, permission: atom(), reason: atom()}`
  #
  # Parameters:
  # - graph_iri - The graph IRI being accessed
  # - user - The user object or :public
  # - permission - The permission requested (:read, :write, :admin)
  # - reason - The reason for denial (:no_public_access, :not_owner, :no_user_permission, :no_role_permission)
  @spec emit_auth_denied_telemetry(String.t() | atom, user() | :public, atom(), atom()) :: :ok
  defp emit_auth_denied_telemetry(graph_iri, user_or_public, permission, reason) do
    user_id =
      case user_or_public do
        :public -> :public
        user when is_map(user) -> Map.get(user, :id, :unknown)
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
