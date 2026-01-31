defmodule TripleStore.IndexProtocol do
  @moduledoc """
  Protocol for index operations on different index types (S21).

  This protocol defines a polymorphic interface for working with different
  index types (triple vs quad indices). It provides a uniform way to:
  - Encode/decode keys
  - Create prefixes for pattern matching
  - Get index metadata

  ## Index Types

  ### Triple Indices
  - `:spo` - Subject-Predicate-Object (primary index)
  - `:pos` - Predicate-Object-Subject
  - `:osp` - Object-Subject-Predicate

  ### Quad Indices
  - `:gspo` - Graph-Subject-Predicate-Object (primary quad index)
  - `:gpos` - Graph-Predicate-Object-Subject
  - `:gosp` - Graph-Object-Subject-Predicate
  - `:spog` - Subject-Predicate-Object-Graph
  - `:posg` - Predicate-Object-Subject-Graph
  - `:ospg` - Object-Subject-Predicate-Graph

  ### Dictionary Indices
  - `:id2str` - ID to string mapping
  - `:str2id` - String to ID mapping

  ### Derived Data
  - `:derived` - Materialized reasoning results
  - `:numeric_range` - Numeric range index
  - `:acl` - Access control lists

  ## Example

      iex> IndexProtocol.key_size(:spo)
      3

      iex> IndexProtocol.arity(:spo)
      3

      iex> IndexProtocol.arity(:gspo)
      4

  """

  @doc """
  Returns the number of components in an index key.
  """
  @callback key_size() :: pos_integer()

  @doc """
  Returns the column family atom for this index.
  """
  @callback column_family() :: atom()

  @doc """
  Returns a human-readable name for the index.
  """
  @callback name() :: String.t()

  @doc """
  Returns the key encoding order as a list of positions.
  For example, :spo returns [:subject, :predicate, :object].
  """
  @callback key_order() :: [atom()]

  @doc """
  Checks if this is a quad index (4 components) vs triple index (3 components).
  """
  @callback quad?() :: boolean()
end

# ===========================================================================
# Index Type Implementations
# ===========================================================================

defmodule TripleStore.IndexProtocol.SPO do
  @moduledoc """
  SPO (Subject-Predicate-Object) index implementation.

  Key encoding: <<subject::64-big, predicate::64-big, object::64-big>>
  """
  @behaviour TripleStore.IndexProtocol

  def key_size, do: 3
  def column_family, do: :spo
  def name, do: "Subject-Predicate-Object"
  def key_order, do: [:subject, :predicate, :object]
  def quad?, do: false
end

defmodule TripleStore.IndexProtocol.POS do
  @moduledoc """
  POS (Predicate-Object-Subject) index implementation.

  Key encoding: <<predicate::64-big, object::64-big, subject::64-big>>
  """
  @behaviour TripleStore.IndexProtocol

  def key_size, do: 3
  def column_family, do: :pos
  def name, do: "Predicate-Object-Subject"
  def key_order, do: [:predicate, :object, :subject]
  def quad?, do: false
end

defmodule TripleStore.IndexProtocol.OSP do
  @moduledoc """
  OSP (Object-Subject-Predicate) index implementation.

  Key encoding: <<object::64-big, subject::64-big, predicate::64-big>>
  """
  @behaviour TripleStore.IndexProtocol

  def key_size, do: 3
  def column_family, do: :osp
  def name, do: "Object-Subject-Predicate"
  def key_order, do: [:object, :subject, :predicate]
  def quad?, do: false
end

defmodule TripleStore.IndexProtocol.GSPO do
  @moduledoc """
  GSPO (Graph-Subject-Predicate-Object) index implementation.

  Key encoding: <<graph::64-big, subject::64-big, predicate::64-big, object::64-big>>
  """
  @behaviour TripleStore.IndexProtocol

  def key_size, do: 4
  def column_family, do: :gspo
  def name, do: "Graph-Subject-Predicate-Object"
  def key_order, do: [:graph, :subject, :predicate, :object]
  def quad?, do: true
end

defmodule TripleStore.IndexProtocol.GPOS do
  @moduledoc """
  GPOS (Graph-Predicate-Object-Subject) index implementation.

  Key encoding: <<graph::64-big, predicate::64-big, object::64-big, subject::64-big>>
  """
  @behaviour TripleStore.IndexProtocol

  def key_size, do: 4
  def column_family, do: :gpos
  def name, do: "Graph-Predicate-Object-Subject"
  def key_order, do: [:graph, :predicate, :object, :subject]
  def quad?, do: true
end

defmodule TripleStore.IndexProtocol.GOSP do
  @moduledoc """
  GOSP (Graph-Object-Subject-Predicate) index implementation.

  Key encoding: <<graph::64-big, object::64-big, subject::64-big, predicate::64-big>>
  """
  @behaviour TripleStore.IndexProtocol

  def key_size, do: 4
  def column_family, do: :gosp
  def name, do: "Graph-Object-Subject-Predicate"
  def key_order, do: [:graph, :object, :subject, :predicate]
  def quad?, do: true
end

defmodule TripleStore.IndexProtocol.SPOG do
  @moduledoc """
  SPOG (Subject-Predicate-Object-Graph) index implementation.

  Key encoding: <<subject::64-big, predicate::64-big, object::64-big, graph::64-big>>
  """
  @behaviour TripleStore.IndexProtocol

  def key_size, do: 4
  def column_family, do: :spog
  def name, do: "Subject-Predicate-Object-Graph"
  def key_order, do: [:subject, :predicate, :object, :graph]
  def quad?, do: true
end

defmodule TripleStore.IndexProtocol.POSG do
  @moduledoc """
  POSG (Predicate-Object-Subject-Graph) index implementation.

  Key encoding: <<predicate::64-big, object::64-big, subject::64-big, graph::64-big>>
  """
  @behaviour TripleStore.IndexProtocol

  def key_size, do: 4
  def column_family, do: :posg
  def name, do: "Predicate-Object-Subject-Graph"
  def key_order, do: [:predicate, :object, :subject, :graph]
  def quad?, do: true
end

defmodule TripleStore.IndexProtocol.OSPG do
  @moduledoc """
  OSPG (Object-Subject-Predicate-Graph) index implementation.

  Key encoding: <<object::64-big, subject::64-big, predicate::64-big, graph::64-big>>
  """
  @behaviour TripleStore.IndexProtocol

  def key_size, do: 4
  def column_family, do: :ospg
  def name, do: "Object-Subject-Predicate-Graph"
  def key_order, do: [:object, :subject, :predicate, :graph]
  def quad?, do: true
end

# ===========================================================================
# Index Protocol Helpers
# ===========================================================================

defmodule TripleStore.IndexProtocol.Helpers do
  @moduledoc """
  Helper functions for working with index types (S21).

  Provides utilities to query index metadata without directly
  accessing the index implementation modules.
  """

  @doc """
  Gets the key size (number of components) for an index type.

  ## Examples

      iex> IndexProtocol.Helpers.key_size(:spo)
      3

      iex> IndexProtocol.Helpers.key_size(:gspo)
      4

  """
  @spec key_size(atom()) :: pos_integer()
  def key_size(:spo), do: 3
  def key_size(:pos), do: 3
  def key_size(:osp), do: 3
  def key_size(:gspo), do: 4
  def key_size(:gpos), do: 4
  def key_size(:gosp), do: 4
  def key_size(:spog), do: 4
  def key_size(:posg), do: 4
  def key_size(:ospg), do: 4
  def key_size(_), do: 0

  @doc """
  Gets the key component order for an index type.

  ## Examples

      iex> IndexProtocol.Helpers.key_order(:spo)
      [:subject, :predicate, :object]

      iex> IndexProtocol.Helpers.key_order(:gspo)
      [:graph, :subject, :predicate, :object]

  """
  @spec key_order(atom()) :: [atom()]
  def key_order(:spo), do: [:subject, :predicate, :object]
  def key_order(:pos), do: [:predicate, :object, :subject]
  def key_order(:osp), do: [:object, :subject, :predicate]
  def key_order(:gspo), do: [:graph, :subject, :predicate, :object]
  def key_order(:gpos), do: [:graph, :predicate, :object, :subject]
  def key_order(:gosp), do: [:graph, :object, :subject, :predicate]
  def key_order(:spog), do: [:subject, :predicate, :object, :graph]
  def key_order(:posg), do: [:predicate, :object, :subject, :graph]
  def key_order(:ospg), do: [:object, :subject, :predicate, :graph]
  def key_order(_), do: []

  @doc """
  Checks if an index type is a quad index (4 components).

  ## Examples

      iex> IndexProtocol.Helpers.quad?(:spo)
      false

      iex> IndexProtocol.Helpers.quad?(:gspo)
      true

  """
  @spec quad?(atom()) :: boolean()
  def quad?(:spo), do: false
  def quad?(:pos), do: false
  def quad?(:osp), do: false
  def quad?(:gspo), do: true
  def quad?(:gpos), do: true
  def quad?(:gosp), do: true
  def quad?(:spog), do: true
  def quad?(:posg), do: true
  def quad?(:ospg), do: true
  def quad?(_), do: false

  @doc """
  Checks if an index type is a triple index (3 components).

  ## Examples

      iex> IndexProtocol.Helpers.triple?(:spo)
      true

      iex> IndexProtocol.Helpers.triple?(:gspo)
      false

  """
  @spec triple?(atom()) :: boolean()
  def triple?(index), do: not quad?(index) and key_size(index) == 3

  @doc """
  Gets all valid triple index types.

  ## Examples

      iex> IndexProtocol.Helpers.triple_indices()
      [:spo, :pos, :osp]

  """
  @spec triple_indices() :: [atom()]
  def triple_indices, do: [:spo, :pos, :osp]

  @doc """
  Gets all valid quad index types.

  ## Examples

      iex> IndexProtocol.Helpers.quad_indices()
      [:gspo, :gpos, :gosp, :spog, :posg, :ospg]

  """
  @spec quad_indices() :: [atom()]
  def quad_indices, do: [:gspo, :gpos, :gosp, :spog, :posg, :ospg]

  @doc """
  Gets all valid index types.

  ## Examples

      iex> IndexProtocol.Helpers.all_indices()
      [:spo, :pos, :osp, :gspo, :gpos, :gosp, :spog, :posg, :ospg]

  """
  @spec all_indices() :: [atom()]
  def all_indices, do: triple_indices() ++ quad_indices()

  @doc """
  Validates an index type.

  ## Returns

  - `:ok` if the index type is valid
  - `{:error, :invalid_index}` if not a recognized index

  ## Examples

      iex> IndexProtocol.Helpers.validate_index(:spo)
      :ok

      iex> IndexProtocol.Helpers.validate_index(:invalid)
      {:error, :invalid_index}

  """
  @spec validate_index(atom()) :: :ok | {:error, :invalid_index}
  def validate_index(index) do
    valid_indices = triple_indices() ++ quad_indices()

    if index in valid_indices do
      :ok
    else
      {:error, :invalid_index}
    end
  end

  @doc """
  Validates an index type, raising an error if invalid.

  ## Examples

      iex> IndexProtocol.Helpers.validate_index!(:spo)
      :ok

      iex> IndexProtocol.Helpers.validate_index!(:invalid)
      ** (ArgumentError) Invalid index type: :invalid

  """
  @spec validate_index!(atom()) :: :ok | no_return()
  def validate_index!(index) do
    case validate_index(index) do
      :ok -> :ok
      {:error, :invalid_index} -> raise ArgumentError, "Invalid index type: #{inspect(index)}"
    end
  end
end
