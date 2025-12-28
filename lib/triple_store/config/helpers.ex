defmodule TripleStore.Config.Helpers do
  @moduledoc """
  Shared helper functions for RocksDB configuration modules.

  This module provides common utilities used across configuration modules:
  - Byte formatting for human-readable output
  - Validation helpers for configuration values
  - Value clamping for safe bounds

  ## Usage

      import TripleStore.Config.Helpers

      # Format bytes
      format_bytes(1_073_741_824)  # => "1.00 GB"

      # Validate values
      validate_positive(10, "count")        # => :ok
      validate_non_negative(-1, "offset")   # => {:error, "offset must be..."}

      # Clamp values
      clamp(100, 50, 200)  # => 100
      clamp(10, 50, 200)   # => 50

  """

  # ============================================================================
  # Byte Formatting
  # ============================================================================

  @doc """
  Formats a byte count as a human-readable string.

  Automatically selects the most appropriate unit (TB, GB, MB, KB, B)
  and formats with 2 decimal places for larger units.

  ## Examples

      iex> TripleStore.Config.Helpers.format_bytes(1_099_511_627_776)
      "1.00 TB"

      iex> TripleStore.Config.Helpers.format_bytes(1_073_741_824)
      "1.00 GB"

      iex> TripleStore.Config.Helpers.format_bytes(1_048_576)
      "1.00 MB"

      iex> TripleStore.Config.Helpers.format_bytes(1024)
      "1 KB"

      iex> TripleStore.Config.Helpers.format_bytes(512)
      "512 B"

  """
  @spec format_bytes(non_neg_integer()) :: String.t()
  def format_bytes(bytes) when bytes >= 1024 * 1024 * 1024 * 1024 do
    :io_lib.format("~.2f TB", [bytes / (1024 * 1024 * 1024 * 1024)]) |> to_string()
  end

  def format_bytes(bytes) when bytes >= 1024 * 1024 * 1024 do
    :io_lib.format("~.2f GB", [bytes / (1024 * 1024 * 1024)]) |> to_string()
  end

  def format_bytes(bytes) when bytes >= 1024 * 1024 do
    :io_lib.format("~.2f MB", [bytes / (1024 * 1024)]) |> to_string()
  end

  def format_bytes(bytes) when bytes >= 1024 do
    "#{div(bytes, 1024)} KB"
  end

  def format_bytes(bytes) do
    "#{bytes} B"
  end

  # ============================================================================
  # Validation Helpers
  # ============================================================================

  @doc """
  Validates that a value is a positive integer (> 0).

  ## Examples

      iex> TripleStore.Config.Helpers.validate_positive(10, "count")
      :ok

      iex> TripleStore.Config.Helpers.validate_positive(0, "count")
      {:error, "count must be a positive integer"}

      iex> TripleStore.Config.Helpers.validate_positive(-1, "count")
      {:error, "count must be a positive integer"}

  """
  @spec validate_positive(term(), String.t()) :: :ok | {:error, String.t()}
  def validate_positive(value, _name) when is_integer(value) and value > 0, do: :ok
  def validate_positive(_, name), do: {:error, "#{name} must be a positive integer"}

  @doc """
  Validates that a value is a non-negative integer (>= 0).

  ## Examples

      iex> TripleStore.Config.Helpers.validate_non_negative(0, "offset")
      :ok

      iex> TripleStore.Config.Helpers.validate_non_negative(10, "offset")
      :ok

      iex> TripleStore.Config.Helpers.validate_non_negative(-1, "offset")
      {:error, "offset must be a non-negative integer"}

  """
  @spec validate_non_negative(term(), String.t()) :: :ok | {:error, String.t()}
  def validate_non_negative(value, _name) when is_integer(value) and value >= 0, do: :ok
  def validate_non_negative(_, name), do: {:error, "#{name} must be a non-negative integer"}

  @doc """
  Validates that a value is at least a minimum value.

  ## Examples

      iex> TripleStore.Config.Helpers.validate_min(10, 5, "count")
      :ok

      iex> TripleStore.Config.Helpers.validate_min(3, 5, "count")
      {:error, "count must be at least 5"}

  """
  @spec validate_min(term(), integer(), String.t()) :: :ok | {:error, String.t()}
  def validate_min(value, min, _name) when is_integer(value) and value >= min, do: :ok
  def validate_min(_, min, name), do: {:error, "#{name} must be at least #{min}"}

  @doc """
  Validates that a value is within a range (inclusive).

  ## Examples

      iex> TripleStore.Config.Helpers.validate_range(10, 1, 100, "level")
      :ok

      iex> TripleStore.Config.Helpers.validate_range(0, 1, 100, "level")
      {:error, "level must be between 1 and 100"}

  """
  @spec validate_range(term(), integer(), integer(), String.t()) :: :ok | {:error, String.t()}
  def validate_range(value, min, max, _name)
      when is_integer(value) and value >= min and value <= max,
      do: :ok

  def validate_range(_, min, max, name), do: {:error, "#{name} must be between #{min} and #{max}"}

  @doc """
  Validates that a value is one of the allowed values.

  ## Examples

      iex> TripleStore.Config.Helpers.validate_one_of(:level, [:level, :universal, :fifo], "style")
      :ok

      iex> TripleStore.Config.Helpers.validate_one_of(:invalid, [:level, :universal, :fifo], "style")
      {:error, "style must be one of [:level, :universal, :fifo]"}

  """
  @spec validate_one_of(term(), list(), String.t()) :: :ok | {:error, String.t()}
  def validate_one_of(value, allowed, name) do
    if value in allowed do
      :ok
    else
      {:error, "#{name} must be one of #{inspect(allowed)}"}
    end
  end

  # ============================================================================
  # Value Clamping
  # ============================================================================

  @doc """
  Clamps a value to be within a minimum and maximum range.

  ## Examples

      iex> TripleStore.Config.Helpers.clamp(100, 50, 200)
      100

      iex> TripleStore.Config.Helpers.clamp(10, 50, 200)
      50

      iex> TripleStore.Config.Helpers.clamp(300, 50, 200)
      200

  """
  @spec clamp(number(), number(), number()) :: number()
  def clamp(value, min_val, max_val) do
    value
    |> max(min_val)
    |> min(max_val)
  end

  # ============================================================================
  # Validation Chain Helpers
  # ============================================================================

  @doc """
  Runs a list of validation functions and returns the first error or :ok.

  Each validation should be a tuple of {function, args} where function
  returns :ok or {:error, reason}.

  ## Examples

      iex> validations = [
      ...>   fn -> TripleStore.Config.Helpers.validate_positive(10, "a") end,
      ...>   fn -> TripleStore.Config.Helpers.validate_positive(5, "b") end
      ...> ]
      iex> TripleStore.Config.Helpers.validate_all(validations)
      :ok

      iex> validations = [
      ...>   fn -> TripleStore.Config.Helpers.validate_positive(-1, "a") end,
      ...>   fn -> TripleStore.Config.Helpers.validate_positive(5, "b") end
      ...> ]
      iex> TripleStore.Config.Helpers.validate_all(validations)
      {:error, "a must be a positive integer"}

  """
  @spec validate_all([(() -> :ok | {:error, String.t()})]) :: :ok | {:error, String.t()}
  def validate_all(validations) do
    Enum.reduce_while(validations, :ok, fn validation, :ok ->
      case validation.() do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end
end
