defmodule TripleStore.Dictionary do
  @moduledoc """
  Dictionary encoding for RDF terms.

  Maps RDF terms (URIs, blank nodes, literals) to 64-bit integer IDs
  with type tagging. This enables compact storage and fast comparisons.

  ## Type Tags

  The high 4 bits of each ID encode the term type:
  - `0b0001` - URI (sequence-based)
  - `0b0010` - Blank node (sequence-based)
  - `0b0011` - Literal (sequence-based, dictionary lookup required)
  - `0b0100` - xsd:integer (inline encoded)
  - `0b0101` - xsd:decimal (inline encoded)
  - `0b0110` - xsd:dateTime (inline encoded)

  ## ID Space Separation

  Types 1-3 (URI, BNode, Literal) use sequence counter allocation.
  Types 4-6 (Integer, Decimal, DateTime) encode values inline.
  This separation prevents any ID collision by design:

      Type 1 (URI):      0x1000_0000_0000_0000 to 0x1FFF_FFFF_FFFF_FFFF
      Type 2 (BNode):    0x2000_0000_0000_0000 to 0x2FFF_FFFF_FFFF_FFFF
      Type 3 (Literal):  0x3000_0000_0000_0000 to 0x3FFF_FFFF_FFFF_FFFF
      Type 4 (Integer):  0x4000_0000_0000_0000 to 0x4FFF_FFFF_FFFF_FFFF
      Type 5 (Decimal):  0x5000_0000_0000_0000 to 0x5FFF_FFFF_FFFF_FFFF
      Type 6 (DateTime): 0x6000_0000_0000_0000 to 0x6FFF_FFFF_FFFF_FFFF

  ## Inline Numeric Encoding

  Numeric types that fit within 60 bits are encoded directly in the ID,
  avoiding dictionary lookup for common values like counts and timestamps.

  ### xsd:integer
  - Bit layout: `[type:4][sign:1][value:59]`
  - Range: `[-2^59, 2^59)` = `[-576460752303423488, 576460752303423487]`
  - Encoding: Two's complement in 60-bit field

  ### xsd:decimal
  - Bit layout: `[type:4][sign:1][exponent:11][mantissa:48]`
  - Exponent: Biased by 1023 (similar to IEEE 754)
  - Precision: ~14-15 significant decimal digits
  - Values outside range fall back to dictionary encoding

  ### xsd:dateTime
  - Bit layout: `[type:4][milliseconds:60]`
  - Range: 1970-01-01 to approximately year 36812066
  - Precision: Milliseconds
  - Timezone: Always normalized to UTC before encoding

  ## Sequence Counter Persistence

  The sequence counter uses `:atomics` for lock-free increments with
  periodic persistence to RocksDB:

  - **Flush interval**: Every 1000 IDs allocated
  - **Recovery**: Load persisted value + 1000 safety margin
  - **Graceful shutdown**: Checkpoint current value

  This ensures no ID reuse even after unexpected crashes, at the cost
  of potentially skipping up to 1000 IDs.

  ## Concurrency Model

  The `get_or_create_id/2` operation uses GenServer serialization to
  ensure atomic create-if-not-exists semantics. This prevents race
  conditions where two processes might create different IDs for the
  same term.

  For read-only operations (`lookup_id/2`, `lookup_term/2`), direct
  NIF calls provide maximum performance without serialization.

  ## Input Validation

  All term encoding operations validate input:
  - **Max term size**: 16KB (16,384 bytes)
  - **Null bytes**: Rejected in URIs (allowed in literals)
  - **Unicode**: Normalized to NFC before encoding

  ## Overflow Protection

  The sequence counter is monitored for overflow:
  - **Max sequence**: `2^59 - 1` per type (over 576 quadrillion)
  - **Error return**: `{:error, :sequence_overflow}` when exhausted
  - **Telemetry**: Alert when reaching 50% utilization
  """

  import Bitwise

  # ===========================================================================
  # Type Definitions
  # ===========================================================================

  @typedoc "64-bit term ID with 4-bit type tag and 60-bit value/sequence"
  @type term_id :: non_neg_integer()

  @typedoc "60-bit sequence number for dictionary-allocated terms"
  @type sequence :: non_neg_integer()

  @typedoc "Term type atom derived from type tag"
  @type term_type :: :uri | :bnode | :literal | :integer | :decimal | :datetime

  @typedoc "RDF term representation (URI, blank node, or literal)"
  @type rdf_term :: RDF.IRI.t() | RDF.BlankNode.t() | RDF.Literal.t()

  @typedoc "Database reference from RocksDB NIF"
  @type db_ref :: reference()

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Type tag constants (high 4 bits of 64-bit ID)
  @type_uri 0b0001
  @type_bnode 0b0010
  @type_literal 0b0011
  @type_integer 0b0100
  @type_decimal 0b0101
  @type_datetime 0b0110

  # Term encoding prefix constants (shared with StringToId/IdToString)
  # These are used to serialize RDF terms to binary keys
  @prefix_uri 1
  @prefix_bnode 2
  @prefix_literal 3

  # Literal subtype constants (shared with StringToId/IdToString)
  @literal_plain 0
  @literal_typed 1
  @literal_lang 2

  # Sequence counter constants
  @max_sequence (1 <<< 59) - 1
  @flush_interval 1000
  @safety_margin 1000

  # Input validation constants
  @max_term_size 16_384

  # Inline encoding constants
  @max_inline_integer (1 <<< 59) - 1
  @min_inline_integer -(1 <<< 59)

  # DateTime encoding (milliseconds since Unix epoch)
  @max_inline_datetime (1 <<< 60) - 1

  # Decimal encoding constants
  @decimal_exponent_bias 1023
  @decimal_exponent_bits 11
  @decimal_mantissa_bits 48
  @max_decimal_exponent (1 <<< @decimal_exponent_bits) - 1
  @max_decimal_mantissa (1 <<< @decimal_mantissa_bits) - 1

  # ===========================================================================
  # Type Tag Accessors
  # ===========================================================================

  @doc """
  Returns the type tag for URIs.

  ## Examples

      iex> TripleStore.Dictionary.type_uri()
      1
  """
  @spec type_uri() :: non_neg_integer()
  def type_uri, do: @type_uri

  @doc """
  Returns the type tag for blank nodes.

  ## Examples

      iex> TripleStore.Dictionary.type_bnode()
      2
  """
  @spec type_bnode() :: non_neg_integer()
  def type_bnode, do: @type_bnode

  @doc """
  Returns the type tag for literals requiring dictionary lookup.

  ## Examples

      iex> TripleStore.Dictionary.type_literal()
      3
  """
  @spec type_literal() :: non_neg_integer()
  def type_literal, do: @type_literal

  @doc """
  Returns the type tag for inline-encoded integers.

  ## Examples

      iex> TripleStore.Dictionary.type_integer()
      4
  """
  @spec type_integer() :: non_neg_integer()
  def type_integer, do: @type_integer

  @doc """
  Returns the type tag for inline-encoded decimals.

  ## Examples

      iex> TripleStore.Dictionary.type_decimal()
      5
  """
  @spec type_decimal() :: non_neg_integer()
  def type_decimal, do: @type_decimal

  @doc """
  Returns the type tag for inline-encoded datetimes.

  ## Examples

      iex> TripleStore.Dictionary.type_datetime()
      6
  """
  @spec type_datetime() :: non_neg_integer()
  def type_datetime, do: @type_datetime

  # ===========================================================================
  # Constant Accessors (for configuration and testing)
  # ===========================================================================

  @doc """
  Returns the maximum sequence number before overflow.

  This is `2^59 - 1`, allowing over 576 quadrillion IDs per type.

  ## Examples

      iex> TripleStore.Dictionary.max_sequence()
      576460752303423487
  """
  @spec max_sequence() :: sequence()
  def max_sequence, do: @max_sequence

  @doc """
  Returns the flush interval for sequence counter persistence.

  The counter is persisted to RocksDB every N allocations.

  ## Examples

      iex> TripleStore.Dictionary.flush_interval()
      1000
  """
  @spec flush_interval() :: pos_integer()
  def flush_interval, do: @flush_interval

  @doc """
  Returns the safety margin added during counter recovery.

  On startup, this value is added to the persisted counter to ensure
  no ID reuse even if a crash occurred before the last flush.

  ## Examples

      iex> TripleStore.Dictionary.safety_margin()
      1000
  """
  @spec safety_margin() :: pos_integer()
  def safety_margin, do: @safety_margin

  @doc """
  Returns the maximum term size in bytes.

  Terms larger than this are rejected with `{:error, :term_too_large}`.

  ## Examples

      iex> TripleStore.Dictionary.max_term_size()
      16384
  """
  @spec max_term_size() :: pos_integer()
  def max_term_size, do: @max_term_size

  @doc """
  Returns the maximum value for inline integer encoding.

  Integers larger than this must use dictionary encoding.

  ## Examples

      iex> TripleStore.Dictionary.max_inline_integer()
      576460752303423487
  """
  @spec max_inline_integer() :: integer()
  def max_inline_integer, do: @max_inline_integer

  @doc """
  Returns the minimum value for inline integer encoding.

  Integers smaller than this must use dictionary encoding.

  ## Examples

      iex> TripleStore.Dictionary.min_inline_integer()
      -576460752303423488
  """
  @spec min_inline_integer() :: integer()
  def min_inline_integer, do: @min_inline_integer

  # ===========================================================================
  # Term Encoding Prefix Accessors (for StringToId/IdToString)
  # ===========================================================================

  @doc """
  Returns the binary prefix for URI term encoding.

  Used in str2id/id2str column families.
  """
  @spec prefix_uri() :: non_neg_integer()
  def prefix_uri, do: @prefix_uri

  @doc """
  Returns the binary prefix for blank node term encoding.

  Used in str2id/id2str column families.
  """
  @spec prefix_bnode() :: non_neg_integer()
  def prefix_bnode, do: @prefix_bnode

  @doc """
  Returns the binary prefix for literal term encoding.

  Used in str2id/id2str column families.
  """
  @spec prefix_literal() :: non_neg_integer()
  def prefix_literal, do: @prefix_literal

  @doc """
  Returns the subtype marker for plain literals (no datatype).
  """
  @spec literal_plain() :: non_neg_integer()
  def literal_plain, do: @literal_plain

  @doc """
  Returns the subtype marker for typed literals.
  """
  @spec literal_typed() :: non_neg_integer()
  def literal_typed, do: @literal_typed

  @doc """
  Returns the subtype marker for language-tagged literals.
  """
  @spec literal_lang() :: non_neg_integer()
  def literal_lang, do: @literal_lang

  # ===========================================================================
  # ID Encoding/Decoding (Task 1.3.1)
  # ===========================================================================

  @doc """
  Encodes a type tag and sequence number into a 64-bit term ID.

  The type tag occupies the high 4 bits, and the sequence/value
  occupies the low 60 bits.

  ## Arguments

  - `type` - Type tag (0-15)
  - `value` - Sequence number or inline value (0 to 2^60-1)

  ## Returns

  - 64-bit integer term ID

  ## Examples

      iex> TripleStore.Dictionary.encode_id(1, 42)
      1152921504606846976 + 42

      iex> id = TripleStore.Dictionary.encode_id(1, 100)
      iex> TripleStore.Dictionary.decode_id(id)
      {:uri, 100}
  """
  @spec encode_id(non_neg_integer(), non_neg_integer()) :: term_id()
  def encode_id(type, value) when type >= 0 and type <= 15 and value >= 0 do
    type <<< 60 ||| value
  end

  @doc """
  Decodes a term ID into its type and sequence/value components.

  ## Arguments

  - `id` - 64-bit term ID

  ## Returns

  - `{type_atom, value}` tuple where type_atom is the term type

  ## Examples

      iex> id = TripleStore.Dictionary.encode_id(1, 42)
      iex> TripleStore.Dictionary.decode_id(id)
      {:uri, 42}

      iex> id = TripleStore.Dictionary.encode_id(4, 12345)
      iex> TripleStore.Dictionary.decode_id(id)
      {:integer, 12345}
  """
  @spec decode_id(term_id()) :: {term_type(), non_neg_integer()} | {:unknown, non_neg_integer()}
  def decode_id(id) when is_integer(id) and id >= 0 do
    type_tag = id >>> 60
    value = id &&& 0x0FFFFFFFFFFFFFFF
    {tag_to_type(type_tag), value}
  end

  @doc """
  Extracts the term type from a term ID without the value.

  This is a fast O(1) operation using bit extraction.

  ## Arguments

  - `id` - 64-bit term ID

  ## Returns

  - Term type atom (`:uri`, `:bnode`, `:literal`, `:integer`, `:decimal`, `:datetime`)
  - `:unknown` for unrecognized type tags

  ## Examples

      iex> id = TripleStore.Dictionary.encode_id(1, 42)
      iex> TripleStore.Dictionary.term_type(id)
      :uri

      iex> id = TripleStore.Dictionary.encode_id(4, 100)
      iex> TripleStore.Dictionary.term_type(id)
      :integer
  """
  @spec term_type(term_id()) :: term_type() | :unknown
  def term_type(id) when is_integer(id) and id >= 0 do
    tag_to_type(id >>> 60)
  end

  @doc """
  Checks if a term ID represents an inline-encoded value.

  Inline types (integer, decimal, datetime) don't require dictionary
  lookup to retrieve the original value.

  ## Examples

      iex> uri_id = TripleStore.Dictionary.encode_id(1, 42)
      iex> TripleStore.Dictionary.inline_encoded?(uri_id)
      false

      iex> int_id = TripleStore.Dictionary.encode_id(4, 42)
      iex> TripleStore.Dictionary.inline_encoded?(int_id)
      true
  """
  @spec inline_encoded?(term_id()) :: boolean()
  def inline_encoded?(id) when is_integer(id) and id >= 0 do
    type_tag = id >>> 60
    type_tag in [@type_integer, @type_decimal, @type_datetime]
  end

  @doc """
  Checks if a term ID represents a dictionary-allocated term.

  Dictionary types (URI, bnode, literal) require lookup in the
  id2str column family to retrieve the original term.

  ## Examples

      iex> uri_id = TripleStore.Dictionary.encode_id(1, 42)
      iex> TripleStore.Dictionary.dictionary_allocated?(uri_id)
      true

      iex> int_id = TripleStore.Dictionary.encode_id(4, 42)
      iex> TripleStore.Dictionary.dictionary_allocated?(int_id)
      false
  """
  @spec dictionary_allocated?(term_id()) :: boolean()
  def dictionary_allocated?(id) when is_integer(id) and id >= 0 do
    type_tag = id >>> 60
    type_tag in [@type_uri, @type_bnode, @type_literal]
  end

  # ===========================================================================
  # Input Validation (Concern C1)
  # ===========================================================================

  @doc """
  Validates a term binary for encoding.

  ## Validation Rules

  1. Size must not exceed `max_term_size/0` (16KB)
  2. URIs must not contain null bytes (0x00)
  3. String should be valid UTF-8

  ## Arguments

  - `term_binary` - Binary representation of the term
  - `term_type` - Type of term (`:uri`, `:bnode`, `:literal`)

  ## Returns

  - `:ok` if valid
  - `{:error, reason}` if invalid

  ## Examples

      iex> TripleStore.Dictionary.validate_term("http://example.org", :uri)
      :ok

      iex> TripleStore.Dictionary.validate_term(<<0>>, :uri)
      {:error, :null_byte_in_uri}
  """
  @spec validate_term(binary(), term_type()) :: :ok | {:error, atom()}
  def validate_term(term_binary, term_type) when is_binary(term_binary) do
    cond do
      byte_size(term_binary) > @max_term_size ->
        {:error, :term_too_large}

      term_type == :uri and contains_null_byte?(term_binary) ->
        {:error, :null_byte_in_uri}

      not String.valid?(term_binary) ->
        {:error, :invalid_utf8}

      true ->
        :ok
    end
  end

  @doc """
  Normalizes a string to NFC Unicode form.

  This ensures that equivalent Unicode representations of the same
  string map to the same dictionary ID.

  ## Examples

      iex> TripleStore.Dictionary.normalize_unicode("café")
      "café"
  """
  @spec normalize_unicode(String.t()) :: String.t()
  def normalize_unicode(string) when is_binary(string) do
    # Use :unicode.characters_to_nfc_binary for NFC normalization
    case :unicode.characters_to_nfc_binary(string) do
      result when is_binary(result) -> result
      # Fallback for invalid Unicode (should not happen after validate_term)
      _ -> string
    end
  end

  # ===========================================================================
  # Inline Numeric Encoding (Task 1.3.5)
  # ===========================================================================

  @doc """
  Checks if an integer value can be inline-encoded.

  ## Range

  Inline encoding supports integers in the range `[-2^59, 2^59)`.

  ## Examples

      iex> TripleStore.Dictionary.inline_encodable_integer?(42)
      true

      iex> TripleStore.Dictionary.inline_encodable_integer?(-100)
      true

      iex> large = TripleStore.Dictionary.max_inline_integer() + 1
      iex> TripleStore.Dictionary.inline_encodable_integer?(large)
      false
  """
  @spec inline_encodable_integer?(integer()) :: boolean()
  def inline_encodable_integer?(value) when is_integer(value) do
    value >= @min_inline_integer and value <= @max_inline_integer
  end

  @doc """
  Encodes an integer value as an inline term ID.

  Uses two's complement representation in the low 60 bits.

  ## Arguments

  - `value` - Integer in range `[-2^59, 2^59)`

  ## Returns

  - `{:ok, term_id}` on success
  - `{:error, :out_of_range}` if value is outside inline range

  ## Examples

      iex> {:ok, id} = TripleStore.Dictionary.encode_integer(42)
      iex> TripleStore.Dictionary.decode_integer(id)
      {:ok, 42}

      iex> {:ok, id} = TripleStore.Dictionary.encode_integer(-100)
      iex> TripleStore.Dictionary.decode_integer(id)
      {:ok, -100}
  """
  @spec encode_integer(integer()) :: {:ok, term_id()} | {:error, :out_of_range}
  def encode_integer(value) when is_integer(value) do
    if inline_encodable_integer?(value) do
      # Two's complement: negative values become large positive in 60-bit space
      encoded_value = value &&& 0x0FFFFFFFFFFFFFFF
      {:ok, encode_id(@type_integer, encoded_value)}
    else
      {:error, :out_of_range}
    end
  end

  @doc """
  Decodes an inline-encoded integer from a term ID.

  ## Arguments

  - `id` - Term ID with integer type tag

  ## Returns

  - `{:ok, integer}` on success
  - `{:error, :not_an_integer}` if type tag is not integer

  ## Examples

      iex> {:ok, id} = TripleStore.Dictionary.encode_integer(42)
      iex> TripleStore.Dictionary.decode_integer(id)
      {:ok, 42}
  """
  @spec decode_integer(term_id()) :: {:ok, integer()} | {:error, :not_an_integer}
  def decode_integer(id) when is_integer(id) and id >= 0 do
    type_tag = id >>> 60

    if type_tag == @type_integer do
      value = id &&& 0x0FFFFFFFFFFFFFFF

      # Convert from two's complement
      decoded =
        if value >= 1 <<< 59 do
          value - (1 <<< 60)
        else
          value
        end

      {:ok, decoded}
    else
      {:error, :not_an_integer}
    end
  end

  @doc """
  Checks if a datetime value can be inline-encoded.

  ## Arguments

  - `datetime` - DateTime struct

  ## Returns

  - `true` if the datetime can be encoded (after 1970-01-01, before year ~36812066)
  - `false` otherwise
  """
  @spec inline_encodable_datetime?(DateTime.t()) :: boolean()
  def inline_encodable_datetime?(%DateTime{} = datetime) do
    ms = DateTime.to_unix(datetime, :millisecond)
    ms >= 0 and ms <= @max_inline_datetime
  end

  @doc """
  Encodes a DateTime as an inline term ID.

  Uses milliseconds since Unix epoch (1970-01-01T00:00:00Z).
  The datetime is normalized to UTC before encoding.

  ## Arguments

  - `datetime` - DateTime struct

  ## Returns

  - `{:ok, term_id}` on success
  - `{:error, :out_of_range}` if datetime is before 1970

  ## Examples

      iex> {:ok, dt} = DateTime.new(~D[2024-01-15], ~T[10:30:00])
      iex> {:ok, id} = TripleStore.Dictionary.encode_datetime(dt)
      iex> {:ok, decoded} = TripleStore.Dictionary.decode_datetime(id)
      iex> DateTime.to_unix(decoded, :millisecond) == DateTime.to_unix(dt, :millisecond)
      true
  """
  @spec encode_datetime(DateTime.t()) :: {:ok, term_id()} | {:error, :out_of_range}
  def encode_datetime(%DateTime{} = datetime) do
    # Normalize to UTC
    utc_datetime = DateTime.shift_zone!(datetime, "Etc/UTC")
    ms = DateTime.to_unix(utc_datetime, :millisecond)

    if ms >= 0 and ms <= @max_inline_datetime do
      {:ok, encode_id(@type_datetime, ms)}
    else
      {:error, :out_of_range}
    end
  end

  @doc """
  Decodes an inline-encoded datetime from a term ID.

  ## Arguments

  - `id` - Term ID with datetime type tag

  ## Returns

  - `{:ok, datetime}` on success (always in UTC)
  - `{:error, :not_a_datetime}` if type tag is not datetime
  """
  @spec decode_datetime(term_id()) :: {:ok, DateTime.t()} | {:error, :not_a_datetime}
  def decode_datetime(id) when is_integer(id) and id >= 0 do
    type_tag = id >>> 60

    if type_tag == @type_datetime do
      ms = id &&& 0x0FFFFFFFFFFFFFFF
      {:ok, DateTime.from_unix!(ms, :millisecond)}
    else
      {:error, :not_a_datetime}
    end
  end

  @doc """
  Encodes a decimal value as an inline term ID.

  Uses a custom floating-point representation:
  - 1 sign bit
  - 11 exponent bits (biased by 1023)
  - 48 mantissa bits

  This provides approximately 14-15 significant decimal digits.

  ## Arguments

  - `decimal` - Decimal struct

  ## Returns

  - `{:ok, term_id}` on success
  - `{:error, :out_of_range}` if value cannot be represented

  ## Precision Guarantees

  - Integers up to 48 bits are represented exactly
  - Decimals with up to 14 significant digits are usually exact
  - Values outside range fall back to dictionary encoding
  """
  @spec encode_decimal(Decimal.t()) :: {:ok, term_id()} | {:error, :out_of_range}
  def encode_decimal(%Decimal{sign: sign, coef: coef, exp: exp}) do
    # Handle zero specially
    if coef == 0 do
      # Zero: sign=0, exponent=0, mantissa=0
      {:ok, encode_id(@type_decimal, 0)}
    else
      # Convert to our format: sign(1) + exponent(11) + mantissa(48)
      # We store the coefficient and exponent directly
      sign_bit = if sign == 1, do: 0, else: 1

      # Biased exponent
      biased_exp = exp + @decimal_exponent_bias

      cond do
        biased_exp < 0 or biased_exp > @max_decimal_exponent ->
          {:error, :out_of_range}

        coef > @max_decimal_mantissa ->
          {:error, :out_of_range}

        true ->
          # Pack: sign(1) + exponent(11) + mantissa(48) = 60 bits
          value =
            sign_bit <<< 59 |||
              biased_exp <<< @decimal_mantissa_bits |||
              coef

          {:ok, encode_id(@type_decimal, value)}
      end
    end
  end

  @doc """
  Decodes an inline-encoded decimal from a term ID.

  ## Arguments

  - `id` - Term ID with decimal type tag

  ## Returns

  - `{:ok, decimal}` on success
  - `{:error, :not_a_decimal}` if type tag is not decimal
  """
  @spec decode_decimal(term_id()) :: {:ok, Decimal.t()} | {:error, :not_a_decimal}
  def decode_decimal(id) when is_integer(id) and id >= 0 do
    type_tag = id >>> 60

    if type_tag == @type_decimal do
      value = id &&& 0x0FFFFFFFFFFFFFFF
      {:ok, decode_decimal_value(value)}
    else
      {:error, :not_a_decimal}
    end
  end

  @spec decode_decimal_value(non_neg_integer()) :: Decimal.t()
  defp decode_decimal_value(0), do: Decimal.new(0)

  defp decode_decimal_value(value) do
    # Unpack: sign(1) + exponent(11) + mantissa(48)
    sign_bit = value >>> 59
    biased_exp = value >>> @decimal_mantissa_bits &&& @max_decimal_exponent
    mantissa = value &&& @max_decimal_mantissa

    sign = if sign_bit == 0, do: 1, else: -1
    exp = biased_exp - @decimal_exponent_bias

    %Decimal{sign: sign, coef: mantissa, exp: exp}
  end

  @doc """
  Checks if a Decimal value can be inline-encoded.

  A decimal can be inline-encoded if:
  - The coefficient fits in 48 bits (max ~281 trillion)
  - The exponent fits in 11 bits after biasing (range -1023 to 1024)

  ## Examples

      iex> TripleStore.Dictionary.inline_encodable_decimal?(Decimal.new("3.14"))
      true

      iex> TripleStore.Dictionary.inline_encodable_decimal?(Decimal.new("0"))
      true
  """
  @spec inline_encodable_decimal?(Decimal.t()) :: boolean()
  def inline_encodable_decimal?(%Decimal{coef: coef, exp: exp}) do
    biased_exp = exp + @decimal_exponent_bias
    coef <= @max_decimal_mantissa and biased_exp >= 0 and biased_exp <= @max_decimal_exponent
  end

  @doc """
  Decodes any inline-encoded term ID to its original value.

  This is a generic dispatcher that determines the term type and calls
  the appropriate decoder function.

  ## Arguments

  - `id` - Term ID that should be inline-encoded (integer, decimal, or datetime)

  ## Returns

  - `{:ok, value}` - The decoded value (integer, Decimal, or DateTime)
  - `{:error, :not_inline_encoded}` - If the ID is not an inline type

  ## Examples

      iex> {:ok, id} = TripleStore.Dictionary.encode_integer(42)
      iex> TripleStore.Dictionary.decode_inline(id)
      {:ok, 42}

      iex> {:ok, id} = TripleStore.Dictionary.encode_decimal(Decimal.new("3.14"))
      iex> {:ok, value} = TripleStore.Dictionary.decode_inline(id)
      iex> Decimal.eq?(value, Decimal.new("3.14"))
      true
  """
  @spec decode_inline(term_id()) ::
          {:ok, integer() | Decimal.t() | DateTime.t()} | {:error, :not_inline_encoded}
  def decode_inline(id) when is_integer(id) and id >= 0 do
    case term_type(id) do
      :integer -> decode_integer(id)
      :decimal -> decode_decimal(id)
      :datetime -> decode_datetime(id)
      _ -> {:error, :not_inline_encoded}
    end
  end

  @doc """
  Checks if an RDF term can be inline-encoded.

  This predicate examines an RDF literal and determines if its value can be
  encoded directly in the 60-bit payload of a term ID, avoiding dictionary
  storage.

  ## Supported Inline Types

  - **xsd:integer**: Values in range [-2^59, 2^59)
  - **xsd:decimal**: Values with coefficient ≤ 48 bits and reasonable exponent
  - **xsd:dateTime**: Timestamps from 1970 onwards

  ## Arguments

  - `term` - An RDF term (only literals with numeric/datetime datatypes qualify)

  ## Returns

  - `true` if the term can be inline-encoded
  - `false` otherwise (including URIs, blank nodes, and non-numeric literals)

  ## Examples

      iex> TripleStore.Dictionary.inline_encodable?(RDF.literal(42))
      true

      iex> TripleStore.Dictionary.inline_encodable?(RDF.literal("hello"))
      false

      iex> TripleStore.Dictionary.inline_encodable?(RDF.iri("http://example.org"))
      false
  """
  @spec inline_encodable?(rdf_term()) :: boolean()
  def inline_encodable?(%RDF.Literal{literal: %RDF.XSD.Integer{value: value}})
      when is_integer(value) do
    inline_encodable_integer?(value)
  end

  def inline_encodable?(%RDF.Literal{literal: %RDF.XSD.Decimal{value: %Decimal{} = value}}) do
    inline_encodable_decimal?(value)
  end

  def inline_encodable?(%RDF.Literal{literal: %RDF.XSD.DateTime{value: %DateTime{} = value}}) do
    inline_encodable_datetime?(value)
  end

  # URIs, blank nodes, and other literal types cannot be inline-encoded
  def inline_encodable?(_term), do: false

  # ===========================================================================
  # Batch Operations (Suggestion S2)
  # ===========================================================================

  @doc """
  Looks up multiple term IDs from a list of RDF terms.

  This is a batch version of `lookup_id/2` for efficient bulk operations.
  Delegates to `StringToId.lookup_ids/2`.

  ## Arguments

  - `db` - Database reference
  - `terms` - List of RDF terms

  ## Returns

  - `{:ok, results}` where results is a list of `{:ok, id}` or `:not_found`
  - `{:error, reason}` on database error

  ## Examples

      iex> Dictionary.lookup_ids(db, [uri1, uri2, bnode1])
      {:ok, [{:ok, 123}, {:ok, 456}, :not_found]}
  """
  @spec lookup_ids(db_ref(), [rdf_term()]) ::
          {:ok, [{:ok, term_id()} | :not_found]} | {:error, term()}
  defdelegate lookup_ids(db, terms), to: TripleStore.Dictionary.StringToId

  @doc """
  Looks up multiple terms from a list of term IDs.

  This is a batch version of `lookup_term/2` for efficient result serialization.
  Delegates to `IdToString.lookup_terms/2`.

  ## Arguments

  - `db` - Database reference
  - `ids` - List of term IDs

  ## Returns

  - `{:ok, results}` where results is a list of `{:ok, term}` or `:not_found`
  - `{:error, reason}` on database error

  ## Note

  For inline-encoded IDs (integers, decimals, datetimes), the values are
  computed directly without database lookup.

  ## Examples

      iex> Dictionary.lookup_terms(db, [uri_id, bnode_id, integer_id])
      {:ok, [{:ok, %RDF.IRI{...}}, {:ok, %RDF.BlankNode{...}}, {:ok, RDF.literal(42)}]}
  """
  @spec lookup_terms(db_ref(), [term_id()]) ::
          {:ok, [{:ok, rdf_term()} | :not_found]} | {:error, term()}
  defdelegate lookup_terms(db, ids), to: TripleStore.Dictionary.IdToString

  @doc """
  Gets or creates IDs for multiple terms atomically.

  This is a batch version of `get_or_create_id/2` for efficient bulk loading.
  Delegates to `Manager.get_or_create_ids/2`.

  ## Arguments

  - `manager` - Manager process reference (GenServer pid or name)
  - `terms` - List of RDF terms

  ## Returns

  - `{:ok, ids}` list of term IDs in same order as input
  - `{:error, reason}` on failure

  ## Concurrency

  This operation is serialized through the Dictionary Manager GenServer to
  ensure atomic create-if-not-exists semantics for all terms in the batch.

  ## Examples

      iex> {:ok, manager} = Manager.start_link(db: db)
      iex> Dictionary.get_or_create_ids(manager, [uri1, uri2, bnode1])
      {:ok, [123, 456, 789]}
  """
  @spec get_or_create_ids(GenServer.server(), [rdf_term()]) ::
          {:ok, [term_id()]} | {:error, term()}
  defdelegate get_or_create_ids(manager, terms), to: TripleStore.Dictionary.Manager

  # ===========================================================================
  # Private Helper Functions
  # ===========================================================================

  @spec tag_to_type(non_neg_integer()) :: term_type() | :unknown
  defp tag_to_type(@type_uri), do: :uri
  defp tag_to_type(@type_bnode), do: :bnode
  defp tag_to_type(@type_literal), do: :literal
  defp tag_to_type(@type_integer), do: :integer
  defp tag_to_type(@type_decimal), do: :decimal
  defp tag_to_type(@type_datetime), do: :datetime
  defp tag_to_type(_), do: :unknown

  @spec contains_null_byte?(binary()) :: boolean()
  defp contains_null_byte?(binary) do
    # Use :binary.match BIF for O(n) performance on large binaries
    # This is more efficient than recursive pattern matching
    :binary.match(binary, <<0>>) != :nomatch
  end
end
