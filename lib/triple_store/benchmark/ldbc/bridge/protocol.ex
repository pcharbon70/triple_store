defmodule TripleStore.Benchmark.LDBC.Bridge.Protocol do
  @moduledoc """
  Versioned local-frame protocol used by external LDBC benchmark drivers.

  Frames are length-prefixed JSON and are accepted only after a handshake that
  identifies the profile, dataset manifest, and executable operation checksum.
  """

  @version 1
  @actions ~w(handshake execute batch reset checkpoint health cancel shutdown)

  @doc "Returns the bridge protocol version."
  @spec version() :: pos_integer()
  def version, do: @version

  @doc "Encodes a frame with a four-byte unsigned network-order length prefix."
  @spec encode(map(), pos_integer()) :: {:ok, binary()} | {:error, term()}
  def encode(frame, max_bytes) when is_map(frame) and is_integer(max_bytes) and max_bytes > 0 do
    with {:ok, payload} <- Jason.encode(frame),
         :ok <- ensure_size(byte_size(payload), max_bytes) do
      {:ok, <<byte_size(payload)::unsigned-big-32, payload::binary>>}
    end
  end

  @doc "Decodes and validates one complete length-prefixed frame."
  @spec decode(binary(), pos_integer()) :: {:ok, map()} | {:error, term()}
  def decode(<<size::unsigned-big-32, payload::binary>>, max_bytes)
      when is_integer(max_bytes) and max_bytes > 0 do
    with :ok <- ensure_size(size, max_bytes),
         true <- byte_size(payload) == size or {:error, :incomplete_frame},
         {:ok, frame} when is_map(frame) <- Jason.decode(payload),
         :ok <- validate(frame) do
      {:ok, frame}
    else
      {:ok, _other} -> {:error, :frame_must_be_an_object}
      {:error, _reason} = error -> error
    end
  end

  def decode(_frame, _max_bytes), do: {:error, :malformed_frame}

  @doc "Validates the common request envelope."
  @spec validate(map()) :: :ok | {:error, term()}
  def validate(%{"id" => id, "action" => action}) when is_binary(id) and action in @actions,
    do: :ok

  def validate(%{"action" => action}) when action not in @actions,
    do: {:error, {:unknown_action, action}}

  def validate(_frame), do: {:error, :invalid_frame_envelope}

  defp ensure_size(size, max_bytes) when size <= max_bytes, do: :ok
  defp ensure_size(_size, _max_bytes), do: {:error, :frame_too_large}
end
