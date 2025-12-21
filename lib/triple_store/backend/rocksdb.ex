defmodule TripleStore.Backend.RocksDB do
  @moduledoc """
  RocksDB backend implementation for TripleStore.

  This module provides the high-level Elixir interface to RocksDB operations.
  Low-level NIF calls are delegated to the `TripleStore.Backend.RocksDB.NIF` module.

  ## Column Families

  The RocksDB instance uses the following column families:

  - `id2str` - Maps 64-bit IDs to string values (URIs, literals, blank nodes)
  - `str2id` - Maps string values to 64-bit IDs (reverse lookup)
  - `spo` - Subject-Predicate-Object index
  - `pos` - Predicate-Object-Subject index
  - `osp` - Object-Subject-Predicate index
  - `derived` - Stores inferred triples from reasoning

  ## Scheduler Notes

  All I/O operations use dirty CPU schedulers via `#[rustler::nif(schedule = "DirtyCpu")]`
  to prevent blocking the BEAM schedulers.
  """
end
