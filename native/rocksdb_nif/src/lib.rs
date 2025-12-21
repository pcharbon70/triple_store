//! RocksDB NIF wrapper for TripleStore
//!
//! This module provides the Rust NIF interface to RocksDB for the TripleStore
//! Elixir application. All I/O operations use dirty CPU schedulers to prevent
//! blocking the BEAM schedulers.

/// Placeholder function to verify NIF loads correctly.
/// Returns the string "rocksdb_nif" to confirm the NIF is operational.
#[rustler::nif]
fn nif_loaded() -> &'static str {
    "rocksdb_nif"
}

rustler::init!("Elixir.TripleStore.Backend.RocksDB.NIF");
