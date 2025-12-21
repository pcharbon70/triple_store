//! RocksDB NIF wrapper for TripleStore
//!
//! This module provides the Rust NIF interface to RocksDB for the TripleStore
//! Elixir application. All I/O operations use dirty CPU schedulers to prevent
//! blocking the BEAM schedulers.

use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use rustler::{Encoder, Env, NifResult, Resource, ResourceArc, Term};
use std::sync::RwLock;

/// Column family names used by TripleStore
const CF_NAMES: [&str; 6] = ["id2str", "str2id", "spo", "pos", "osp", "derived"];

/// Database reference wrapper for safe cross-NIF-boundary passing.
/// Uses RwLock to allow concurrent reads with exclusive writes.
pub struct DbRef {
    db: RwLock<Option<DB>>,
    path: String,
}

#[rustler::resource_impl]
impl Resource for DbRef {}

impl DbRef {
    fn new(db: DB, path: String) -> Self {
        DbRef {
            db: RwLock::new(Some(db)),
            path,
        }
    }
}

/// Atoms for Elixir interop
mod atoms {
    rustler::atoms! {
        ok,
        error,
        not_found,
        already_closed,
        // Column family atoms
        id2str,
        str2id,
        spo,
        pos,
        osp,
        derived,
        // Error types
        open_failed,
        close_failed,
        invalid_cf,
    }
}

/// Placeholder function to verify NIF loads correctly.
/// Returns the string "rocksdb_nif" to confirm the NIF is operational.
#[rustler::nif]
fn nif_loaded() -> &'static str {
    "rocksdb_nif"
}

/// Opens a RocksDB database at the given path with column families.
///
/// Creates the database and all required column families if they don't exist.
/// Returns a ResourceArc containing the database handle.
///
/// # Arguments
/// * `path` - Path to the database directory
///
/// # Returns
/// * `{:ok, db_ref}` on success
/// * `{:error, reason}` on failure
#[rustler::nif(schedule = "DirtyCpu")]
fn open(env: Env, path: String) -> NifResult<Term> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    // Create column family descriptors
    let cf_descriptors: Vec<ColumnFamilyDescriptor> = CF_NAMES
        .iter()
        .map(|name| {
            let cf_opts = Options::default();
            ColumnFamilyDescriptor::new(*name, cf_opts)
        })
        .collect();

    match DB::open_cf_descriptors(&opts, &path, cf_descriptors) {
        Ok(db) => {
            let db_ref = ResourceArc::new(DbRef::new(db, path));
            Ok((atoms::ok(), db_ref).encode(env))
        }
        Err(e) => Ok((atoms::error(), (atoms::open_failed(), e.to_string())).encode(env)),
    }
}

/// Closes the database and releases all resources.
///
/// After calling close, the database handle is no longer valid.
/// Subsequent operations will return `{:error, :already_closed}`.
///
/// # Arguments
/// * `db_ref` - The database reference to close
///
/// # Returns
/// * `:ok` on success
/// * `{:error, :already_closed}` if already closed
#[rustler::nif(schedule = "DirtyCpu")]
fn close(env: Env, db_ref: ResourceArc<DbRef>) -> NifResult<Term> {
    let mut db_guard = db_ref
        .db
        .write()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    if db_guard.is_none() {
        return Ok((atoms::error(), atoms::already_closed()).encode(env));
    }

    // Drop the database to close it
    *db_guard = None;
    Ok(atoms::ok().encode(env))
}

/// Returns the path of the database.
///
/// # Arguments
/// * `db_ref` - The database reference
///
/// # Returns
/// * `{:ok, path}` with the database path
#[rustler::nif]
fn get_path(env: Env, db_ref: ResourceArc<DbRef>) -> NifResult<Term> {
    Ok((atoms::ok(), db_ref.path.clone()).encode(env))
}

/// Lists all column families in the database.
///
/// # Returns
/// * List of column family name atoms
#[rustler::nif]
fn list_column_families(env: Env) -> NifResult<Term> {
    let cf_atoms: Vec<Term> = vec![
        atoms::id2str().encode(env),
        atoms::str2id().encode(env),
        atoms::spo().encode(env),
        atoms::pos().encode(env),
        atoms::osp().encode(env),
        atoms::derived().encode(env),
    ];
    Ok(cf_atoms.encode(env))
}

/// Checks if the database is open.
///
/// # Arguments
/// * `db_ref` - The database reference
///
/// # Returns
/// * `true` if open, `false` if closed
#[rustler::nif]
fn is_open(db_ref: ResourceArc<DbRef>) -> NifResult<bool> {
    let db_guard = db_ref
        .db
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;
    Ok(db_guard.is_some())
}

rustler::init!("Elixir.TripleStore.Backend.RocksDB.NIF");
