//! RocksDB NIF wrapper for TripleStore
//!
//! This module provides the Rust NIF interface to RocksDB for the TripleStore
//! Elixir application. All I/O operations use dirty CPU schedulers to prevent
//! blocking the BEAM schedulers.

use rocksdb::{ColumnFamilyDescriptor, DBIteratorWithThreadMode, IteratorMode, Options, WriteBatch, DB};
use rustler::{Binary, Encoder, Env, ListIterator, NewBinary, NifResult, Resource, ResourceArc, Term};
use std::sync::{Arc, Mutex, RwLock};

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

/// Iterator reference wrapper for safe cross-NIF-boundary passing.
/// Stores the iterator along with its prefix for bounds checking.
/// The iterator is wrapped in a Mutex because it needs mutable access for next().
/// We also store an Arc to the DbRef to keep the database alive.
pub struct IteratorRef {
    /// The RocksDB iterator. Uses 'static lifetime with raw pointer internally.
    /// Safety: The DbRef Arc keeps the database alive for the iterator's lifetime.
    iterator: Mutex<Option<DBIteratorWithThreadMode<'static, DB>>>,
    /// Reference to the database to keep it alive
    _db_ref: Arc<ResourceArc<DbRef>>,
    /// The prefix used for this iterator (for bounds checking)
    prefix: Vec<u8>,
    /// Column family name for this iterator
    cf_name: String,
}

#[rustler::resource_impl]
impl Resource for IteratorRef {}

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
        get_failed,
        put_failed,
        delete_failed,
        batch_failed,
        invalid_operation,
        // Operation types for batch - these map to Elixir atoms :put and :delete
        put,
        delete,
        // Iterator atoms
        iterator_end,
        iterator_failed,
        iterator_closed,
    }
}

/// Converts a column family atom to its string name.
/// Returns None if the atom is not a valid column family.
fn cf_atom_to_name(cf_atom: rustler::Atom) -> Option<&'static str> {
    if cf_atom == atoms::id2str() {
        Some("id2str")
    } else if cf_atom == atoms::str2id() {
        Some("str2id")
    } else if cf_atom == atoms::spo() {
        Some("spo")
    } else if cf_atom == atoms::pos() {
        Some("pos")
    } else if cf_atom == atoms::osp() {
        Some("osp")
    } else if cf_atom == atoms::derived() {
        Some("derived")
    } else {
        None
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

/// Gets a value from a column family.
///
/// # Arguments
/// * `db_ref` - The database reference
/// * `cf` - The column family atom (:id2str, :str2id, :spo, :pos, :osp, :derived)
/// * `key` - The key as a binary
///
/// # Returns
/// * `{:ok, value}` if found
/// * `:not_found` if key doesn't exist
/// * `{:error, :already_closed}` if database is closed
/// * `{:error, {:invalid_cf, cf}}` if column family is invalid
/// * `{:error, {:get_failed, reason}}` on other errors
#[rustler::nif(schedule = "DirtyCpu")]
fn get<'a>(
    env: Env<'a>,
    db_ref: ResourceArc<DbRef>,
    cf: rustler::Atom,
    key: Binary<'a>,
) -> NifResult<Term<'a>> {
    let cf_name = match cf_atom_to_name(cf) {
        Some(name) => name,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    let db_guard = db_ref
        .db
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let db = match db_guard.as_ref() {
        Some(db) => db,
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    let cf_handle = match db.cf_handle(cf_name) {
        Some(cf) => cf,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    match db.get_cf(&cf_handle, key.as_slice()) {
        Ok(Some(value)) => {
            let mut binary = NewBinary::new(env, value.len());
            binary.as_mut_slice().copy_from_slice(&value);
            Ok((atoms::ok(), Binary::from(binary)).encode(env))
        }
        Ok(None) => Ok(atoms::not_found().encode(env)),
        Err(e) => Ok((atoms::error(), (atoms::get_failed(), e.to_string())).encode(env)),
    }
}

/// Puts a key-value pair into a column family.
///
/// # Arguments
/// * `db_ref` - The database reference
/// * `cf` - The column family atom
/// * `key` - The key as a binary
/// * `value` - The value as a binary
///
/// # Returns
/// * `:ok` on success
/// * `{:error, :already_closed}` if database is closed
/// * `{:error, {:invalid_cf, cf}}` if column family is invalid
/// * `{:error, {:put_failed, reason}}` on other errors
#[rustler::nif(schedule = "DirtyCpu")]
fn put<'a>(
    env: Env<'a>,
    db_ref: ResourceArc<DbRef>,
    cf: rustler::Atom,
    key: Binary<'a>,
    value: Binary<'a>,
) -> NifResult<Term<'a>> {
    let cf_name = match cf_atom_to_name(cf) {
        Some(name) => name,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    let db_guard = db_ref
        .db
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let db = match db_guard.as_ref() {
        Some(db) => db,
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    let cf_handle = match db.cf_handle(cf_name) {
        Some(cf) => cf,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    match db.put_cf(&cf_handle, key.as_slice(), value.as_slice()) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), (atoms::put_failed(), e.to_string())).encode(env)),
    }
}

/// Deletes a key from a column family.
///
/// # Arguments
/// * `db_ref` - The database reference
/// * `cf` - The column family atom
/// * `key` - The key to delete
///
/// # Returns
/// * `:ok` on success (even if key didn't exist)
/// * `{:error, :already_closed}` if database is closed
/// * `{:error, {:invalid_cf, cf}}` if column family is invalid
/// * `{:error, {:delete_failed, reason}}` on other errors
#[rustler::nif(schedule = "DirtyCpu")]
fn delete<'a>(
    env: Env<'a>,
    db_ref: ResourceArc<DbRef>,
    cf: rustler::Atom,
    key: Binary<'a>,
) -> NifResult<Term<'a>> {
    let cf_name = match cf_atom_to_name(cf) {
        Some(name) => name,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    let db_guard = db_ref
        .db
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let db = match db_guard.as_ref() {
        Some(db) => db,
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    let cf_handle = match db.cf_handle(cf_name) {
        Some(cf) => cf,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    match db.delete_cf(&cf_handle, key.as_slice()) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), (atoms::delete_failed(), e.to_string())).encode(env)),
    }
}

/// Checks if a key exists in a column family.
///
/// # Arguments
/// * `db_ref` - The database reference
/// * `cf` - The column family atom
/// * `key` - The key to check
///
/// # Returns
/// * `{:ok, true}` if key exists
/// * `{:ok, false}` if key doesn't exist
/// * `{:error, :already_closed}` if database is closed
/// * `{:error, {:invalid_cf, cf}}` if column family is invalid
/// * `{:error, {:get_failed, reason}}` on other errors
#[rustler::nif(schedule = "DirtyCpu")]
fn exists<'a>(
    env: Env<'a>,
    db_ref: ResourceArc<DbRef>,
    cf: rustler::Atom,
    key: Binary<'a>,
) -> NifResult<Term<'a>> {
    let cf_name = match cf_atom_to_name(cf) {
        Some(name) => name,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    let db_guard = db_ref
        .db
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let db = match db_guard.as_ref() {
        Some(db) => db,
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    let cf_handle = match db.cf_handle(cf_name) {
        Some(cf) => cf,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    // Check if key exists by attempting to get it
    match db.get_cf(&cf_handle, key.as_slice()) {
        Ok(Some(_)) => Ok((atoms::ok(), true).encode(env)),
        Ok(None) => Ok((atoms::ok(), false).encode(env)),
        Err(e) => Ok((atoms::error(), (atoms::get_failed(), e.to_string())).encode(env)),
    }
}

/// Atomically writes multiple key-value pairs to column families.
///
/// # Arguments
/// * `db_ref` - The database reference
/// * `operations` - List of `{cf, key, value}` tuples
///
/// # Returns
/// * `:ok` on success
/// * `{:error, :already_closed}` if database is closed
/// * `{:error, {:invalid_cf, cf}}` if column family is invalid
/// * `{:error, {:batch_failed, reason}}` on other errors
#[rustler::nif(schedule = "DirtyCpu")]
fn write_batch<'a>(
    env: Env<'a>,
    db_ref: ResourceArc<DbRef>,
    operations: Term<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = db_ref
        .db
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let db = match db_guard.as_ref() {
        Some(db) => db,
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    let mut batch = WriteBatch::default();

    // Parse the list of operations
    let iter: ListIterator = operations
        .decode()
        .map_err(|_| rustler::Error::Term(Box::new("expected list")))?;

    for item in iter {
        // Each item should be a tuple {cf, key, value}
        let tuple = rustler::types::tuple::get_tuple(item)
            .map_err(|_| rustler::Error::Term(Box::new("expected tuple")))?;

        if tuple.len() == 3 {
            // Simple format: {cf, key, value} - treat as put
            let cf_atom: rustler::Atom = tuple[0]
                .decode()
                .map_err(|_| rustler::Error::Term(Box::new("expected atom for cf")))?;
            let key: Binary = tuple[1]
                .decode()
                .map_err(|_| rustler::Error::Term(Box::new("expected binary for key")))?;
            let value: Binary = tuple[2]
                .decode()
                .map_err(|_| rustler::Error::Term(Box::new("expected binary for value")))?;

            let cf_name = match cf_atom_to_name(cf_atom) {
                Some(name) => name,
                None => return Ok((atoms::error(), (atoms::invalid_cf(), cf_atom)).encode(env)),
            };

            let cf_handle = match db.cf_handle(cf_name) {
                Some(cf) => cf,
                None => return Ok((atoms::error(), (atoms::invalid_cf(), cf_atom)).encode(env)),
            };

            batch.put_cf(&cf_handle, key.as_slice(), value.as_slice());
        } else if tuple.len() == 4 {
            // Extended format: {:put, cf, key, value}
            let op_atom: rustler::Atom = tuple[0]
                .decode()
                .map_err(|_| rustler::Error::Term(Box::new("expected atom for operation")))?;
            let cf_atom: rustler::Atom = tuple[1]
                .decode()
                .map_err(|_| rustler::Error::Term(Box::new("expected atom for cf")))?;
            let key: Binary = tuple[2]
                .decode()
                .map_err(|_| rustler::Error::Term(Box::new("expected binary for key")))?;
            let value: Binary = tuple[3]
                .decode()
                .map_err(|_| rustler::Error::Term(Box::new("expected binary for value")))?;

            if op_atom != atoms::put() {
                return Ok((atoms::error(), (atoms::invalid_operation(), op_atom)).encode(env));
            }

            let cf_name = match cf_atom_to_name(cf_atom) {
                Some(name) => name,
                None => return Ok((atoms::error(), (atoms::invalid_cf(), cf_atom)).encode(env)),
            };

            let cf_handle = match db.cf_handle(cf_name) {
                Some(cf) => cf,
                None => return Ok((atoms::error(), (atoms::invalid_cf(), cf_atom)).encode(env)),
            };

            batch.put_cf(&cf_handle, key.as_slice(), value.as_slice());
        } else {
            return Ok((atoms::error(), atoms::invalid_operation()).encode(env));
        }
    }

    match db.write(batch) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), (atoms::batch_failed(), e.to_string())).encode(env)),
    }
}

/// Atomically deletes multiple keys from column families.
///
/// # Arguments
/// * `db_ref` - The database reference
/// * `operations` - List of `{cf, key}` tuples
///
/// # Returns
/// * `:ok` on success
/// * `{:error, :already_closed}` if database is closed
/// * `{:error, {:invalid_cf, cf}}` if column family is invalid
/// * `{:error, {:batch_failed, reason}}` on other errors
#[rustler::nif(schedule = "DirtyCpu")]
fn delete_batch<'a>(
    env: Env<'a>,
    db_ref: ResourceArc<DbRef>,
    operations: Term<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = db_ref
        .db
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let db = match db_guard.as_ref() {
        Some(db) => db,
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    let mut batch = WriteBatch::default();

    // Parse the list of operations
    let iter: ListIterator = operations
        .decode()
        .map_err(|_| rustler::Error::Term(Box::new("expected list")))?;

    for item in iter {
        // Each item should be a tuple {cf, key}
        let tuple = rustler::types::tuple::get_tuple(item)
            .map_err(|_| rustler::Error::Term(Box::new("expected tuple")))?;

        if tuple.len() != 2 {
            return Ok((atoms::error(), atoms::invalid_operation()).encode(env));
        }

        let cf_atom: rustler::Atom = tuple[0]
            .decode()
            .map_err(|_| rustler::Error::Term(Box::new("expected atom for cf")))?;
        let key: Binary = tuple[1]
            .decode()
            .map_err(|_| rustler::Error::Term(Box::new("expected binary for key")))?;

        let cf_name = match cf_atom_to_name(cf_atom) {
            Some(name) => name,
            None => return Ok((atoms::error(), (atoms::invalid_cf(), cf_atom)).encode(env)),
        };

        let cf_handle = match db.cf_handle(cf_name) {
            Some(cf) => cf,
            None => return Ok((atoms::error(), (atoms::invalid_cf(), cf_atom)).encode(env)),
        };

        batch.delete_cf(&cf_handle, key.as_slice());
    }

    match db.write(batch) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), (atoms::batch_failed(), e.to_string())).encode(env)),
    }
}

/// Atomically performs mixed put and delete operations.
///
/// # Arguments
/// * `db_ref` - The database reference
/// * `operations` - List of operations:
///   - `{:put, cf, key, value}` for puts
///   - `{:delete, cf, key}` for deletes
///
/// # Returns
/// * `:ok` on success
/// * `{:error, :already_closed}` if database is closed
/// * `{:error, {:invalid_cf, cf}}` if column family is invalid
/// * `{:error, {:invalid_operation, op}}` if operation type is invalid
/// * `{:error, {:batch_failed, reason}}` on other errors
#[rustler::nif(schedule = "DirtyCpu")]
fn mixed_batch<'a>(
    env: Env<'a>,
    db_ref: ResourceArc<DbRef>,
    operations: Term<'a>,
) -> NifResult<Term<'a>> {
    let db_guard = db_ref
        .db
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let db = match db_guard.as_ref() {
        Some(db) => db,
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    let mut batch = WriteBatch::default();

    // Parse the list of operations
    let iter: ListIterator = operations
        .decode()
        .map_err(|_| rustler::Error::Term(Box::new("expected list")))?;

    for item in iter {
        let tuple = rustler::types::tuple::get_tuple(item)
            .map_err(|_| rustler::Error::Term(Box::new("expected tuple")))?;

        if tuple.is_empty() {
            return Ok((atoms::error(), atoms::invalid_operation()).encode(env));
        }

        let op_atom: rustler::Atom = tuple[0]
            .decode()
            .map_err(|_| rustler::Error::Term(Box::new("expected atom for operation")))?;

        if op_atom == atoms::put() {
            // {:put, cf, key, value}
            if tuple.len() != 4 {
                return Ok((atoms::error(), atoms::invalid_operation()).encode(env));
            }

            let cf_atom: rustler::Atom = tuple[1]
                .decode()
                .map_err(|_| rustler::Error::Term(Box::new("expected atom for cf")))?;
            let key: Binary = tuple[2]
                .decode()
                .map_err(|_| rustler::Error::Term(Box::new("expected binary for key")))?;
            let value: Binary = tuple[3]
                .decode()
                .map_err(|_| rustler::Error::Term(Box::new("expected binary for value")))?;

            let cf_name = match cf_atom_to_name(cf_atom) {
                Some(name) => name,
                None => return Ok((atoms::error(), (atoms::invalid_cf(), cf_atom)).encode(env)),
            };

            let cf_handle = match db.cf_handle(cf_name) {
                Some(cf) => cf,
                None => return Ok((atoms::error(), (atoms::invalid_cf(), cf_atom)).encode(env)),
            };

            batch.put_cf(&cf_handle, key.as_slice(), value.as_slice());
        } else if op_atom == atoms::delete() {
            // {:delete, cf, key}
            if tuple.len() != 3 {
                return Ok((atoms::error(), atoms::invalid_operation()).encode(env));
            }

            let cf_atom: rustler::Atom = tuple[1]
                .decode()
                .map_err(|_| rustler::Error::Term(Box::new("expected atom for cf")))?;
            let key: Binary = tuple[2]
                .decode()
                .map_err(|_| rustler::Error::Term(Box::new("expected binary for key")))?;

            let cf_name = match cf_atom_to_name(cf_atom) {
                Some(name) => name,
                None => return Ok((atoms::error(), (atoms::invalid_cf(), cf_atom)).encode(env)),
            };

            let cf_handle = match db.cf_handle(cf_name) {
                Some(cf) => cf,
                None => return Ok((atoms::error(), (atoms::invalid_cf(), cf_atom)).encode(env)),
            };

            batch.delete_cf(&cf_handle, key.as_slice());
        } else {
            return Ok((atoms::error(), (atoms::invalid_operation(), op_atom)).encode(env));
        }
    }

    match db.write(batch) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), (atoms::batch_failed(), e.to_string())).encode(env)),
    }
}

// ============================================================================
// Iterator Operations
// ============================================================================

/// Creates a prefix iterator for a column family.
///
/// The iterator returns all key-value pairs where the key starts with the given prefix.
/// The iterator must be closed with `iterator_close` when done.
///
/// # Arguments
/// * `db_ref` - The database reference
/// * `cf` - The column family atom
/// * `prefix` - The prefix to iterate over
///
/// # Returns
/// * `{:ok, iterator_ref}` on success
/// * `{:error, :already_closed}` if database is closed
/// * `{:error, {:invalid_cf, cf}}` if column family is invalid
#[rustler::nif(schedule = "DirtyCpu")]
fn prefix_iterator<'a>(
    env: Env<'a>,
    db_ref: ResourceArc<DbRef>,
    cf: rustler::Atom,
    prefix: Binary<'a>,
) -> NifResult<Term<'a>> {
    let cf_name = match cf_atom_to_name(cf) {
        Some(name) => name,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    let db_guard = db_ref
        .db
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let db = match db_guard.as_ref() {
        Some(db) => db,
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    let cf_handle = match db.cf_handle(cf_name) {
        Some(cf) => cf,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    let prefix_bytes = prefix.as_slice().to_vec();

    // Create the iterator with prefix mode
    // Safety: We use unsafe to extend the lifetime because we're storing
    // the db_ref Arc which keeps the database alive
    let iterator = db.iterator_cf(&cf_handle, IteratorMode::From(&prefix_bytes, rocksdb::Direction::Forward));

    // SAFETY: We keep the DbRef alive via Arc, so the iterator remains valid
    let static_iterator: DBIteratorWithThreadMode<'static, DB> = unsafe {
        std::mem::transmute(iterator)
    };

    let iter_ref = ResourceArc::new(IteratorRef {
        iterator: Mutex::new(Some(static_iterator)),
        _db_ref: Arc::new(db_ref.clone()),
        prefix: prefix_bytes,
        cf_name: cf_name.to_string(),
    });

    Ok((atoms::ok(), iter_ref).encode(env))
}

/// Gets the next key-value pair from the iterator.
///
/// # Arguments
/// * `iter_ref` - The iterator reference
///
/// # Returns
/// * `{:ok, key, value}` if there's a next item with matching prefix
/// * `:end` if the iterator is exhausted or prefix no longer matches
/// * `{:error, :iterator_closed}` if iterator was closed
/// * `{:error, {:iterator_failed, reason}}` on error
#[rustler::nif(schedule = "DirtyCpu")]
fn iterator_next<'a>(env: Env<'a>, iter_ref: ResourceArc<IteratorRef>) -> NifResult<Term<'a>> {
    let mut iter_guard = iter_ref
        .iterator
        .lock()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let iterator = match iter_guard.as_mut() {
        Some(iter) => iter,
        None => return Ok((atoms::error(), atoms::iterator_closed()).encode(env)),
    };

    match iterator.next() {
        Some(Ok((key, value))) => {
            // Check if key still has the prefix
            if !key.starts_with(&iter_ref.prefix) {
                return Ok(atoms::iterator_end().encode(env));
            }

            let mut key_binary = NewBinary::new(env, key.len());
            key_binary.as_mut_slice().copy_from_slice(&key);

            let mut value_binary = NewBinary::new(env, value.len());
            value_binary.as_mut_slice().copy_from_slice(&value);

            Ok((atoms::ok(), Binary::from(key_binary), Binary::from(value_binary)).encode(env))
        }
        Some(Err(e)) => {
            Ok((atoms::error(), (atoms::iterator_failed(), e.to_string())).encode(env))
        }
        None => Ok(atoms::iterator_end().encode(env)),
    }
}

/// Seeks the iterator to a specific key.
///
/// After seeking, the iterator will return keys >= target that match the prefix.
/// This is essential for Leapfrog Triejoin in Phase 3.
///
/// # Arguments
/// * `iter_ref` - The iterator reference
/// * `target` - The key to seek to
///
/// # Returns
/// * `:ok` on success
/// * `{:error, :iterator_closed}` if iterator was closed
#[rustler::nif(schedule = "DirtyCpu")]
fn iterator_seek<'a>(
    env: Env<'a>,
    iter_ref: ResourceArc<IteratorRef>,
    target: Binary<'a>,
) -> NifResult<Term<'a>> {
    let mut iter_guard = iter_ref
        .iterator
        .lock()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let iterator = match iter_guard.as_mut() {
        Some(iter) => iter,
        None => return Ok((atoms::error(), atoms::iterator_closed()).encode(env)),
    };

    // Get the database reference to create a new iterator at the seek position
    let db_ref = &iter_ref._db_ref;
    let db_guard = db_ref
        .db
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let db = match db_guard.as_ref() {
        Some(db) => db,
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    let cf_handle = match db.cf_handle(&iter_ref.cf_name) {
        Some(cf) => cf,
        None => return Ok((atoms::error(), atoms::iterator_closed()).encode(env)),
    };

    // Create new iterator at the seek position
    let target_bytes = target.as_slice();
    let new_iterator = db.iterator_cf(&cf_handle, IteratorMode::From(target_bytes, rocksdb::Direction::Forward));

    // SAFETY: We keep the DbRef alive via Arc, so the iterator remains valid
    let static_iterator: DBIteratorWithThreadMode<'static, DB> = unsafe {
        std::mem::transmute(new_iterator)
    };

    // Replace the old iterator
    *iterator = static_iterator;

    Ok(atoms::ok().encode(env))
}

/// Closes the iterator and releases resources.
///
/// # Arguments
/// * `iter_ref` - The iterator reference
///
/// # Returns
/// * `:ok` on success
/// * `{:error, :iterator_closed}` if already closed
#[rustler::nif]
fn iterator_close<'a>(env: Env<'a>, iter_ref: ResourceArc<IteratorRef>) -> NifResult<Term<'a>> {
    let mut iter_guard = iter_ref
        .iterator
        .lock()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    if iter_guard.is_none() {
        return Ok((atoms::error(), atoms::iterator_closed()).encode(env));
    }

    // Drop the iterator
    *iter_guard = None;

    Ok(atoms::ok().encode(env))
}

/// Collects all remaining key-value pairs from an iterator into a list.
///
/// This is a convenience function that consumes the iterator and returns
/// all matching entries. Useful for small result sets where streaming isn't needed.
///
/// # Arguments
/// * `iter_ref` - The iterator reference
///
/// # Returns
/// * `{:ok, [{key, value}, ...]}` with all remaining entries
/// * `{:error, :iterator_closed}` if iterator was closed
/// * `{:error, {:iterator_failed, reason}}` on error
#[rustler::nif(schedule = "DirtyCpu")]
fn iterator_collect<'a>(env: Env<'a>, iter_ref: ResourceArc<IteratorRef>) -> NifResult<Term<'a>> {
    let mut iter_guard = iter_ref
        .iterator
        .lock()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let iterator = match iter_guard.as_mut() {
        Some(iter) => iter,
        None => return Ok((atoms::error(), atoms::iterator_closed()).encode(env)),
    };

    let mut results: Vec<Term<'a>> = Vec::new();

    for result in iterator.by_ref() {
        match result {
            Ok((key, value)) => {
                // Check if key still has the prefix
                if !key.starts_with(&iter_ref.prefix) {
                    break;
                }

                let mut key_binary = NewBinary::new(env, key.len());
                key_binary.as_mut_slice().copy_from_slice(&key);

                let mut value_binary = NewBinary::new(env, value.len());
                value_binary.as_mut_slice().copy_from_slice(&value);

                results.push((Binary::from(key_binary), Binary::from(value_binary)).encode(env));
            }
            Err(e) => {
                return Ok((atoms::error(), (atoms::iterator_failed(), e.to_string())).encode(env));
            }
        }
    }

    Ok((atoms::ok(), results).encode(env))
}

rustler::init!("Elixir.TripleStore.Backend.RocksDB.NIF");
