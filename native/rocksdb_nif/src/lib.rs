//! RocksDB NIF wrapper for TripleStore
//!
//! This module provides the Rust NIF interface to RocksDB for the TripleStore
//! Elixir application. All I/O operations use dirty CPU schedulers to prevent
//! blocking the BEAM schedulers.

use rocksdb::{ColumnFamilyDescriptor, DBIteratorWithThreadMode, IteratorMode, Options, ReadOptions, SliceTransform, SnapshotWithThreadMode, WriteBatch, WriteOptions, DB};
use rustler::{Binary, Encoder, Env, ListIterator, NewBinary, NifResult, Resource, ResourceArc, Term};
use std::sync::{Arc, Mutex, RwLock};

/// Column family names used by TripleStore
const CF_NAMES: [&str; 7] = ["id2str", "str2id", "spo", "pos", "osp", "derived", "numeric_range"];

/// Column families that use prefix extraction (8-byte prefix = first component ID)
/// These CFs benefit from prefix bloom filters and native prefix iteration.
const PREFIX_CFS: [&str; 4] = ["spo", "pos", "osp", "numeric_range"];

/// Prefix length in bytes (64-bit ID = 8 bytes)
const PREFIX_LENGTH: usize = 8;

/// Shared database handle that stays alive as long as any iterator/snapshot references it.
/// This is the core fix for the use-after-free issue: iterators hold an Arc<SharedDb>,
/// so the DB cannot be dropped while any iterator is alive.
struct SharedDb {
    db: DB,
    path: String,
}

/// Database reference wrapper for safe cross-NIF-boundary passing.
/// Uses RwLock<Option<Arc<SharedDb>>> so that:
/// - close() sets the Option to None (marking as closed for new operations)
/// - But the Arc<SharedDb> may still exist in iterators/snapshots
/// - The actual DB is only dropped when the last Arc is dropped
pub struct DbRef {
    inner: RwLock<Option<Arc<SharedDb>>>,
}

#[rustler::resource_impl]
impl Resource for DbRef {}

/// Iterator reference wrapper for safe cross-NIF-boundary passing.
/// Stores the iterator along with its prefix for bounds checking.
/// The iterator is wrapped in a Mutex because it needs mutable access for next().
pub struct IteratorRef {
    /// The RocksDB iterator. Uses 'static lifetime with raw pointer internally.
    /// SAFETY: The Arc<SharedDb> keeps the actual database alive for the iterator's lifetime.
    /// This is safe because SharedDb is only dropped when all Arc references are dropped,
    /// and we hold one here.
    iterator: Mutex<Option<DBIteratorWithThreadMode<'static, DB>>>,
    /// Direct reference to the shared database - keeps the DB alive even after close()
    db: Arc<SharedDb>,
    /// The prefix used for this iterator (for bounds checking)
    prefix: Vec<u8>,
    /// Column family name for this iterator
    cf_name: String,
}

#[rustler::resource_impl]
impl Resource for IteratorRef {}

/// Snapshot reference wrapper for point-in-time consistent reads.
/// Stores the snapshot along with a reference to the database to keep it alive.
pub struct SnapshotRef {
    /// The RocksDB snapshot. Uses 'static lifetime with raw pointer internally.
    /// SAFETY: The Arc<SharedDb> keeps the actual database alive for the snapshot's lifetime.
    /// This is safe because SharedDb is only dropped when all Arc references are dropped,
    /// and we hold one here.
    snapshot: Mutex<Option<SnapshotWithThreadMode<'static, DB>>>,
    /// Direct reference to the shared database - keeps the DB alive even after close()
    db: Arc<SharedDb>,
}

#[rustler::resource_impl]
impl Resource for SnapshotRef {}

/// Snapshot iterator reference for iterating over a snapshot.
pub struct SnapshotIteratorRef {
    /// The RocksDB iterator over snapshot.
    /// SAFETY: The Arc<SharedDb> keeps the actual database alive for the iterator's lifetime.
    iterator: Mutex<Option<DBIteratorWithThreadMode<'static, DB>>>,
    /// Direct reference to the shared database - keeps the DB alive even after close()
    /// Prefixed with _ because it's used for Drop semantics, not directly read.
    _db: Arc<SharedDb>,
    /// The prefix used for this iterator (for bounds checking)
    prefix: Vec<u8>,
    /// Column family name for this iterator (kept for potential future debugging)
    _cf_name: String,
}

#[rustler::resource_impl]
impl Resource for SnapshotIteratorRef {}

impl DbRef {
    fn new(db: DB, path: String) -> Self {
        DbRef {
            inner: RwLock::new(Some(Arc::new(SharedDb { db, path }))),
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
        numeric_range,
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
        // Snapshot atoms
        snapshot_released,
        // Flush atoms
        flush_failed,
        // SetOptions atoms
        set_options_failed,
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
    } else if cf_atom == atoms::numeric_range() {
        Some("numeric_range")
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

    // Create column family descriptors with appropriate prefix extractors
    let cf_descriptors: Vec<ColumnFamilyDescriptor> = CF_NAMES
        .iter()
        .map(|name| {
            let mut cf_opts = Options::default();

            // Configure prefix extractor for index column families
            // This enables bloom filter benefits for prefix iteration
            if PREFIX_CFS.contains(name) {
                cf_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(PREFIX_LENGTH));
                // Enable prefix bloom filter in memtable for faster lookups
                cf_opts.set_memtable_prefix_bloom_ratio(0.1);
            }

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

/// Closes the database and releases the main reference.
///
/// After calling close, the database handle is no longer valid for new operations.
/// Subsequent operations on DbRef will return `{:error, :already_closed}`.
///
/// IMPORTANT: Existing iterators and snapshots will continue to work after close()
/// because they hold their own Arc<SharedDb> reference. The actual database is only
/// dropped when the last reference (including any active iterators/snapshots) is dropped.
/// This prevents use-after-free bugs.
///
/// # Arguments
/// * `db_ref` - The database reference to close
///
/// # Returns
/// * `:ok` on success
/// * `{:error, :already_closed}` if already closed
#[rustler::nif(schedule = "DirtyCpu")]
fn close(env: Env, db_ref: ResourceArc<DbRef>) -> NifResult<Term> {
    let mut guard = db_ref
        .inner
        .write()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    if guard.is_none() {
        return Ok((atoms::error(), atoms::already_closed()).encode(env));
    }

    // Remove our reference. The actual DB may still be alive if iterators/snapshots
    // hold Arc<SharedDb> references. The DB is only dropped when the last Arc is dropped.
    *guard = None;
    Ok(atoms::ok().encode(env))
}

/// Returns the path of the database.
///
/// # Arguments
/// * `db_ref` - The database reference
///
/// # Returns
/// * `{:ok, path}` with the database path
/// * `{:error, :already_closed}` if database is closed
#[rustler::nif]
fn get_path(env: Env, db_ref: ResourceArc<DbRef>) -> NifResult<Term> {
    let guard = db_ref
        .inner
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    match guard.as_ref() {
        Some(shared_db) => Ok((atoms::ok(), shared_db.path.clone()).encode(env)),
        None => Ok((atoms::error(), atoms::already_closed()).encode(env)),
    }
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
        atoms::numeric_range().encode(env),
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
    let guard = db_ref
        .inner
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;
    Ok(guard.is_some())
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

    let guard = db_ref
        .inner
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let shared_db = match guard.as_ref() {
        Some(db) => db,
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    let cf_handle = match shared_db.db.cf_handle(cf_name) {
        Some(cf) => cf,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    match shared_db.db.get_cf(&cf_handle, key.as_slice()) {
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

    let guard = db_ref
        .inner
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let shared_db = match guard.as_ref() {
        Some(db) => db,
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    let cf_handle = match shared_db.db.cf_handle(cf_name) {
        Some(cf) => cf,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    match shared_db.db.put_cf(&cf_handle, key.as_slice(), value.as_slice()) {
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

    let guard = db_ref
        .inner
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let shared_db = match guard.as_ref() {
        Some(db) => db,
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    let cf_handle = match shared_db.db.cf_handle(cf_name) {
        Some(cf) => cf,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    match shared_db.db.delete_cf(&cf_handle, key.as_slice()) {
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

    let guard = db_ref
        .inner
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let shared_db = match guard.as_ref() {
        Some(db) => db,
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    let cf_handle = match shared_db.db.cf_handle(cf_name) {
        Some(cf) => cf,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    // Check if key exists by attempting to get it
    match shared_db.db.get_cf(&cf_handle, key.as_slice()) {
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
/// * `sync` - Whether to sync to disk (true = fsync after write, false = defer to OS)
///
/// For bulk loading, set `sync=false` to improve throughput. Data is still
/// protected by the WAL, but fsync is deferred to the OS. This can provide
/// 2-3x throughput improvement for large batch operations.
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
    sync: bool,
) -> NifResult<Term<'a>> {
    let guard = db_ref
        .inner
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let shared_db = match guard.as_ref() {
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

            let cf_handle = match shared_db.db.cf_handle(cf_name) {
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

            let cf_handle = match shared_db.db.cf_handle(cf_name) {
                Some(cf) => cf,
                None => return Ok((atoms::error(), (atoms::invalid_cf(), cf_atom)).encode(env)),
            };

            batch.put_cf(&cf_handle, key.as_slice(), value.as_slice());
        } else {
            return Ok((atoms::error(), atoms::invalid_operation()).encode(env));
        }
    }

    // Create WriteOptions with sync setting
    let mut write_opts = WriteOptions::default();
    write_opts.set_sync(sync);

    match shared_db.db.write_opt(batch, &write_opts) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), (atoms::batch_failed(), e.to_string())).encode(env)),
    }
}

/// Atomically deletes multiple keys from column families.
///
/// # Arguments
/// * `db_ref` - The database reference
/// * `operations` - List of `{cf, key}` tuples
/// * `sync` - Whether to sync to disk (true = fsync after write, false = defer to OS)
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
    sync: bool,
) -> NifResult<Term<'a>> {
    let guard = db_ref
        .inner
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let shared_db = match guard.as_ref() {
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

        let cf_handle = match shared_db.db.cf_handle(cf_name) {
            Some(cf) => cf,
            None => return Ok((atoms::error(), (atoms::invalid_cf(), cf_atom)).encode(env)),
        };

        batch.delete_cf(&cf_handle, key.as_slice());
    }

    // Create WriteOptions with sync setting
    let mut write_opts = WriteOptions::default();
    write_opts.set_sync(sync);

    match shared_db.db.write_opt(batch, &write_opts) {
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
/// * `sync` - Whether to sync to disk (true = fsync after write, false = defer to OS)
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
    sync: bool,
) -> NifResult<Term<'a>> {
    let guard = db_ref
        .inner
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let shared_db = match guard.as_ref() {
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

            let cf_handle = match shared_db.db.cf_handle(cf_name) {
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

            let cf_handle = match shared_db.db.cf_handle(cf_name) {
                Some(cf) => cf,
                None => return Ok((atoms::error(), (atoms::invalid_cf(), cf_atom)).encode(env)),
            };

            batch.delete_cf(&cf_handle, key.as_slice());
        } else {
            return Ok((atoms::error(), (atoms::invalid_operation(), op_atom)).encode(env));
        }
    }

    // Create WriteOptions with sync setting
    let mut write_opts = WriteOptions::default();
    write_opts.set_sync(sync);

    match shared_db.db.write_opt(batch, &write_opts) {
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

    let guard = db_ref
        .inner
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let shared_db = match guard.as_ref() {
        Some(db) => Arc::clone(db),
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    let cf_handle = match shared_db.db.cf_handle(cf_name) {
        Some(cf) => cf,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    let prefix_bytes = prefix.as_slice().to_vec();

    // Configure read options for prefix iteration
    // For CFs with prefix extractors, we need to handle different prefix lengths:
    // - If prefix >= PREFIX_LENGTH bytes: use prefix-based iteration for bloom filter benefits
    // - If prefix < PREFIX_LENGTH bytes: use total_order_seek to avoid bloom filter issues
    let mut read_opts = ReadOptions::default();
    if PREFIX_CFS.contains(&cf_name) {
        if prefix_bytes.len() >= PREFIX_LENGTH {
            // Use prefix-based seek for bloom filter benefits
            read_opts.set_prefix_same_as_start(true);
            read_opts.set_total_order_seek(false);
        } else {
            // Short prefix: use total_order_seek to avoid incorrect bloom filter behavior
            read_opts.set_total_order_seek(true);
        }
    }

    // Create the iterator with configured read options
    let iterator = shared_db.db.iterator_cf_opt(
        &cf_handle,
        read_opts,
        IteratorMode::From(&prefix_bytes, rocksdb::Direction::Forward),
    );

    // SAFETY: We keep the SharedDb alive via Arc, so the iterator remains valid.
    // The Arc<SharedDb> is stored in IteratorRef and will keep the DB alive
    // even if DbRef.close() is called, preventing use-after-free.
    let static_iterator: DBIteratorWithThreadMode<'static, DB> = unsafe {
        std::mem::transmute(iterator)
    };

    let iter_ref = ResourceArc::new(IteratorRef {
        iterator: Mutex::new(Some(static_iterator)),
        db: shared_db,
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

    // Access the database directly from our Arc<SharedDb>
    let cf_handle = match iter_ref.db.db.cf_handle(&iter_ref.cf_name) {
        Some(cf) => cf,
        None => return Ok((atoms::error(), atoms::iterator_closed()).encode(env)),
    };

    // Create new iterator at the seek position
    let target_bytes = target.as_slice();
    let new_iterator = iter_ref.db.db.iterator_cf(&cf_handle, IteratorMode::From(target_bytes, rocksdb::Direction::Forward));

    // SAFETY: We keep the SharedDb alive via Arc, so the iterator remains valid.
    // The Arc<SharedDb> is stored in IteratorRef and keeps the DB alive.
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

// ============================================================================
// Snapshot Operations
// ============================================================================

/// Creates a snapshot of the database for consistent point-in-time reads.
///
/// A snapshot provides a consistent view of the database at the time of creation.
/// All reads using the snapshot will see the same data, regardless of subsequent
/// writes to the database.
///
/// # Arguments
/// * `db_ref` - The database reference
///
/// # Returns
/// * `{:ok, snapshot_ref}` on success
/// * `{:error, :already_closed}` if database is closed
#[rustler::nif(schedule = "DirtyCpu")]
fn snapshot<'a>(env: Env<'a>, db_ref: ResourceArc<DbRef>) -> NifResult<Term<'a>> {
    let guard = db_ref
        .inner
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let shared_db = match guard.as_ref() {
        Some(db) => Arc::clone(db),
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    let snap = shared_db.db.snapshot();

    // SAFETY: We keep the SharedDb alive via Arc, so the snapshot remains valid.
    // The Arc<SharedDb> is stored in SnapshotRef and will keep the DB alive
    // even if DbRef.close() is called, preventing use-after-free.
    let static_snapshot: SnapshotWithThreadMode<'static, DB> = unsafe {
        std::mem::transmute(snap)
    };

    let snap_ref = ResourceArc::new(SnapshotRef {
        snapshot: Mutex::new(Some(static_snapshot)),
        db: shared_db,
    });

    Ok((atoms::ok(), snap_ref).encode(env))
}

/// Gets a value from a column family using a snapshot.
///
/// This provides point-in-time consistent reads - the value returned
/// is what existed at the time the snapshot was created.
///
/// # Arguments
/// * `snapshot_ref` - The snapshot reference
/// * `cf` - The column family atom
/// * `key` - The key as a binary
///
/// # Returns
/// * `{:ok, value}` if found
/// * `:not_found` if key doesn't exist
/// * `{:error, :snapshot_released}` if snapshot was released
/// * `{:error, {:invalid_cf, cf}}` if column family is invalid
/// * `{:error, {:get_failed, reason}}` on other errors
#[rustler::nif(schedule = "DirtyCpu")]
fn snapshot_get<'a>(
    env: Env<'a>,
    snapshot_ref: ResourceArc<SnapshotRef>,
    cf: rustler::Atom,
    key: Binary<'a>,
) -> NifResult<Term<'a>> {
    let cf_name = match cf_atom_to_name(cf) {
        Some(name) => name,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    let snap_guard = snapshot_ref
        .snapshot
        .lock()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let snapshot = match snap_guard.as_ref() {
        Some(snap) => snap,
        None => return Ok((atoms::error(), atoms::snapshot_released()).encode(env)),
    };

    // Access the database directly from our Arc<SharedDb>
    let cf_handle = match snapshot_ref.db.db.cf_handle(cf_name) {
        Some(cf) => cf,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    // Use ReadOptions with snapshot
    let mut read_opts = ReadOptions::default();
    read_opts.set_snapshot(snapshot);

    match snapshot_ref.db.db.get_cf_opt(&cf_handle, key.as_slice(), &read_opts) {
        Ok(Some(value)) => {
            let mut binary = NewBinary::new(env, value.len());
            binary.as_mut_slice().copy_from_slice(&value);
            Ok((atoms::ok(), Binary::from(binary)).encode(env))
        }
        Ok(None) => Ok(atoms::not_found().encode(env)),
        Err(e) => Ok((atoms::error(), (atoms::get_failed(), e.to_string())).encode(env)),
    }
}

/// Creates a prefix iterator over a snapshot.
///
/// The iterator returns all key-value pairs where the key starts with the given prefix,
/// using the consistent view from the snapshot.
///
/// # Arguments
/// * `snapshot_ref` - The snapshot reference
/// * `cf` - The column family atom
/// * `prefix` - The prefix to iterate over
///
/// # Returns
/// * `{:ok, iterator_ref}` on success
/// * `{:error, :snapshot_released}` if snapshot was released
/// * `{:error, {:invalid_cf, cf}}` if column family is invalid
#[rustler::nif(schedule = "DirtyCpu")]
fn snapshot_prefix_iterator<'a>(
    env: Env<'a>,
    snapshot_ref: ResourceArc<SnapshotRef>,
    cf: rustler::Atom,
    prefix: Binary<'a>,
) -> NifResult<Term<'a>> {
    let cf_name = match cf_atom_to_name(cf) {
        Some(name) => name,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    let snap_guard = snapshot_ref
        .snapshot
        .lock()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let snapshot = match snap_guard.as_ref() {
        Some(snap) => snap,
        None => return Ok((atoms::error(), atoms::snapshot_released()).encode(env)),
    };

    // Access the database directly from our Arc<SharedDb>
    let cf_handle = match snapshot_ref.db.db.cf_handle(cf_name) {
        Some(cf) => cf,
        None => return Ok((atoms::error(), (atoms::invalid_cf(), cf)).encode(env)),
    };

    let prefix_bytes = prefix.as_slice().to_vec();

    // Create read options with snapshot and prefix bounds
    let mut read_opts = ReadOptions::default();
    read_opts.set_snapshot(snapshot);

    // Configure prefix iteration based on CF type and prefix length
    if PREFIX_CFS.contains(&cf_name) {
        if prefix_bytes.len() >= PREFIX_LENGTH {
            // Use prefix-based seek for bloom filter benefits
            read_opts.set_prefix_same_as_start(true);
            read_opts.set_total_order_seek(false);
        } else {
            // Short prefix: use total_order_seek to avoid incorrect bloom filter behavior
            read_opts.set_total_order_seek(true);
        }
    }

    // Create the iterator with snapshot
    let iterator = snapshot_ref.db.db.iterator_cf_opt(
        &cf_handle,
        read_opts,
        IteratorMode::From(&prefix_bytes, rocksdb::Direction::Forward),
    );

    // SAFETY: We keep the SharedDb alive via Arc, so the iterator remains valid.
    // The Arc<SharedDb> is stored in SnapshotIteratorRef and will keep the DB alive
    // even if DbRef.close() is called, preventing use-after-free.
    let static_iterator: DBIteratorWithThreadMode<'static, DB> = unsafe {
        std::mem::transmute(iterator)
    };

    let iter_ref = ResourceArc::new(SnapshotIteratorRef {
        iterator: Mutex::new(Some(static_iterator)),
        _db: Arc::clone(&snapshot_ref.db),
        prefix: prefix_bytes,
        _cf_name: cf_name.to_string(),
    });

    Ok((atoms::ok(), iter_ref).encode(env))
}

/// Gets the next key-value pair from a snapshot iterator.
///
/// # Arguments
/// * `iter_ref` - The snapshot iterator reference
///
/// # Returns
/// * `{:ok, key, value}` if there's a next item with matching prefix
/// * `:iterator_end` if the iterator is exhausted or prefix no longer matches
/// * `{:error, :iterator_closed}` if iterator was closed
/// * `{:error, {:iterator_failed, reason}}` on error
#[rustler::nif(schedule = "DirtyCpu")]
fn snapshot_iterator_next<'a>(
    env: Env<'a>,
    iter_ref: ResourceArc<SnapshotIteratorRef>,
) -> NifResult<Term<'a>> {
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

/// Closes a snapshot iterator and releases resources.
///
/// # Arguments
/// * `iter_ref` - The snapshot iterator reference
///
/// # Returns
/// * `:ok` on success
/// * `{:error, :iterator_closed}` if already closed
#[rustler::nif]
fn snapshot_iterator_close<'a>(
    env: Env<'a>,
    iter_ref: ResourceArc<SnapshotIteratorRef>,
) -> NifResult<Term<'a>> {
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

/// Collects all remaining key-value pairs from a snapshot iterator into a list.
///
/// # Arguments
/// * `iter_ref` - The snapshot iterator reference
///
/// # Returns
/// * `{:ok, [{key, value}, ...]}` with all remaining entries
/// * `{:error, :iterator_closed}` if iterator was closed
/// * `{:error, {:iterator_failed, reason}}` on error
#[rustler::nif(schedule = "DirtyCpu")]
fn snapshot_iterator_collect<'a>(
    env: Env<'a>,
    iter_ref: ResourceArc<SnapshotIteratorRef>,
) -> NifResult<Term<'a>> {
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

/// Releases a snapshot and frees resources.
///
/// After calling release, the snapshot handle is no longer valid.
///
/// # Arguments
/// * `snapshot_ref` - The snapshot reference
///
/// # Returns
/// * `:ok` on success
/// * `{:error, :snapshot_released}` if already released
#[rustler::nif]
fn release_snapshot<'a>(
    env: Env<'a>,
    snapshot_ref: ResourceArc<SnapshotRef>,
) -> NifResult<Term<'a>> {
    let mut snap_guard = snapshot_ref
        .snapshot
        .lock()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    if snap_guard.is_none() {
        return Ok((atoms::error(), atoms::snapshot_released()).encode(env));
    }

    // Drop the snapshot
    *snap_guard = None;

    Ok(atoms::ok().encode(env))
}

/// Flushes the Write-Ahead Log (WAL) to disk.
///
/// This ensures all buffered writes are persisted to the WAL. When `sync` is true,
/// it also calls fsync to ensure data is physically written to storage.
///
/// Use this after bulk loading with sync=false to ensure all data is durable
/// before considering the load complete.
///
/// # Arguments
/// * `db_ref` - The database reference
/// * `sync` - When true, calls fsync after flushing (fully durable). When false,
///            only flushes to OS buffer cache.
///
/// # Returns
/// * `:ok` on success
/// * `{:error, :already_closed}` if database is closed
/// * `{:error, {:flush_failed, reason}}` on failure
#[rustler::nif(schedule = "DirtyCpu")]
fn flush_wal<'a>(
    env: Env<'a>,
    db_ref: ResourceArc<DbRef>,
    sync: bool,
) -> NifResult<Term<'a>> {
    let guard = db_ref
        .inner
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let shared_db = match guard.as_ref() {
        Some(db) => db,
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    match shared_db.db.flush_wal(sync) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), (atoms::flush_failed(), e.to_string())).encode(env)),
    }
}

/// Sets options on all column families at runtime.
///
/// This allows dynamic reconfiguration of RocksDB settings without restarting.
/// Options are passed as a list of {key, value} tuples where both key and value
/// are strings.
///
/// # Mutable Options (subset)
/// - "level0_file_num_compaction_trigger" - Files in L0 to trigger compaction
/// - "level0_slowdown_writes_trigger" - Files in L0 to slow down writes
/// - "level0_stop_writes_trigger" - Files in L0 to stop writes
/// - "target_file_size_base" - Target file size in bytes
/// - "max_bytes_for_level_base" - Maximum bytes in base level
/// - "write_buffer_size" - Size of write buffer (for new memtables)
/// - "max_write_buffer_number" - Maximum number of write buffers
/// - "disable_auto_compactions" - Disable automatic compactions ("true"/"false")
///
/// # Arguments
/// * `db_ref` - The database reference
/// * `options` - List of {key, value} tuples as strings
///
/// # Returns
/// * `:ok` on success
/// * `{:error, :already_closed}` if database is closed
/// * `{:error, {:set_options_failed, reason}}` on failure
#[rustler::nif(schedule = "DirtyCpu")]
fn set_options<'a>(
    env: Env<'a>,
    db_ref: ResourceArc<DbRef>,
    options: Vec<(String, String)>,
) -> NifResult<Term<'a>> {
    let guard = db_ref
        .inner
        .read()
        .map_err(|_| rustler::Error::Term(Box::new("lock poisoned")))?;

    let shared_db = match guard.as_ref() {
        Some(db) => db,
        None => return Ok((atoms::error(), atoms::already_closed()).encode(env)),
    };

    // Convert options to the format expected by RocksDB: &[(&str, &str)]
    let opts: Vec<(&str, &str)> = options
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    // Apply options to all column families
    for cf_name in CF_NAMES.iter() {
        let cf = match shared_db.db.cf_handle(cf_name) {
            Some(cf) => cf,
            None => {
                return Ok((
                    atoms::error(),
                    (
                        atoms::set_options_failed(),
                        format!("column family '{}' not found", cf_name),
                    ),
                )
                .encode(env))
            }
        };

        if let Err(e) = shared_db.db.set_options_cf(&cf, &opts) {
            return Ok((
                atoms::error(),
                (
                    atoms::set_options_failed(),
                    format!("failed to set options on '{}': {}", cf_name, e),
                ),
            )
            .encode(env));
        }
    }

    Ok(atoms::ok().encode(env))
}

rustler::init!("Elixir.TripleStore.Backend.RocksDB.NIF");

#[cfg(test)]
mod tests {
    use super::CF_NAMES;
    use rocksdb::{ColumnFamilyDescriptor, Direction, IteratorMode, Options, ReadOptions, WriteBatch, DB};
    use tempfile::TempDir;

    fn setup_db() -> (TempDir, DB) {
        let tmp = TempDir::new().expect("temp dir");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_descriptors: Vec<ColumnFamilyDescriptor> = CF_NAMES
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect();

        let db = DB::open_cf_descriptors(&opts, tmp.path(), cf_descriptors).expect("open db");

        (tmp, db)
    }

    #[test]
    fn cf_names_unique_and_non_empty() {
        let mut unique = std::collections::HashSet::new();
        for name in CF_NAMES.iter() {
            assert!(!name.is_empty());
            assert!(unique.insert(name));
        }
    }

    #[test]
    fn basic_put_get() {
        let (_tmp, db) = setup_db();
        let cf = db.cf_handle("spo").expect("cf handle");

        db.put_cf(&cf, b"key1", b"value1").expect("put");
        let result = db.get_cf(&cf, b"key1").expect("get");
        assert_eq!(result, Some(b"value1".to_vec()));
    }

    #[test]
    fn iterator_prefix_bounds() {
        let (_tmp, db) = setup_db();
        let cf = db.cf_handle("spo").expect("cf handle");

        db.put_cf(&cf, b"aaa1", b"").expect("put");
        db.put_cf(&cf, b"aaa2", b"").expect("put");
        db.put_cf(&cf, b"bbb1", b"").expect("put");

        let prefix = b"aaa";
        let iter = db.iterator_cf(&cf, IteratorMode::From(prefix, Direction::Forward));

        let keys: Vec<Vec<u8>> = iter
            .take_while(|result| {
                result
                    .as_ref()
                    .map(|(key, _)| key.starts_with(prefix))
                    .unwrap_or(false)
            })
            .filter_map(|result| result.ok())
            .map(|(key, _)| key.to_vec())
            .collect();

        assert_eq!(keys.len(), 2);
        assert!(keys.iter().all(|key| key.starts_with(prefix)));
    }

    #[test]
    fn snapshot_isolation() {
        let (_tmp, db) = setup_db();
        let cf = db.cf_handle("spo").expect("cf handle");

        db.put_cf(&cf, b"key1", b"v1").expect("put");

        let snap = db.snapshot();

        db.put_cf(&cf, b"key1", b"v2").expect("put");
        db.put_cf(&cf, b"key2", b"v3").expect("put");

        let mut read_opts = ReadOptions::default();
        read_opts.set_snapshot(&snap);

        let result = db.get_cf_opt(&cf, b"key1", &read_opts).expect("get snapshot");
        assert_eq!(result, Some(b"v1".to_vec()));

        let result = db.get_cf_opt(&cf, b"key2", &read_opts).expect("get snapshot");
        assert_eq!(result, None);
    }

    #[test]
    fn write_batch_atomicity() {
        let (_tmp, db) = setup_db();
        let cf = db.cf_handle("spo").expect("cf handle");

        let mut batch = WriteBatch::default();
        batch.put_cf(&cf, b"k1", b"v1");
        batch.put_cf(&cf, b"k2", b"v2");
        batch.put_cf(&cf, b"k3", b"v3");

        db.write(batch).expect("write batch");

        assert!(db.get_cf(&cf, b"k1").expect("get").is_some());
        assert!(db.get_cf(&cf, b"k2").expect("get").is_some());
        assert!(db.get_cf(&cf, b"k3").expect("get").is_some());
    }
}
