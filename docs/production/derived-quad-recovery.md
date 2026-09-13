# Derived Quad Recovery

## Scope

Use this procedure only for quad stores that may have been materialized with
global `storage_strategy: :per_graph_cf` before the canonical GSPO writer fix.
That writer placed SPOG bytes in the `derived` column family. Both the malformed
and canonical layouts are 32 bytes, so key length cannot identify affected rows
and mixed contents must not be rewritten in place.

Global `:per_graph_cf` derivations belong to graph ID `0`. The global evaluator
has already removed unique premise graph ownership, so recovery must not infer a
source graph from the four stored integers.

## Backup and Assessment

1. Stop writers and reasoning jobs for the selected store.
2. Create and verify a full backup with `TripleStore.Backup` or the established
   filesystem backup procedure. Record the store path, schema version, reasoning
   configuration, rule profile, and application release that produced it.
3. Export or count the explicit `gspo`, `gpos`, `spog`, and `posg` indices. These
   values are the authoritative input and the preservation baseline.
4. Record current `derived`, derivation-provenance, and graph-reasoning-status
   counts. Treat them as diagnostic evidence only; do not decode ambiguous bytes
   to decide which rows to keep.

## Rebuild

1. Work on a restored disposable copy first.
2. Clear the selected store's complete `derived` column family through
   `DerivedStore.clear_all/1`. If provenance and graph status are maintained for
   the workflow, clear or reset them through their existing scoped APIs.
3. Rerun global materialization from the authoritative explicit quad indices
   with the same reviewed rules and `storage_strategy: :per_graph_cf`.
4. Allow the materializer to write graph ID `0` GSPO entries through
   `DerivedStore.insert_derived_quads/2`. Do not copy old derived bytes into the
   rebuilt store.

## Verification and Promotion

1. Close and reopen the disposable store, then read graph `0` through
   `DerivedStore.lookup_derived_quads_in_graph/2`.
2. Confirm every raw derived key decodes as `{0, subject, predicate, object}` and
   that expected entailments are present.
3. Compare all four explicit-index baselines with the pre-rebuild values. A
   derived rebuild must not modify explicit facts.
4. Verify provenance and graph reasoning status were regenerated for the new
   run, and execute the relevant reasoning and deletion regression suites.
5. Repeat the same backup, clear, materialize, reopen, and verification steps on
   the stopped production store. Retain the backup until application-level
   queries and incremental maintenance have been verified.
