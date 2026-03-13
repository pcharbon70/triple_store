# TripleStore Control Planes

## Purpose

This document defines the control planes used by `TripleStore` and the responsibilities that belong to each one.

## Plane Model

| Plane | Responsibilities | Main Ownership |
|---|---|---|
| **Embedding Plane** | Starts the library, chooses store paths and schema, configures optional helper processes, integrates health and telemetry into the host application | Calling application |
| **Public API Plane** | Primary caller-facing lifecycle and high-level operations through `TripleStore`, plus documented expert module entry points | `TripleStore` and expert public modules |
| **Coordination Plane** | OTP supervision, dictionary-manager lifecycle, temporary or managed transaction lifecycle, snapshot registry, plan-cache lifecycle, dynamic statistics helpers, scheduled helper wiring | `TripleStore.Application` and runtime processes |
| **Storage Plane** | Dictionary encoding, graph ID handling, triple and quad index policy, batch fanout, loader/exporter behavior, schema-aware RDF conversion | `TripleStore.Adapter`, `TripleStore.Dictionary.*`, `TripleStore.Index`, `TripleStore.QuadIndex`, `TripleStore.QuadOperations`, `TripleStore.Loader`, `TripleStore.Exporter` |
| **Query Plane** | SPARQL parsing facade, algebra compilation, optimization, execution, graph clause semantics, graph authorization, validation, query logging, update semantics | `TripleStore.SPARQL.*` |
| **Reasoning Plane** | Rule selection and compilation, semi-naive evaluation, graph-scoped materialization, incremental maintenance, derived-store policy, status and provenance | `TripleStore.Reasoner.*` |
| **Native Adapter Plane** | RocksDB I/O and SPARQL parsing via bounded native surfaces | `TripleStore.Backend.RocksDB.ErlangAdapter`, `TripleStore.SPARQL.Parser.NIF` |
| **Data Plane** | On-disk RocksDB column families, snapshot handles, backup directories, graph-backup files, persisted statistics and provenance bytes | RocksDB files and filesystem artifacts |
| **Operations Plane** | Telemetry, metrics, Prometheus export, health, backup, graph backup, restore, alert thresholds, scheduled backup | `TripleStore.Telemetry`, `TripleStore.Health`, `TripleStore.Backup`, `TripleStore.GraphBackup`, related modules |

## Control Plane Rules

- The Public API Plane defines primary caller-visible behavior and MUST remain the only canonical source for facade semantics.
- Expert modules MAY expose narrower or lower-level behavior, but their semantics MUST still map to the owning plane in the matrix.
- The Coordination Plane owns process lifecycle and serialized SPARQL update execution when `Transaction` is used; it does not own query or reasoning meaning.
- The Storage Plane owns dictionary encoding, graph ID treatment, and index fanout semantics for both triple and quad schemas.
- The Query Plane owns SPARQL query and update semantics, including graph clause interpretation and authorization hooks.
- The Reasoning Plane owns derivation meaning, graph-scoped reasoning configuration, and provenance semantics.
- The Native Adapter Plane MUST NOT redefine query, storage, or reasoning semantics.
- The Data Plane stores canonical bytes but does not interpret them independently of the higher semantic planes.
- The Operations Plane MUST report on the same runtime and data model used by the other planes.

## Canonical Ownership Reference

Detailed ownership for specs and runtime areas is defined in:

- [contracts/control_plane_ownership_matrix.md](contracts/control_plane_ownership_matrix.md)

## Control Plane ADR

- [adr/ADR-0001-control-plane-authority.md](adr/ADR-0001-control-plane-authority.md)
