# TripleStore Control Planes

## Purpose

This document defines the control planes used by `TripleStore` and the responsibilities that belong to each one.

## Plane Model

| Plane | Responsibilities | Main Ownership |
|---|---|---|
| **Embedding Plane** | Starts the library, chooses paths/configuration, invokes the API, integrates health/telemetry into a host application | Calling application |
| **Public API Plane** | `open`, `close`, `load`, `query`, `update`, `materialize`, `export`, `backup`, `restore`, `health`, `stats` | `TripleStore` |
| **Coordination Plane** | OTP supervision, dictionary-manager lifecycle, transaction serialization, plan-cache invalidation, snapshot/statistics coordination | `TripleStore.Application` and store-local coordination processes |
| **Query Plane** | SPARQL AST production, algebra compilation, optimization, execution, property paths, plan/result caching | `TripleStore.SPARQL.*` and query-support modules |
| **Reasoning Plane** | Rule compilation, semi-naive evaluation, incremental maintenance, derived-store policy, reasoning status | `TripleStore.Reasoner.*` |
| **Native Adapter Plane** | RocksDB I/O and SPARQL parsing through Rustler NIFs | `TripleStore.Backend.RocksDB.NIF`, `TripleStore.SPARQL.Parser.NIF` |
| **Data Plane** | On-disk RocksDB column families and backup directories | RocksDB files and filesystem copies |
| **Operations Plane** | Telemetry, metrics, health, backup, restore, scheduled backup, Prometheus export | `TripleStore.Telemetry`, `TripleStore.Health`, `TripleStore.Backup`, related modules |

## Control Plane Rules

- The Public API Plane defines caller-visible behavior and MUST remain the only supported entry point for external consumers.
- The Coordination Plane owns single-writer and manager-lifecycle semantics.
- The Query Plane owns query meaning even when parsing happens natively.
- The Reasoning Plane owns derivation meaning even when storage writes happen through the RocksDB NIF.
- The Native Adapter Plane MUST NOT redefine query, transaction, or reasoning semantics.
- The Data Plane stores canonical bytes but does not interpret them independently of the higher planes.
- The Operations Plane MUST report on the same canonical system model used by the other planes.

## Canonical Ownership Reference

Detailed ownership for specs and runtime areas is defined in:

- [contracts/control_plane_ownership_matrix.md](contracts/control_plane_ownership_matrix.md)

## Control Plane ADR

- [adr/ADR-0001-control-plane-authority.md](adr/ADR-0001-control-plane-authority.md)
