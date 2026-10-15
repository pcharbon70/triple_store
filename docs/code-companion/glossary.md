---
title: Glossary
description: Terms used throughout the TripleStore Code Companion
---

# Glossary

**Column family (CF)**
: A named RocksDB key space. The triple and quad schemas use different index CFs.

**Dictionary ID**
: A tagged 64-bit integer used internally for an RDF term. Some supported values
  are encoded inline; others receive persisted sequence-backed IDs.

**Explicit fact**
: A triple or quad loaded or inserted by a caller, stored in the schema's
  explicit indices.

**Derived fact**
: An inferred triple or quad stored separately in the `derived` persistence
  surface when a reasoning workflow persists it.

**Graph ID**
: A dictionary-backed graph identifier in quad storage. `0` is reserved for
  the default graph.

**Managed stream**
: A lazy result path that owns native iterators and must release them on normal
  exhaustion, early halt, and failure.

**Native adapter**
: The bounded interface to the Rust SPARQL parser or RocksDB NIF. Elixir modules
  retain query, storage, transaction, and reasoning decisions.
