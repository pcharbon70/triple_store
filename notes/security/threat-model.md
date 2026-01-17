# Triple Store Security: Threat Model and Assumptions

## Document Version

- **Created:** 2025-01-10
- **Last Updated:** 2025-01-10
- **Status:** Phase 1 (Quad Storage Foundation)

---

## Overview

This document describes the security assumptions and threat model for the Triple Store implementation. It outlines the threats we consider, the protections in place, and the security boundaries of the system.

---

## Security Boundaries

### Trusted Components

The following components are considered trusted and operate within the same security boundary:

1. **Application Code** - The Elixir application using the Triple Store
2. **Database Files** - RocksDB data files on disk
3. **Runtime Environment** - BEAM VM and host operating system

### Untrusted Inputs

The following inputs are considered untrusted and must be validated:

1. **User-Provided Data** - RDF terms (URIs, literals, blank nodes) from SPARQL queries
2. **SPARQL Queries** - Query strings from external sources
3. **Database Paths** - File paths specified at runtime (when opening databases)
4. **Network Data** - Any data received from network connections (future)

---

## Threat Categories

### 1. Path Traversal Attacks

**Threat:** An attacker could use path traversal sequences (e.g., "../") to access files outside the intended database directory.

**Mitigations:**

| Location | Mitigation | Status |
|----------|-----------|--------|
| `ErlangAdapter.validate_path/1` | Path expansion and directory containment checks | Implemented |
| `ErlangAdapter.validate_path/1` | Null byte detection | Implemented |
| `ErlangAdapter.validate_path/1` | Absolute path restrictions | Implemented |

**Remaining Risks:**

- Symbolic links could potentially bypass containment checks (considered acceptable given trusted runtime environment)

### 2. Resource Exhaustion (DoS)

**Threat:** An attacker could consume excessive resources (memory, disk, CPU) causing denial of service.

**Mitigations:**

| Threat | Mitigation | Status |
|--------|-----------|--------|
| Large queries | No query size limit currently | Not Implemented |
| Large result sets | No result set size limit currently | Not Implemented |
| Disk space exhaustion | RocksDB has built-in limits | Partial |
| ID space exhaustion | 64-bit ID space makes this impractical | Mitigated |

**Recommendations:**

- Add query timeout mechanism
- Add result set size limits
- Consider rate limiting for ID allocation

### 3. Data Injection

**Threat:** An attacker could inject malicious data that causes unexpected behavior.

**Mitigations:**

| Location | Mitigation | Status |
|----------|-----------|--------|
| Term encoding | Dictionary encoding with type tagging | Implemented |
| NIF boundaries | Input validation at Elixir/NIF boundary | Partial |
| SPARQL parsing | SPARQL parser NIF validates syntax | Future (Phase 2) |

### 4. Schema Confusion

**Threat:** An attacker could attempt to open a triple store as a quad store or vice versa, causing data corruption.

**Mitigations:**

| Location | Mitigation | Status |
|----------|-----------|--------|
| Schema versioning | Schema version 1 (triple) vs 2 (quad) stored in DB | Implemented |
| `is_quad_store?/1` | Schema detection on open | Implemented |
| Open validation | Hard error on schema mismatch | Implemented |

### 5. ID 0 Collision

**Threat:** An attacker could somehow allocate ID 0, causing confusion with the default graph identifier.

**Mitigations:**

| Location | Mitigation | Status |
|----------|-----------|--------|
| Dictionary encoding | Type tagging prevents ID 0 allocation | Implemented |
| `valid_graph_id?/1` | Validates named graph IDs > 0 | Implemented |
| Default graph constant | ID 0 reserved via design | Implemented |

**Analysis:** Type tagging in the dictionary encodes term types in the ID space, making it mathematically impossible for a real term to be allocated ID 0. Each type uses offset encoding:

- URIs: IDs start from 1 (type tag 0)
- Blank nodes: IDs start from highest URI ID + 1
- Literals: IDs start from highest blank node ID + 1

This ensures ID 0 is never returned by `get_or_create_id/2`.

---

## Security Assumptions

### Trusted Environment

The Triple Store makes the following assumptions about its operating environment:

1. **File System** - The file system is trusted and not subject to race conditions or TOCTOU attacks beyond normal OS protections
2. **BEAM VM** - The Erlang VM is trusted and provides expected isolation
3. **Memory Safety** - The NIF code (erlang-rocksdb) is memory-safe and correctly implements the RocksDB API

### Application-Level Security

The Triple Store does NOT provide:

- Authentication or authorization (delegated to application layer)
- Encryption at rest (delegated to filesystem/OS)
- Audit logging (planned for future phases)
- Access control (delegated to application layer)

### Network Security

When network access is added (future phases), the following will be required:

1. TLS for all network communications
2. Authentication mechanisms
3. Rate limiting for DoS protection
4. Input sanitization for network-provided data

---

## Known Security Limitations

### 1. No Query Timeout

Long-running queries could consume excessive resources.

**Mitigation:** Application should implement timeouts at the process level.

### 2. No Result Set Limits

Queries returning large result sets could exhaust memory.

**Mitigation:** Application should use streaming APIs (to be added) for large queries.

### 3. No Audit Logging

Sensitive operations are not logged for compliance.

**Mitigation:** Planned for future phases.

### 4. Direct NIF Usage

QuadOperations module uses the NIF directly instead of ErlangAdapter.

**Mitigation:** Planned refactoring to use ErlangAdapter consistently.

---

## Security Testing Recommendations

1. **Fuzz Testing** - Test dictionary encoding with malformed inputs
2. **Boundary Testing** - Test ID exhaustion scenarios
3. **Path Testing** - Test path validation with encoded traversal attempts
4. **Schema Testing** - Test opening wrong schema types
5. **Concurrency Testing** - Test concurrent operations for race conditions

---

## Change History

| Date | Change | Author |
|------|--------|--------|
| 2025-01-10 | Initial threat model created | Phase 1 Review |
