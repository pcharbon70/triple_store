# ADR-0001: Control-Plane Authority Precedence

## Status

Accepted

## Context

The new `specs/` system introduces multiple baseline and area documents that can overlap on runtime ownership statements.

Without an explicit precedence rule, future docs can drift on whether semantics live in the public API, Elixir coordination/query/reasoning layers, native adapters, or the data plane.

## Decision

1. `specs/contracts/control_plane_ownership_matrix.md` is the canonical ownership authority.
2. Component and area docs MUST match that matrix.
3. Baseline docs MAY summarize mixed ownership but MUST NOT silently redefine it.
4. Conflicts are resolved in this order:
   - control-plane ownership matrix
   - this ADR
   - baseline and area docs
5. Canonical module namespace references in these specs MUST use `TripleStore.*`.

## Consequences

- Ownership drift becomes reviewable.
- Native adapters cannot casually become semantic authorities.
- Data-plane and operations-plane references remain explicitly bounded.
- Future validation tooling can check ownership declarations mechanically.

## Related Requirements

`REQ-CP-001`, `REQ-CP-002`, `REQ-CP-003`, `REQ-CP-004`, `REQ-CP-005`
