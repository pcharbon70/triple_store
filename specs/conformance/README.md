# TripleStore Conformance Index

This directory defines review and validation scenarios for the `TripleStore` specs system.

## Documents

- [scenario_catalog.md](scenario_catalog.md)
- [spec_conformance_matrix.md](spec_conformance_matrix.md)

## Scope

These docs now drive repository-local traceability validation through `mix conformance`.

Current automation validates the documentation graph:

- matrix rows reference real contracts, scenarios, and spec paths
- contract requirements are covered by the conformance matrix
- area acceptance mappings reference valid `REQ-*` families and `SCN-*` scenarios
- acceptance evidence paths resolve on disk

It does not yet execute scenario-specific implementation tests automatically.
