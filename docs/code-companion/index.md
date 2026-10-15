---
title: TripleStore Code Companion
description: Guided, evidence-aware reading paths through TripleStore
---

# TripleStore Code Companion

The Code Companion helps contributors follow an RDF operation through the
current Elixir implementation, its native boundaries, persistent data, and
nearby tests. Each chapter identifies the source revision it inspected and
separates contract requirements from what the code and tests currently show.

Start with [store lifecycle and explicit data](store-lifecycle-and-explicit-data.md),
then follow the [SPARQL query path](sparql-query-path.md) and
[reasoning and derived facts](reasoning-and-derived-facts.md). Continue with
[SPARQL update and transactions](sparql-update-and-transactions.md) and
[operations and recovery](operations-and-recovery.md). The chapters are
ordered for reading; they are not a claim about the historical order in which
features were implemented.

The [specification contracts](https://github.com/pcharbon70/triple_store/blob/main/specs/contracts/README.md)
define required behavior. The [user guides](https://github.com/pcharbon70/triple_store/blob/main/guides/user/README.md)
teach API workflows; the [developer guides](https://github.com/pcharbon70/triple_store/blob/main/guides/developer/README.md)
provide wider subsystem reference material. This Companion is a source-reading
path, not a standards certification or a substitute for those documents.

## Reading ledger

The [machine-readable chapter ledger](chapters.tsv) records a permanent reading
step, source revision inspected, state, and scope. `current` means the chapter
is maintained as part of this reading path; it does not mean its claims were
retested after every later code change. A replaced chapter remains in the
ledger as `superseded`, with its replacement added as a new step.

| Step | Chapter | Inspected revision | State | Scope |
| ---: | --- | --- | --- | --- |
| 1 | [Store lifecycle and explicit data](store-lifecycle-and-explicit-data.md) | `29608a6` | `current` | Follow open, RDF term encoding, schema-specific index writes, and close. |
| 2 | [SPARQL query path](sparql-query-path.md) | `29608a6` | `current` | Trace query text through parsing, planning, execution, and result boundaries. |
| 3 | [Reasoning and derived facts](reasoning-and-derived-facts.md) | `29608a6` | `current` | Compare local in-memory materialization with graph-scoped derived persistence. |
| 4 | [SPARQL update and transactions](sparql-update-and-transactions.md) | `29608a6` | `current` | Follow serialized update requests through staged operations and one explicit-index batch. |
| 5 | [Operations and recovery](operations-and-recovery.md) | `29608a6` | `current` | Connect telemetry and health to full-store and graph-scoped recovery paths. |

## Source coverage

The [coverage map](coverage.tsv) assigns one chapter the primary explanation of
each listed source or test file. It helps readers find the owner when a mapped
file changes. Other chapters may refer to that file without acquiring another
coverage row. Unlisted repository files are outside this initial reading path;
the map is neither line coverage nor test coverage.

## Maintaining the Companion

Start a new chapter from the [module guide template](module-guide-template.md).
Add an append-only row to `chapters.tsv`, reproduce its metadata in the chapter
and this overview, add a navigation entry in `mkdocs.yml`, and map its primary
source and test files in `coverage.tsv`. Update an existing owning chapter when
its mapped implementation changes. Give each mapped file exactly one owner and
explain it in that chapter. Define new terms in the [glossary](glossary.md).

Use the relevant `REQ-*` contract and `AC-*` area spec as intent, then inspect
code and adjacent tests for current behavior. Record exact commands and results
only after running them; a test file's existence is evidence of a case, not a
passing result. Review the rendered page and run
`bash scripts/docs/verify-code-companion.sh` before handing off guide changes.
