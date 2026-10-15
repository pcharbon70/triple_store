---
title: Module guide template
description: Structure for a new Code Companion chapter
---

# Module guide template

Copy this page to a new chapter file and replace the prompts. Add frontmatter
fields `chapter_step`, `inspected_revision`, and `chapter_state` matching
`chapters.tsv`. The H1 must match the ledger title.

## Chapter record

| Field | Value |
| --- | --- |
| Step | `{step}` |
| Inspected revision | `{full commit SHA}` |
| State | `{current or superseded}` |
| Scope | {Exact ledger summary} |

## Problem and mental model

State the reader's concrete question and use a Mermaid diagram when a flow or
relationship is easier to see than to describe.

## Important files

List mapped repository-relative source and test paths in backticks. Explain
their responsibilities and keep each path's primary coverage owner unique.

## Execution flow

Follow control and data from the public or expert entry point to the result.
Explain schema differences and branches that change semantics.

## Data, ownership, and lifetime

Describe key representations, who owns processes and native resources, and
where cleanup must occur.

## Failure and trust boundaries

Describe tagged errors, rollback limits, authorization, and native adapter
boundaries. State what a successful call does and does not establish.

## Tests and evidence limits

Name relevant test files and exact cases. Record commands, results, and dirty
state only if observed. Identify what the evidence does not prove.

## Normative connections

Link the applicable contract and area spec headings. Distinguish required
behavior from observed implementation and report discrepancies.

## Revision and maintenance

State which changes require this chapter and its coverage rows to be revisited.
