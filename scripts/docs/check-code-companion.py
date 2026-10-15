#!/usr/bin/env python3
"""Check the Code Companion reading ledger, chapter metadata, and coverage map."""

from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs/code-companion"
CHAPTERS = DOCS / "chapters.tsv"
COVERAGE = DOCS / "coverage.tsv"
CHAPTER_FIELDS = (
    "# step",
    "guide-path",
    "title",
    "inspected-revision",
    "state",
    "summary",
)
COVERAGE_FIELDS = ("# source-path", "guide-path", "role")
RESERVED = {"index.md", "glossary.md", "module-guide-template.md"}
HEADINGS = (
    "Chapter record",
    "Problem and mental model",
    "Important files",
    "Execution flow",
    "Data, ownership, and lifetime",
    "Failure and trust boundaries",
    "Tests and evidence limits",
    "Normative connections",
    "Revision and maintenance",
)


def reject(message: str) -> None:
    raise ValueError(message)


def repo_file(value: str, prefix: str | None = None) -> Path:
    if not value or value.startswith("/") or "\\" in value:
        reject(f"unsafe repository path: {value!r}")
    if any(part in ("", ".", "..") for part in value.split("/")):
        reject(f"unsafe repository path: {value!r}")
    if prefix and not value.startswith(prefix):
        reject(f"path must start with {prefix}: {value!r}")
    path = (ROOT / value).resolve()
    if not path.is_relative_to(ROOT) or not path.is_file():
        reject(f"missing or escaping repository file: {value}")
    return path


def rows(path: Path, header: tuple[str, ...]) -> list[tuple[str, ...]]:
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream, delimiter="\t")
        if next(reader, None) != list(header):
            reject(f"{path.relative_to(ROOT)} has an unexpected header")
        result = []
        for line, row in enumerate(reader, 2):
            if not row or row[0].startswith("#"):
                continue
            if len(row) != len(header) or any(not cell or cell != cell.strip() for cell in row):
                reject(f"{path.relative_to(ROOT)}:{line} has malformed fields")
            result.append(tuple(row))
        return result


def metadata(text: str, name: str) -> dict[str, str]:
    match = re.match(r"\A---\n(.*?)\n---\n", text, re.S)
    if not match:
        reject(f"{name} needs opening YAML frontmatter")
    values = {}
    for line in match.group(1).splitlines():
        if ": " not in line:
            reject(f"{name} has unsupported frontmatter: {line!r}")
        key, raw = line.split(": ", 1)
        if key in values:
            reject(f"{name} repeats {key}")
        try:
            values[key] = json.loads(raw) if raw.startswith('"') else raw
        except json.JSONDecodeError as error:
            reject(f"{name} has invalid {key}: {error}")
    return values


def check() -> None:
    required = (
        "mkdocs.yml",
        "requirements-docs.txt",
        "docs/code-companion/index.md",
        "docs/code-companion/glossary.md",
        "docs/code-companion/module-guide-template.md",
        "scripts/docs/build.sh",
        "scripts/docs/serve.sh",
        "scripts/docs/preview.sh",
        "scripts/docs/verify-code-companion.sh",
    )
    for name in required:
        repo_file(name)

    nav = repo_file("mkdocs.yml").read_text(encoding="utf-8")
    index = repo_file("docs/code-companion/index.md").read_text(encoding="utf-8")
    template = repo_file("docs/code-companion/module-guide-template.md").read_text(
        encoding="utf-8"
    )
    for page in RESERVED:
        if page not in nav:
            reject(f"MkDocs navigation omits {page}")
    for heading in HEADINGS:
        if f"## {heading}" not in template.splitlines():
            reject(f"module guide template lacks {heading}")

    entries = rows(CHAPTERS, CHAPTER_FIELDS)
    if not entries:
        reject("chapter ledger is empty")
    guides = set()
    for expected_step, entry in enumerate(entries, 1):
        step, guide_path, title, revision, state, summary = entry
        if step != str(expected_step):
            reject(f"chapter steps must be contiguous; expected {expected_step}")
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            reject(f"step {step} needs a full source revision")
        if state not in ("current", "superseded"):
            reject(f"step {step} has invalid state {state!r}")
        if "|" in title + summary:
            reject(f"step {step} contains a Markdown table separator")
        guide = repo_file(guide_path, "docs/code-companion/")
        if guide.name in RESERVED or guide.suffix != ".md" or guide_path in guides:
            reject(f"invalid or duplicate chapter path: {guide_path}")
        guides.add(guide_path)
        content = guide.read_text(encoding="utf-8")
        lines = content.splitlines()
        meta = metadata(content, guide_path)
        expected = {
            "title": title,
            "chapter_step": step,
            "inspected_revision": revision,
            "chapter_state": state,
        }
        for key, value in expected.items():
            if str(meta.get(key)) != value:
                reject(f"{guide_path} {key} disagrees with chapters.tsv")
        if f"# {title}" not in lines:
            reject(f"{guide_path} H1 disagrees with chapters.tsv")
        for heading in HEADINGS:
            if f"## {heading}" not in lines:
                reject(f"{guide_path} lacks {heading}")
        visible = (
            f"| Step | `{step}` |",
            f"| Inspected revision | `{revision}` |",
            f"| State | `{state}` |",
            f"| Scope | {summary} |",
        )
        if any(row not in lines for row in visible):
            reject(f"{guide_path} chapter record disagrees with chapters.tsv")
        index_row = (
            f"| {step} | [{title}]({guide.name}) | `{revision[:7]}` | "
            f"`{state}` | {summary} |"
        )
        if index_row not in index.splitlines():
            reject(f"overview disagrees with step {step}")
        if guide.name not in nav:
            reject(f"MkDocs navigation omits {guide.name}")

    authored = {
        f"docs/code-companion/{path.name}"
        for path in DOCS.glob("*.md")
        if path.name not in RESERVED
    }
    if authored != guides:
        reject(f"ledger and chapters differ: {sorted(authored ^ guides)}")
    if len(re.findall(r"^\| [1-9]\d* \|", index, re.M)) != len(entries):
        reject("overview row count differs from chapters.tsv")

    mapped = set()
    owner_count = {guide: 0 for guide in guides}
    for source, owner, _role in rows(COVERAGE, COVERAGE_FIELDS):
        repo_file(source)
        if source.startswith("docs/code-companion/") or source in mapped:
            reject(f"invalid or duplicate coverage source: {source}")
        if owner not in guides:
            reject(f"coverage owner absent from chapters.tsv: {owner}")
        guide_text = repo_file(owner).read_text(encoding="utf-8")
        if f"`{source}`" not in guide_text:
            reject(f"{owner} does not name mapped path {source}")
        mapped.add(source)
        owner_count[owner] += 1
    for owner, count in owner_count.items():
        if count == 0:
            reject(f"{owner} has no mapped files")


if __name__ == "__main__":
    try:
        check()
    except ValueError as error:
        print(f"Code Companion metadata check failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    print("Code Companion metadata check passed")
