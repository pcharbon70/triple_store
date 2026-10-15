#!/usr/bin/env python3
"""Generate SNB BI answer rows with the pinned external Cypher oracle.

This script is benchmark evidence tooling. It does not add Cypher support to
TripleStore: TripleStore continues to execute its workload through SPARQL and
the registered RDF graph adapters. The external engine only supplies expected
rows for cross-model correctness comparison.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import pathlib
import platform
import subprocess
import sys
from typing import Any, Iterable

import neo4j
from neo4j import GraphDatabase


REFERENCE_COMMIT = "5f7967235593eefa98b336e240039780b6a76a9a"
EXPECTED_FIXTURE_FILES = {
    "entities/forum.csv",
    "entities/organisation.csv",
    "entities/person.csv",
    "entities/place.csv",
    "entities/tag.csv",
    "parameters/bi-read-1.csv",
    "relationships/forum_hasMember_person.csv",
    "relationships/person_knows_person.csv",
    "updates/bi-batch.csv",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream-checkout", required=True, type=pathlib.Path)
    parser.add_argument("--fixture-root", required=True, type=pathlib.Path)
    parser.add_argument("--run-spec", required=True, type=pathlib.Path)
    parser.add_argument("--output-dir", required=True, type=pathlib.Path)
    parser.add_argument("--bolt-uri", default="bolt://localhost:7687")
    parser.add_argument("--image-reference", required=True)
    parser.add_argument("--image-id", required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--apoc-checksum", required=True)
    parser.add_argument("--gds-checksum", required=True)
    return parser.parse_args()


def sha256_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def sha256_file(path: pathlib.Path) -> str:
    return sha256_bytes(path.read_bytes())


def git_head(checkout: pathlib.Path) -> str:
    completed = subprocess.run(
        ["git", "-C", str(checkout), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def load_json(path: pathlib.Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object in {path}")
    return value


def csv_rows(path: pathlib.Path, expected_headers: list[str]) -> Iterable[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream, delimiter="|")
        if reader.fieldnames != expected_headers:
            raise ValueError(
                f"unexpected headers in {path}: {reader.fieldnames!r} != {expected_headers!r}"
            )
        yield from reader


def epoch_datetime(value: str) -> dt.datetime:
    return dt.datetime.fromtimestamp(int(value) / 1_000, dt.timezone.utc)


def source_checksum(fixture_root: pathlib.Path) -> str:
    actual = {
        str(path.relative_to(fixture_root))
        for path in fixture_root.rglob("*.csv")
    }
    if actual != EXPECTED_FIXTURE_FILES:
        raise ValueError(
            f"fixture CSV coverage changed: expected={sorted(EXPECTED_FIXTURE_FILES)!r} "
            f"actual={sorted(actual)!r}"
        )

    checksums = [
        (relative, sha256_file(fixture_root / relative))
        for relative in EXPECTED_FIXTURE_FILES
    ]
    encoded = "\n".join(
        f"{relative}\0{checksum}" for relative, checksum in sorted(checksums)
    )
    return sha256_bytes(encoded.encode("utf-8"))


def load_fixture(session: Any, fixture_root: pathlib.Path) -> None:
    session.run("MATCH (node) DETACH DELETE node").consume()

    people = []
    person_headers = [
        "id",
        "firstName",
        "lastName",
        "gender",
        "birthday",
        "creationDate",
        "locationIP",
        "browserUsed",
        "languages",
        "email",
    ]
    for row in csv_rows(fixture_root / "entities/person.csv", person_headers):
        people.append(
            {
                "id": int(row["id"]),
                "firstName": row["firstName"],
                "lastName": row["lastName"],
                "gender": row["gender"],
                "birthday": dt.date.fromisoformat(row["birthday"]),
                "creationDate": epoch_datetime(row["creationDate"]),
                "locationIP": row["locationIP"],
                "browserUsed": row["browserUsed"],
                "speaks": row["languages"].split(";") if row["languages"] else [],
                "email": row["email"].split(";") if row["email"] else [],
            }
        )
    session.run("UNWIND $rows AS row CREATE (node:Person) SET node = row", rows=people).consume()

    forums = [
        {
            "id": int(row["id"]),
            "title": row["title"],
            "creationDate": epoch_datetime(row["creationDate"]),
        }
        for row in csv_rows(
            fixture_root / "entities/forum.csv", ["id", "title", "creationDate"]
        )
    ]
    session.run("UNWIND $rows AS row CREATE (node:Forum) SET node = row", rows=forums).consume()

    places = [
        {"id": int(row["id"]), "name": row["name"]}
        for row in csv_rows(fixture_root / "entities/place.csv", ["id", "name"])
    ]
    session.run("UNWIND $rows AS row CREATE (node:Place) SET node = row", rows=places).consume()

    tags = [
        {"id": int(row["id"]), "name": row["name"]}
        for row in csv_rows(fixture_root / "entities/tag.csv", ["id", "name"])
    ]
    session.run("UNWIND $rows AS row CREATE (node:Tag) SET node = row", rows=tags).consume()

    for row in csv_rows(
        fixture_root / "entities/organisation.csv", ["id", "name", "type"]
    ):
        subtype = row["type"]
        if subtype not in {"Company", "University"}:
            raise ValueError(f"unsupported Organisation subtype: {subtype}")
        session.run(
            f"CREATE (node:Organisation:{subtype} {{id: $id, name: $name}})",
            id=int(row["id"]),
            name=row["name"],
        ).consume()

    for row in csv_rows(
        fixture_root / "relationships/person_knows_person.csv",
        ["Person1.id", "Person2.id", "creationDate"],
    ):
        session.run(
            "MATCH (left:Person {id: $left}), (right:Person {id: $right}) "
            "CREATE (left)-[:KNOWS {creationDate: $creation_date}]->(right)",
            left=int(row["Person1.id"]),
            right=int(row["Person2.id"]),
            creation_date=epoch_datetime(row["creationDate"]),
        ).consume()

    for row in csv_rows(
        fixture_root / "relationships/forum_hasMember_person.csv",
        ["Forum.id", "Person.id", "joinDate"],
    ):
        session.run(
            "MATCH (forum:Forum {id: $forum}), (person:Person {id: $person}) "
            "CREATE (forum)-[:HAS_MEMBER {creationDate: $join_date}]->(person)",
            forum=int(row["Forum.id"]),
            person=int(row["Person.id"]),
            join_date=epoch_datetime(row["joinDate"]),
        ).consume()


def convert_parameter(type_name: str, value: Any) -> Any:
    if type_name in {"Date", "DateTime"}:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(dt.timezone.utc)
    return value


def operation_parameters(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        name: convert_parameter(descriptor["type"], descriptor["value"])
        for name, descriptor in entry["parameters"].items()
    }


def query_sources(upstream: pathlib.Path) -> dict[str, str]:
    paths = [
        pathlib.Path("common/result_mapping.py"),
        pathlib.Path("cypher/queries.py"),
        *[pathlib.Path(f"cypher/queries/bi-{number}.cypher") for number in range(1, 21)],
        pathlib.Path("cypher/queries/bi-19-create-graph.cypher"),
        pathlib.Path("cypher/queries/bi-19-drop-graph.cypher"),
        pathlib.Path("cypher/queries/bi-20-create-graph.cypher"),
        pathlib.Path("cypher/queries/bi-20-drop-graph.cypher"),
    ]
    return {str(path): sha256_file(upstream / path) for path in paths}


def runtime_versions(session: Any) -> dict[str, str]:
    plugins = session.run("RETURN apoc.version() AS apoc, gds.version() AS gds").single()
    database = session.run(
        "CALL dbms.components() YIELD versions RETURN versions[0] AS neo4j"
    ).single()
    return {
        "neo4j": database["neo4j"],
        "apoc": plugins["apoc"],
        "graph_data_science": plugins["gds"],
        "python_driver": neo4j.__version__,
    }


def precompute(session: Any, upstream: pathlib.Path) -> None:
    for number in (19, 20):
        for action in ("drop", "create"):
            query = (
                upstream / f"cypher/queries/bi-{number}-{action}-graph.cypher"
            ).read_text(encoding="utf-8")
            session.run(query).consume()


def validate_run_spec(spec: dict[str, Any], source_digest: str) -> None:
    if spec.get("schema_version") != 1:
        raise ValueError("unsupported run-spec schema")
    if spec.get("catalog_id") != "ldbc-snb-bi-v1.0.3":
        raise ValueError("unexpected catalog identity")
    if spec.get("reference_source", {}).get("commit") != REFERENCE_COMMIT:
        raise ValueError("reference-source commit does not match the pinned implementation")
    if spec.get("dataset", {}).get("source_checksum") != source_digest:
        raise ValueError("fixture source checksum does not match the run specification")

    entries = spec.get("entries")
    if not isinstance(entries, list) or len(entries) != 31:
        raise ValueError("the run specification must contain all 31 variants")
    identities = {
        (entry["operation_id"], entry["variant"], entry["parameter_row_index"])
        for entry in entries
    }
    if len(identities) != 31:
        raise ValueError("the run specification contains duplicate answer identities")


def generate(args: argparse.Namespace) -> pathlib.Path:
    repository_root = pathlib.Path(__file__).resolve().parents[1]
    upstream = args.upstream_checkout.resolve()
    fixture_root = args.fixture_root.resolve()
    run_spec_path = args.run_spec.resolve()
    output_dir = args.output_dir.resolve()

    if git_head(upstream) != REFERENCE_COMMIT:
        raise ValueError("upstream checkout is not at the pinned v1.0.3 commit")

    spec = load_json(run_spec_path)
    fixture_digest = source_checksum(fixture_root)
    validate_run_spec(spec, fixture_digest)

    for label, checksum in {
        "APOC": args.apoc_checksum,
        "GDS": args.gds_checksum,
    }.items():
        if not checksum.startswith("sha256:") or len(checksum) != 71:
            raise ValueError(f"invalid {label} checksum: {checksum}")

    sys.path.insert(0, str(upstream / "common"))
    sys.path.insert(0, str(upstream / "cypher"))
    from queries import read_query_fun

    driver = GraphDatabase.driver(args.bolt_uri, auth=None)
    try:
        with driver.session() as session:
            versions = runtime_versions(session)
            if versions["neo4j"] != "5.1.0":
                raise ValueError(f"unexpected Neo4j version: {versions['neo4j']}")
            if versions["python_driver"] != "4.4.8":
                raise ValueError(
                    f"unexpected Neo4j Python driver: {versions['python_driver']}"
                )

            load_fixture(session, fixture_root)
            precompute(session, upstream)

            answers = []
            for entry in spec["entries"]:
                number = entry["number"]
                query_path = upstream / f"cypher/queries/bi-{number}.cypher"
                rows_json = session.write_transaction(
                    read_query_fun,
                    number,
                    query_path.read_text(encoding="utf-8"),
                    operation_parameters(entry),
                )
                rows = json.loads(rows_json)
                answers.append(
                    {
                        "operation_id": entry["operation_id"],
                        "variant": entry["variant"],
                        "parameter_row_index": entry["parameter_row_index"],
                        "parameters": {
                            name: descriptor["value"]
                            for name, descriptor in entry["parameters"].items()
                        },
                        "rows": rows,
                        "rows_checksum": sha256_bytes(
                            json.dumps(
                                rows,
                                ensure_ascii=False,
                                separators=(",", ":"),
                                sort_keys=True,
                            ).encode("utf-8")
                        ),
                    }
                )
    finally:
        driver.close()

    bundle = {
        "schema_version": 1,
        "bundle_id": "snb-bi-smoke-v1.0.3-row0-reference-answers",
        "catalog_id": spec["catalog_id"],
        "dataset": spec["dataset"],
        "parameter_source": spec["parameter_source"],
        "reference_source": spec["reference_source"],
        "mapping_boundary": {
            "source": "LDBC SNB CSV fixture",
            "external_oracle": "Neo4j property graph running pinned upstream Cypher",
            "triple_store": "RDF snb-rdf-v1 mapping with SPARQL and explicit graph adapters",
            "purpose": "answer generation only; no Cypher support in TripleStore",
        },
        "runtime": {
            **versions,
            "image_reference": args.image_reference,
            "image_id": args.image_id,
            "image_digest": args.image_digest,
            "plugin_checksums": {
                "apoc": args.apoc_checksum,
                "graph_data_science": args.gds_checksum,
            },
            "python": platform.python_version(),
        },
        "generation": {
            "tool": str(pathlib.Path(__file__).resolve().relative_to(repository_root)),
            "tool_checksum": sha256_file(pathlib.Path(__file__)),
            "run_spec": str(run_spec_path.relative_to(repository_root)),
            "run_spec_checksum": sha256_file(run_spec_path),
            "query_source_checksums": query_sources(upstream),
        },
        "answers": answers,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    answer_path = output_dir / "answers.json"
    encoded = json.dumps(bundle, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    answer_path.write_text(encoded, encoding="utf-8")
    (output_dir / "SHA256SUMS").write_text(
        f"{sha256_file(answer_path).removeprefix('sha256:')}  answers.json\n",
        encoding="utf-8",
    )
    return answer_path


def main() -> None:
    path = generate(parse_args())
    print(path)


if __name__ == "__main__":
    main()
