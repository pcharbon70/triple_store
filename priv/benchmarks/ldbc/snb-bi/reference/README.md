# SNB BI reference answers

`manifest.exs` binds each answer artifact to one catalog operation and variant,
dataset and RDF mapping checksum, parameter row, operation source, and reference
implementation revision. Every entry declares whether its purpose is an
`accepted_answer` or a `contract_fixture`; embedded storage alone does not make
an artifact qualification evidence. The checked-in answer under `smoke/` is a
small contract fixture generated for the repository's Apache-2.0 smoke dataset.
This purpose field is required by manifest schema version 2. Answer payloads
remain schema version 1.

Full-scale generated answer sets are not committed. Their manifest entries use
an external immutable address and SHA-256 checksum. Copy an externally addressed
artifact into a controlled local reference root and replace the entry with an
embedded relative path only after verifying the declared checksum.

## Generated smoke answer bundle

`generated/smoke-v1.0.3/answers.json` contains independently generated row-zero
answers for all 31 canonical variants. `SHA256SUMS` fixes the complete bundle,
and `run-spec.json` binds the generation input to the current smoke source and
RDF output checksums, canonical parameter rows, and pinned v1.0.3 source.

`manifest.exs` binds each canonical variant to the bundle as an
`accepted_answer`. `ReferenceAnswers` verifies the complete bundle checksum,
selects the exact operation, variant, and parameter-row identity, verifies the
selected parameter row and row checksum, and then decodes the rows through the
operation's typed result contract. The checked-in smoke workflow therefore has
complete accepted-answer coverage while remaining a reduced diagnostic profile.

The pinned upstream project supplies a Neo4j/Cypher reference implementation.
It is used here only as an external answer oracle over the source CSV fixture.
TripleStore remains an RDF triple/quad store: it runs the workload through
SPARQL translations and explicit RDF graph adapters and does not support Cypher.
The generated bundle records this model boundary, the upstream query checksums,
Neo4j/APOC/GDS versions, Python driver version, and immutable container identity.

Regenerate the bundle with an upstream checkout at commit
`5f7967235593eefa98b336e240039780b6a76a9a`, Neo4j 5.1.0 with APOC and GDS,
and the upstream-declared Python driver:

```sh
python3 -m venv /tmp/ldbc-snb-bi-reference
/tmp/ldbc-snb-bi-reference/bin/pip install neo4j==4.4.8 python-dateutil
/tmp/ldbc-snb-bi-reference/bin/python \
  scripts/generate_ldbc_snb_bi_reference_answers.py \
  --upstream-checkout /path/to/ldbc_snb_bi \
  --fixture-root priv/benchmarks/ldbc/fixtures/snb-bi \
  --run-spec priv/benchmarks/ldbc/snb-bi/reference/run-spec.json \
  --output-dir priv/benchmarks/ldbc/snb-bi/reference/generated/smoke-v1.0.3 \
  --bolt-uri bolt://localhost:7687 \
  --image-reference neo4j:5.1.0 \
  --image-id sha256:43eb20e91b26463d9a206c03ae97182ee6a83e63b83255bba256fb5f517603ef \
  --image-digest neo4j@sha256:8e5d3d32ab4d18544694091a825a13a8e7a48af35a177a6e7645f74a9186de4a \
  --apoc-checksum sha256:f45f7b313e48288c35c01fec33d390626d4a5cbddab298332127dc1150f6007d \
  --gds-checksum sha256:480fd5836cc8bc1563ed4a22c7a9a71acc122c4df3bba59d2ff3e31399e7f3bc
sha256sum -c priv/benchmarks/ldbc/snb-bi/reference/generated/smoke-v1.0.3/SHA256SUMS
```
