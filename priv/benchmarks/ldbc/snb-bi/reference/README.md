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
