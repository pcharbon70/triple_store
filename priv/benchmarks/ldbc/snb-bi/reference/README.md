# SNB BI reference answers

`manifest.exs` binds each accepted answer to one catalog operation and variant,
dataset and RDF mapping checksum, parameter row, operation source, and reference
implementation revision. Embedded answers under `smoke/` are deliberately small
contract fixtures generated for the repository's Apache-2.0 smoke dataset.

Full-scale generated answer sets are not committed. Their manifest entries use
an external immutable address and SHA-256 checksum. Copy an externally addressed
artifact into a controlled local reference root and replace the entry with an
embedded relative path only after verifying the declared checksum.
