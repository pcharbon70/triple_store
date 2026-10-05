# LDBC Source and Provenance Policy

TripleStore's LDBC benchmark tooling uses only upstream inputs recorded in
`priv/benchmarks/ldbc/sources.exs`. Each entry names an immutable release or
commit, its full Git checksum, license and notice, runtime requirements, supported
platforms, owning profiles, and readiness level.

## Updating a source

An upstream change requires a reviewed pull request that:

1. changes the source manifest to a released tag or exact commit;
2. records the resolved 40-character commit checksum;
3. reviews the upstream license, notice, runtime, platform, and audit status;
4. regenerates affected operation catalogs, fixtures, and expected answers;
5. records the source entry and transformation-tool versions in every generated artifact;
6. runs the manifest, catalog, capability, and benchmark integration gates; and
7. explains whether results remain comparable with the previous profile version.

Moving branches such as `main`, `master`, `HEAD`, and `latest` are invalid source
versions. Work-in-progress sources are pinned by exact commit and belong to a
separate profile until an upstream stable release and the required validation are
available.

## Repository asset policy

The repository may contain manifests, operation metadata, hand-written mappings,
expected-result hashes, and small deterministic fixtures when their upstream
license allows redistribution. Committed derived assets must contain their source
entry ID, source checksum, transformation name and version, and output checksum.

Generated datasets, release archives, driver builds, database images, and large
answer sets stay outside Git. Their future acquisition layer must verify the
manifest checksum before use and keep them in an ignored external cache.

Clean-checkout smoke tests run without network access and use only committed,
license-compatible fixtures. A profile requiring an external artifact must return
`{:error, {:external_asset_missing, source_id, path}}`; it must never silently
download data or substitute another version.

## Notices

All currently pinned LDBC sources use Apache-2.0 and carry `NOTICE.txt`. Any copied
or transformed upstream asset must retain the notice required by its source.
Manifest validation confirms that license and notice metadata exist; the later
dataset phase will validate notice files next to committed benchmark assets.
