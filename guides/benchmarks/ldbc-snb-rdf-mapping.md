# Canonical SNB RDF mapping

The mapping identifier is `snb-rdf-v1`. SNB BI and SNB Interactive must use
this mapping for every comparable run. Their generated source files, update
layouts, graph partitions, parameters, and manifests remain distinct.

## Identity and graph partitioning

Every entity IRI has this form:

```text
https://ldbcouncil.org/snb/entity/{EntityType}/{source-id}
```

The IRI depends only on the canonical entity type and source ID. Dictionary IDs,
file order, worker count, and load order cannot change it. The concrete entity
types are Person, Forum, Post, Comment, Organisation, Place, Tag, and TagClass.
Post and Comment also receive the Message class without replacing their concrete
class.

All SNB benchmark stores use the quad schema. Initial and update data are placed
in fixed graphs:

```text
https://ldbcouncil.org/snb/graph/{snb_bi|snb_interactive}/{initial|updates}
```

Changing the schema or graph partition changes the mapping version and makes
the resulting run a different profile.

## Properties and datatypes

Classes and predicates use `https://ldbcouncil.org/snb/ontology/`. Strings,
country names, language values, IP addresses, browser names, URLs, and content
are RDF strings. Arrays such as email addresses and spoken languages become one
triple per value. Integers use `xsd:integer`, dates use `xsd:date`, and source
epoch-millisecond timestamps are converted deterministically to UTC
`xsd:dateTime` lexical forms. Missing optional values emit no statement;
invalid non-empty values fail conversion.

Message, Place, and Organisation subtypes remain their concrete classes and
retain their source `type` property. Country and place relationships use the
same `isLocatedIn` edge as the source graph, so conversion does not replace a
relationship with a label string.

## Relationships with properties

Every SNB relationship emits its direct RDF edge. When an edge carries values
such as `creationDate`, `joinDate`, `classYear`, or `workFrom`, the mapping also
emits a deterministic relationship resource with `rdf:subject`,
`rdf:predicate`, and `rdf:object`, plus the property predicates. The resource
IRI hashes the relationship type and both canonical endpoints. This keeps the
direct edge available to SPARQL patterns while preserving all edge data.

## Conversion and generator boundary

`TripleStore.Benchmark.LDBC.SNB.Converter` streams entity and relationship rows
and uses a disk-backed reference index. A missing endpoint, malformed row,
unknown property, invalid datatype, or reordered update fails conversion.
Initial data, ordered update records, and parameter documents are separate
manifest components. Parameter documents carry the RDF dataset checksum and
scale factor.

Spark Datagen is pinned for BI and Hadoop Datagen is pinned for Interactive v1.
`SNB.Generator` creates explicit scale, serializer, output, memory, core, and
update-partition command plans. Execution verifies the Git commit and requires
`allow_external: true`; test runs only use the checked-in smoke CSV inputs.
