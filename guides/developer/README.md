# Developer Guides

Technical documentation for TripleStore internals and implementation details.

## Guides

| Guide | Description |
|-------|-------------|
| [00-architecture-overview.md](00-architecture-overview.md) | High-level system architecture, component overview, data flow |
| [01-storage-layer.md](01-storage-layer.md) | RocksDB integration, dictionary encoding, triple indexing |
| [02-sparql-engine.md](02-sparql-engine.md) | SPARQL parsing, algebra, optimization, execution |
| [03-reasoning-engine.md](03-reasoning-engine.md) | OWL 2 RL rules, semi-naive evaluation, TBox caching |
| [04-query-optimization.md](04-query-optimization.md) | Cost model, cardinality estimation, join enumeration |
| [05-telemetry-monitoring.md](05-telemetry-monitoring.md) | Telemetry events, metrics collection, Prometheus |

## Reading Order

For new developers:

1. Start with **Architecture Overview** to understand the overall system design
2. Read **Storage Layer** to understand how data is persisted
3. Continue with **SPARQL Engine** for query processing
4. Explore **Reasoning Engine** for OWL 2 RL inference
5. Review **Query Optimization** for performance considerations
6. Finish with **Telemetry & Monitoring** for observability

## Key Concepts

### Dictionary Encoding

All RDF terms are encoded as 64-bit integers with type tags:

```
Type 1 (URI):      0x1xxx_xxxx_xxxx_xxxx
Type 2 (BNode):    0x2xxx_xxxx_xxxx_xxxx
Type 3 (Literal):  0x3xxx_xxxx_xxxx_xxxx
Type 4 (Integer):  0x4xxx_xxxx_xxxx_xxxx (inline)
Type 5 (Decimal):  0x5xxx_xxxx_xxxx_xxxx (inline)
Type 6 (DateTime): 0x6xxx_xxxx_xxxx_xxxx (inline)
```

### Triple Indices

Three indices provide O(log n) access for all query patterns:

| Pattern | Index |
|---------|-------|
| `(S, P, O)`, `(S, P, ?)`, `(S, ?, ?)` | SPO |
| `(?, P, O)`, `(?, P, ?)` | POS |
| `(?, ?, O)`, `(S, ?, O)` | OSP |

### Query Execution

Iterator-based lazy evaluation with streaming:

```
BGP Scan → Filter → Join → Project → LIMIT
```

### Reasoning

Forward-chaining materialization with semi-naive evaluation:

```
delta = explicit_facts
while delta ≠ ∅:
    new_facts = apply_rules(delta)
    store(new_facts)
    delta = new_facts
```
