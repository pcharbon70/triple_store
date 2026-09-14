# Query Optimization

The optimizer combines statistics, cardinality estimation, a cost model, join
enumeration, rule-based rewrites, and optional worst-case-optimal execution.
The behavior is implemented under `lib/triple_store/sparql/` and
`lib/triple_store/statistics/`.

## Planning inputs

`Statistics.collect/2` builds statistics and `Statistics.save/2` persists them.
`Statistics.get/1` reads current statistics, while `refresh/2` recollects and
saves them. Quad helpers expose graph-specific and all-graph summaries. The
optimizer can operate without complete statistics, but estimates are then less
informed.

Cardinality and cost modules compare index scans and join alternatives.
`JoinEnumeration` produces join orders. Leapfrog modules handle compatible
multiway joins. Selection depends on algebra shape and supported pattern forms;
it should not be promised for every complex query.

## Plan cache

`TripleStore.SPARQL.PlanCache` is supervised by the application:

~~~elixir
alias TripleStore.SPARQL.PlanCache

plan =
  PlanCache.get_or_compute(parsed_query, fn ->
    build_plan.(parsed_query)
  end)

stats = PlanCache.stats()
:ok = PlanCache.invalidate()
~~~

The cache normalizes query structure, has a configurable size and TTL, and
evicts entries. A transaction invalidates a named plan cache when that
coordinator has a `:plan_cache` configured. The facade's temporary update
coordinator does not configure one, and direct loader/index mutations do not
invalidate this cache. It stores plans, not query results.

## Result cache and correctness

`TripleStore.Query.Cache` is opt-in through query options. Production keys
include an open-store identity so results from different databases do not
collide. Successful supported mutations invalidate that store in all active
named result caches. ACL-controlled quad requests bypass caching.

When changing planning, verify result equivalence as well as latency. Include
both schemas, bound and unbound graph patterns, empty inputs, and early stream
termination where relevant. Benchmark targets are goals; only generated reports
for the tested commit and environment are measurements.
