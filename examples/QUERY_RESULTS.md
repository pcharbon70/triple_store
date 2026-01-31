# Ash Framework Codebase Analysis

This document contains the results of running various SPARQL queries against the Ash Framework codebase represented as RDF.

**Dataset:** `examples/ash.ttl` (313,187 quads)
**Generated:** 2026-01-31 20:34:17.955536Z

---

## Table of Contents

1. [Hub Modules](#hub-modules) - Most connected modules in the codebase
2. [Entry Points](#entry-points) - Modules with few incoming dependencies
3. [Module Clusters](#module-clusters) - Code organization by namespace
4. [Call Graph](#call-graph) - Incoming/outgoing calls for Ash.Changeset
5. [API Surface](#api-surface) - Public functions per module
6. [Type Usage](#type-usage) - Type definitions across the codebase
7. [Error Patterns](#error-patterns) - Error module hierarchy
8. [Impact Analysis](#impact-analysis) - What depends on Ash.Changeset
9. [Complexity](#complexity) - Modules with most outgoing dependencies

---

## 1. Hub Modules

The most connected modules in the codebase - modules that are called by many others and call many others. These are critical architectural components where changes have wide-ranging impact.

| Module | Incoming Calls | Outgoing Calls | Total |
|--------|---------------|----------------|-------|
| `Elixir.Access` |  3225 |     0 |  3225 |
| `Enum` |  2638 |     0 |  2638 |
| `Ash.Changeset` |   594 |  1867 |  2461 |
| `Ash.Query` |   556 |  1235 |  1791 |
| `Elixir.Kernel` |  1605 |     0 |  1605 |
| `Ash.Actions.Read` |    48 |  1520 |  1568 |
| `Map` |  1498 |     0 |  1498 |
| `Ash.Resource.Info` |   859 |   405 |  1264 |
| `Keyword` |  1230 |     0 |  1230 |
| `Ash.Actions.Update.Bulk` |    11 |  1085 |  1096 |
| `Ash.Filter` |   120 |   960 |  1080 |
| `Ash` |   230 |   831 |  1061 |
| `changeset` |  1033 |     0 |  1033 |
| `query` |   983 |     0 |   983 |
| `Ash.Actions.ManagedRelationships` |    19 |   805 |   824 |
| `Ash.CodeInterface` |    56 |   768 |   824 |
| `relationship` |   733 |     0 |   733 |
| `Ash.Resource` |   523 |   202 |   725 |
| `Ash.Actions.Read.Relationships` |     7 |   683 |   690 |
| `Ash.Actions.Destroy.Bulk` |     1 |   687 |   688 |

### Key Insights

- **Elixir.Access**, **Enum**, **Kernel**, **Map**, and **Keyword** are standard library modules with high incoming calls but no tracked outgoing calls in this dataset.
- **Ash.Changeset** (2461 total) and **Ash.Query** (1791 total) are the framework's core hub modules.
- **Ash.Actions.Read** has 1568 total calls with very high outgoing (1520), indicating it orchestrates many other modules.

---

## 2. Entry Points

Modules with few or no incoming dependencies - the "edges" of the dependency graph. These are good starting points for learning the codebase.

| Module | Incoming Calls |
|--------|---------------|
| `Ash.Policy.Check.Expression` |   0 |
| `Ash.Type.DateTime` |   0 |
| `Ash.Type.Vector` |   0 |
| `Ash.Resource.Validation.Changing` |   0 |
| `Ash.Resource.Validation.Negate` |   0 |
| `Ash.Resource.Transformers.BelongsToAttribute` |   0 |
| `Ash.Union` |   0 |
| `Ash.Policy.Check.AccessingFrom` |   0 |
| `Ash.Type.Struct` |   0 |
| `Ash.Type.Tuple` |   0 |
| `Ash.Resource.Verifiers.VerifyAcceptedByDomain` |   0 |
| `Ash.Type.Integer` |   0 |
| `Ash.Resource.Transformers.GetByReadActions` |   0 |
| `Ash.Reactor.ActionStep` |   0 |
| `Ash.Policy.SimpleCheck` |   0 |
| `Ash.Type.String` |   0 |
| `Ash.Resource.Validation.Builtins` |   0 |
| `Ash.Resource.Change.Builtins` |   0 |
| `Ash.Error.Invalid.NonCountableAction` |   0 |
| `Ash.Type.Term` |   0 |
| `Ash.Query.Function.IsNil` |   0 |
| `Ash.Error.Changes.NoSuchAttribute` |   0 |
| `Ash.Reactor.BuilderUtils` |   0 |
| `Ash.Reactor.ChangeStep` |   0 |
| `Ash.Type.Union` |   0 |
| `Ash.Expr.SAT` |   0 |
| `Ash.Query.Operator.Has` |   0 |
| `Ash.Query.Function.FromNow` |   0 |
| `Ash.Policy.Check.RelatesToActorVia` |   0 |
| `Ash.Resource.Verifiers.ValidateArgumentsToCodeInterface` |   0 |

### Key Insights

- Modules with **0 incoming calls** may be dynamically invoked or represent leaf nodes in the call graph.
- Entry points with **1-5 calls** are good onboarding candidates as they have fewer prerequisites.

---

## 3. Module Clusters

Code organization by namespace, revealing domain boundaries and architectural layers.

| Namespace | Module Count |
|-----------|--------------|
| `Ash.Resource` |  147 |
| `Ash.Error` |   87 |
| `Ash.Query` |   56 |
| `Ash` |   45 |
| `Ash.Type` |   39 |
| `Ash.Reactor` |   36 |
| `Ash.Policy` |   34 |
| `Ash.Actions` |   17 |
| `Ash.Domain` |   11 |
| `Mix.Tasks` |   10 |
| `Ash.DataLayer` |    6 |
| `Ash.Notifier` |    5 |
| `Ash.Page` |    3 |
| `Ash.Filter` |    2 |
| `Simple` |    1 |
| `Field` |    1 |
| `UpdateOpts` |    1 |
| `ShadowDomain` |    1 |
| `Pagination` |    1 |
| `Ash.TypedStruct` |    1 |
| `StreamOpts` |    1 |
| `GetOpts` |    1 |
| `Dsl` |    1 |
| `BulkCreateOpts` |    1 |
| `TransactionOpts` |    1 |

### Key Insights

- **Ash.Actions** cluster contains the business logic layer.
- **Ash.Error** cluster handles error types.
- **Ash.Query** and **Ash.Resource** clusters represent core domain abstractions.
- **Ash.Data** and **Ash.Policy** clusters show separation of concerns.

---

## 4. Call Graph: Ash.Changeset

Analysis of **Ash.Changeset** - the framework's primary data transformation abstraction.

### Incoming Calls (who calls Ash.Changeset)

Sample of 30 modules that call Ash.Changeset functions:

| `Ash.Actions.Update.Bulk` | `/...` → `fully_atomic_changeset/...` |
| `Ash.EmbeddableType` | `/...` → `set_context/...` |
| `Ash.Actions.Update` | `/...` → `set_action_select/...` |
| `Ash.Resource.Validation.AttributeDoesNotEqual` | `/...` → `get_attribute/...` |
| `Ash.Resource.Change.BeforeTransaction` | `/...` → `before_transaction/...` |
| `Ash.Changeset` | `/...` → `add_error/...` |
| `Ash.Changeset` | `/...` → `t/...` |
| `Ash.Resource.Change.Select` | `/...` → `select/...` |
| `Ash.Error.Invalid` | `/...` → `t/...` |
| `Ash.Domain.Interface` | `/...` → `t/...` |
| `Ash.DataLayer.Ets` | `/...` → `apply_attributes/...` |
| `Ash.Actions.Create.Bulk` | `/...` → `add_error/...` |
| `Ash.Actions.ManagedRelationships` | `/...` → `put_context/...` |
| `Ash.DataLayer.Ets` | `/...` → `set_context/...` |
| `Ash.Actions.Create` | `/...` → `require_values/...` |
| `Ash.CodeInterface` | `/...` → `new/...` |
| `Ash.Generator` | `/...` → `t/...` |
| `Ash.Actions.Create.Bulk` | `/...` → `run_after_transactions/...` |
| `Ash.EmbeddableType` | `/...` → `new/...` |
| `Ash.Subject` | `/...` → `before_action/...` |
| `Ash.Resource.Change` | `/...` → `t/...` |
| `Ash.Actions.Update.Bulk` | `/...` → `set_action_select/...` |
| `Ash.Actions.Update.Bulk` | `/...` → `run_after_actions/...` |
| `Ash.Resource.Validation.AttributeEquals` | `/...` → `get_attribute/...` |
| `Ash.Actions.Helpers` | `/...` → `before_action/...` |
| `Ash` | `/...` → `for_create/...` |
| `Ash.Resource.Change.ManageRelationship` | `/...` → `manage_relationship/...` |
| `Ash.BulkResult` | `/...` → `t/...` |
| `Ash.Actions.Create.Bulk` | `/...` → `apply_atomic_constraints/...` |
| `Ash.Actions.Update.Bulk` | `/...` → `add_error/...` |

### Outgoing Calls (what Ash.Changeset calls)

Sample of 30 external functions called by Ash.Changeset:

| `Ash.Changeset` → `Map` | `put/...` |
| `Ash.Changeset` → `relationship` | `source_attribute/...` |
| `Ash.Changeset` → `Ash.Resource.Info` | `short_name/...` |
| `Ash.Changeset` → `changeset` | `resource/...` |
| `Ash.Changeset` → `Elixir.Kernel` | `to_string/...` |
| `Ash.Changeset` → `action` | `name/...` |
| `Ash.Changeset` → `changeset` | `resource/...` |
| `Ash.Changeset` → `Map` | `delete/...` |
| `Ash.Changeset` → `Enum` | `empty?/...` |
| `Ash.Changeset` → `changeset` | `arguments/...` |
| `Ash.Changeset` → `changeset` | `domain/...` |
| `Ash.Changeset` → `Elixir.Kernel` | `to_string/...` |
| `Ash.Changeset` → `Ash.Query` | `limit/...` |
| `Ash.Changeset` → `Enum` | `reduce/...` |
| `Ash.Changeset` → `required_attribute` | `name/...` |
| `Ash.Changeset` → `changeset` | `resource/...` |
| `Ash.Changeset` → `Keyword` | `keyword?/...` |
| `Ash.Changeset` → `Ash.ToTenant` | `t/...` |
| `Ash.Changeset` → `Elixir.Kernel` | `to_string/...` |
| `Ash.Changeset` → `action` | `type/...` |
| `Ash.Changeset` → `Enum` | `reduce/...` |
| `Ash.Changeset` → `Elixir.Access` | `get/...` |
| `Ash.Changeset` → `opts` | `order_is_key/...` |
| `Ash.Changeset` → `Enum` | `map/...` |
| `Ash.Changeset` → `DateTime` | `utc_now/...` |
| `Ash.Changeset` → `changeset` | `action/...` |
| `Ash.Changeset` → `Elixir.Access` | `get/...` |
| `Ash.Changeset` → `Ash.Resource.Info` | `changes/...` |
| `Ash.Changeset` → `changeset` | `context/...` |
| `Ash.Changeset` → `Ash.Query` | `loading?/...` |

---

## 5. API Surface

Public functions per module - a measure of interface complexity.

| Module | Public Functions |
|--------|-----------------|
| `Ash.Changeset` |  93 |
| `Ash.Type` |  92 |
| `Ash.Resource.Info` |  81 |
| `Ash` |  68 |
| `Ash.Query` |  61 |
| `Ash.Type.NewType` |  46 |
| `Ash.CodeInterface` |  41 |
| `Ash.DataLayer` |  41 |
| `Ash.Filter` |  41 |
| `Ash.Domain.Interface` |  38 |
| `Ash.DataLayer.Ets` |  37 |
| `Ash.EmbeddableType` |  28 |
| `Ash.Expr` |  25 |
| `Ash.Actions.Helpers` |  23 |
| `Ash.DataLayer.Mnesia` |  22 |
| `Ash.Resource.Validation.Builtins` |  21 |
| `Ash.Resource` |  20 |
| `Ash.ActionInput` |  20 |
| `Ash.Type.Union` |  20 |
| `Ash.Generator` |  20 |
| `Ash.Policy.Check.Builtins` |  19 |
| `Ash.Resource.Change.Builtins` |  18 |
| `Ash.Resource.Builder` |  18 |
| `Ash.Subject` |  18 |
| `Ash.Actions.Read` |  18 |
| `Ash.Type.Enum` |  17 |
| `Ash.Helpers` |  17 |
| `Ash.Policy.Authorizer` |  16 |
| `Ash.Domain.Info` |  16 |
| `Ash.Query.Operator` |  16 |

### Key Insights

- Modules with **50+ public functions** may be doing too much or providing rich abstractions.
- **Ash.Changeset** has the largest API surface, reflecting its central role.

---

## 6. Type Usage

Type definitions appearing across the codebase.

| Type Name | Occurrences |
|-----------|-------------|
| `t` |   96 |
| `context` |    6 |
| `ref` |    5 |
| `before_transaction_fun` |    4 |
| `around_transaction_fun` |    4 |
| `after_transaction_fun` |    4 |
| `after_action_fun` |    3 |
| `actor` |    3 |
| `options` |    3 |
| `before_action_fun` |    3 |
| `path` |    2 |
| `error` |    2 |
| `action` |    2 |
| `data_layer_query` |    2 |
| `raw` |    2 |
| `page_opts_type` |    2 |
| `page_opts_opts` |    2 |
| `type` |    2 |
| `page_opts` |    2 |
| `error_keyword_option` |    1 |
| `around_result` |    1 |
| `path_input` |    1 |
| `cardinality` |    1 |
| `bulk_update_options` |    1 |
| `load_statement` |    1 |
| `rewrite_data` |    1 |
| `state` |    1 |
| `around_action_fun` |    1 |
| `manage_relationship_type` |    1 |
| `ash_error_subject` |    1 |

### Key Insights

- **t** is the conventional main type name, appearing in most modules.
- **opts** and **options** are common for configuration.
- Specialized types like **changeset**, **query**, **action** reflect domain concepts.

---

## 7. Error Patterns

Error module hierarchy showing how errors are organized.

- `Ash.Error`, `Ash.Error.Action.InvalidArgument`, `Ash.Error.Changes.ActionRequiresActor`
- `Ash.Error.Changes.InvalidArgument`, `Ash.Error.Changes.InvalidAttribute`, `Ash.Error.Changes.InvalidChanges`
- `Ash.Error.Changes.InvalidRelationship`, `Ash.Error.Changes.NoSuchAttribute`, `Ash.Error.Changes.NoSuchRelationship`
- `Ash.Error.Changes.Required`, `Ash.Error.Changes.StaleRecord`, `Ash.Error.Exception`
- `Ash.Error.Forbidden`, `Ash.Error.Forbidden.CannotFilterCreates`, `Ash.Error.Forbidden.DomainRequiresActor`
- `Ash.Error.Forbidden.DomainRequiresAuthorization`, `Ash.Error.Forbidden.ForbiddenField`, `Ash.Error.Forbidden.InitialDataRequired`
- `Ash.Error.Forbidden.MustPassStrictCheck`, `Ash.Error.Forbidden.Placeholder`, `Ash.Error.Forbidden.Policy`
- `Ash.Error.Framework`, `Ash.Error.Framework.AssumptionFailed`, `Ash.Error.Framework.CanNotBeAtomic`
- `Ash.Error.Framework.FlagAssertionFailed`, `Ash.Error.Framework.InvalidReturnType`, `Ash.Error.Framework.MustBeAtomic`
- `Ash.Error.Framework.PendingCodegen`, `Ash.Error.Framework.SynchronousEngineStuck`, `Ash.Error.Framework.UnsupportedSubject`
- `Ash.Error.Invalid`, `Ash.Error.Invalid.ActionRequiresPagination`, `Ash.Error.Invalid.AtomicsNotSupported`
- `Ash.Error.Invalid.InvalidActionType`, `Ash.Error.Invalid.InvalidCustomInput`, `Ash.Error.Invalid.InvalidPrimaryKey`
- `Ash.Error.Invalid.LimitRequired`, `Ash.Error.Invalid.MultipleResults`, `Ash.Error.Invalid.NoIdentityFound`
- `Ash.Error.Invalid.NoMatchingBulkStrategy`, `Ash.Error.Invalid.NoPrimaryAction`, `Ash.Error.Invalid.NoSuchAction`
- `Ash.Error.Invalid.NoSuchInput`, `Ash.Error.Invalid.NoSuchResource`, `Ash.Error.Invalid.NonCountableAction`
- `Ash.Error.Invalid.NonStreamableAction`, `Ash.Error.Invalid.PaginationRequired`, `Ash.Error.Invalid.ResourceNotAllowed`
- `Ash.Error.Invalid.TenantRequired`, `Ash.Error.Invalid.Timeout`, `Ash.Error.Invalid.TimeoutNotSupported`
- `Ash.Error.Invalid.Unavailable`, `Ash.Error.Load.InvalidQuery`, `Ash.Error.Load.NoSuchRelationship`
- `Ash.Error.Page.InvalidKeyset`, `Ash.Error.Query.AggregatesNotSupported`, `Ash.Error.Query.CalculationRequiresPrimaryKey`
- `Ash.Error.Query.CalculationsNotSupported`, `Ash.Error.Query.InvalidArgument`, `Ash.Error.Query.InvalidCalculationArgument`
- `Ash.Error.Query.InvalidExpression`, `Ash.Error.Query.InvalidFilterReference`, `Ash.Error.Query.InvalidFilterValue`
- `Ash.Error.Query.InvalidLimit`, `Ash.Error.Query.InvalidLoad`, `Ash.Error.Query.InvalidOffset`
- `Ash.Error.Query.InvalidPage`, `Ash.Error.Query.InvalidQuery`, `Ash.Error.Query.InvalidSortOrder`
- `Ash.Error.Query.LockNotSupported`, `Ash.Error.Query.NoComplexSortsWithKeysetPagination`, `Ash.Error.Query.NoReadAction`
- `Ash.Error.Query.NoSuchAttribute`, `Ash.Error.Query.NoSuchField`, `Ash.Error.Query.NoSuchFilterPredicate`
- `Ash.Error.Query.NoSuchFunction`, `Ash.Error.Query.NoSuchOperator`, `Ash.Error.Query.NoSuchRelationship`
- `Ash.Error.Query.NotFound`, `Ash.Error.Query.ReadActionRequired`, `Ash.Error.Query.ReadActionRequiresActor`
- `Ash.Error.Query.Required`, `Ash.Error.Query.UnsortableField`, `Ash.Error.Query.UnsupportedPredicate`
- `Ash.Error.SimpleDataLayer.NoDataProvided`, `Ash.Error.Stacktrace`, `Ash.Error.Unknown`
- `Ash.Error.Unknown.UnknownError`, `Ash.Query.Function.Error`

### Key Insights

- **Ash.Error.*** namespace provides well-organized error categories.
- **Error.Query**, **Error.Invalid**, **Error.Forbidden** show domain-specific classification.
- This enables broad rescue clauses like `rescue Error.Query`.

---

## 8. Impact Analysis: Ash.Changeset

What would be affected if **Ash.Changeset** changes? Modules ordered by call site count.

| Module | Call Sites |
|--------|------------|
| `Ash.Actions.ManagedRelationships` |   87 |
| `Ash.Actions.Update.Bulk` |   50 |
| `Ash.Changeset` |   45 |
| `Ash.Actions.Create.Bulk` |   36 |
| `Ash.Seed` |   33 |
| `Ash.Actions.Destroy.Bulk` |   27 |
| `Ash.Actions.Update` |   27 |
| `Ash.Actions.Create` |   23 |
| `Ash.Actions.Destroy` |   20 |
| `Ash.CodeInterface` |   19 |
| `Ash.Subject` |   18 |
| `Ash.EmbeddableType` |   16 |
| `Ash` |   16 |
| `Ash.DataLayer` |   15 |
| `Ash.Can` |   15 |
| `Ash.Resource.Change` |   15 |
| `Ash.DataLayer.Ets` |   10 |
| `Ash.Actions.Helpers` |    9 |
| `Ash.Generator` |    9 |
| `Ash.DataLayer.Mnesia` |    7 |
| `Ash.Resource.Change.DebugLog` |    4 |
| `Ash.Domain.Interface` |    4 |
| `Ash.Resource.Change.Select` |    4 |
| `Ash.Resource.Change.CascadeDestroy` |    4 |
| `Ash.Policy.Authorizer` |    4 |
| `Ash.Resource.Verifiers.ValidateManagedRelationshipOpts` |    3 |
| `Ash.Resource.Change.BeforeAction` |    3 |
| `Ash.DataLayer.Simple` |    3 |
| `Ash.Resource.Change.AfterTransaction` |    3 |
| `Ash.Resource.Change.BeforeTransaction` |    3 |

### Key Insights

- High-impact modules like **Ash.Actions** variants and **Ash.Resource** depend heavily on Changeset.
- Changes to Ash.Changeset require careful testing across many dependents.

---

## 9. Complexity

Modules with the most outgoing dependencies (calls to external modules).

| Module | Outgoing Calls |
|--------|----------------|
| `Ash.Actions.Read` |   90 |
| `Ash.Query` |   90 |
| `Ash.Filter` |   87 |
| `Ash.Changeset` |   78 |
| `Ash` |   71 |
| `Ash.Actions.Update.Bulk` |   60 |
| `Ash.Actions.Destroy.Bulk` |   52 |
| `Ash.Policy.Authorizer` |   51 |
| `Ash.DataLayer.Ets` |   49 |
| `Ash.Actions.Read.Calculations` |   48 |
| `Ash.Actions.Create.Bulk` |   48 |
| `Ash.Actions.Read.Relationships` |   41 |
| `Ash.CodeInterface` |   40 |
| `Ash.Actions.Helpers` |   39 |
| `Ash.Resource.Info` |   39 |
| `Ash.ActionInput` |   39 |
| `Ash.Expr` |   36 |
| `Ash.Actions.Update` |   36 |
| `Ash.Domain.Info` |   34 |
| `Ash.Can` |   34 |
| `Ash.Resource` |   34 |
| `Ash.Actions.ManagedRelationships` |   32 |
| `Ash.Actions.Create` |   31 |
| `Ash.Type` |   31 |
| `Ash.EmbeddableType` |   30 |
| `Ash.DataLayer` |   27 |
| `Ash.DataLayer.Mnesia` |   26 |
| `Ash.Generator` |   26 |
| `Ash.Helpers` |   25 |
| `Ash.Query.Aggregate` |   24 |

### Key Insights

- Modules with **100+ outgoing calls** are complex and may need refactoring.
- Orchestration modules like **Ash.Actions.Read** naturally have high outgoing calls.
- Consider whether complex modules could delegate more or be split up.

---

*Generated by TripleStore codebase insight queries*
