# `ClrAsyncEnumerableConvention`

A second calling convention, `CLR_ASYNC_ENUMERABLE`, mirroring `ClrEnumerableConvention` node for node and
rule for rule. A node's `Implement` returns an expression whose value is an `IAsyncEnumerable<TRow>` where
the synchronous convention's is an `IEnumerable<TRow>`. Everything about a *row* is unchanged: the same
`ClrPhysType`, the same `JavaRowFormat`, the same synthetic record types, the same Rex translation, the same
adapter invariant at every Java boundary.

Its rows feed `DbDataReader.ReadAsync`.

## 1. What actually differs

**Only the sequence is asynchronous.** A row-level delegate — predicate, selector, key selector,
comparator, accumulator adder, result selector — stays a synchronous `Func<…>`, because every one is CPU
work Calcite's generators write and there is nothing in one to await. `ClrAsyncEnumerableCalc` differs from
`ClrEnumerableCalc` in two places: the generic method it calls and the sequence type it declares. The forty
lines of `translateCondition` and `translateProjects` plumbing between them are identical.

That is why every node is duplicated rather than shared. Measured over the 30 node files, `IEnumerable`
appears in **none** of them — a node never names the sequence type, it comes out of `physType.RowType`
through a generic method — and `ClrBuiltInMethod` appears 1–5 times, median 1. `Window` is 1026 lines and
one reference. Sharing the bodies would mean threading a sequence abstraction through all 45 to
parameterise a median of one identifier, and rewriting a working convention to do it.

### Cancellation carries itself

The compiled root is `Func<DataContext, IAsyncEnumerable<object>>`. A token enters at
`GetAsyncEnumerator(CancellationToken)`, and every operator in `ClrAsyncEnumerableDefaults` is an
`async IAsyncEnumerable<T>` iterator declaring `[EnumeratorCancellation]` and consuming its input through
`WithCancellation`. So the token reaches the leaf without appearing in the plan: the tree passes `default`
at every call, and `ClrAsyncBuiltInMethod.Call` appends it because `Expression.Call` applies no default
argument.

That only works because the compiler substitutes the token given to `GetAsyncEnumerator` for a parameter
that arrived as `default`, and because the substitution survives composition. Both are measured, in
`ClrAsyncEnumerableCancellationTests`, over a compiled tree built the way a node builds one. Had they not
held, every operator would have ignored cancellation and nothing else would have said so.

`DataContext.Variable.CANCEL_FLAG` stays as it is; Calcite's own generated blocks read it.

### Nothing is read until the first read

`ExecuteReaderAsync` parses, plans and compiles on the calling thread and then composes the operator chain.
Every operator is an async iterator, so calling one builds a state machine and runs none of it, and
`GetAsyncEnumerator` is the same. The table is not touched until the first `ReadAsync`. There is nothing
there to await rather than work being hidden — asserted by `ShouldReadNothingUntilTheFirstRead`.

### The root is `IAsyncEnumerable<object>`, and the conversion cannot fail

`IAsyncEnumerable<out T>` is covariant, so a reference row type reaches `IAsyncEnumerable<object>` free. A
value type would not, and would fail badly: `Expression.Convert` **builds** for
`IAsyncEnumerable<int>` → `<object>` and throws at run time, because the target is an interface.

It cannot arise. `ClrPhysTypeImpl` boxes what the type factory answers, `RequireRowType` holds every node to
`IAsyncEnumerable<physType.RowType>`, and the only other shape at the root is `Slice0<object>`. So the
element type is always a reference type and `ImplementRoot` carries no boxing pass. The guard is the
row-type discipline, not a hop at the end.

Deferring the cast to the consumer does not work either: `IAsyncEnumerable<int>` is not assignable to
`<object>` at all, so there is nothing to cast back from. For a reference row type the round trip is the
same instance both ways.

**Single-column results as real .NET types, later.** A one-column INTEGER leaves as a `java.lang.Integer`
today. Handing back a boxed `System.Int32` the consumer unboxes with `(int)row` works and changes nothing
about the plan's shape — but it moves in step with `ClrPrepareImpl.AvaticaTypeOf` and
`CalciteResultRow`/`CalciteTypeMap`, or the column metadata describes a value the reader is no longer
handed. A decision for both conventions, not this one.

### Three async-only traps

- **`System.Linq.AsyncEnumerable` does not exist on net8.0** — measured; it arrived in .NET 10. So `Where`,
  `Select`, `Skip` and `Take` are written out where their counterparts delegate to `System.Linq`.
- **`ConfiguredCancelableAsyncEnumerable<T>` is a struct and is not an `IAsyncEnumerable<T>`.** What
  `WithCancellation` returns can be `await foreach`ed and nothing else, so each operator applies it at its
  own loop.
- **`ValueTask<bool>` may be awaited once.** That bites `MergeJoin` and `AsofJoin`, the two that drive
  enumerators by hand.

## 2. The table SPI

Four interfaces, mirroring the two of Calcite's that a scan cares about, twice:

| | Calcite | synchronous | asynchronous |
|---|---|---|---|
| called, yields `object?[]` | `ScannableTable` | `IClrScannableTable` | `IClrAsyncScannableTable` |
| names its element type, yields an expression | `QueryableTable` | `IClrQueryableTable` | `IClrAsyncQueryableTable` |

`QueryableTable.asQueryable` has no counterpart: a linq4j `Queryable` is Java's LINQ, translated by
`LixToRelTranslator`, which is package-private and takes a `Prepare`.

They exist so a .NET table author does not have to build one of Calcite's `Enumerator`s by hand only for the
scan to read it straight back out. Elements are nullable because Calcite declares them so
(`Enumerable<@Nullable Object[]>`) — a field is null wherever its column is.

There is no cancellation parameter on `ScanAsync`: a token enters an `IAsyncEnumerable` at
`GetAsyncEnumerator`, which is where .NET puts it.

**Calcite's own tables are reached the way Calcite reaches them** — `getExpression(Queryable.class)`,
translated — in both scans. Calling `ScannableTable.scan` directly was tried and reverted: it means
re-deriving `deduceElementType`'s precedence by hand, and that broke seventy tests, because a
`ReflectiveSchema` table is a `QueryableTable` **and** a `ScannableTable` and `deduceElementType` answers
`QueryableTable` first. That access is what Calcite maintains and what changes when Calcite changes.

`DeduceElementType` and `Format` on both scans mirror `EnumerableTableScan`'s, name for name and branch for
branch.

## 3. One converter, and it goes one way

`EnumerableToClrAsyncEnumerableConverter` reads an `EnumerableConvention` sub-plan: Calcite's implementor
produces the linq4j block, `LixToClrTranslator` translates it, `FromJavaAsync` wraps the `Enumerable`.
Nothing suspends — a linq4j sequence is pulled — and nothing blocks; it completes synchronously, which costs
a state machine and no thread.

It is what stops the convention having to plan a query whole or not at all. A node it has none of — a table
function, a MATCH_RECOGNIZE, a recursive query's transient scan — is planned by Calcite underneath, and the
rest of the query is still asynchronous.

**A direct table scan needs no converter**, because the scan reads Calcite's tables itself. The converter is
for what the planner hands over whole.

**There is no converter out, and there should not be.** A sequence going the other way would become a linq4j
`Enumerator`, whose `moveNext` returns a `boolean` with nowhere to await, so it would block once per row.
Nothing requires it: Calcite cannot read an `IClrAsyncScannableTable` or an `IClrAsyncQueryableTable` by any
route, so no plan can put a Calcite node *above* an asynchronous one. A query that would need it does not
plan — `MATCH_RECOGNIZE` over an asynchronous table is exactly that, and is tested as a refusal.

The synchronous convention does have a converter out and pays for it: `ClrEnumerableToEnumerableConverter`
stashes its sub-plan for generated Java to call back into. `TODO.md` has the fix — translating a CLR tree
into a linq4j one so the sub-plan becomes part of the block Janino compiles.

**Compiling is not planning.** That converter used to call `Compile()` inside its own `Implement`, doing JIT
work while the plan was still being assembled and once per converter. It stashes a `ClrPlan<TRows>` now,
which compiles itself the first time it runs — at most once per prepared statement, the stash belonging to
the `ClrSignature`.

## 4. Nodes this convention does not have

`Interpreter` (Calcite's is a pull-based `Enumerable`), `TableFunctionScan` (both paths hand a linq4j
`Enumerable` through a generator of Calcite's), table modify, and MATCH_RECOGNIZE (impossible in either
convention — `PassedRowsInputGetter` and `PrevInputGetter` are package-private types Calcite casts to by
name). `TableSpool` and `RepeatUnion` exist but are out of `Rules()`: the iterative side scans a
`TransientTable`, which only Calcite's interpreter reads.

All of them are planned by Calcite under the converter, and all are covered by the differential tests.

Kept, with the reason recorded: `Values`, `Collect` and `Uncollect` touch a materialized row *value*, not a
stream. `Sort`, `Aggregate`, `Distinct`, the set operations, `Window` and a hash join's build side all
buffer — buffering an asynchronous source is `await foreach` then yield, which blocks nothing. A blocking
operator is not a synchronous one.

## 5. Four fused operators

`SingletonAggregate`, `SingletonJavaList`, `SingletonJavaMap` and `CombineQueryResults` each fuse a pair the
synchronous convention nests in the tree. There `Singleton(Aggregate(source, …))` is two calls, the inner
returning a row; here the inner must be awaited and **an expression tree cannot await**, so the composition
is an operator. `Aggregate`, `ToJavaList` and `ToJavaMap` are correspondingly absent from
`ClrAsyncBuiltInMethod`, as are `FromJava` and `ToJava`, whose callers cannot exist here.

`SingletonAggregate` is also lazier than the pair it replaces: it folds on the first `MoveNextAsync` rather
than when the plan is invoked.

## 6. Prepare and the ADO.NET surface

`ClrPrepareImpl` takes a flag rather than gaining a copy — the convention, the program, the root traits and
the compiler are a convention's own and nothing else is, which `ClrPreparingStmt` already scoped.

`ClrSignature` holds an `IClrBindableBase` and has `Bind` and `BindAsync`, each refusing the other's plan.
`IClrBindableBase.ElementType` is a `System.Type`: a compiled plan is a delegate over CLR types. What the
type factory called the row stays on `ClrPrepareResult.ElementType`, for `Meta.CursorFactory.deduce`.

`CalciteSession` has both halves of each operation — `ExecuteReader`/`ExecuteReaderAsync`,
`ExecuteNonQuery`/`ExecuteNonQueryAsync` — rather than one async method the synchronous entry point blocks
on. That funnelling is what once made a session-level convention choice look possible, and it handed a
synchronous caller a reader whose `Read` threw.

**There is no fallback.** `ExecuteReaderAsync` prepares into this convention and throws if it cannot.
Preparing the synchronous plan instead would return a reader that looks asynchronous and blocks a thread per
row, which a caller cannot tell from the outside. Since the converter landed, the cases that would have
needed a fallback plan anyway.

`CalciteResult` is abstract, with `CalciteEnumerableResult` and `CalciteAsyncEnumerableResult` under it.
**Both read methods are on both**: `DbDataReader` is a contract, and a consumer that knows nothing but that
interface calls `Read`. So a synchronous plan answers `ReadAsync` with a completed task and an asynchronous
plan blocks in `Read`. Neither is the sync-over-async this convention refuses — that rule is about a plan's
*internals*, where a converter would insert blocking nobody chose and nobody could see. At the boundary the
caller is choosing in the open.

`CalciteDataReader` overrides `CloseAsync` and `DisposeAsync`. Without them `DbDataReader` falls back to the
synchronous `Dispose`, which can only finish a disposal the plan already completed itself — a table closing
a connection in its `finally` would have been abandoned, with every row correct and nothing to say so.

A token given only to `ReadAsync` cannot reach the leaf: an `IAsyncEnumerable` takes its token at
`GetAsyncEnumerator`, which happened when the reader was made. It stops the reader between rows. Nothing can
change that short of a token every operator threads.

There is no asynchronous DML and cannot be: a table modification is not a node either convention implements.

## 7. Tests

**`ClrAsyncEnumerableDifferentialTests`** runs the same SQL through both conventions over one copy of the
rows and requires the same answers. It compares against `ClrEnumerableConvention` rather than Calcite, which
is not weaker: the synchronous convention is checked against Calcite query by query, so agreeing with it is
agreeing with Calcite. It is also the only comparison available, this convention reading a table Calcite has
no SPI for.

**The fixture suspends on every row.** One that completed synchronously would exercise none of the
resumption or disposal behaviour that distinguishes the two, and an operator that dropped its continuation
would look correct.

`ClrAsyncEnumerablePlanCancellationTests` requires a real compiled plan to stop when the caller does —
streaming, sorting, aggregating, and abandoned by breaking out of the loop. The blocking operators are the
point: one that ignored its token produces exactly the right rows and simply runs the leaf to exhaustion, so
every differential test passes.

`ClrAsyncEnumerableAdoNetTests` covers the whole path and asserts *which* convention ran.
`ClrTableSpiTests` covers the four table interfaces, which otherwise compile and never run.

## 8. What the tests cannot see

**The differential tests compare answers, not implementations.** Three divergences from Calcite have been
found by being asked about rather than by testing — the async `Format`, the direct `ScannableTable.scan`,
and `OrderByWithFetchAndOffset` sorting its whole input where linq4j keeps a bounded `TreeMap`. All three
returned the right rows and had to pass.

So a green suite is not the stopping condition for a port. `TODO.md` has the audit: every member read
against its Calcite counterpart, verdict recorded.
