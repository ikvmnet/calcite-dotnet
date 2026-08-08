# `ClrAsyncEnumerableConvention` — plan

A second calling convention, `CLR_ASYNC_ENUMERABLE`, mirroring `ClrEnumerableConvention` node for node
and rule for rule. A node's `Implement` returns an expression whose value is an
`IAsyncEnumerable<TRow>` where the sync convention's is an `IEnumerable<TRow>`. Everything about a
*row* is unchanged: the same `ClrPhysType`, the same `JavaRowFormat`, the same synthetic record types,
the same Rex translation, the same boxing rules, the same adapter invariant at every Java boundary.

Its rows feed `DbDataReader.ReadAsync` on the ADO.NET surface.

## 1. What actually differs

**Only the sequence is asynchronous.** A row-level delegate — predicate, selector, key selector,
comparator, accumulator adder, result selector — stays a synchronous `Func<…>`, because every one of
them is CPU work Calcite's generators write and there is nothing in one to await. Which means
`ClrAsyncEnumerableCalc` differs from `ClrEnumerableCalc` in exactly two places: the generic method it
calls, and the sequence type it declares. The forty lines of `RexToLixTranslator.translateCondition`
and `translateProjects` plumbing between them are identical.

That is the shape of the whole port. It is why duplicating every node is the right answer rather than
an unfortunate one: each copy is a two-line delta from a file that has already been debugged, and the
alternative — one node class serving two conventions through a sequence abstraction — puts a working
convention at risk to save edits that a script makes anyway.

### The root is `IAsyncEnumerable<object>`, and a scalar row has to be boxed to reach it

`IAsyncEnumerable<out T>` and `IAsyncEnumerator<out T>` are covariant — the same declaration
`IEnumerable<out T>` carries, measured off the reference assembly rather than remembered. So a row of a
reference type reaches `IAsyncEnumerable<object>` by a free reference conversion, exactly as it does in
the sync convention.

A row of a value type does not, and the way it fails is the dangerous one. Measured on net8.0:

| | build | run |
|---|---|---|
| `Expression.Convert(IAsyncEnumerable<string>, IAsyncEnumerable<object>)` | OK | OK |
| `Expression.Convert(IAsyncEnumerable<int>, IAsyncEnumerable<object>)` | **OK** | `InvalidCastException` |

`Expression.Convert` accepts the second one **without complaint** — the target is an interface, so the
tree emits a cast the runtime is entitled to attempt — and it throws when the query runs. There is no
compile-time signal at all.

**But it cannot arise, because a row of these conventions is never a value type.** `ClrPhysTypeImpl`
boxes on the way in — `javaRowClass = ClrPrimitive.Box(ClrTypes.Resolve(javaRowType))`, and `RowType`
returns that — and `ClrPrimitive.Box` maps a Java primitive to its box class and returns everything else
unchanged. A row is a synthetic record, an `object[]`, a `java.util.List`, or a boxed scalar; never a
struct. `RequireRowType` then forces every node's result to be exactly `IEnumerable<physType.RowType>`,
and the only other shape `ImplementRoot` produces is `Slice0<object>`. So the element type at the root is
a reference type on every path there is.

So `ClrAsyncEnumerableRelImplementor.ImplementRoot` carries **no** boxing pass. The conversion to
`IAsyncEnumerable<object>` is a plain variance conversion, free and total. What keeps it safe is
`RequireRowType` naming `IAsyncEnumerable<>` and the row type exactly — the guard is the row-type
discipline, not a hop at the end.

### Two defects in the sync convention, found establishing the above

Neither is the async port's to fix, and both mislead it if left:

- **`ClrEnumerableRelImplementor.BoxScalars` is unreachable.** Its `elementType.IsValueType` test cannot
  be true, for the reason just given. It is dead on every path, and it was the basis for the first two
  answers I gave about this — the hop it performs is not happening today.
- **`ClrPhysType.RowType`'s doc comment states the opposite of what `ClrPhysTypeImpl` does.** It says
  "Unboxed, as Calcite's is: the physical type of a one column row of `INTEGER NOT NULL` is `int`", and
  that a sequence carries "this boxed … through `ClrEnumUtils.Boxed`". The implementation boxes in the
  constructor and `RowType` is that. `CLAUDE.md` has it right — "a row of this convention is boxed, and
  the physical type says so" — so the comment is stale from before the convention got a physical type of
  its own, and describes Calcite's `PhysType.getRowType`, which this deliberately diverges from.

Deleting `BoxScalars` and correcting the comment is a small change to the sync convention that
`ClrEnumerableDifferentialTests` covers already; do it before the port copies either.

**The cast cannot be deferred to the consumer, and the boxing is not overhead.** The obvious escape —
let the tree hand back an `IAsyncEnumerable<object>` whose runtime object is really an
`IAsyncEnumerable<int>`, and cast down where the rows are read — does not exist. Measured:

| | |
|---|---|
| `IAsyncEnumerable<Row>` → `<object>` → back to `<Row>` | OK both ways, **same instance** |
| `IAsyncEnumerable<int>` → `<object>` | not assignable; the `is` check is false |
| a type implementing *both* closings, `<object>` → `<int>` | OK, same instance |

For a reference row type the round trip already works and costs nothing: variance is a reference
conversion, not a wrapper. For `int` there is nothing to cast back *from* — the iterator does not
implement `IAsyncEnumerable<object>`, so the reference cannot be held in the first place. The third row
is the only shape that would work, and it requires the sequence to implement `IAsyncEnumerator<object>`
as well, whose `Current` returns `object` — which moves the box from the `Select` to the property and
makes it a *CLR* box, `System.Int32`, where a reader of a Calcite result expects `java.lang.Integer`.

### Single-column results as real .NET types, later

The intent is that a one-column result eventually leaves as a real .NET value — a boxed `System.Int32`
the consumer unboxes with `(int)row` — while the compiled expression still returns
`IAsyncEnumerable<object>` and nothing about the plan's shape moves. The measurements above say that
works: the element cast is what the consumer makes, and the sequence stays `<object>` either way.

What it is **not** is a use for `BoxScalars`. Today a one-column INTEGER leaves as a `java.lang.Integer`
— already a reference, already boxed, by `ClrPhysTypeImpl`. The future change converts a Java box to a
CLR value, which is a different conversion in a different direction from the dead method's, and the only
thing they share is a position.

Where that conversion goes is open. Inside the plan a value must stay Java's — Calcite's generated
blocks read it and its comparators compare it, and two representations of one value in a plan is the
failure `JavaValues` exists to prevent. So the candidates are the outermost hop of `ImplementRoot`, or
the reader. Whichever, it moves in step with the metadata: `ClrPrepareImpl.AvaticaTypeOf` derives
`ColumnMetaData.Rep` from `typeFactory.getJavaClass(...)`, and `CalciteResultRow` / `CalciteTypeMap` read
a row against it. Change the representation without changing those and the column metadata describes a
value the reader is no longer handed — which surfaces as a wrong `GetFieldType`, not as an exception.

This is a decision for both conventions at once, not the async one's to make.

The sequence-level cast is still not available whichever box is chosen — `IAsyncEnumerable<object>` to
`IAsyncEnumerable<int>` fails for the reason in the table above. The cast the consumer makes is on the
element.

Three async-specific traps around value types, none of which the sync port has:

- **`System.Linq.AsyncEnumerable` does not exist on net8.0** — it arrived in .NET 10, and the library
  targets net8.0. Measured: absent. So there is no `Select` to lean on for the boxing hop, and no
  built-in operator for anything else either. `ClrAsyncEnumerableDefaults` writes all of them, which was
  already the plan; the point is that there is no fallback if one is missed.
  `TaskAsyncEnumerableExtensions` — `WithCancellation`, `ConfigureAwait` — *is* present.
- **`ConfiguredCancelableAsyncEnumerable<T>` is a struct and is not an `IAsyncEnumerable<T>`.** What
  `source.WithCancellation(ct)` returns can be `await foreach`ed and nothing else: it cannot be stored
  as a sequence, returned, or passed to another operator. Every operator therefore applies it at its own
  `await foreach` rather than threading a "cancelable sequence" through.
- **`ValueTask<bool>` from `MoveNextAsync` may be awaited once.** Any operator holding an enumerator by
  hand — `MergeJoin`, `AsofJoin` — must not stash the `ValueTask` and await it twice. This is the
  async-only failure mode in the two nodes §8 already flags as unproven.

### Cancellation carries itself

The compiled root is `Func<DataContext, IAsyncEnumerable<object>>`. A token enters at
`IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken)`, and every operator in
`ClrAsyncEnumerableDefaults` is an `async IAsyncEnumerable<T>` iterator declaring
`[EnumeratorCancellation] CancellationToken cancellationToken = default` and consuming its input as
`await foreach (var row in source.WithCancellation(cancellationToken))`. So the token flows from
`CalciteDataReader.ReadAsync` down to the leaf without ever appearing in the plan: no expression-tree
parameter, no `DataContext` variable, no stash. The expression tree passes nothing and the language
does the threading.

`DataContext.Variable.CANCEL_FLAG` stays as it is, because Calcite's own generated blocks read it and
those blocks are unchanged.

### No converters — not to Calcite's convention, and not to ours

This is the caveat, and it is larger than it sounds.

- `IEnumerable` → `IAsyncEnumerable` is async-over-sync: a blocking pull behind an awaited interface.
- `IAsyncEnumerable` → `IEnumerable` is sync-over-async.

So `ClrAsyncEnumerableRules.Rules()` holds **no converter rule in either direction**, to
`EnumerableConvention` or to `ClrEnumerableConvention`, and
`ClrAsyncEnumerableConvention.canConvertConvention` returns false as the sync one's does.

The consequence: **an async plan has to cover the whole query by itself.** Where today a node this
project has no implementation for lands in `EnumerableConvention` and a converter carries its rows, an
async plan that cannot cover every node throws `CannotPlanException`. That is the intended behaviour
rather than a defect — the alternative is a plan that silently blocks a thread per row — but it means
the node list below has to be honest about what is missing, because nothing rescues a gap.

## 2. The leaf

```csharp
public interface IClrAsyncScannableTable : org.apache.calcite.schema.Table
{
    IAsyncEnumerable<object[]> ScanAsync(DataContext root);
}
```

`ScannableTable.scan(DataContext)` member for member, with the sequence swapped. No cancellation
parameter: the token arrives at the returned sequence's `GetAsyncEnumerator`, which is where .NET puts
it and what lets the plan stay ignorant of it.

The values in the `object[]` are Java's — `java.lang.Integer`, `java.lang.String`, `BigDecimal` —
exactly as `ScannableTable`'s are, because everything downstream is Calcite's and **every boundary
where a value crosses between the two runtimes is an adapter**. A table handing back CLR primitives is
the third leak of that invariant waiting to happen.

`ClrAsyncEnumerableTableScan.CanHandle` accepts this interface and **nothing else** — not
`ScannableTable`, not `QueryableTable`, not `FilterableTable`, not `ProjectableFilterableTable`. A
schema may hold both kinds of table; a *query* that touches a synchronous one simply will not plan
asynchronously.

The node is shorter than its sync counterpart, not longer. `object[]` means `Format()` has the two
branches `ClrEnumerableTableScan.Format` takes for an `Object[]` element type — `ARRAY`, or `SCALAR`
for one column, with the same `Slice0` — and there is no `getExpression(Queryable.class)`, no
`ToEnumerable`, and no `FromJava`: the table is stashed as a constant and `ScanAsync` is called
directly. The one linq4j hop the sync scan makes is absent.

An async *filterable* or *projectable* table — push-down — is deliberately out of scope for round one.
It is a second interface and a second `CanHandle` branch, and it can be added without disturbing this.

## 3. Nodes that cannot exist asynchronously

Each of these was checked against what the node actually does, not assumed.

| node | why |
|---|---|
| `Interpreter` and its rule | Calcite's `Interpreter` produces a pull-based linq4j `Enumerable`. There is nothing to await, and wrapping it is async-over-sync. |
| `TableFunctionScan` | Both paths are blocked. The TVF path does `ToJava(sub-plan)` → `RexToLixTranslator.translateTableFunction` → `FromJava(result)`; `ToJava` of an async sequence is sync-over-async. The schema-table-function path calls a function whose SPI returns a `Queryable`. An async TVF needs an async table-function SPI, which is its own piece of work. |
| both converter rules | above |
| `TableSpool` + `RepeatUnion` | The spool's *write* is fine — it writes rows into a `java.util.Collection` one at a time. The read is not: the iterative side scans the `TransientTable` the spool filled, `CanHandle` refuses a `TransientTable` (CALCITE-3673), and today that scan works only because it crosses a converter into `EnumerableConvention` and is interpreted. With no converter, `WITH RECURSIVE` cannot be planned asynchronously at all. |
| table modify / DML | Already outside `ClrEnumerableRules` — a write goes to `EnumerableConvention` through the converter. With no converter there is no asynchronous DML, and `ExecuteNonQueryAsync` keeps the synchronous path. |
| `MATCH_RECOGNIZE` | Impossible in either convention, for the reason in `CLAUDE.md`: `PassedRowsInputGetter` and `PrevInputGetter` are package-private *types* Calcite casts to by name. Unchanged here, and now without the converter that made a MATCH_RECOGNIZE query still run. |

`TableSpool` and `RepeatUnion` are ported anyway — they are cheap, they are the half that is not
blocked, and leaving a hole in the file set is how the hole gets forgotten. Their rules stay out of
`Rules()` until there is an async transient table for the iterative scan to read, and the node's
remarks say so.

Nodes that stay, with the reason recorded so that nobody "fixes" them later:

- **`Values`, `Collect`, `Uncollect`.** The collection each touches is a materialized row *value* — a
  `java.util.List` in a field, a constant array — not a stream. Reading one blocks nothing. This is
  not async-over-sync in any sense the rule is about.
- **`Sort`, `Aggregate`, `SortedAggregate`, `Distinct`, `Union`, `Intersect`, `Minus`, `Window`, and a
  hash join's build side.** All of them buffer. Buffering an asynchronous source is `await foreach`
  and then yield; it is fully asynchronous and blocks nothing. A blocking operator is not a
  synchronous one.

## 4. What is shared and what is copied

**Shared, untouched** — verified rather than assumed:

- `ClrPhysType`, `ClrPhysTypeImpl`, `JavaRowFormatExtensions`, `PhysTypeImplWorkaround`. A row's shape
  has nothing to do with how the sequence delivers it — with one exception. `ConvertTo` takes a
  *sequence* expression and returns one, baking `ClrBuiltInMethod.Select` into the type; it is the only
  member of the pair that is not about a row. It is **not** moved out, because it is
  `PhysType.convertTo` and this type mirrors `PhysType` member for member. `ConvertToAsync` is added
  beside it instead — an additive divergence, the same kind the type already carries with `RelRowType`
  and `Format`. Callers: `ClrEnumerableAggregate` and `ClrEnumerableCollect`.
- `ClrEnumUtils` — grepped: no `IEnumerable` anywhere in it. It builds selectors, predicates and
  comparators, all per-row.
- `ClrEnumerableTraitsUtils` — pure trait arithmetic; constructs no node.
- `LixToClrTranslator`, `ClrTypes`, `ClrPrimitive`, `AnonymousClasses`, `SyntheticRecord`,
  `SyntheticRecordEmitter`, and all of `Linq4j/Function`.
- All of `Interop`: `JavaValues`, `JavaSequences`, `JavaComparisons`, `JavaLists`,
  `JavaEqualityComparer`, `JavaWrapped`. `JavaSequences` gains **no** async member — that is the
  converter, under another name.
- `ClrEnumerablePrefer` and `ClrEnumerablePrefers`. It is about row format, not about sequences, and
  a second copy of the same five values would be noise.
- The whole prepare pipeline: `ClrPrepare`, `ClrPreparingStmt`, `ClrPrepareResult`, `ClrPrepareImpl`,
  `ClrSignature`, `PrepareContext`, `StatementDataContext`. `ClrPreparingStmt`'s own comment already
  scopes this — "three things are still a convention's own and nothing else is: the convention a plan
  must end in, the program that gets it there, and the compiler at the end."

**Copied**, into `Adapter/AsyncEnumerable/`, namespace
`Apache.Calcite.Extensions.Adapter.AsyncEnumerable`, each named `ClrAsyncEnumerable*`:

| | count |
|---|---|
| infrastructure — convention, rel, result, implementor, interpretable, bindable, defaults, built-in method table, programs, rules, rel factories | 11 |
| nodes | 30 |
| rule files | ~24 |
| prepare — preparing stmt, prepare result | 2 |

Two of them are large: `ClrAsyncEnumerableDefaults` (~2263 lines) and `ClrAsyncEnumerableWindow`
(~1026). The rest are small deltas.

### How small the deltas are, and why the nodes are still duplicated

Measured over the 30 node files:

| | lines | `ClrBuiltInMethod` refs | `IEnumerable` refs |
|---|---|---|---|
| `ClrEnumerableWindow` | 1026 | 1 | 0 |
| `ClrEnumerableMergeJoin` | 504 | 1 | 0 |
| `ClrEnumerableAggregateBase` | 435 | 0 | 0 |
| every node | — | 1–5, median 1 | **0** |
| `ClrEnumerableDefaults` | 2263 | 0 | **82** |

`IEnumerable` appears in no node file at all. A node never names the sequence type: it comes out of
`physType.RowType` through a generic method. So a node's whole convention-specific surface is one to
five identifiers, and in the two largest it is one.

That is an argument for sharing the bodies, and it is a real one — every future fix to `Window` will
have to land twice, which is the failure mode that once left three nodes wrong under a check that
boxed. It is not taken, because the machinery to share them — a common implementor base, a sequence
abstraction, `VisitChild` and `Result` made generic — has to be threaded through all 45 bodies to
parameterise a median of one identifier, and building it means rewriting a working convention against
a suite that covers one side only, for a consumer that does not exist yet.

**Duplicate now, factor later if the drift is real.** The asymmetry is reversibility:
duplicate-then-factor is held by two suites and any divergence shows the same day;
factor-then-duplicate is held by nothing. Note also where the 82 references are — `Defaults` is the one
file that genuinely cannot be shared, and sharing the nodes would not have touched it.

**Kept deliberately distinct rather than shared:**

- `ClrAsyncEnumerableResult`, separate from `ClrEnumerableResult` despite being structurally
  identical. A shared result type would let a synchronous expression pass through an asynchronous
  implementor and compile. The type *is* the check.
- `ClrAsyncEnumerableRelImplementor.RequireRowType` requires `IAsyncEnumerable<>` specifically, with
  the same refusal-rather-than-repair discipline that caught three wrong nodes in the sync convention.

## 5. Prepare and the ADO.NET surface

`IClrBindable` gains a sibling and a shared base:

```csharp
public interface IClrBindableBase { java.lang.reflect.Type ElementType { get; } }
public interface IClrBindable : IClrBindableBase { IEnumerable<object> Bind(DataContext root); }
public interface IClrAsyncBindable : IClrBindableBase { IAsyncEnumerable<object> Bind(DataContext root); }
```

`ClrSignature.Bindable` becomes `IClrBindableBase?`; `Bind` and a new `BindAsync` each refuse the wrong
kind with a message that names which convention prepared the statement. `ClrPrepareImpl.Describe` needs
no change — it reads only `ElementType`.

`ClrPrepareImpl` gets an asynchronous entry point rather than a copy. `CreatePlanner` registers
`ClrAsyncEnumerableRules` instead of `ClrEnumerableRules`, and `GetPreparingStmt` returns a
`ClrAsyncEnumerablePreparingStmt`; everything else — parsing, DDL, statement kind, parameter and column
metadata, the cursor factory — is already convention-neutral and the class says so.

In `Apache.Calcite.Data`:

- `CalciteResult` holds an `IAsyncEnumerator<object>?` alternative to its `IEnumerator<object>?`,
  gains a real `ReadAsync`, and gains `DisposeAsync`.
- **Which convention to prepare into is the caller's to say, not the session's.** This was got wrong
  first and the ADO.NET tests broke on it at once: `ExecuteReader` and `ExecuteReaderAsync` arrive at the
  *same* session method, because the synchronous one blocks on the asynchronous one, so deciding inside
  the session handed a synchronous caller a reader whose `Read` throws. `CalciteCommand` passes a flag.
- `ExecuteDbDataReaderAsync` asks for asynchronous and falls back **on `CannotPlanException` only** —
  the one signal that means the query cannot be covered, as opposed to failing. That is not
  sync-over-async: reading a synchronous plan completes synchronously and blocks nothing. What the
  fallback must never do is wrap the synchronous plan in an `IAsyncEnumerable`.
- **A token reaches the leaf only if it was given to `ExecuteReaderAsync`.** An `IAsyncEnumerable` takes
  its token at `GetAsyncEnumerator`, which happens once when the reader is made; `DbDataReader.ReadAsync`
  offers one per call and there is nowhere to put a later one. A token passed only to `ReadAsync` stops
  the reader between rows but cannot interrupt a table already waiting on I/O. Nothing can be done about
  that without giving every operator a token the plan threads, which is the design this deliberately does
  not have.
- **A table that is only an `IClrAsyncScannableTable` cannot be read synchronously at all.** Neither this
  project's synchronous convention nor Calcite's own has a scan for it, and no converter can carry its
  rows, so `ExecuteReader` fails to plan. That is the premise arriving at the surface rather than a gap: a
  schema may hold both kinds of table, but an individual query is one or the other.
- The fallback is **observable** — a property on the reader, or a hook a test can assert on. A silent
  fallback is how "it is asynchronous now" stops being true without anyone noticing.
- `CalciteDataReader.Read()` over an asynchronous result **throws** rather than blocking. That is
  precisely the sync-over-async the rule forbids, and it is the one place a caller could stumble into
  it by accident.
- `ExecuteNonQueryAsync` is unchanged: there is no asynchronous DML.

## 6. Tests

**The check that matters is `ClrAsyncEnumerableDifferentialTests`**, and it compares against
`ClrEnumerableConvention` rather than against Calcite's. The sync convention is already checked against
Calcite query by query, so transitivity gives the same oracle, and the asynchronous side needs its own
tables regardless — asking Calcite directly would mean maintaining two unrelated schemas and comparing
across three.

- The schema is an async mirror: `AsyncSalesTable`, `AsyncSortedTable` and the rest implementing
  `IClrAsyncScannableTable` over the **same** `object[][]` arrays the sync tables use. Lift those
  arrays into one holder so the two sides cannot drift; two copies of the fixture is a differential
  test that compares nothing.
- Every query in `ClrEnumerableDifferentialTests` that the async convention can plan gets copied over.
- **At least one table must actually suspend** — `await Task.Yield()` per row, or a `Channel`. A
  fixture whose `IAsyncEnumerable` never suspends exercises none of the resumption, ordering or
  disposal behaviour that distinguishes this convention from the other one, and every test would pass
  over a sequence that is asynchronous in name only.
- A test that a query touching a synchronous-only table **fails to plan** rather than converting.
- A one-column query of a primitive type — `SELECT id FROM …` — as its own named test rather than left
  to the general sweep. It is the case §1 measures: the tree builds either way, and if `BoxScalars` is
  missing or wrong the failure is an `InvalidCastException` at the root with nothing pointing at the
  cause. Assert the boxed value is `java.lang.Integer`, not `System.Int32`, or the test passes on the
  wrong representation.
- Cancellation: cancel mid-enumeration and assert both the `OperationCanceledException` and that the
  leaf observed it — once per buffering node, since those are where a token gets dropped.
- Disposal: partially enumerate under `await using` and assert the leaf was disposed. Nested
  `await foreach` disposal is the classic leak in a hand-written async iterator, and `MergeJoin` holds
  two enumerators by hand.
- `ClrAsyncEnumerableAdoNetTests` mirroring `ClrEnumerableAdoNetTests`, plus the `Read()`-throws case.

## 7. Order of work

1. `IClrAsyncScannableTable`, the convention, `ClrAsyncEnumerableRel`, `Result`, the implementor,
   `IClrAsyncBindable`, `ClrAsyncBindable`, `ClrAsyncEnumerableInterpretable`.
2. `ClrAsyncEnumerableDefaults` and `ClrAsyncBuiltInMethod` — the whole operator set. Everything
   depends on it and it is the largest single piece.
3. Scan, Values, Calc, Project, Filter and their rules — enough for `SELECT … WHERE`. **Stand the
   differential harness up here**, before the other forty nodes are written blind.
4. Sort, Limit, LimitSort; the set operations; Aggregate and SortedAggregate.
5. The joins — hash, nested loop, merge, asof, batch, and the mark variants — then Correlate,
   ConditionalCorrelate, Combine.
6. Window, MergeUnion, Collect, Uncollect, TableSpool and RepeatUnion (rules left out of the list).
7. Programs, the rules list, the preparing statement, `ClrPrepareImpl`'s async entry.
8. `Apache.Calcite.Data`: bindable plumbing, `CalciteResult`, `ExecuteReaderAsync`, the reader.
9. `CLAUDE.md`, `Apache.Calcite.Data/DESIGN.md`, and the packaged READMEs.

## 8. Not proven

- Whether `ClrEnumerableWindow`'s `Hoist` and the `AggImplementor` / `WinAggImplementor` blocks port
  unchanged. They take a `BlockBuilder` and write per-row code, so they should — but `Window` is where
  every wrong assumption in this port has landed so far, and it translates more linq4j than any other
  node.
- Whether `MergeJoin` and `AsofJoin` port cleanly. They are the two written with explicit enumerator
  control and local functions rather than as straight iterators; the local functions become
  `async ValueTask<bool>` and `yield return` is not available inside them. `MergeJoin` is 300 lines of
  this.
- Whether `Aggregate` — which returns a `TResult`, not a sequence — needs anything beyond an awaited
  fold.
- The count of rule files above is from the file listing, not from a rule-by-rule audit. `SetOpRules`,
  `CollectRules` and `RecursionRules` each hold more than one rule.
