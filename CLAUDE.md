# Apache.Calcite for .NET

Apache Calcite under IKVM, with an ADO.NET adapter, an ADO.NET client surface, and `Apache.Calcite.Linq`
— calling conventions that execute a plan as `System.Linq.Expressions` instead of Janino.

## Projects

| project | job |
|---|---|
| `Apache.Calcite.Adapter.AdoNet` | pushes a plan down to an ADO.NET provider |
| `Apache.Calcite.Data` | the `DbConnection` / `DbCommand` surface |
| `Apache.Calcite.Extensions` | IKVM interop helpers |
| `Apache.Calcite.Linq` | `ClrEnumerableConvention`; the async one is not written yet |

`TODO.md` has the outstanding work, sized and reasoned. Read it before planning anything.

## Building

- Build the **solution**: `dotnet build Apache.Calcite.sln`. A bare `dotnet build` fails — more than one
  project in the root.
- **The check that matters is `ClrEnumerableDifferentialTests`.** It runs the same SQL through this
  convention and through `EnumerableConvention` and requires the same rows. Every defect worth having
  found in the Linq project was found by it, three of them in nodes already believed done. Add a query
  there rather than writing an assertion by hand: the expected answer is whatever Calcite says.
- **Calcite is checked out at `D:\calcite` — but it is 1.42.0-SNAPSHOT and the projects reference
  1.41.0.** Read that source, then check the member exists. `PhysType.generateNullAwareAccessor`,
  `JoinInfo.nullExclusionFlags` and `org.apache.calcite.rel.core.Asof` are 1.42 only, and
  `EnumerableTableModify` is implemented there with private helpers that are part of an UPDATE fix 1.41
  does not have.
- `global.json` has `rollForward: latestMajor`, so builds pick the **.NET 11 preview SDK**, not 10.0.302.
  That SDK's `dotnet sln add` also rewrites every project with x64/x86 configurations — edit the solution
  by hand.

## Apache.Calcite.Linq: the rules that hold

**Where linq4j may appear.** A node holds linq4j only where a generator of Calcite's produced one or takes
one, and it is translated where it is produced rather than composed into a larger tree first. That is four
things: Rex (`translateCondition`, `translateProjects`, `translateLiteral`, everything `RexImpTable`
reaches); `PhysType`'s expression members and `JavaRowFormat.field`; a table's own
`getExpression(Queryable.class)`, which the schema SPI defines as linq4j; and the block Calcite's
implementor produces for an `EnumerableConvention` sub-plan at a converter. Everything else is
`System.Linq.Expressions` directly. **`ClrEnumerableWindow` violates this today — see `TODO.md`.**

**`JavaCast` is for what Java the language converts and an expression tree will not** — boxing, unboxing,
numeric promotion, `byte` sign extension. It is not a way to make one type into another where they ought
already to agree; converting a value that already has the type wanted only absorbs the case where it does
not. Measured across every plan the tests run, what is actually asked for is four reference conversions
and one boxing.

**A linq4j call's recorded `Method` is advisory.** Janino writes the tree out as source and the *Java
compiler* resolves both the overload and the receiver from that text. `Linq4j.asEnumerable` is named
against the array overload and passed a list; `size()` on a `SortedMultiMap` is named on `Collection`.
`MethodResolver.Rebind` and `RebindReceiver` fix both, and refuse to guess where an argument is statically
`object`, because then every overload fits.

**A lambda linq4j declared against one of its functional interfaces is one**, so `SamAdapters` wraps it
where it is built; an operator of this convention takes the delegate and asks for it back through
`TranslateSelector`. Deciding it at the call site instead breaks sixteen tests.

**An adapter converts values in both directions.** `JavaValues.As` / `From`. Taking a `java.lang.Integer`
and casting it to a CLR `int`, or handing an `int` back boxed the CLR way, leaves two representations of
one value in a plan and Calcite's own comparators fail on them. This is the invariant the whole port
exists to keep, and it leaked through the adapters once already.

**`Rules()` and `CalcRules()` are two passes, not one.** `VolcanoCost.isLt` compares the row count and
nothing else — cpu and io are dead code behind `if (true)` — so a project and a calc are never cheaper
than one another and the planner keeps whichever it saw first. `Programs.standard` runs the calc rules
afterwards as a hep pass. A caller must do the same, must run `Programs.subQuery` before the planner, and
— to reach `ClrEnumerableCorrelate` at all — must **not** decorrelate.

**A join boxes its rows.** Calcite builds the selector and predicate against boxed rows because linq4j's
`Function2` and `Predicate2` erase to `Object`, and because an outer join compares a row to null. A
delegate is typed where those interfaces were not.

**A block consumed apart from what reads it needs a non-optimising `BlockBuilder`.** An optimising one
inlines a declaration used once, leaving a reference already built into a translated sub-plan pointing at
a variable that no longer exists. `ClrEnumerableCorrelate` needs this.

**Calcite keeps a lot of what a port needs package private** — `EnumUtils.joinSelector`,
`generatePredicate`, `fieldTypes`, `fieldRowTypes`, `javaClass`, `EnumerableAggregateBase`'s four helpers,
`PhysTypeImpl.of(typeFactory, javaRowType)`, `EnumerableWindow`'s constructor. Expect to port rather than
reuse, or to find a public route: a `ConverterRule`'s `convert` is public even when its node's constructor
is not.

## Traps

- **Java enum ordinals are not stable across versions; names are.** Dispatch on `switch (x.name())` with
  `nameof(...)` labels. Never `ordinal()`, never IKVM's `__Enum` shadow.
- **`(java.lang.Class)typeof(X)`, never `(java.lang.reflect.Type)typeof(X)`.**
- **`java.lang.Object` resolves to an IKVM stub that is not `System.Object`** and is not assignable from a
  string — but every signature IKVM compiles uses `System.Object`. `TypeResolver` special-cases it.
- **Java's `byte` is IKVM's `byte`, which is unsigned.** Widening one has to go by way of an `sbyte`.
- **Refuse in `matches`, never in `Implement`.** `Implement` runs after `findBestExp`.
- **`Expression.Convert` on `object` emits `unbox.any`**, which demands an exact runtime type.
- **A one-column result is the value, not a one-element row.**
- Multi-file edits: **write the script to the scratchpad with `Write`, then run it.** Bash heredocs break
  on apostrophes, repeatedly.

## Working with this user

Comprehension questions ("what is X for?", "why would we do Y?") are **defect reports**. Go read the code,
and Calcite's, and expect to find something wrong. In this session that pattern caught, in order: a
gratuitous divergence in `Calc`, a cost analysis that was simply false, calling a spool a write, calling a
deliberate Calcite refusal a blocker to work around, and `Window` translating far more linq4j than the rule
allows. Every one was a case of reasoning past what had been verified, and usually toward the cheaper
conclusion.

Do not report scope reductions as findings. Do not turn remaining work into questions. Say plainly what is
done, what is not, and what is unproven.
