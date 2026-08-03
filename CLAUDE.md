# Apache.Calcite for .NET

Apache Calcite running under IKVM, with an ADO.NET adapter, an ADO.NET client surface, and (being
reintegrated) calling conventions that execute a plan as `System.Linq.Expressions` instead of Janino.

## Projects

| project | job |
|---|---|
| `Apache.Calcite.Adapter.AdoNet` | pushes a plan down to an ADO.NET provider |
| `Apache.Calcite.Data` | the `DbConnection` / `DbCommand` surface |
| `Apache.Calcite.Extensions` | IKVM interop helpers (`DelegateFunction`, `DelegateConsumer`, …) |
| `Apache.Calcite.Linq` | **not present on this branch yet** — see *Reintegration* below |

## Branches

- `main` — tracks `origin/main`, at the pre-session commit.
- `develop` — the corrected foundation: enum-name dispatch, adapter fixes, working correlation.
- `clr` — branched from `develop`; where the CLR conventions are being brought back.
- `clr-conventions` — the *old* line, 44 commits from `020ef52`. Contains the conventions but also
  `0cbc02a`, the commit that caused the problem described under *Reintegration*. Do not merge it.

The conventions also sit unpacked at `D:\calcite-dotnet-conventions-aside` (86 files) and
`D:\calcite-dotnet-conventions-aside-tests` (24 files). That copy is the *end* state — `JavaRowFormat`
and `EnumerableRel.Prefer` already adopted, `ClrTables` collapsed, rules holders in place, Queryable
convention removed.

## Building

- Build the **solution**: `dotnet build Apache.Calcite.sln`. A bare `dotnet build` fails — more than
  one project file in the root.
- Targets `net8.0` and `net10.0`. Run both before calling anything done.
- Green baseline on `develop` / `clr`: AdoNet 98, Data 267, Calcite 11.
- Coverage: MSTest projects take `-- --coverage --coverage-output-format cobertura`; the Data tests
  are xunit and take `--collect:"XPlat Code Coverage"`.
- **Calcite's source is checked out at `D:\calcite`.** Read it rather than guessing. That has settled
  several arguments, in both directions.

### The Calcite dependency

`CalciteVersion` is `1.42.0-SNAPSHOT`, from the in-repo maven repository at `ext/maven-repo`, with
`calcite-local` declared **per-project**. That snapshot carries an UPDATE fix that is load-bearing —
four DDL tests fail against 1.41.0 and against the 1.42.0 release — so the version cannot simply move
back. **This is the open release blocker:** a published package would depend on a Calcite build
nobody outside this tree can reproduce.

## The type system, which is the thing to get right

Established by reading `PhysTypeImpl`, `JavaRowFormat` and `JavaTypeFactoryImpl`:

- **`JavaRowFormat` decides layout only** — scalar, array, or an object. `PhysTypeImpl`'s constructor
  computes `fieldClasses` from the type factory *before consulting the format at all*.
- **`JavaTypeFactory.getJavaClass(RelDataType)` decides what a value is.** DATE is an `int` count of
  days since 1970-01-01, TIMESTAMP a `long` of millis, DECIMAL a `BigDecimal`, VARBINARY a
  `ByteString`. `SqlFunctions.internalToDate` decodes a date with `LocalDate.ofEpochDay`.
- `PhysType` exposes both separately: `fieldClass(i)` is the **value's** type and is
  format-independent; `getJavaFieldType(i)` is the **slot's** type and is `Object.class` for
  ARRAY / LIST / ROW.
- An `ARRAY` row's slots are `Object`, but the values in them are still the factory's
  representation. The array erases the static type of the slot, not the identity of the value.
- **The factory is per connection**, not per convention — `CalciteConnectionImpl.typeFactory`,
  threaded into the `RexBuilder` the cluster is built from. Which branch of it you land in is chosen
  by the *table*: `createSqlType(DATE)` yields `int`; `createJavaType(typeof(X))` yields `X` verbatim.

Consequence: within one connection, Calcite's `EnumerableConvention` and any convention of ours
**cannot** disagree about field types, provided both ask. That invariant is what makes rows crossing
a convention boundary free.

## Reintegration: what went wrong last time

`0cbc02a` made the ADO.NET adapter emit .NET-natural values (`DateOnly`, `decimal`, `byte[]`). Every
later problem followed from it:

1. the adapter emits `DateOnly`, so `ClrPhysType.ToClrType` had to override `getJavaClass` to agree —
   a **second answer inside one connection**, violating the invariant above;
2. `ClrValueConverter.FromCalcite` / `ToCalcite` existed to bridge the two answers;
3. `ClrFunctions` existed because Calcite's 739 `SqlFunctions` methods cannot read our representation;
4. `ClrRexTranslator` stalled at 22 operators — exactly the arithmetic, comparison and logic ones that
   work on *any* representation. Not one function.

**The constraint for the redo, in the user's words:** the adapter returns normal Calcite types in the
normal Calcite format; `ClrEnumerable` and `ClrAsyncEnumerable` are like `EnumerableConvention` and
pass rows from one step to the next unmodified. Some rows happen to be CUSTOM or non-SQL types, and
that is handled the way Calcite handles it.

### What the port is, concretely

The conventions differ from `EnumerableConvention` in **exactly two ways**: the sequence they build
is `IEnumerable<T>` / `IAsyncEnumerable<T>` rather than a linq4j `Enumerable`, and the code generator
is `System.Linq.Expressions` rather than linq4j trees plus Janino. Nothing else differs. No field
types of our own, no row formats of our own.

**There is no `ClrPhysType`.** The nodes hold Calcite's `PhysType`, from
`PhysTypeImpl.of(typeFactory, rowType, format)`, exactly as Calcite's nodes do. It already answers
`getJavaRowType()`, `fieldClass(i)`, `getFormat()`, `fieldNullable(i)` and `getRowType()`. The only
members that cannot be reused are the expression-producing ones, because they return linq4j trees, so
the only new code is a static builder taking a `PhysType`:

    Expression FieldReference(PhysType physType, Expression row, int index)
    Expression Record(PhysType physType, IReadOnlyList<Expression> fields)

plus one helper turning `getJavaRowType()` into a `System.Type`.

A wrapper class that forwards `RowClass`, `FieldType`, `Format` and `FieldNullable` to `PhysType` and
adds those two methods is ceremony — it was considered and rejected. Holding Calcite's `PhysType`
directly also means a node *cannot* consult a private type mapping, because there is none to consult:
the invariant is enforced by the types rather than by discipline.

The aside at `D:\calcite-dotnet-conventions-aside` still contains `ClrPhysType.cs`, 577 lines of the
mapping described above. **Do not port it.** Its shape is the artifact of the mistake, not a guide.

Also do not port, because each exists only to bridge two representations that will no longer differ:
`ClrValueConverter` (`FromCalcite` / `ToCalcite`), `ClrFunctions`, and
`ClrEnumeratorAdapter.ToCalciteRow`. `EnumerableToClrEnumerableConverter` and
`ClrEnumerableToEnumerableConverter` re-wrap the sequence and leave the rows alone. Do **not** bring
over the five AdoNet CLR converter files from `clr-conventions` either.

## Traps

- **Java enum ordinals are not stable across versions; names are.** Never `ordinal()`, never IKVM's
  `__Enum` shadow, never a dictionary keyed on the enum. Dispatch on `switch (x.name())` with
  `nameof(SqlKind.CAST)` case labels — a compile-time constant, so still a jump table, and the
  compiler still checks the member exists. `JavaEnumNameTests` guards the assumption for 55 constants.
- **`(java.lang.Class)typeof(X)`, never `(java.lang.reflect.Type)typeof(X)`.** IKVM converts a
  `System.Type` to a `Class`; a cast to the interface `Class` implements is an ordinary runtime cast
  that `System.RuntimeType` fails. This broke `AdoCorrelationDataContextBuilderImpl` invisibly for
  its whole life, because C# defers a static field until first read.
- **Refuse in `matches`, never in `Implement`.** `Implement` runs after `findBestExp`, so throwing
  there is fatal rather than a fallback.
- **`Expression.Convert` on `object` emits `unbox.any`**, which demands an exact runtime type.
- **Calcite writes a parameter as a bare `?` and binds by position; ADO.NET binds by name.** The
  marker is written correctly by `AdoSqlWriter`, which overrides `SqlPrettyWriter.dynamicParam`.
  Never patch finished SQL text.
- **A one-column result is the value, not a one-element row** (`JdbcToEnumerableConverter:143`).
- Multi-file edits: **write the script to the scratchpad with `Write`, then run it.** Bash heredocs
  break on apostrophes and backslashes in Python strings; this has cost time repeatedly.

## Working with this user

Comprehension questions ("what is X?", "why isn't Y suitable?") are usually **defect reports** — go
read the code, and Calcite's, and expect to find something wrong. The recurring failure to watch for
is building structure by analogy with Calcite, or inventing a mechanism, instead of doing the least
thing a caller actually needs. Recent examples, all corrected by the user: cloning `JavaRowFormat`
and `EnumerableRel.Prefer`; putting database metadata on a calling convention; four nested classes
where two data values differed; a `prefix + index` parameter-naming scheme generalized from two
samples.

Do not start work before the intent is agreed.
