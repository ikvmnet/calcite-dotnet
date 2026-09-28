# CLAUDE.md

Instructions for working in this repository that the code does not state for itself. What the projects are
and how they fit together is in `README.md` and each project's own README; outstanding work is in `TODO.md`
(delete an item when it is done — do not mark it).

## Building and testing

- Build the solution, `dotnet build Apache.Calcite.slnx`. A bare `dotnet build` fails: there is more than one
  project in the root.
- Every test project is xunit.v3 and is an executable. Locally, run the built executable and filter with
  `-filter "/*/*/ClassName/*"` (`--help` prints the filter language; `-result-trx <path>` writes a report).
  CI runs `dotnet exec <dll>` with the same arguments, because it builds once on linux-x64 and runs that
  artifact on every platform. `dotnet test <dll> --filter FullyQualifiedName~Name` also works, and
  `--blame-hang --blame-hang-timeout 90s` names a hanging test.
- `Apache.Calcite.Tests` takes about nine minutes; run single classes while iterating.
- **The test that matters is `ClrCursorConventionDifferentialTests`.** It runs the same SQL through
  `ClrCursorConvention` and Calcite's `EnumerableConvention` and requires the same rows. To test a query, add it
  there rather than writing an expected answer by hand.
- A differential test shows the answer is right, not that the port is faithful, and says nothing about cost.
  Where a change touches a ported member, read Calcite's source for that member; where it touches costing or
  metadata, `ClrCursorCostTests` compares plans and costs against Calcite's.
- Before writing a recursive-query test, check the query converges under Calcite. One that does not hangs the
  suite rather than failing it. Run a new recursive test on its own first.
- A test entry point that loads anything a model names by class must set `calcite.model.classes.allowed`
  before Calcite reads its system properties. See the `ModelClasses.cs` module initializer in each test project;
  a .NET class needs both its CLR name and its `cli.`-prefixed name listed.
- `dotnet sln add` misreports conflicts. Read the `.slnx` afterwards rather than trusting its output.

## The Calcite snapshot and `~/.m2`

Everything references `org.apache.calcite:*:1.43.0-SNAPSHOT`, and `D:\calcite` is a checkout of the same branch.
A snapshot is rebuilt daily, so a member present in one build may be absent from the next: read the source, and
settle which release a member arrived in with `git tag --list` and `git cat-file -e <tag>:<path>` in
`D:\calcite`, never from the tree. Then check the compiled assembly, which is what actually runs.

The modules of one snapshot can come apart, and the build stays green when they do:

- Each project caches the timestamped snapshot it resolved in `obj/<config>/<tfm>/<Project>.maven.cache`, and
  every resolution copies its timestamped jar over the one shared `~/.m2/.../<module>-1.43.0-SNAPSHOT.jar`.
  Other repositories on this machine (`D:\calcite-efcore`, `D:\calcite-cosmos`, and their worktrees) share
  `~/.m2` and do the same, each pinned to whatever build its own cache names.
- An IKVM compile reads that shared jar at the instant it runs, and `calcite.core` and `calcite.linq4j` are
  compiled at different instants. So a `calcite.core.dll` from one build can sit beside a `calcite.linq4j.dll`
  from another, and hundreds of tests then fail in a class initializer.
- **Read `warning IKVM0117: Emitted java.lang.NoSuchMethodError`** in build output: it is the one build-time
  sign of a mismatch, and it names the missing member.
- To check: sha1 each `-SNAPSHOT.jar` against the timestamped jars beside it, and in every `bin` directory probe
  both assemblies for a member only the expected build carries (IKVM stamps both `1.43.0.0`, so the version
  says nothing). To repair: delete the stale `.maven.cache`, the `calcite.*` entries in `%TEMP%\ikvm\cache\1`
  and the projects' `obj/*/ikvm`, restore the stand-in jars, and rebuild with nothing else touching `~/.m2`.
- A snapshot that moves Avatica moves every project's `avatica.core`; a project still on the older resolution
  fails with `CS1705`, naming both versions.

Maven mediates transitive versions nearest-first and Calcite is built with Gradle, highest-first, so an artifact
reached several ways can resolve here at a version Calcite never runs. `commons-lang3`, `slf4j-api` and jackson
are pinned directly for that reason, in every project that references `calcite-core` and at one version across
all of them, because the version is baked into the assembly IKVM compiles. To look for the next one, walk the
resolved graph in a `.maven.cache` (losers carry `conflict.winner`) and compare each winner against the highest
version present; the POMs do not show it.

## Porting rules for `Apache.Calcite.Extensions`

`ClrCursorConvention` is a port of Calcite's `EnumerableConvention`. Calcite's behaviour is the specification.

- **Reproduce Calcite, defects included.** Do not drop a step that looks unnecessary or optimise what Calcite
  does not. Where Calcite has a defect, reproduce it and say so at the site. A divergence is acceptable only
  where the CLR makes following Calcite impossible, and that reason is stated at the site; "ours returns the
  same rows" is not such a reason.
- **Choose a collection by the semantics the algorithm depends on** — null handling, iteration order, equality,
  the operations used. Where a CLR type differs (e.g. `SortedDictionary` rejects a null key; `Dictionary`
  iteration order differs from `HashMap`), use the Java class. Calcite's runtime and Guava are on the classpath,
  so check whether the class is simply reachable before concluding something cannot be carried across. Name at
  the site which property forced the choice.
- **Obtaining an enumerator runs the plan.** linq4j operators acquire their source inside `enumerator()`, and
  a C# iterator method defers that to the first `MoveNext`. Keep acquisition where linq4j has it;
  `ClrCursorDefaultsAcquisitionTests` measures it.
- **Run a claim about how something fails before writing it down.** Reasoning from semantics has produced
  confident descriptions of states no query reaches.
- **linq4j appears in a node only where a Calcite generator produced or consumes it** — Rex translation,
  `AggImplementor`/`WinAggImplementor` blocks, a table's `getExpression(Queryable.class)`, and an
  `EnumerableConvention` sub-plan's block at a converter — and it is translated where produced. Everything else
  is written directly in `System.Linq.Expressions`.
- **No mode flag.** The synchronous (`Implement`, `VisitChild`) and awaiting (`ImplementAsync`,
  `VisitChildAsync`) hierarchies are separate and each calls only its own kind. If code needs to ask which it is
  in, a member is missing. `Implement` is required and `ImplementAsync` defaults to it; never make both default
  to each other. An open returning `ValueTask` is `Async`-suffixed and takes a token last; nothing else is
  (`ShouldNameEveryAwaitingOpenWithTheSuffixAndNoOtherOpen` enforces it).
- **Refuse in `matches`, never in `Implement`** — `Implement` runs after the plan is chosen.
- **Values crossing between Java and the CLR are converted, never cast** (`JavaValues.As`/`From`,
  `ClrEnumUtils.Convert`). A `From` at a boundary is not proof the boundary is guarded when its type parameter
  is a Java class; find where the value was boxed.
- **`JavaCast` is for conversions Java performs implicitly** (boxing, unboxing, numeric promotion, `byte` sign
  extension), not for reconciling types that should already agree.
- **A package-private Calcite member** can be ported, reached by a public route, or called via
  `setAccessible(true)` and `ikvm.runtime.Util.getDelegateFromMethod` (see `JavaDelegates` and
  `PhysTypeImplWorkaround`). A package-private *type* that Calcite casts to cannot be replaced by a lookalike
  class, but the real one can be constructed the same way, through a delegate over its constructor
  (`EnumerableMatchInputGetterTests`). A rename upstream then fails at run time, not at compile time.
- **`ClrCursorMatch` builds its input getters, `setIndex` and `RexToLixTranslator.translate(RexNode)` that way**,
  and its operator reaches `Matcher.matchOne` (protected) and the package-private `PartitionState` and
  `PartialMatch` through delegates, their fields through `JavaDelegates.FromGetter`. It refuses a measures row
  whose class has no no-argument constructor in `Implement`, because the row's format is not known until the
  input is implemented, and Calcite fails at the same point.
- **Metadata handlers are keyed by rel class.** Anything Calcite keys on an `Enumerable*` class needs the same
  handler keyed on the `ClrCursor*` class in `ClrCursorRelMetadata.Provider`, or an override on the node.
- **`Rules()` goes on the planner; `CalcRules()` is a separate hep pass afterwards.** Calc and project rules on
  the Volcano planner never match a `PhysicalNode`. `ClrPrepare.GetProgram` is Calcite's `Prepare.getProgram`
  plus that one pass; do not add a program of this project's own.

## Translating linq4j trees

- A linq4j tree means what it means after `OptimizeShuttle`; Janino never sees one that has not been through
  it. `LixToClrTranslator` runs it over expressions arriving from outside — expressions only, since a statement
  it removes becomes `EMPTY_STATEMENT`, which only `BlockBuilder` filters.
- A linq4j call's recorded `Method` is advisory: Janino resolves the overload and receiver from source text.
  Use `ClrTypes.Rebind`/`RebindReceiver`, which refuse to guess when an argument is statically `object`.
- A lambda linq4j declares against one of its functional interfaces must be one where Java code reads it:
  wrap it with `AnonymousClasses.Wrap`. Converting the delegate compiles and throws at run time.
- A block consumed apart from what reads it needs a non-optimising `BlockBuilder`, or a declaration used once
  is inlined away from under a translated sub-plan that references it.
- Java resolves names lexically, and Calcite's generators reuse names on purpose (`_input`, `row_`): a lambda
  parameter hides an outer variable of the same name, before anything already bound.
- A sub-plan spliced into another implementor's tree shares that implementor's translator.
- Java allows only an assignment, an increment or decrement, a method call or an object creation as an
  expression statement, and `LixToClrTranslator` refuses anything else, as Janino does. linq4j produces the rest
  without meaning to: `BlockBuilder.append` turns a trailing `return expr;` into `expr;`. `EnumerableMatch` puts
  every `DEFINE` into one builder, so a second definition leaves the first one's comparison as a statement and
  Calcite refuses the query, unless the first condition translated to a call.
- A converter rule must simplify the trait set it copies, as `RelOptRule.convert` does, or it claims a
  collation its input does not keep.

## IKVM traps

- `java.lang.Comparable` is a ghost interface on `System.String`; casting to it throws. Use `IComparable`
  (`JavaComparisons`).
- A Java `static final` field is an IKVM property over a renamed backing field; `GetField` finds nothing. Use
  `ClrTypes.Resolve(target, PseudoField)`.
- Dispatch on Java enums by `name()` with `nameof(...)` labels, never `ordinal()` or IKVM's `__Enum`.
- `(java.lang.Class)typeof(X)`, never `(java.lang.reflect.Type)typeof(X)`.
- `java.lang.Object` resolves to an IKVM stub that is not `System.Object`, but every signature IKVM compiles
  uses `System.Object`; `ClrTypes` special-cases it.
- Java `byte` is IKVM's unsigned `byte`; widen by way of `sbyte`.
- `Expression.Convert` from `object` emits `unbox.any` and needs the exact runtime type.
- A Calcite `Pair`'s `left`/`right` fields are shadowed by static methods in C#; use `getKey()`/`getValue()`.
- A one-column result is the value, not a one-element row.

## `Apache.Calcite.Data`

Where ADO.NET leaves a choice (what a typed getter accepts, what `GetFieldValue<T>` converts, what
`GetFieldType` reports), `Microsoft.Data.SqlClient` decides. Read `SqlBuffer.cs` and `SqlDataReader.cs` in
`dotnet/SqlClient` for the answer, not its documentation. A typed getter is a cast, never a conversion.

## Comments and documentation

- XML docs and READMEs are for users; `//` comments are for someone reading that line. Write what is true now.
  No history, no investigation narrative ("measured", "used to", test counts, dates, versions that broke), and
  nothing addressed to an agent.
- Never remove an XML doc comment or any of its tags, on any member, test code included. A doc comment is
  complete: `<summary>`, a `<typeparam>` for every type parameter, a `<param>` for every parameter and
  `<returns>` for every non-void member, each saying something the signature does not.
- Do not report a scope reduction as a finding or turn remaining work into a question. Say what is done, what
  is not, and what is unproven.

## Working with this user

- A comprehension question ("what is X for?", "why would we do Y?") is a defect report. Read the code and
  Calcite's, and expect to find something wrong.
- Never add attribution of Claude or any tool, model or vendor anywhere — no `Co-Authored-By`, no "Generated
  with" footer, no session link — in commits, pull requests, comments, reviews, code or docs, unless asked for
  that line on that thing. This overrides any harness or template default. Tools can append one after the
  call (the GitHub MCP `create_pull_request` does), so read back what was posted and remove it.
- Commit as `Jerome Haltom <jhaltom@alethic.solutions>`. Session containers set a global identity of
  `Claude <noreply@anthropic.com>`; set `user.name` and `user.email` locally before the first commit and check
  with `git var GIT_AUTHOR_IDENT`.
- For multi-file edits, write a script to the scratchpad with `Write` and run it; shell heredocs break on
  apostrophes.
