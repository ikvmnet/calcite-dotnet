# Apache.Calcite for .NET

Apache Calcite under IKVM, with an ADO.NET adapter, an ADO.NET client surface, and
`Apache.Calcite.Extensions` — calling conventions that execute a plan as `System.Linq.Expressions`
instead of Janino, and the prepare pipeline that gets a statement to one.

## Projects

| project | job |
|---|---|
| `Apache.Calcite.Adapter.AdoNet` | pushes a plan down to an ADO.NET provider |
| `Apache.Calcite.Data` | the `DbConnection` / `DbCommand` surface |
| `Apache.Calcite.Extensions` | `ClrCursorConvention`, the prepare pipeline, and the IKVM interop helpers |
| `Apache.Calcite.Geography` | optional; the `CLR_ST_GEOG_*` operator table and a geodesic evaluator over Google's S2. There is no `GEOGRAPHY` type — a geography is Calcite's `GEOMETRY` and the operator's name is what says to read it geodesically, which is what lets these be declared on a schema. Nothing else references it, and it references nothing else here |

`TODO.md` has the outstanding work, sized and reasoned: the ADO.NET adapter's gaps and what more it
could push, a plan cache, the test suites not yet written, and the decisions not yet taken. It holds
nothing that is done — a finished item is deleted from it, not marked.

**Where ADO.NET leaves `Apache.Calcite.Data` a choice, `Microsoft.Data.SqlClient` settles it** — what a
typed getter accepts, what `GetFieldValue<T>` converts, what `GetFieldType` claims for a column whose
type is not known until a row is read — and the way to find the answer is to read `SqlBuffer.cs` and
`SqlDataReader.cs` in `dotnet/SqlClient`, not the documentation and not memory. A typed getter is a
*cast* and never a conversion, and `sql_variant` is `ANY`. `src/Apache.Calcite.Data/DESIGN.md`, *The
driver this one is modelled on*, has the reading. Not to be confused with
`Apache.Calcite.Adapter.AdoNet`'s SQL Server support, which is a back end a plan is pushed down to.

## Building

- Build the **solution**: `dotnet build Apache.Calcite.slnx`. A bare `dotnet build` fails — more than one
  project in the root.
- **The check that matters is `ClrCursorConventionDifferentialTests`, which reads every plan four ways.**
  It runs the same SQL through this convention and through `EnumerableConvention` and requires the same rows. Every defect worth having
  found in the convention was found by it, three of them in nodes already believed done. Add a query
  there rather than writing an assertion by hand: the expected answer is whatever Calcite says. It lives
  in `Apache.Calcite.Tests`, with the rest of the convention and prepare tests.
- **Every test project is `xunit.v3`**, and there is no second framework to remember. Four were on
  `MSTest.Sdk` and `Apache.Calcite.Data.Tests` was on xunit 2; a note here used to say `dotnet test` could
  not run the MSTest ones at all under the .NET 10 SDK, and that whole exception is gone. A v3 project is an
  **executable** — the package writes the entry point and the assembly runs itself on
  Microsoft.Testing.Platform — and it also carries `xunit.runner.visualstudio` and `Microsoft.NET.Test.Sdk`,
  so every way of running it works:
  - the built executable, which is what to reach for locally:
    `src\Apache.Calcite.Tests\bin\Debug\net8.0\Apache.Calcite.Tests.exe`, and `-filter` takes a query in
    `/assembly/namespace/class/method` form, wildcards included — `-filter "/*/*/ClrCursorSortTests/*"`.
    `--help` prints the whole filter language. `-result-trx <path>` writes the report, relative to where
    the runner was invoked; the tests run in the assembly's directory, which it changes to first.
  - `dotnet test <dll> --filter FullyQualifiedName~Name`, which is vstest.console.
    `--blame-hang --blame-hang-timeout 90s` names the test that hangs.
  - **`dotnet exec <dll>`, which is what CI runs**, and takes the same arguments the executable does. The
    other two launch the app host, and a build emits one, for its own RID. CI builds once on linux-x64 and
    runs that artifact on five platforms, so under `dotnet test` every job but that one failed before a
    test ran — `Exec format error`, or `Could not find app host executable` on Windows, where it is `.exe`.

  `Apache.Calcite.Tests` is about nine minutes end to end; a single test is seconds.
- **The four converted suites run one test at a time, and say so.** MSTest did that by default and xunit does
  not: it runs test collections against one another, and IKVM state is process wide — the boot class path,
  the Calcite system properties a module initializer sets, and every class initializer they feed.
  `AssemblyInfo.cs` in each carries `[assembly: Parallelization(Mode = ParallelMode.None)]` for that reason.
  `Apache.Calcite.Data.Tests` has no such line, deliberately: it was parallel under xunit 2 and still is.
- **Everything references 1.43.0-SNAPSHOT, and `D:\calcite` is that same branch.** The snapshot comes from
  `https://repository.apache.org/content/repositories/snapshots/`, named once in `Directory.Build.props`
  rather than per project. **1.43 is unreleased**: it was targeted for the end of August 2026 and slipped,
  Avatica 1.29.0 being the named blocker. Avatica 1.29.0 is on Maven Central now — first resolved here
  2026-09-22, and the 2026-09-25 snapshot depends on it — so the blocker is gone and the release is not yet
  out; on the recent cadence (1.38→1.42 ran 2 to 7 months apart) October to December 2026 is the honest
  range. **A snapshot that moves Avatica moves every project's `avatica.core` with it**, and a project still
  pinned to the older resolution fails to compile against one that moved — `CS1705`, naming both versions. Nothing of this is released either, which is
  why the move was affordable.

  **A snapshot is a moving target**, rebuilt daily, so a member that is there today may not be there in the
  build that ships. Pin nothing on memory: read the source, and expect the tree and the jar to differ. The
  two that have already bitten are in this file — `ClassNameFilter`'s allowlist, which appeared partway
  through the August snapshots, and `FLAT_PRODUCT`.

  **And the modules of one snapshot can come apart in `.m2`, which the build will not tell you.** Every
  project caches the timestamped snapshot it resolved in `obj\Debug\<tfm>\<Project>.maven.cache`, and
  Maven copies that timestamped jar over the one shared `calcite-core-1.43.0-SNAPSHOT.jar`. So two projects
  built on different days hold different builds, each rewrites the other's jar, and a copy that fails
  because the jar is in use leaves a `.jar.<digits>.tmp` behind and the module keeps whatever it had.
  Measured: a September `calcite-core` over an August `calcite-linq4j` threw
  `NoSuchMethodError: Types.isValidJavaIdentifier` — added upstream 2026-08-18, CALCITE-7726 — out of
  `JavaTypeFactoryImpl.createSyntheticType`, so **179 of 855 tests failed and the build was green**. The one
  sign at build time is `warning IKVM0117: Emitted java.lang.NoSuchMethodError in ...`, which names the
  method. **Read it.** The fix is to delete the stale `.maven.cache` and rebuild; check afterwards that
  every `-SNAPSHOT.jar` copy is the same timestamped build.

  **And the project rewriting it is usually in another repository.** `~/.m2` is shared, and
  `D:\calcite-efcore`, `D:\calcite-cosmos` and every `.claude/worktrees` copy of them resolve calcite too,
  each pinned by its own `.maven.cache` to whatever build it last saw. **A cache hit still materialises its
  artifact**, so deleting the older timestamped jars does not help: the other repository re-downloads the
  exact build its cache names and stamps it back onto the stand-in. Measured 2026-09-17 —
  `calcite-core-1.43.0-20260916.044432-244.jar` deleted, and back twenty minutes later out of a concurrent
  `Apache.Calcite.Cosmos.Adapter.Tests` build, with `calcite-core-1.43.0-SNAPSHOT.jar` pointing at it again
  while every project here had resolved 248. **It bites whenever an IKVM compile runs**, which is whenever
  that assembly is missing from `%TEMP%\ikvm\cache\1` — a deleted `.maven.cache`, a newly resolved version,
  a cleared cache. The compile reads the stand-in, so it takes whatever build was in the file at that
  instant, and **the two modules are compiled at different instants**: measured the same day, a
  `calcite.core.dll` from 248 beside a `calcite.linq4j.dll` from 253, which put `BuiltInMethod`'s class
  initializer on an `EnumerableDefaults.ieJoin` that was not there and failed **611 of 887 tests, 339 of 506
  and 47 of 200, with the build green**. Earlier the same morning the same race stopped compilation on
  `org.apache.calcite.util.UuidValue` not existing.
  **Check the pair, never one of them.** The jars: sha1 each `-SNAPSHOT.jar` against the timestamped jars
  beside it. The assemblies: probe for a member the build you expect is the first to carry — `deepEquals0`
  or `UuidValue` in `calcite.core.dll`, `ieJoin` in `calcite.linq4j.dll` — in **every** `bin` directory,
  because IKVM stamps both `1.43.0.0` and nothing in either names the snapshot. To redo it properly:
  restore the stand-ins, delete the `calcite.*` entries from the IKVM cache and the projects' `obj/*/ikvm`,
  and build with nothing else on the machine touching `~/.m2`.

  | arrived in | |
  |---|---|
  | 1.41 | `rel.core.AsofJoin`, `EnumerableAsofJoin`, `ENUMERABLE_ASOFJOIN_RULE` |
  | 1.42 | `EnumerableCombine`, `EnumerableConditionalCorrelate` and their rules, `EnumUtils.markJoinSelector` and the mark-join paths, `PhysType.generateNullAwareAccessor`, `JoinInfo.nullExclusionFlags` |
  | 1.43 | `org.apache.calcite.rel.core.Asof`, `FetchOffsetRoundingPolicy`, `RexImplementorTable(s)`, `EnumerableTableModify`'s five private helpers (CALCITE-7510), `TopDownGeneralDecorrelator`, and `case UUID` in `JavaTypeFactoryImpl.getJavaClass` |

  **Maven mediates by nearest and Gradle by highest, and Calcite is built with Gradle.** So an artifact
  the closure reaches more than one way can resolve here at a version Calcite never runs. It has bitten
  twice. `commons-lang3` arrives three ways at the same depth, uzaygezen-core 0.2 asking for 3.1,
  commons-text 1.11.0 for 3.13.0 and aggdesigner-algorithm 6.1 for 3.18.0, so Maven broke the tie on
  declaration order and took 3.1, from 2011; `StringEscapeUtils` then died in its own class initializer
  on `Range.of`, and `CONTAINS_SUBSTR` with it. And the json-path jackson edge declared in `Dependencies`
  on the calcite-core reference resolves at the **root** of the graph, ahead of calcite-core's own, so it
  sets jackson for the whole closure rather than for json-path: written at 2.18.6 when that was Calcite's,
  it held the closure there after CALCITE-7738 moved Calcite to 2.22.2 three weeks later, and said nothing.
  Both are pinned directly now, in every project that names calcite-core and at one version across all of
  them: the stub is baked in when IKVM compiles the jar, so two projects on different versions emit
  different assemblies.
  **The next one is found in the resolved graph, not in the poms.** Every project caches it as
  `obj\Debug\<tfm>\<Project>.maven.cache`, a JSON tree carrying `conflict.winner` on every loser: walk it
  and compare what Maven picked against the highest version present. `slf4j-api` is the one disagreement
  left, 1.7.25 against 2.0.17, and is not a defect — json-path names only `debug`, `trace`, `error`,
  `isDebugEnabled` and `getLogger(Class)`, every one of them 1.x.

  **What 1.43 cost to move to, measured, was one node.** `EnumerableUncollect` was reworked:
  `BuiltInMethod.FLAT_PRODUCT` became `FLAT_ZIP` and took a fourth `isOuter` argument, `FLAT_LIST` gained a
  `FLAT_LIST_OUTER` variant, and a struct element kept whole became a new `FlatProductInputType.STRUCT`.
  `ClrCursorUncollect` carries all of it, in both bodies. `Uncollect`'s five-argument constructor
  survives and infers the two fields the class gained, which is why nothing else failed to compile.

  **And two tests changed meaning rather than breaking.** `ShouldReachEveryBuiltInMethod` is a census of
  Calcite's table and counts 607 where 1.42 had 594 — all thirteen resolve, which is the thing it is for.
  And a `CAST(any AS UUID)` used to convert nothing, because `getJavaClass` had no UUID case and the target
  class was `Object`; 1.43 gave UUID a class, so the same statement now asks for an `Object`-to-`UUID`
  conversion over a string and throws. Both conventions throw alike, so it is reproduced rather than ours —
  `ShouldAgreeOnRefusingAUuidCastOfAnAnyColumn`.

  **1.43's DELETE cannot compile over a one-column table** *under Janino*. CALCITE-7510 emits
  `(int) sinkRow` from a `sinkRow` declared `Object`; javac accepts that and **Janino does not** — measured.
  The fix is upstream's: declare `sinkRow` as the row's boxed type rather than `Object`. It costs this
  project nothing, because `ClrPrepareImpl` translates Calcite's tree instead of compiling it. The two
  `CalciteDdlTests` this held were skipped until `fc3621e` typed a scan's rows by the physical row type;
  **they pass, and nothing in the suite is skipped.** The claim that they are red outlived the fix by four
  commits in this file.

  **1.43's model loads no class by name unless told to.** From the August 2026 snapshots `ClassNameFilter`
  carries an allowlist beside the denylist, read from the `calcite.model.classes.allowed` system property,
  and an empty allowlist — the default — rejects every `factory`, function class and driver a model names,
  Calcite's own `AbstractSchema$Factory` included. `Apache.Calcite.Data.Tests` sets the property in a module
  initializer, before `CalciteSystemProperty` reads it; a .NET class needs both its CLR name and its IKVM
  `cli.` name allowed, because Calcite writes `getClass().getName()` into the model it synthesises for
  `SchemaFactory`. **Every entry point needs one**: `Apache.Calcite.Geography.Tests` too, because
  `SqlSpatialTypeOperatorTable`'s constructor registers `SpatialTypeFunctions` through
  `ModelHandler.addFunctions` — 44 of its 200 tests failed on that one line until it had one.

  **`AsofJoin` is not `Asof`** — `rel.core.AsofJoin`, `EnumerableAsofJoin` and `ENUMERABLE_ASOFJOIN_RULE`
  are 1.41; `rel.core.Asof` is a different class and is 1.43. A claim that `AsofJoin` was 1.42 stood in this
  file for a while on the strength of the wrong class name.
- **The version this file gives has been wrong twice, both times by reading the tree.** It said the tree was
  1.42.0-SNAPSHOT after upstream had moved to 1.43, and called the TableModify rewrite a 1.42 feature when
  1.42's copy of that file is byte-identical to 1.41's. `git tag --list` and `git cat-file -e <tag>:<path>`
  settle it in one command each; the tree settles nothing.
- **Read the tag, and then check the assembly, because they differ.** `git archive calcite-1.41.0 …` is the
  source; `calcite.core.dll` is what runs. `RelOptUtil.registerDefaultRules` registers
  `EnumerableRules.ENUMERABLE_RULES` in the assembly — measured by counting the planner's rules across the
  call — and the tag's text of that method gives no sign of it until its last third.
- `global.json` pins the **.NET 10 SDK** — `version: 10.0.100`, `rollForward: latestMinor`, so the newest
  installed 10.x is taken and 11 is never reached. The workflow installs 9.0.x and 10.0.x and nothing else,
  so the `latestMajor` this used to carry meant that a machine with the .NET 11 preview installed built on
  a different SDK from CI, and said nothing. `dotnet sln add` misreports under either: re-adding a project
  failed naming a *different* project as the conflict, so read the solution after rather than the
  command's output. Under a `.sln` the preview SDK went further and rewrote every project with x64/x86
  configurations; under the `.slnx` it does not — measured, by round-tripping a project through
  `dotnet sln remove` and `add` and hashing every csproj either side.

## Apache.Calcite.Extensions: the rules that hold

**A port reproduces Calcite's logic, bugs included.** This is a port, and Calcite's behaviour is the
specification — not SQL, and not what the algorithm ought to be. Do not drop a step because it looks
unnecessary. Do not optimize what Calcite does not optimize. Where Calcite has a defect, reproduce the
defect and say so at the site: `EnumerableWindow`'s outer guard is still false after row 0, so an
`UNBOUNDED`/`UNBOUNDED` frame with `EXCLUDE` never excludes anything, and `ClrCursorWindow` shares that.
Correctness against SQL is a later argument to have upstream; a divergence introduced here is a defect we
own alone and cannot diff against anything.

**Reach for the type whose semantics match, not the runtime's.** A CLR type is fine where it agrees on what
the algorithm actually depends on — null handling, iteration order, equality, and whether the operations the
code is written in terms of exist. `List<T>` for an `ArrayList` is fine. `SortedDictionary` for a `TreeMap`
is not: it rejects a null key *before* consulting the comparer, which breaks `NULLS FIRST` over a single
nullable column, and it has no `lastKey` or `headMap`, so the limit sort had to be re-expressed rather than
transcribed — and both defects came out of that re-expression. `Dictionary` for a `HashMap` is not, wherever
iteration order reaches the output, which is why the join lookups are `java.util.HashMap`. When the Java
type is the one that fits, use it — we run on IKVM — and name at the site which property forced it.

**"Cannot be reproduced" is usually "did not look for the class."** We run on IKVM, and Calcite's runtime
and Guava are both on the classpath — `ImmutableList`, `HashMultiset` and `SortedMultiMap` are already used
here. So a Java collection Calcite's behaviour depends on is generally *available*, not merely imitable:
`nestedLoopJoinAsList` holds its unmatched right rows in Guava's `Sets.newIdentityHashSet()`, and that was
written as a CLR `HashSet` over a reference-equality comparer on the grounds that the CLR has no
`System.identityHashCode`. It does not need one. The order those rows come out in is that map's, and using
the map is the whole of the fix. Before writing that something cannot be carried across, name the class and
check whether it is simply reachable.

**A divergence recorded as deliberate is a scope reduction unless Calcite cannot be followed.** The bar is
whether the CLR makes it impossible — a method returning `IAsyncEnumerable` cannot await before it returns,
and that is a real one, stated at each site. "Ours holds the same rows in the same order and only reaches
them sooner" is not: that was `Window`'s laziness, filed under record-do-not-fix for a while, and Calcite
builds an `ArrayList` and returns `Linq4j.asEnumerable(list)`. Whether ours is better is not the question
the port gets to answer.

**Obtaining an enumerator runs the plan; `yield` defers it.** linq4j's operators acquire their source's
enumerator inside `enumerator()` — `where` on the spot, `orderBy` draining its whole input there, the JDBC
leaf executing its statement there — and deferral is the marked exception (the CALCITE-2909 memoized join
lookups). A C# iterator method defers everything to its first `MoveNext`, acquisition included, so
re-expressing an operator as one silently moves that seam and no row-comparing test can see it. That is why
a node of this convention hands up an *open*: evaluating it is `enumerator()`, and an operator that defers
takes an opener, which reads at the site. Timing is held by `ClrCursorDefaultsAcquisitionTests`, which reads
a counting leaf's acquisitions and its rows as two numbers.

**A claim about how something fails is a claim to run.** Reasoning from a comparator's semantics gave "a
CLR-boxed row element and a Java-boxed one compare unequal, their hashes agree, so a set operator quietly
keeps both copies" — a description of a state no query reaches. One query over a table holding CLR-boxed
values stops on its first row, because what reads a field is `SqlFunctions.toInt` or a cast to the boxed
type the row type declares. The difference between those two stories is a per-row conversion on every
scan, nearly added to fix a defect that was in the test table.

**A recursive query can fail to terminate under Calcite too, and then there is no oracle.** A repeat union's
spool is cleared by a round that wrote nothing, so a step that aggregates the working table oscillates: the
round that counts one is empty and empties the table, and the round after it counts zero and emits again.
Under UNION ALL that runs forever whichever way the plan is read. Deduplication is what ends it, the second copy being
a row the sequence already returned. Before writing a recursive test, check that the shape converges —
`SameRel` will hang rather than fail, and a hung suite looks like an infinite loop in the operator that was
just changed. It is worth running the new test alone first.

**A differential test compares answers, not implementations.** It is a strong oracle for correctness and no
oracle at all for faithfulness. Three divergences from Calcite have been found by reading rather than by
testing, and every one returned the right rows: the async `Format` missing its `Row` case, a scan that
re-derived `deduceElementType`'s precedence by hand, and a limit sort that sorted its whole input where
linq4j keeps a bounded `TreeMap`. A green suite does not mean a member has been ported. Reading Calcite's
source for that member is what settles it.

**Where linq4j may appear.** A node holds linq4j only where a generator of Calcite's produced one or takes
one, and it is translated where it is produced rather than composed into a larger tree first. That is four
things: Rex (`translateCondition`, `translateProjects`, `translateLiteral`, everything `RexImpTable`
reaches); the blocks an `AggImplementor` or a `WinAggImplementor` writes, which take a `BlockBuilder` and
read linq4j state slots; a table's own `getExpression(Queryable.class)`, which the schema SPI defines as
linq4j; and the block Calcite's implementor produces for an `EnumerableConvention` sub-plan at a converter.
Everything else is `System.Linq.Expressions` directly.

**A physical type is not one of them.** `ClrPhysType` answers every question about a row in
`System.Linq.Expressions` — it mirrors `PhysType` member for member and `ClrPhysTypeImpl` mirrors
`PhysTypeImpl`, private helpers included — and `JavaRowFormatExtensions` answers the members of
`JavaRowFormat` that are linq4j or package private. What a row *is* stays Calcite's: the format goes
through `JavaRowFormat.optimize` and the row class is the type factory's own answer, resolved.

**Calcite's `PhysType` survives only where the shared Rex machinery takes one**, and each of those builds
it where it is called, from the type factory, row type and format a `ClrPhysType` already carries:
`translateProjects` for its storage types, `InputGetterImpl`, `AggResultContextImpl`'s key,
`translateTableFunction`, the correlate input getter, and `EnumerableRelImplementor.result` at the
converter out — which casts to `PhysTypeImpl`, so nothing else will do. A node is in one convention or the
other and so is its physical type; there is no adapter between them and none is wanted.

**A row of this convention is boxed, and the physical type says so.** `ClrPhysType.RowType` is
`getJavaRowType()` boxed, because a CLR cursor states its element type and nothing autoboxes at the
boundary. Calcite can leave the choice to its callers — there is no `Enumerable<int>` in Java, so the
element is a reference whatever the physical type says, and javac inserts the conversion. Here it is
decided once, and `ClrCursorRelImplementor.Result` refuses a cursor that disagrees.

**`JavaCast` is for what Java the language converts and an expression tree will not** — boxing, unboxing,
numeric promotion, `byte` sign extension. It is not a way to make one type into another where they ought
already to agree; converting a value that already has the type wanted only absorbs the case where it does
not. Measured across every plan the tests run, what is actually asked for is four reference conversions
and one boxing.

**A linq4j tree means what it means after `OptimizeShuttle`.** Every tree Calcite compiles has been through
it, because a node hands its tree to `BlockBuilder.append` and that runs it; a tree we translate directly has
not. Its own class comment says the pass is not a tweak — "without optimization, expressions such as
`false == null` will be left in, which are invalid to Janino". `generateNullAwareAccessor` writes
`field == null ? null : List1(field)` for every key, and where the field is a primitive that comparison is
not Java at all. Janino would reject it; the CLR converts a null to an `int` and throws at run time.
`ExpressionTranslator` runs the shuttle over an expression arriving from outside — **expressions only**, since
a statement it rewrites away becomes `EMPTY_STATEMENT`, which `BlockBuilder` filters and a bare block does not.

**A linq4j call's recorded `Method` is advisory.** Janino writes the tree out as source and the *Java
compiler* resolves both the overload and the receiver from that text. `Linq4j.asEnumerable` is named
against the array overload and passed a list; `size()` on a `SortedMultiMap` is named on `Collection`.
`MethodResolver.Rebind` and `RebindReceiver` fix both, and refuse to guess where an argument is statically
`object`, because then every overload fits.

**A lambda linq4j declared against one of its functional interfaces is one**, so `AnonymousClasses.Wrap`
makes it one where a Java API is going to read it — `ClrCursorWindow.Hoist` does this for the key
selector `BinarySearch` takes as a `Function1`. Converting the delegate instead compiles and throws at run
time. Everywhere else an operator of this convention takes the delegate as it stands, the generators
answering in expression trees rather than in linq4j selectors that had to be translated back.

**An adapter converts values in both directions.** `JavaValues.As` / `From`. Taking a `java.lang.Integer`
and casting it to a CLR `int`, or handing an `int` back boxed the CLR way, leaves two representations of
one value in a plan and Calcite's own comparators fail on them. This is the invariant the whole port
exists to keep, and it has leaked three times: once through the SAM adapters, once through
`JavaSequences.FromJava`, which cast where it had to convert and so could not carry a one-column result of
a primitive across the converter, and once through the sequence convention's `LazyCollectionSpool`, which wrote
CLR rows into the table's `java.util.Collection` and left `SqlFunctions.toInt` refusing a `System.Int32` when
the interpreter read them back — that one did not cast, it simply did not convert, and no test had ever
written to a spool. **Every boundary where a value crosses between the two runtimes is an
adapter**, a sequence included — if it casts, it is wrong.

**But `JavaValues.From` is not what keeps that invariant, and mostly cannot be.** It branches on
`typeof(T).IsValueType`, the static type parameter — so wherever a call site instantiates it from a
`PhysType.RowType` it compiles away to nothing, that type always being `ClrPrimitive.Box(...)` and therefore
a Java class. The spool's guard is inert for exactly this reason. What actually holds the line there is
upstream: `JavaRowFormatExtensions.ObjectArray` builds every array element through `ClrEnumUtils.Convert`,
which boxes the Java way. `From` earns its keep only in the `Delegate*` SAM adapters, where `T` really is a
CLR primitive. Do not read a `From` at a boundary as proof the boundary is guarded — go and find where the
value was boxed.

**`Rules()` and `CalcRules()` are two passes, not one.** `VolcanoCost.isLt` compares the row count and
nothing else — cpu and io are dead code behind `if (true)` — so a project and a calc are never cheaper
than one another and the planner keeps whichever it saw first. `Programs.standard` runs the calc rules
afterwards as a hep pass. A caller must do the same, and must run `Programs.subQuery` before the planner.
**It should decorrelate**, and the claim here that it must not — on the grounds that decorrelation would
leave the convention's correlate unreachable — was false and stood for a while. Measured: a scalar sub-query
and an `EXISTS` do become joins, which is what Calcite means and what the prepare pipeline has always
done, but an `UNNEST` over a correlation variable cannot be decorrelated and keeps its correlate. That is
how Calcite reaches its own `EnumerableCorrelate` under `Programs.standard` too. Leaving the pass out
bought nothing and cost every correlated sub-query the join Calcite would have given it.

**There is no program of this project's own, and there is nothing for one to do.** `ClrPrepare.GetProgram`
is `Prepare.getProgram` — `Programs.sequence(Programs.standard(), Programs.hep(calcRules, true, provider))`,
Calcite's program as it stands with one pass added after it. **One member, and nothing overrides it.** The
calc list is this convention's, the five it shares with Calcite going in once, so the program is the same for
every statement. There is no second hook and no per-convention override; `Hook.PROGRAM` replaces the whole of
it, as upstream.

**Nothing of `standard`'s is replaced**, `Programs.calc` included: that pass is `RelOptRules.CALC_RULES`, it
still runs, and it still has work, because a plan may hold nodes of any of the three conventions. `standard`'s
planner pass installs no rules and plans with whatever the planner carries; the calc pass is the only one that
knows a convention by name, and it can only be *added* to. Our list is Calcite's rule for rule with the three
that name a node swapped, per convention, so four of the five shared rules cannot match by then
(`FILTER_TO_CALC` and `PROJECT_TO_CALC` want a `LogicalFilter` and a `LogicalProject`, the two calc merges want
a `LogicalCalc` input) and `CALC_MERGE` is the one that earns the second run, matching any `Calc` over any
`Calc`. **The reason it cannot go on the planner is not only cost.** `VolcanoPlanner.addRule` skips registering
a `TransformationRule`'s operand against any `PhysicalNode`, and every node of this convention is one — so
`CALC_MERGE`, `FILTER_TO_CALC`, `PROJECT_TO_CALC` and the two calc merges would sit on the planner and never
match a node of ours. The `VolcanoCost` argument (row count only, cpu and io dead behind `if (true)`) is true
and is the weaker half.

**`ClrRelOptUtil.RegisterDefaultRules` is `RelOptUtil.registerDefaultRules` and then `ClrCursorRules.Rules()`** —
the whole of the job a caller driving its own planner has, which `ClrPrepareImpl.CreatePlanner` and the tests
each used to spell out. There is one list because there is one convention.
`EnumerableRules.TO_INTERPRETER` is registered by Calcite's call and `ClrCursorInterpreterRule` is not
registered by ours, so an interpreted node still lands in `EnumerableConvention` under a converter.

A caller driving a `Frameworks` planner has the same job and only that job — get the rules on first
(`AddRulesProgram` in the tests), then run `Programs.standard` — and the classes that spelled `standard`'s six
passes out by hand are gone.

**There is one Clr convention, `ClrCursorConvention`, and its plan is an open rather than a sequence.** There
were three: `ClrEnumerableConvention` and `ClrAsyncEnumerableConvention`, merged into one with two bodies per
node, and then the cursor convention beside it, node for node. The sequence convention was removed once the
cursor convention reached parity with it, measured; a sequence states once, at `GetEnumerator` or
`GetAsyncEnumerator`, whether it will be pulled or awaited, and a `DbDataReader` does not. **A plan has no
mode**, so `EXPLAIN` does not tell a caller how the rows will be read, and a plan cache would hold one entry
for a statement.

A node's expression evaluates to an *opened* `IClrCursor<TRow>`, so the tree of calls *is* linq4j's
`enumerator()` cascade: a sort drains where its open is evaluated, a leaf executes there, and an operator that
acquires a source later — `concat` at its turn inside `moveNext`, `union` after draining its first — takes that
source as a delegate built by `Opener`/`OpenerAsync`. `Implement` composes opens that acquire synchronously
and `ImplementAsync` opens that await, in `ValueTask<IClrCursor<TRow>>`; both produce the *same cursor
class*, whose `Read` and `ReadAsync(token)` step one set of fields, and `ImplementRoot` runs both bodies and
puts the two opens on one `ClrCursorFactory`.

- **Two parallel call hierarchies, sync and async, and no mode anywhere.** `VisitChild` calls only
  `Implement` and `VisitChildAsync` only `ImplementAsync`. A body calls the visit of its own kind, so the kind
  is settled statically at every step and `ClrCursorRelImplementor` holds no state about it. **The dispatch
  has been rebuilt twice and removed twice.** First as `implementor.Methods` and `implementor.Call`, which
  made the operator set a runtime value. Then as a `bool async` field with one `VisitChild` branching on it —
  the same dispatch one level down, and it forced every result to be type-tested on the way back. There is no
  third place for it to hide: if something has to ask which kind it is in, the hierarchy is missing a member.
- **A deferred source is visited through both hierarchies from both bodies**, because the advance that
  reaches it may be either and the cursor needs the opener of that kind. That is the one place the two
  hierarchies meet, and it is not a mode: it is the consumer's per-advance choice reaching an acquisition
  that happens per advance.
- **The awaiting hierarchy's token is a real parameter**, declared by the awaiting root's lambda and
  redeclared by each deferred opener so that it shadows — measured to work in both the compiler and the
  interpreter — and `ClrCursorBuiltInMethod.CallAsync` passes the implementor's parameter, never `default`.
- **One operator class, and the suffix is load bearing.** `ClrCursorDefaults` holds every operator as a
  cursor class whose two advances step one set of fields: the opens that acquire synchronously, and then the
  `Async`-suffixed opens that await. Because the two sets do not differ by declaring type, everything that
  has to tell them apart reads the name, and `ShouldNameEveryAwaitingOpenWithTheSuffixAndNoOtherOpen` keeps
  that exact: an open returning a `ValueTask` carries the suffix and ends in a token, and one returning a
  cursor does not.
- **`Implement` is required; `ImplementAsync` is optional and defaults to it.** That is .NET's own shape,
  measured against the 10.0 reference assemblies rather than remembered: `DbCommand.ExecuteDbDataReader`,
  `DbDataReader.Read`, `DbConnection.Open` and `Stream.Read` are abstract, and every `Async` counterpart is
  virtual over them. Two defaults calling each other would compile for a node overriding neither and then
  recurse until the process dies, and a `StackOverflowException` cannot be caught.
- **Each fork has its own result type.** `Implement` answers a `ClrCursorResult` and `ImplementAsync` a
  `ClrCursorAsyncResult`; `Result` and `ResultAsync` each require their own kind of open and name the node
  that broke it. Crossing is `implementor.Pulled` and `implementor.Awaited`, unconditional and written at the
  site that wants one. Going to awaiting costs a completed `ValueTask` and no thread; going to synchronous
  **blocks the calling thread**, through `ClrCursors.Block`.
- **The default is safe exactly when a body does not visit a child, which is not the same as having no
  input.** The awaiting hierarchy hands a body awaiting inputs, so a node that inherits the default composes
  a synchronously opened input into an awaiting operator and `Expression.Call` refuses it. The interpreter
  has an input and one body and is fine, because it stashes the child node rather than asking for it as an
  open. `ShouldAgreeOnAWindowTableFunction` holds the table function scan's two input visits. An adapter with
  only an awaiting client writes `ImplementAsync` and `Implement` as a delegation to it, which is safe for the
  same reason: a converter out of an adapter visits no child.
- **The table SPI is one interface per table kind, with both halves on it**, matching the nodes: `Scan` is
  required and `ScanAsync` defaults to it, and the same for a queryable table's two expressions and a cursor
  table's two opens. Two interfaces for one kind of table could not answer the question the scan actually
  had: each source builder asked whether the table was of the *other* kind before its own, so a table
  implementing both sent the two into mutual recursion and **overflowed the stack while the plan was being
  built** — measured. One interface makes that unrepresentable. An awaiting-only table writes `Scan` by
  draining its own `ScanAsync`. **The operator tables stay internal** — `ClrCursorDefaults`,
  `ClrCursorBuiltInMethod` and `ClrCursors` are what this convention's plans are built from, not a toolkit
  for an adapter. **Know what that costs the implementer**, because the obvious drain is wrong: blocking on
  an awaiting call directly deadlocks under a synchronization context, since these operators await without
  `ConfigureAwait(false)` and the continuation is promised at the moment of suspension, inside the call and
  before any wait. The context has to be nulled before the call. Measured twice, once when a four-line drain
  in `AsyncTestSchema` hung `ShouldReadAnAsynchronousLeafSynchronouslyUnderASynchronizationContext` for
  thirty seconds. An awaiting-only table has to get that right, and the test tables here show the shape.
- **Where the plan leaves as a sequence it defers.** `IClrPrepare.Signature.Bind` and `BindAsync` read the
  cursor as an `IEnumerable` or an `IAsyncEnumerable` for a caller that wants one. `GetAsyncEnumerator`
  cannot await, so the awaiting one opens in its first `MoveNextAsync`, stated at the site; nothing else in
  a plan has to.

**Two facts the port fixed the moment a plan could be rooted in this convention, both measured.** **A
converter rule must simplify the trait set it copies**: a merge join carries two collations, `RelSet.add`
simplifies a rel's traits before choosing its subset, so the converter's input subset carries none and a
converter claiming both claims a sort its input does not keep — `ORDER BY` over an `IN` sub-query lost its
sort under a cursor root, and `RelOptRule.convert` simplifies for the same reason. And **a sub-plan spliced
into another implementor's tree shares its translator**: a correlate declares its outer row's field reads into
a block it translates itself, and a sub-plan under a converter that translated with a translator of its own
referred to a variable no lambda declares, failing at `Compile`.

**A Clr sub-plan under Calcite's correlate reads its correlation variable through the `DataContext`, and that
is the only channel there is.** `EnumerableCorrelate` makes the outer row a parameter of the Java lambda it
generates and places the inner block inside it, so Calcite's own sub-plans read the variable lexically; a
plan of ours is a delegate compiled apart from that lambda and cannot. The converter out therefore asks
Calcite's getter for every field of every variable the sub-plan uses (which declares the reads into the
correlate's own block, in scope where the converter's call lands), passes them as an `Object[]` to
`JavaPlans.BindCursorCorrelated`, and the sub-plan's implementor registers the variable as an `ARRAY`-format
row read from `DataContext.get(name)`. `ShouldReadACorrelationVariableUnderCalcitesCorrelate` holds it. The
reverse direction — Calcite's sub-plan under our correlate — is lexical, by replaying our registrations onto
Calcite's implementor and translating its block into our tree.

**The converter out is a Janino call into a stashed plan, and two things about the generated source were
only found when a Clr node first ran under a Calcite node.** linq4j writes a class name with every `$` as `.`
(`Types.className`, for `Outer.Inner`), so IKVM's `$$`-mangled name for a generic instantiation cannot be
named: the plan is stashed as `Object` and cast in `JavaPlans`. And IKVM exposes an `internal` class to Java
as package-private, which Janino silently drops as a candidate — the error lists the method it refused —
so `JavaPlans` is public. A probe with the same signature on a public class compiled; on the internal one it
did not.

**A join boxes its rows.** Calcite builds the selector and predicate against boxed rows because linq4j's
`Function2` and `Predicate2` erase to `Object`, and because an outer join compares a row to null. A
delegate is typed where those interfaces were not.

**A block consumed apart from what reads it needs a non-optimising `BlockBuilder`.** An optimising one
inlines a declaration used once, leaving a reference already built into a translated sub-plan pointing at
a variable that no longer exists. `ClrCursorCorrelate` needs this.

**Calcite keeps a lot of what a port needs package private** — `EnumUtils.joinSelector`,
`generatePredicate`, `fieldTypes`, `fieldRowTypes`, `javaClass`, `EnumerableAggregateBase`'s four helpers,
`PhysTypeImpl.of(typeFactory, javaRowType)` **and `PhysTypeImpl`'s own constructor** — so
`PhysTypeImplWorkaround` has to go back out through the public `of`, and cannot pass the row class
through the way Calcite does — `EnumerableWindow`'s five private helpers and its constructor,
and **every `RexToLixTranslator.translate` overload** — only the `translateList` forms are public, and
`translateList(operands, storageTypes)` is `translate(operand, storageType)` once per element, so a list of
one is the same call by a reachable name. Expect to port rather than reuse, or to find a public route: a
`ConverterRule`'s `convert` is public even when its node's constructor is not.

**A Calcite `Pair`'s `left` and `right` are unreachable from C#** — the fields are shadowed by the static
methods of the same name, and C# resolves the member to the method group. `Pair` is a `Map.Entry`, so
`getKey()` and `getValue()` are the way in.

**A package private *type* Calcite casts to cannot be ported at all.** The rule above is about members, and
a member can be written again. `EnumerableMatch.PassedRowsInputGetter` and `PrevInputGetter` are types:
`RexToLixTranslator.implementPrev` and `RexImpTable.LastImplementor` cast the input getter to them by name,
and `RexToLixTranslator` suppresses its field-read cache for a `PrevInputGetter` specifically. A class of
the same shape fails all three casts, and IKVM makes them `internal`, so C# can name them and cannot
construct them. **Reflection is not the answer** — where this happens, either let Calcite build that part
of the block, or accept that the node cannot be written. A MATCH_RECOGNIZE query still runs: the planner
leaves the whole subtree in `EnumerableConvention` and one converter carries the rows.

**An anonymous class of one method is a lambda; one of several is a thing.** `Anonymous` turns the first
into a lambda and the second into an object holding a delegate per method — `DelegateEnumerator` for the
four-method `Enumerator` a calc generates. Either way the class's fields become variables of the block that
builds the lambdas, so what they share has the lifetime one instance would have had.

**Do not claim a missing node "still runs because the converters carry it" without running it.** The
converter only ever saw a bare scan for a long time, because every mixed-convention test leaves the calc on
the Clr side; a whole class of generated block could not cross and nothing said so.
`ShouldCarryACalcAcrossTheConverter` plans with Calcite's rules plus this convention's converter rule alone,
which is the only way to make the converter meet a real generated block.

**A block Calcite generates can rely on two Java facts an expression tree does not have**, and both are
handled in `ExpressionTranslator` because both are general rather than one node's problem. linq4j hoists a
sub-expression it can prove constant into a *field* of the anonymous class it is generating, so an
anonymous class is not always a bare SAM; each field becomes a variable of the block that builds the
lambda. And Java resolves a *name*, so a parameter shadows an outer variable Calcite deliberately gave the
same name — `ExpressionTranslator` gives a lambda's parameters, and an anonymous method's, a lexical scope by
name for exactly that, **consulted before what the object is already bound to**: a parameter does not
outrank an outer variable of its name, it makes it unreachable, and Calcite relies on that both ways round.
**Two generators, one name, is the normal case rather than the odd one**: a window table function is
`_input` from `TumbleImplementor` and `_input` from `tumblingWindowSelector`; a MATCH_RECOGNIZE predicate is
`row_` the input row and `row_` the `MemoryFactory.Memory` around it. Neither ran for want of that scope,
and it was described in this file before it was in the code.

**"Janino cannot name a CLR class" was an IKVM regression, not a fact about Janino, and it is fixed.** The
claim stood in seven places here: IKVM names a CLR class `cli.Namespace.Type`, `EnumerableConvention` writes
that name into generated Java source, and Janino answered "Cannot determine simple type name cli", so a .NET
UDF, table function or metadata handler had no plan under Calcite's own engine. What actually happened is
narrower. `IKVM.Maven.Sdk` stamps every `MavenReference` assembly with
`CustomAssemblyClassLoaderAttribute(AppDomainAssemblyClassLoader)`, and Janino compiles against
`calcite-core`'s own loader — that loader walks every loaded assembly and answers `cli.` names.
`CustomAssemblyClassLoaderAttribute` was made **`internal`** in 8.14.0 by the IKVM.Reflection-into-CoreLib
move, and `RuntimeAssemblyClassLoader.GetCustomClassLoader` reads it with
`Assembly.GetCustomAttributes(type, false)`, which the CLR skips when the attribute type is not visible
outside its own assembly. The stamp was in the metadata and unreadable, so calcite-core fell back to a
per-assembly loader that sees nobody else's types. **8.16.0 makes it public again** (ikvm `e0a12705b3`,
ikvm#723); it was public at 8.13.0, and this repo has only ever been on 8.14.0 and 8.15.0 — the whole of the
broken window. Measured at one commit either side: at 8.15 `revise` throws and the UDF queries fail to
compile; at 8.16 the handler compiles and `MY_SUM` and `NUMBERS` give the same rows read either way, so
those three tests are differential like the rest. A tree still holds the method rather than its name, so
this convention never cared — but the *capability* argument is gone, and what is left of
`ClrRelMetadataProvider`'s reason is the compile it saves. Note the loader only sees assemblies already
loaded in the AppDomain, and setting `MavenClassLoader` empty turns it off.

## Traps

- **`java.lang.Comparable` is a ghost interface IKVM gives to `System.String`.** The CLR type system does
  not see it, so a cast to it throws — in an expression tree *and* in C# we write, because IKVM emits the
  ghost conversion only for the Java it compiles itself. `IComparable` is the way in, and
  `JavaComparisons` is where the `Utilities` comparisons that take a `Comparable` go through it.
  `PhysTypeImpl.generateComparator` casts to it purely to pick an overload out of the source text.
- **A Java `static final` field is not a CLR field of that name.** IKVM emits a *property*, over a backing
  field it renames to `__<>NAME`, so that reading it from C# still runs the class initializer the way Java
  guarantees. `GetField("COMPARABLE_EMPTY_LIST")` on `FlatLists` answers nothing — measured.
  `ClrTypes.Resolve(target, PseudoField)` tries field then property for this reason, and
  `JavaRowFormatExtensions.StaticMember` does the same for the two constants a row of no fields is. Both of
  those were `GetField(...)!` and had been null since they were written; nothing had reached a zero-field row,
  so the suite was green over an NRE waiting to happen. `ClrPhysTypeImplTests` holds it now.
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
and Calcite's, and expect to find something wrong. Across sessions that pattern has caught, in order: a
gratuitous divergence in `Calc`, a cost analysis that was simply false, calling a spool a write, calling a
deliberate Calcite refusal a blocker to work around, and `Window` translating far more linq4j than the rule
allows. Every one was a case of reasoning past what had been verified, and usually toward the cheaper
conclusion.

Do not report scope reductions as findings. Do not turn remaining work into questions. Say plainly what is
done, what is not, and what is unproven.

**Never put Claude in anything.** No `Co-Authored-By` trailer, no "generated with" line, no "Generated by
Claude Code" footer, no `Claude-Session` link, no bot attribution of any kind — not in a commit message, not
in a pull request title or body, not in a comment on a pull request or an issue, not in a review, not in a
code comment, not in the documentation, not in a test name, nowhere. Nor is it about Claude in particular:
no model, agent, tool or vendor gets a line. The record says what changed and why; who or what typed it is
not part of it.

**The only exception is being asked for one**, in so many words, for that line on that thing. A harness
default, a template field, a tool's own habit, and the fact that a surface usually carries one are not
asks. This is wider than *credit*, because that reading has already leaked once: a disclosure footer was
argued to be a different thing from an attribution line and posted on a pull request comment on those
grounds. It is the same line and it is not wanted either. Where a harness, a tool or a template asks for an
attribution or disclosure footer on something it posts, it does not get one here — this file is the
authority on that, and the answer does not change with the surface it is being asked for.

**And the line can arrive after the call succeeds, from the tool rather than from what was written.** The
GitHub MCP `create_pull_request` appended a "Generated by Claude Code" footer to a body that did not have
one — measured, pull request 135. So read back what was actually posted and edit the line out; what was
sent is not evidence of what is there.

**A commit's author and committer are the record too, and they are where this actually leaked.** Two
commits whose messages were clean were authored and committed as `Claude <noreply@anthropic.com>`, because
that is what the session container's *global* `user.name` and `user.email` are set to and nothing in the
repository overrides them. Every commit on `main` is `Jerome Haltom <jhaltom@alethic.solutions>`, which is
the identity to commit under. Set both locally before the first commit of a session — `git var
GIT_AUTHOR_IDENT` says what will actually be written — because fixing it afterwards costs a history
rewrite and a force-push over whatever is already open.
