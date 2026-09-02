# Apache.Calcite for .NET

Apache Calcite under IKVM, with an ADO.NET adapter, an ADO.NET client surface, and
`Apache.Calcite.Extensions` — calling conventions that execute a plan as `System.Linq.Expressions`
instead of Janino, and the prepare pipeline that gets a statement to one.

## Projects

| project | job |
|---|---|
| `Apache.Calcite.Adapter.AdoNet` | pushes a plan down to an ADO.NET provider |
| `Apache.Calcite.Data` | the `DbConnection` / `DbCommand` surface |
| `Apache.Calcite.Extensions` | `ClrEnumerableConvention` and `ClrAsyncEnumerableConvention`, the prepare pipeline, and the IKVM interop helpers |

`TODO.md` has the outstanding work on the ADO.NET adapter, sized and reasoned, and the findings of the
operator audit against linq4j — 45 methods read side by side, 17 of them divergent.

**`Apache.Calcite.Data` follows `Microsoft.Data.SqlClient`.** Where ADO.NET leaves a provider a choice
— what a typed getter accepts, what `GetFieldValue<T>` converts, what `GetFieldType` claims for a
column whose type is not known until a row is read — the answer is whatever SqlClient does, and the way
to find it is to read `SqlBuffer.cs` and `SqlDataReader.cs` in `dotnet/SqlClient` rather than the
documentation or memory. Two things settled from there so far: a typed getter is a *cast* and never a
conversion (`SqlBuffer.Int32` is `(int)Value` for anything it does not store, so `GetGuid` does not
parse a string and `GetInt32` does not read a `bigint`), and `sql_variant` is `ANY` — its field type is
`typeof(object)` and its `SqlBuffer` carries the variant's *inner* storage type, so the value's class
stands in for the type the column does not declare and does nothing more than stand in for it.
`src/Apache.Calcite.Data/DESIGN.md`, *The driver this one is modelled on*, has the reading.

## Building

- Build the **solution**: `dotnet build Apache.Calcite.slnx`. A bare `dotnet build` fails — more than one
  project in the root.
- **The check that matters is `ClrEnumerableDifferentialTests`.** It runs the same SQL through this
  convention and through `EnumerableConvention` and requires the same rows. Every defect worth having
  found in the convention was found by it, three of them in nodes already believed done. Add a query
  there rather than writing an assertion by hand: the expected answer is whatever Calcite says. It lives
  in `Apache.Calcite.Tests`, with the rest of the convention and prepare tests.
- **`dotnet test --filter` is silently ignored here** — the project runs on Microsoft.Testing.Platform, and
  `dotnet test` does not forward the flag to the test app. It runs the whole suite and reports success, so a
  run that looks like one test is 619. Run the built executable instead, which honours it and is faster than
  the `dotnet test` host by about a third:
  `src\Apache.Calcite.Tests\bin\Debug\net8.0\Apache.Calcite.Tests.exe --filter FullyQualifiedName~Name`.
  The whole suite is about 133 seconds that way against about 215 through `dotnet test`; a single test is
  seconds. `--blame-hang --blame-hang-timeout 90s` names the test that hangs.
- **Calcite is checked out at `D:\calcite`, and it is 1.43.0-SNAPSHOT.** The projects reference **1.42.0**,
  which is released; `Apache.Calcite.Data.Tests` alone references **1.43.0-SNAPSHOT**, from
  `https://repository.apache.org/content/repositories/snapshots/`, for `calcite-server` and the
  `EnumerableTableModify` rewrite. **Three versions, so "it is in the tree" settles nothing.** Read the
  source, then check the member against the tag you actually reference:

  | in | |
  |---|---|
  | 1.42 (referenced) | `EnumerableCombine`, `EnumerableConditionalCorrelate` and their rules, `EnumUtils.markJoinSelector` and the mark-join paths, `PhysType.generateNullAwareAccessor`, `JoinInfo.nullExclusionFlags` |
  | 1.43 (unreleased) | `org.apache.calcite.rel.core.Asof`, `FetchOffsetRoundingPolicy`, `RexImplementorTable(s)`, and `EnumerableTableModify`'s five private helpers — the UPDATE/DELETE/INSERT rewrite, CALCITE-7510 |

  **1.43's DELETE cannot compile over a one-column table** *under Janino*. CALCITE-7510 emits
  `(int) sinkRow` from a `sinkRow` declared `Object`; javac accepts that and **Janino does not** — measured.
  The fix is upstream's: declare `sinkRow` as the row's boxed type rather than `Object`. It costs this
  project nothing, because `ClrEnumerablePrepare` translates Calcite's tree instead of compiling it. The two
  `CalciteDdlTests` this held were skipped until `fc3621e` typed a scan's rows by the physical row type;
  **they pass, and nothing in the suite is skipped.** The claim that they are red outlived the fix by four
  commits in this file.

  **`AsofJoin` is neither** — `rel.core.AsofJoin`, `EnumerableAsofJoin` and `ENUMERABLE_ASOFJOIN_RULE` are
  all in 1.41, and a claim that it was 1.42 stood in this file for a while on the strength of the wrong
  class name. `rel.core.Asof` is a different class and is 1.43, not 1.42 as this file said.
- **The version this file gives has been wrong twice, both times by reading the tree.** It said the tree was
  1.42.0-SNAPSHOT after upstream had moved to 1.43, and called the TableModify rewrite a 1.42 feature when
  1.42's copy of that file is byte-identical to 1.41's. `git tag --list` and `git cat-file -e <tag>:<path>`
  settle it in one command each; the tree settles nothing.
- **Read the tag, and then check the assembly, because they differ.** `git archive calcite-1.41.0 …` is the
  source; `calcite.core.dll` is what runs. `RelOptUtil.registerDefaultRules` registers
  `EnumerableRules.ENUMERABLE_RULES` in the assembly — measured by counting the planner's rules across the
  call — and the tag's text of that method gives no sign of it until its last third.
- `global.json` has `rollForward: latestMajor`, so builds pick the **.NET 11 preview SDK**, not 10.0.302.
  That SDK's `dotnet sln add` rewrote every project with x64/x86 configurations while the solution was a
  `.sln`. It does **not** under the `.slnx` — measured, by round-tripping a project through
  `dotnet sln remove` and `add` and hashing every csproj either side. It does still misreport: re-adding a
  project failed naming a *different* project as the conflict, so read the solution after rather than the
  command's output.

## Apache.Calcite.Extensions: the rules that hold

**A port reproduces Calcite's logic, bugs included.** This is a port, and Calcite's behaviour is the
specification — not SQL, and not what the algorithm ought to be. Do not drop a step because it looks
unnecessary. Do not optimize what Calcite does not optimize. Where Calcite has a defect, reproduce the
defect and say so at the site: `EnumerableWindow`'s outer guard is still false after row 0, so an
`UNBOUNDED`/`UNBOUNDED` frame with `EXCLUDE` never excludes anything, and `ClrEnumerableWindow` shares that.
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
re-expressing an operator as one silently moved that seam, across the whole of both conventions, and no
row-comparing test could see it. `ClrEnumerable` and `Acquiring` in `Runtime` put a factory where
`enumerator()` is; the one sanctioned exception is a drain that must await, which acquires eagerly and
drains in the first `MoveNextAsync`, stated at the site. Timing is held by `AcquisitionTimingTests`, which
reads a counting leaf's acquisitions and its rows as two numbers — and note the operators whose eagerness
is *call*-time are linq4j's own call-time drains (`union`, `distinct`, `asofJoin`, `groupBy`, the window,
`nestedLoopJoinAsList`): do not "fix" them toward the factory.

**A claim about how something fails is a claim to run.** Reasoning from a comparator's semantics gave "a
CLR-boxed row element and a Java-boxed one compare unequal, their hashes agree, so a set operator quietly
keeps both copies" — a description of a state no query reaches. One query over a table holding CLR-boxed
values stops on its first row, because what reads a field is `SqlFunctions.toInt` or a cast to the boxed
type the row type declares. The difference between those two stories is a per-row conversion on every
scan, nearly added to fix a defect that was in the test table.

**A recursive query can fail to terminate under Calcite too, and then there is no oracle.** A repeat union's
spool is cleared by a round that wrote nothing, so a step that aggregates the working table oscillates: the
round that counts one is empty and empties the table, and the round after it counts zero and emits again.
Under UNION ALL that runs forever in both conventions. Deduplication is what ends it, the second copy being
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
`getJavaRowType()` boxed, because a CLR sequence states its element type and nothing autoboxes at the
boundary. Calcite can leave the choice to its callers — there is no `Enumerable<int>` in Java, so the
element is a reference whatever the physical type says, and javac inserts the conversion. Here it is
decided once, and `ClrEnumerableRelImplementor.Result` refuses a sequence that disagrees.

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
makes it one where a Java API is going to read it — `ClrEnumerableWindow.Hoist` does this for the key
selector `BinarySearch` takes as a `Function1`. Converting the delegate instead compiles and throws at run
time. Everywhere else an operator of this convention takes the delegate as it stands, the generators
answering in expression trees rather than in linq4j selectors that had to be translated back.

**An adapter converts values in both directions.** `JavaValues.As` / `From`. Taking a `java.lang.Integer`
and casting it to a CLR `int`, or handing an `int` back boxed the CLR way, leaves two representations of
one value in a plan and Calcite's own comparators fail on them. This is the invariant the whole port
exists to keep, and it has leaked three times: once through the SAM adapters, once through
`JavaSequences.FromJava`, which cast where it had to convert and so could not carry a one-column result of
a primitive across the converter, and once through `ClrEnumerableDefaults.LazyCollectionSpool`, which wrote
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
leave `ClrEnumerableCorrelate` unreachable — was false and stood for a while. Measured: a scalar sub-query
and an `EXISTS` do become joins, which is what Calcite means and what the prepare pipeline has always
done, but an `UNNEST` over a correlation variable cannot be decorrelated and keeps its correlate. That is
how Calcite reaches its own `EnumerableCorrelate` under `Programs.standard` too. Leaving the pass out
bought nothing and cost every correlated sub-query the join Calcite would have given it.

**There is no program of this project's own, and there is nothing for one to do.** `ClrPrepare.GetProgram`
is `Prepare.getProgram` — `Programs.sequence(Programs.standard(), Programs.hep(calcRules, true, provider))`,
Calcite's program as it stands with one pass added after it. **One member, and nothing overrides it.** The
calc list is both conventions' — the five they share with Calcite going in once — so the program is the same
for every statement, the convention a statement ends in being the result convention demanded of the root and
nothing in the program. There is no second hook and no per-convention override; `Hook.PROGRAM` replaces the
whole of it, as upstream.

**Nothing of `standard`'s is replaced**, `Programs.calc` included: that pass is `RelOptRules.CALC_RULES`, it
still runs, and it still has work, because a plan may hold nodes of any of the three conventions. `standard`'s
planner pass installs no rules and plans with whatever the planner carries; the calc pass is the only one that
knows a convention by name, and it can only be *added* to. Our list is Calcite's rule for rule with the three
that name a node swapped, per convention, so four of the five shared rules cannot match by then
(`FILTER_TO_CALC` and `PROJECT_TO_CALC` want a `LogicalFilter` and a `LogicalProject`, the two calc merges want
a `LogicalCalc` input) and `CALC_MERGE` is the one that earns the second run, matching any `Calc` over any
`Calc`. **The reason it cannot go on the planner is not only cost.** `VolcanoPlanner.addRule` skips registering
a `TransformationRule`'s operand against any `PhysicalNode`, and every node of both conventions is one — so
`CALC_MERGE`, `FILTER_TO_CALC`, `PROJECT_TO_CALC` and the two calc merges would sit on the planner and never
match a node of ours. The `VolcanoCost` argument (row count only, cpu and io dead behind `if (true)`) is true
and is the weaker half.

**`ClrRelOptUtil.RegisterDefaultRules` registers both Clr conventions, always, and the mode chooses only
the root's trait.** It is `RelOptUtil.registerDefaultRules` and then both `Rules()` lists — the whole of the
job a caller driving its own planner has, which `ClrPrepareImpl.CreatePlanner` and the tests each used to
spell out. `CreatePlanner` used to branch on `async` and register one convention. That was wrong for a reason
no test could see: a schema may bring rules of its own, and nothing here can tell which convention an
adapter's rules target, so a half-loaded planner refuses such an adapter for no visible reason. It also left
both cross-convention converters inert — each list holds the converter *into* its own convention, whose
in-trait no rule on a one-convention planner can produce. The consequence is real and deliberate:
`Synchronous=true` no longer refuses an async-only table, it bridges to it and `Read` blocks there, and
`ShouldBridgeAnAsyncOnlyTableInSynchronousMode` holds that. The old refusal was one by omission, not a check.
`EnumerableRules.TO_INTERPRETER` is registered by Calcite's call and `ClrEnumerableInterpreterRule` is not
registered by ours, so an interpreted node still lands in `EnumerableConvention` under a converter.

A caller driving a `Frameworks` planner has the same job and only that job — get the rules on first
(`AddRulesProgram` in the tests), then run `Programs.standard` — and two classes that spelled `standard`'s six
passes out by hand, `ClrEnumerablePrograms` and `ClrAsyncEnumerablePrograms`, are gone.

**A join boxes its rows.** Calcite builds the selector and predicate against boxed rows because linq4j's
`Function2` and `Predicate2` erase to `Object`, and because an outer join compares a row to null. A
delegate is typed where those interfaces were not.

**A block consumed apart from what reads it needs a non-optimising `BlockBuilder`.** An optimising one
inlines a declaration used once, leaving a reference already built into a translated sub-plan pointing at
a variable that no longer exists. `ClrEnumerableCorrelate` needs this.

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
compile; at 8.16 the handler compiles and `MY_SUM` and `NUMBERS` give the same rows in both conventions, so
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
  so the suite was green over an NRE waiting to happen. `ZeroFieldRowTests` holds it now.
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

**Never credit Claude as a contributor.** No `Co-Authored-By` trailer, no "generated with" line, no bot
attribution — not in a commit message, not in a pull request body, not anywhere in the history. The commit
message says what changed and why; who typed it is not part of the record.
