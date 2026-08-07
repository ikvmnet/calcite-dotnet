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

`TODO.md` has the outstanding work on the ADO.NET adapter, sized and reasoned. The CLR convention is
ported and has no list of its own.

## Building

- Build the **solution**: `dotnet build Apache.Calcite.sln`. A bare `dotnet build` fails — more than one
  project in the root.
- **The check that matters is `ClrEnumerableDifferentialTests`.** It runs the same SQL through this
  convention and through `EnumerableConvention` and requires the same rows. Every defect worth having
  found in the Linq project was found by it, three of them in nodes already believed done. Add a query
  there rather than writing an assertion by hand: the expected answer is whatever Calcite says.
- **Calcite is checked out at `D:\calcite`, and it is 1.43.0-SNAPSHOT.** The projects reference **1.42.0**,
  which is released; `Apache.Calcite.Data.Tests` alone references **1.43.0-SNAPSHOT**, from
  `https://repository.apache.org/content/repositories/snapshots/`, for `calcite-server` and the
  `EnumerableTableModify` rewrite. **Three versions, so "it is in the tree" settles nothing.** Read the
  source, then check the member against the tag you actually reference:

  | in | |
  |---|---|
  | 1.42 (referenced) | `EnumerableCombine`, `EnumerableConditionalCorrelate` and their rules, `EnumUtils.markJoinSelector` and the mark-join paths, `PhysType.generateNullAwareAccessor`, `JoinInfo.nullExclusionFlags` |
  | 1.43 (unreleased) | `org.apache.calcite.rel.core.Asof`, `FetchOffsetRoundingPolicy`, `RexImplementorTable(s)`, and `EnumerableTableModify`'s five private helpers — the UPDATE/DELETE/INSERT rewrite, CALCITE-7510 |

  **1.43's DELETE cannot compile over a one-column table**, and two `CalciteDdlTests` are red for it
  rather than skipped. CALCITE-7510 emits `(int) sinkRow` from a `sinkRow` declared `Object`; javac
  accepts that and **Janino does not** — measured. The fix is upstream's: declare `sinkRow` as the row's
  boxed type rather than `Object`.

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
  That SDK's `dotnet sln add` also rewrites every project with x64/x86 configurations — edit the solution
  by hand.

## Apache.Calcite.Linq: the rules that hold

**Where linq4j may appear.** A node holds linq4j only where a generator of Calcite's produced one or takes
one, and it is translated where it is produced rather than composed into a larger tree first. That is four
things: Rex (`translateCondition`, `translateProjects`, `translateLiteral`, everything `RexImpTable`
reaches); `PhysType`'s expression members and `JavaRowFormat.field`; a table's own
`getExpression(Queryable.class)`, which the schema SPI defines as linq4j; and the block Calcite's
implementor produces for an `EnumerableConvention` sub-plan at a converter. Everything else is
`System.Linq.Expressions` directly.

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

**A lambda linq4j declared against one of its functional interfaces is one**, so `SamAdapters` wraps it
where it is built; an operator of this convention takes the delegate and asks for it back through
`TranslateSelector`. Deciding it at the call site instead breaks sixteen tests.

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
`PhysTypeImpl.of(typeFactory, javaRowType)`, `EnumerableWindow`'s five private helpers and its constructor,
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

**A user-defined function written in .NET runs in this convention and in no plan Janino compiles.** IKVM
names a CLR class `cli.Namespace.Type`; `EnumerableConvention` writes that name into generated Java source
and Janino does not resolve a `cli.` name, so the plan fails to compile — for a grouped aggregate and a
windowed one alike. A tree holds the method rather than its name. So a UDF is the one thing the
differential tests cannot use Calcite as the oracle for.

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

**Never credit Claude as a contributor.** No `Co-Authored-By` trailer, no "generated with" line, no bot
attribution — not in a commit message, not in a pull request body, not anywhere in the history. The commit
message says what changed and why; who typed it is not part of the record.
