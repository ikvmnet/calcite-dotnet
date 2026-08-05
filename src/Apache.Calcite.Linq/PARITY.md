# `ClrEnumerableConvention` against `EnumerableConvention`

Class by class and member by member, against **Calcite 1.42.0** — the version the projects reference. The
source read is `git archive calcite-1.42.0 core/src/main/java/org/apache/calcite/adapter/enumerable` out of
`D:\calcite`, not that repository's working tree, which is 1.43.0-SNAPSHOT and differs. The package is 109
files plus seven in `enumerable/impl`.

**How this was moved from 1.41 to 1.42.** Not by re-reading 109 files: by diffing the two tags over the
package — 21 files, 985 insertions, 97 deletions — and re-deriving only what that diff touched. Every row
below whose class the diff did not touch stands as it was verified at 1.41. The four files 1.42 added and
the seventeen it changed were each read and checked against the port. 9.21 records what that found.

**Read the tag, then check the assembly.** They are not the same. `RelOptUtil.registerDefaultRules` registers
`EnumerableRules.ENUMERABLE_RULES` in the compiled assembly — measured at 1.41.0 by counting the planner's
rules before and after the call — and the tag's text of that method gives no sign of it until its last third.

Three columns throughout: Calcite's member, ours, and what differs. `—` means nothing corresponds, and every
one of those is accounted for in §5 — outstanding, nothing yet argues for it — or in §6, with the argument.

**Every point is numbered `section.item`** — 2.7 is the merge join, 6.9 is the row-format divergence, 9.3 is
what the rebuild corrected about the join selector. Cite them by number. The numbers are the addressing
scheme, not a ranking; nothing about 6.1 makes it more important than 6.15. When a point is resolved it is
struck from its section, and the numbers after it are **not** renumbered until the whole file is rebuilt —
so a number cited in a commit message keeps meaning what it meant.

**Rebuilt from the code, 2026-08-04.** Every row below was re-derived from the two sources rather than
carried over. What that pass corrected is listed in §9, because a wrong row in this file is worse than a
missing one: it is a divergence that looks accounted for.

**§1 to §4 re-derived against `calcite-1.42.0`, 2026-08-05**, which is what 9.20 said was still owed: not by
diffing 1.41 against 1.42 and trusting the untouched rows, but by reading each Calcite class's `implement`
and each of ours side by side. It found five divergences no row in this file recorded, four of them wrong
answers rather than missing features, and it found them because a diff-driven pass cannot: three of the five
are a single *call* inside a body this file had already marked as verified — two of them changed by 1.42,
the third never matching any release.
**Nothing was accepted from argument alone.** Thirteen queries were run through both conventions to decide
what the reading only suspected; seven disagreed, and each one is quoted with its rows in 5.7 to 5.11. The
four that agreed are recorded too, in 9.24, because a suspicion that measures clean is worth as much as one
that does not.

---

## 1. The convention, the interface, the implementor

### 1.1 `EnumerableConvention` → `ClrEnumerableConvention`

| Calcite | ours | |
|---|---|---|
| `INSTANCE` (enum constant) | `Instance` (static field, private ctor) | C# has no enum implementing an interface |
| `COST_MULTIPLIER = 1.0d` | `CostMultiplier = 1.0d` | read by `ClrEnumerableWindow.computeSelfCost` and `EnumerableToClrEnumerableConverter`; Calcite's is read by `EnumerableWindow.computeSelfCost` and nothing else |
| `toString`, `getInterface`, `getName`, `getTraitDef`, `satisfies`, `register` | `Convention.Impl` base, given `"CLR_ENUMERABLE"` and `typeof(ClrEnumerableRel)` | equivalent |
| `enforce` | `enforce` | statement for statement |
| `canConvertConvention` | `canConvertConvention` | both false |
| `useAbstractConvertersForConversion` | `useAbstractConvertersForConversion` | both true |
| `getRelFactories` | `getRelFactories` | same four factories, from `ClrEnumerableRelFactories` |

### 1.2 `EnumerableRelFactories` → `ClrEnumerableRelFactories`

| Calcite | ours | |
|---|---|---|
| `ENUMERABLE_TABLE_SCAN_FACTORY` / `TableScanFactoryImpl.createScan` | `ClrEnumerableTableScanFactory` / `TableScanFactoryImpl.createScan` | |
| `ENUMERABLE_PROJECT_FACTORY` / `ProjectFactoryImpl.createProject` | same shape | plus the deprecated 4-argument overload, which C# must implement and Java defaults |
| `ENUMERABLE_FILTER_FACTORY` / `FilterFactoryImpl.createFilter` | same shape | plus the deprecated 2-argument overload |
| `ENUMERABLE_SORT_FACTORY` / `SortFactoryImpl.createSort` | same shape | plus the deprecated 5-argument overload |

### 1.3 `EnumerableRel` → `ClrEnumerableRel`

| Calcite | ours | |
|---|---|---|
| `implement(EnumerableRelImplementor, Prefer)` | `Implement(ClrEnumerableRelImplementor, ClrEnumerablePrefer)` | returns `ClrEnumerableResult`, whose payload is an `Expression` rather than a `BlockStatement`; 6.1 |
| `Result` (nested class) | `ClrEnumerableResult` | same three fields |
| `Prefer` (nested enum) | `ClrEnumerablePrefer` | own type, 1.5 and 6.6 |
| `passThroughTraits` default → null | same | |
| `deriveTraits` default → null | same | |
| `getDeriveMode` default → `LEFT_FIRST` | same | |
| — | `passThrough`, `derive` ×2 forwarded to `PhysicalNode.__DefaultMethods` | C# does not inherit the defaults of an interface IKVM compiled |

### 1.4 `EnumerableRules` → `ClrEnumerableRules`

| Calcite | ours | |
|---|---|---|
| a `public static final` field per rule | a `public static readonly` field per rule | one instance, so a caller can add or remove a named rule |
| `ENUMERABLE_RULES` (public list, 24 entries) | private list of 24 behind `Rules()` | the accessor is the counterpart of `rules()`; 3.3 has the reckoning |
| `rules()` | `Rules()` | |
| `ENUMERABLE_LIMIT_SORT_RULE`, `ENUMERABLE_SORTED_AGGREGATE_RULE`, `ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE` — fields, none of the three in the list | the same three, as fields and out of the list | Calcite registers none of the three by default anywhere in core, and neither does this; the harness turns each on per test and per side |
| `RelOptRules.CALC_RULES` | `CalcRules()` | rule for rule and in the same order, less `Bindables.FROM_NONE_RULE`, which belongs to a convention not ported |

### 1.5 `EnumerableRel.Prefer` → `ClrEnumerablePrefer`

| Calcite | ours | |
|---|---|---|
| `ARRAY`, `ARRAY_NICE`, `CUSTOM`, `CUSTOM_NICE`, `ANY` | `Array`, `ArrayNice`, `Custom`, `CustomNice`, `Any` | |
| `preferCustom`, `preferArray`, `prefer`, `of` | `PreferCustom`, `PreferArray`, `Prefer`, `Of` | extension methods on `ClrEnumerablePrefers`: a C# enum carries no methods |
| — | `ToCalcite`, `FromCalcite` | one call per converter, and nowhere else |

### 1.6 `EnumerableRelImplementor` → `ClrEnumerableRelImplementor`

Calcite's extends `JavaRelImplementor`; the `getRexBuilder`, `getTypeFactory` and `getRootExpression` rows
are that base's.

| Calcite | ours | |
|---|---|---|
| `map` (public field) | `Map` | |
| `corrVars` | `corrVars` | |
| `allCorrelateVariables` (protected field) | `AllCorrelateVariables` | a `DelegateFunction1` where Java had a method reference |
| `stashedParameters` | **—** | nothing is stashed; see `Stash` |
| ctor | ctor | |
| `visitChild` | `VisitChild` | Calcite's asserts the child is the parent's input at that ordinal; a Java `assert` is off by default and ours omits it |
| `implementRoot` | `ImplementRoot` | returns a `LambdaExpression`, not a `ClassDeclaration`, and does not walk the block for the `GotoStatement` carrying the value, because the value is the expression. It wraps a failing node in `IllegalStateException("Unable to implement " + <the plan>)` as Calcite's does, catching `System.Exception` where Calcite catches `RuntimeException` — .NET has no checked exceptions, so that is the same set — and carrying the original as the inner exception, .NET having no suppressed one |
| `classDecl`, `TypeFinder`, `TypeRegistrar` | **—** | they exist to declare synthetic types as members of the generated class; `SyntheticRecordEmitter` emits them at runtime instead; 6.15 |
| `stash` | `Stash` | ours returns a constant: an expression tree can hold the object Janino could not name |
| `registerCorrelVariable` / `clearCorrelVariable` / `getCorrelVariableGetter` | `RegisterCorrelVariable` / `ClearCorrelVariable` / `GetCorrelVariableGetter` | |
| the `InputGetter` lambda `registerCorrelVariable` puts in the map | `CorrelInputGetter` (nested class) | same two statements |
| `result` | `Result` | |
| `getConformance` | `Conformance` | |
| `getRexBuilder` | `RexBuilder` | |
| `getTypeFactory` | `TypeFactory` | |
| `getRootExpression` | `RootExpression` | both return `DataContext.ROOT` |
| — | `Root`, `Translator` | ours: the CLR parameter the `DataContext` arrives by, and the one `ExpressionTranslator` that serves the whole plan |
| — | `BoxScalars` | CLR only: a one-column result of a primitive would otherwise be boxed the CLR way. Not `EnumerableInterpretable.box`, which is something else — see 1.7 |

### 1.7 `EnumerableInterpretable` → `ClrEnumerableInterpretable`

| Calcite | ours | |
|---|---|---|
| `toBindable(parameters, spark, rel, prefer)` | `ToBindable(parameters, spark, rel, prefer)` | same four arguments in the same order; the Spark branch is refused rather than taken, 6.16 |
| `getBindable`, `compileToBindable`, `StaticFieldDetector`, `BINDABLE_CACHE` | **—** | Janino compilation and its cache; there is nothing to port; 6.15 |
| `box`, `BoxEnumerable`, `BoxEnumerator` | **—** | wraps each row in a one-element `Object[]` for the interpreter, which is not ported |
| `EnumerableNode`, and the class being an `InterpretableRel` (ctor, `copy`, `implement(InterpreterImplementor)`) | **—** | the interpreter is not ported; ours is a static helper |
| — | `ClrBindable` | ours: the compiled delegate as a `Bindable` and `Typed`, which is what Janino's generated class was |

---

## 2. Nodes

`EnumerableX` → `ClrEnumerableX` throughout. Calcite's 34 node classes are all accounted for: 29 in 2.1 to
2.20, `EnumerableInterpretable` in 1.7, `EnumerableInterpreter` in 2.21, and `EnumerableMatch`,
`EnumerableTableModify` and `EnumerableBindable` in 5.1 — the last of those three by decision rather than
difficulty, which is 6.18.

### Scan and values

#### 2.1 `EnumerableTableScan`

| Calcite | ours | |
|---|---|---|
| ctor, asserting the convention and `canHandle` | ctor, both assertions kept | |
| `create` | `Create` | statement for statement |
| `canHandle(Table)`, `canHandle(RelOptTable)` | `CanHandle(Table)`, `CanHandle(RelOptTable)` | |
| `deduceElementType` | reused from Calcite (public static) | |
| `deduceFormat` | `DeduceFormat` | |
| `getExpression(PhysType)` | inlined in `Implement`, its `IllegalStateException` and message included | |
| `toEnumerable`, `toRows`, `fieldExpression`, `format`, `hasCollectionField` | `ToEnumerable`, `ToRows`, `FieldExpression`, `Format`, `HasCollectionField` | `toRows` reshapes to match an optimised physical type, which is half of the argument in 6.9 |
| `passThrough`, `getDeriveMode` | `passThrough`, `getDeriveMode` | |
| `copy`, `implement` | `copy`, `Implement` | |

#### 2.2 `EnumerableValues`

| Calcite | ours | |
|---|---|---|
| ctor, `create`, `copy`, `implement` | same four | |
| `passThrough` (the tuples must already be in that order), `getDeriveMode` | `passThrough`, `getDeriveMode` | |

### 2.3 Project, filter, calc

| Calcite | ours | |
|---|---|---|
| `EnumerableProject`: 2 ctors, `create`, `copy`, `implement`, `passThroughTraits`, `deriveTraits` | 1 ctor and the rest | the second ctor is deprecated in Calcite |
| `EnumerableFilter`: ctor, `create`, `copy`, `implement`, `passThroughTraits`, `deriveTraits` | all six | |
| `EnumerableCalc`: 2 ctors, `create`, `copy`, `implement`, `getProgram`, `passThroughTraits`, `deriveTraits` | 1 ctor and the rest, plus `Exps` | `getProgram` is `Calc`'s either way; `Exps` is the local Calcite writes twice inline |

`ClrEnumerableProject.Implement` and `ClrEnumerableFilter.Implement` throw, as Calcite's do: the calc rules
run afterwards and a calc is never worse.

### 2.4 Sort and limit

| Calcite | ours | |
|---|---|---|
| `EnumerableSort`: ctor, `create`, `copy`, `implement` | all four | one argument differs; 6.9 |
| `EnumerableLimit`: ctor, `create`, `copy`, `explainTerms`, `implement`, `getExpression` | all six, `getExpression` as `Count` | one argument differs; 6.9 |
| `EnumerableLimitSort`: ctor, `create`, `copy`, `implement` | all four | both call the one `Count`, as both call the one `getExpression`; one argument differs; 6.9 |

### Joins

#### 2.5 `EnumerableHashJoin`

| Calcite | ours | |
|---|---|---|
| 2 ctors, `create`, `copy`, `implement` | 1 ctor and the rest | |
| `implementHashJoin`, `implementHashSemiJoin`, `implementHashMarkJoin` | `ImplementHashJoin`, `ImplementHashSemiJoin`, `ImplementHashMarkJoin` | the mark-join path is 1.42; see 2.19 for what LEFT_MARK is. All three key on `generateNullAwareAccessor`, which two of them did not until 5.7 |
| `EnumerableDefaults.leftMarkHashJoin`, `leftMarkHashJoinOptimized`, `leftMarkHashJoinGeneral`, `HashTableWithNullSafeKeySet` | `ClrEnumerableDefaults.LeftMarkHashJoin` | both algorithms, in one method: the build side is the same table either way, and only the miss is decided differently |
| `EnumerableDefaults.hashJoin`, `hashEquiJoin_`, `hashJoinWithPredicate_` | `ClrEnumerableDefaults.HashJoin` | one method over typed delegates; 6.2. It kept a build-side null key only after 5.8, and the unmatched tail of a right join depends on it |
| `computeSelfCost`, `passThroughTraits`, `deriveTraits`, `getDeriveMode` | all four | |
| — | `NullAwareAccessor`, `Accessor`, `Predicate` | ours, factored out of the two bodies. `NullAwareAccessor` is what all three key on; `Accessor` is the plain one, and the mark join's null-safe lookup is the only caller Calcite has for it |

#### 2.6 `EnumerableNestedLoopJoin`

| Calcite | ours | |
|---|---|---|
| 2 ctors, `create`, `copy`, `implement`, `computeSelfCost`, `passThroughTraits`, `deriveTraits`, `getDeriveMode` | 1 ctor and the rest | |
| `implementNLJoin`, `implementNLMarkJoin` | `ImplementNLJoin`, `ImplementNLMarkJoin` | the mark-join path is 1.42 |
| `EnumerableDefaults.leftMarkNestedLoopJoin` | `ClrEnumerableDefaults.LeftMarkNestedLoopJoin` | one entry point onto `LeftMarkJoin`, as Calcite writes it onto `leftMarkJoinInternal` |

#### 2.7 `EnumerableMergeJoin`

| Calcite | ours | |
|---|---|---|
| ctor, with both sanity checks and the join-type refusal | ctor, all three | |
| `joinInfo`, hiding `Join.joinInfo`, built by `JoinInfo.createWithStrictEquality` | the same field, hidden with `new` | 1.42; the algorithm stops at a null, so IS NOT DISTINCT FROM cannot be a join key |
| `isMergeJoinSupported`, `getCollation`, `getCollations` | `IsMergeJoinSupported`, `GetCollation`, `GetCollations` | |
| `create`, `copy`, `implement`, `computeSelfCost`, `getDeriveMode` | all five | |
| `passThroughTraits` (six cases), `deriveTraits` | both, and the six cases | one guard added; 6.8 |
| `buildMapping`, `extendCollation`, `intersectCollationAndJoinKey` | `BuildMapping`, `ExtendCollation`, `IntersectCollationAndJoinKey` | private there, ported; 6.5 |
| `EnumerableDefaults.mergeJoin`, `isMergeJoinSupported`, `compareNullsLastForMergeJoin` | `ClrEnumerableDefaults.MergeJoin`, `IsMergeJoinSupported`, `CompareNullsLastForMergeJoin` | the algorithm over typed delegates, as an iterator rather than an enumerator with a state machine; 6.2 |
| — | `AllLessThan`, `AllAtLeast`, `Predicate` | ours, factored out — the first two are the key-range tests `passThroughTraits` writes inline in four of its six cases |

#### 2.8 `EnumerableBatchNestedLoopJoin`

| Calcite | ours | |
|---|---|---|
| ctor, `create`, `copy`, `implement` | all four | |
| `computeSelfCost`, `passThroughTraits`, `deriveTraits`, `getDeriveMode`, `explainTerms` | all five | |
| `EnumerableDefaults.correlateBatchJoin` | `ClrEnumerableDefaults.CorrelateBatchJoin` | the algorithm over typed delegates as an iterator; the right input is read lazily across the batch's first left row and cached for the rest of the batch, as Calcite reads it, the semi and anti drain included; 6.2 |

#### 2.9 `EnumerableAsofJoin`

| Calcite | ours | |
|---|---|---|
| ctor, `create`, `copy(traitSet, condition, …)` (throws), `copy(traitSet, inputs)`, `implement` | all five | `implement` keys on `generateAccessorWithoutNulls`, which ours did not until 5.7 |
| `computeSelfCost`, `passThroughTraits`, `deriveTraits` | all three | |
| `generateTimestampComparator`, `getTimestampFieldIndex` | `GenerateTimestampComparator`, `GetTimestampFieldIndex` | |
| `EnumerableDefaults.asofJoin` | `ClrEnumerableDefaults.AsofJoin` | the algorithm reimplemented over typed delegates, as the hash join's is. The index is a `java.util.HashMap`, because the emitted order is that map's; 6.10 |

#### 2.10 `EnumerableCorrelate`

| Calcite | ours | |
|---|---|---|
| ctor, `create`, `copy`, `implement`, `passThroughTraits`, `deriveTraits`, `getDeriveMode` | all seven | the sequences are boxed, as every other join here boxes them; 6.12 |

### 2.11 Set operations and recursion

| Calcite | ours | |
|---|---|---|
| `EnumerableUnion` / `EnumerableIntersect` / `EnumerableMinus`: ctor, `copy`, `implement` | the same three each | |
| `EnumerableMergeUnion`: ctor (with both checks), `create`, `copy`, `implement` | all four | extends the union node, as Calcite's extends `EnumerableUnion`. The ctor checks `getCollations()` rather than `getCollation()`, which is 1.42: a slot may hold a `RelCompositeTrait`, and each required collation must be satisfied by at least one of the input's |
| `EnumerableDefaults.mergeUnion`, `MergeUnionEnumerator` | `ClrEnumerableDefaults.MergeUnion` | the k-way merge as an iterator, duplicate set cleared per key as Calcite clears it |
| `EnumerableRepeatUnion`: ctor, `copy`, `implement` | the same three | ours is public where Calcite's ctor is package private; one argument differs, 6.9. `Implement` threw on the only query that reaches it until 5.9 |
| `EnumerableTableSpool`: ctor, `create`, `copy`, `implement` (refuses anything but LAZY) | all four, the refusal included | one argument differs; 6.9. Both read the table out of the bound root schema; ours held the collection as a plan-time constant until 5.9 |

### 2.12 Collect, uncollect, table function

| Calcite | ours | |
|---|---|---|
| `EnumerableCollect`: 2 ctors, `create`, `copy`, `implement` | 1 ctor and the rest | |
| `EnumerableUncollect`: 3 ctors, `create`, `copy`, `implement` | 1 ctor and the rest | the two extra ctors are deprecated or take the item aliases Calcite's `create` leaves empty |
| `EnumerableTableFunctionScan`: ctor, `copy`, `implement`, `defaultTableFunctionImplement`, `isQueryable` | the same five, `defaultTableFunctionImplement` as `DefaultTableFunctionImplement` | `isQueryable` is a faithful port and returns false for a `ScannableTable`, which is what makes 6.9 reachable |
| `isImplementorDefined`, `tvfImplementorBasedImplement` | `IsImplementorDefined`, `TvfImplementorBasedImplement` | the window-TVF path, refused in the rule's `matches` until 5.2. `RexToLixTranslator.translateTableFunction` is public and does the work; the sequence crosses into Java for that call |

Nothing is ours-only here. The row format both nodes compute inline is Calcite's four cases in Calcite's
order, and its `PhysTypeImpl.of(…, format, false)` is Calcite's own — this is the one node where Calcite
already refuses to re-optimise, which is half of 6.9's argument.

### Aggregate

Two classes on both sides.

#### 2.13 `EnumerableAggregateBase` → `ClrEnumerableAggregateBase`

| Calcite | ours | |
|---|---|---|
| ctor | ctor | |
| `hasOrderedCall` | `HasOrderedCall` | |
| `declareParentAccumulator` | `DeclareParentAccumulator` | |
| `implementLambdaFactory` | `ImplementLambdaFactory` | **returns** the factory where Calcite declares it into the block it was handed, and takes the adders as a list where Calcite reads them off `AggImpState.accumulatorAdder` — that field is typed as a linq4j `Expression` and ours are CLR ones |
| `createAccumulatorAdders` | `CreateAccumulatorAdders` | returns the adders for the same reason, and takes a CLR parameter beside each linq4j one: a lambda is built against the first and the tree is bound to the second |
| `createAggStateTypes` | `CreateAggStateTypes` | three parameters added; see below |
| `AggContextImpl` (protected inner class) | `ClrAggContext` (nested, takes what the inner class read off its enclosing node) | ported, being unreachable; 6.5 |
| the anonymous `AggAddContextImpl` inside `createAccumulatorAdders` | `ClrAggAddContext` **extends** the public `AggAddContextImpl` | reused |
| `PhysTypeImpl.of(typeFactory, javaRowType)`, called from `EnumerableAggregate.implement` | `AccumulatorPhysType` | package private there; this is the public route to the same thing; 6.5 |
| — | `Function0Of`, `Function1Of`, `Function2Of` | a delegate has to be wrapped where Java passed a lambda to a functional interface |

Ours are static, because `ClrEnumerableWindow` needs two of them and is not an aggregate. Three of Calcite's
four are instance methods that never read `this`, so making them static costs nothing.
**`createAggStateTypes` is the exception**: it constructs `AggContextImpl`, which is an inner class and reads
`getInput().getRowType()`, `groupSet` and `groupSets` off the enclosing node. Ours takes those three as
parameters and hands them to `ClrAggContext`. That is the whole of the signature difference.

#### 2.14 `EnumerableAggregate` → `ClrEnumerableAggregate`

| Calcite | ours | |
|---|---|---|
| 2 ctors, `copy`, `implement` | 1 ctor, `copy`, `Implement` | |
| `implement`'s four branches: grouping sets, no group key, distinct-only, plain group by | all four | two of them were missing until 5.10 and 5.11, and a `ROLLUP` answered with the wrong rows |

The constructor refuses what the node cannot implement — a distinct call, a within-distinct call, and an
aggregation `RexImpTable` has no implementor for — by throwing `InvalidRelException` for the rule to catch.
Ours refuses the same three. An aggregate call carrying its own ordering is implemented, not refused:
`hasOrderedCall` picks `LazyAggregateLambdaFactory` over a `SourceSorter` per ordered call and a
`BasicLazyAccumulator` per unordered one, which are Calcite's classes and public.

#### 2.15 `EnumerableSortedAggregate` → `ClrEnumerableSortedAggregate`

| Calcite | ours | |
|---|---|---|
| ctor (asserts the convention and nothing else), `copy`, `passThroughTraits`, `implement` | all four | one guard added, in 6.8 |
| `EnumerableDefaults.sortedGroupBy`, `SortedAggregateEnumerator` | `ClrEnumerableDefaults.SortedGroupBy` | one walk of the input, holding only the accumulator of the group being read; 6.2 |

The sorted aggregate's constructor does **not** throw `InvalidRelException` and refuses nothing; only
`EnumerableAggregate`'s does. Its `implement` refuses a non-simple group with `Util.needToImplement`, and
ours with the same refusal spelled as an `UnsupportedOperationException`.

### 2.16 `EnumerableWindow` → `ClrEnumerableWindow`

| Calcite | ours | |
|---|---|---|
| ctor, `copy(traitSet, inputs)`, `copy(constants)`, `implement`, `computeSelfCost` | all five | |
| `translateBound`, `declareAndResetState`, `implementResult`, `WindowRelInputGetter`, `getBlockBuilderWinAggFrameResultContextFunction` and the anonymous `WinAggFrameResultContext` it returns | `TranslateBound`, `DeclareAndResetState`, `ImplementResult`, `WindowRelInputGetter`, `ClrWinAggFrameResultContext` | five private members, ported; 6.5 |
| `getPartitionIterator`, `getRowCollationKey`, `implementAdd`, `buildExcludeGuard` | `ClrEnumerableDefaults.Window` and `WindowLoop` | four more private members; the loop is owned rather than generated — 6.2, and it is why 2.13's accumulator helpers are shared |
| `sampleOfTheGeneratedWindowedAggregate` | **—** | a documentation method |
| `WinAggAddContextImpl`, `WinAggResultContextImpl` (public, `impl`) | `ClrWinAggAddContext`, `ClrWinAggResultContext` **extend them** | reused |
| `WinAggContext` (interface) | `ClrWinAggContext` implements it | |
| the aggregate state and last result, as locals of the generated method | fields of a synthetic record threaded through the loop | 6.3 |
| — | `ImplementGroup`, `Hoist`, `PartitionSelector`, `WithReturn`, `RexArguments` | ours. `ImplementGroup` is the per-group body Calcite writes inline in `implement`; `RexArguments` is what its anonymous `WinAggAddContextImpl` computes inline |

### 2.17 `EnumerableTraitsUtils` → `ClrEnumerableTraitsUtils`

| Calcite | ours | |
|---|---|---|
| `isCollationOnTrivialExpr` | `IsCollationOnTrivialExpr` | |
| `passThroughTraitsForProject`, `deriveTraitsForProject` | `PassThroughTraitsForProject`, `DeriveTraitsForProject` | |
| `passThroughTraitsForJoin`, `deriveTraitsForJoin` | `PassThroughTraitsForJoin`, `DeriveTraitsForJoin` | |

Package private in Calcite, so ported — 6.5. One substitution: Calcite writes `collation.apply(mapping)`,
whose implementation is `RexUtil.apply(mapping, this)`; the interface method is a generic default that IKVM
erases, so the public static is called instead. It is the same call.

### 2.19 `EnumerableConditionalCorrelate` → `ClrEnumerableConditionalCorrelate`

New in 1.42, with `JoinRelType.LEFT_MARK`.

| Calcite | ours | |
|---|---|---|
| ctor, `create`, `copy(… condition)`, `copy(…)` (throws), `implement` | all five, the refusal included | |
| `passThroughTraits`, `deriveTraits`, `getDeriveMode` | all three | |
| `EnumerableDefaults.correlateLeftMarkJoin` | `ClrEnumerableDefaults.CorrelateLeftMarkJoin` | the correlated entry point onto the one walk |

A correlate carrying a condition, which is what a correlated IN, SOME or EXISTS becomes when the sub-query
rules rewrite it to a mark join rather than to a plain correlate. Its join type is always LEFT_MARK; Calcite
refuses every other and so does this.

**A mark join returns every left row with one column appended, and that column is three-valued** — true
where some right row matched, false where none did, null where a comparison was unknown. The third value is
the point: it is what makes `x IN (…)` over a nullable column answer UNKNOWN rather than FALSE. So the
predicate is `NullablePredicate2` rather than `Predicate2` in Calcite, and `java.lang.Boolean` rather than
`bool` here, and `EnumUtils.generatePredicate` gained the `nullable` overload that produces it.

### 2.20 `EnumerableCombine` → `ClrEnumerableCombine`

New in 1.42.

| Calcite | ours | |
|---|---|---|
| ctor, `copy`, `implement`, `computeSelfCost`, `explainTerms`, `deriveRowType` | ctor, `copy`, `Implement`; the rest are `Combine`'s either way | |

Combines several query roots into one, for multi-root optimisation in the planner: one column per query,
each row holding that query's values for that row index as a map, and the row count the largest of the
inputs. Everything it calls is public Calcite runtime — `SqlFunctions.map`,
`SqlFunctions.combineQueryResults` — so the node is the tree `EnumerableCombine` describes and nothing more.

**No SQL statement produces a `Combine`.** A caller builds one with `RelBuilder.combine`, so the
differential harness cannot reach this node by parsing a query. `ShouldCombineTwoQueries` builds one, and
runs it: three names against two ids, so the shorter query runs out and the row that has nothing to hold is
null.

**`RelBuilder.create` opens a `jdbc:calcite:` connection, and that is not an obstacle.** A `RelBuilder`
needs a `RelOptCluster` and a `RelOptSchema`, which it cannot invent; `Frameworks.withPrepare` produces them
by standing up the prepare machinery, and that machinery is anchored on a `CalciteServerStatement`. The
connection is not for I/O — it is the object that owns the root schema and the type factory. It failed here
only because the test project had never put calcite-core on IKVM's boot class path, so the driver's
reflective `Class.forName` could not find its factory; the AdoNet tests already do that in one line, and this
one does now. `RelBuilder.proto(context).create(cluster, schema)` is the other way in, and takes neither.

### 2.21 `EnumerableInterpreter` → `ClrEnumerableInterpreter`

| Calcite | ours | |
|---|---|---|
| ctor (asserting the convention), `create`, `copy`, `computeSelfCost`, `implement` | all five | |
| `EnumerableInterpreterRule` | `ClrEnumerableInterpreterRule`, same 0.5 factor | 3.1 says why it is a field rather than a member of `Rules()` |

Calcite's `Interpreter` does the work and is public, so nothing about interpreting is ours: the node is
`new Interpreter(root, stash(input))` either way. Two differences, both the boundary rather than the
algorithm. The node is stashed as a **constant** rather than through the `DataContext`, which is 1.6's
account of `Stash`. And the slice for a one-column row happens on the Java side, where Calcite slices it, so
that what crosses is already the shape the physical type describes — `FromJava` then converts each value to
the type that physical type gives it, which for an optimised ARRAY of one column is the column's own.
Slicing after the crossing instead leaves an `IEnumerable<object>` where the physical type says `int`, and
the calc above it will not take it. Same family as 6.9.

### 2.18 Converters — ours only

`ClrEnumerableToEnumerableConverter` and `EnumerableToClrEnumerableConverter`, with a rule each, are what
lets one plan hold both conventions. They have no Calcite counterpart. Both override `computeSelfCost` with
the multiplier of the convention they produce — `EnumerableConvention.COST_MULTIPLIER` and
`ClrEnumerableConvention.CostMultiplier`, which are both 1.0.

---

## 3. Rules

### 3.1 Rule for rule

| Calcite rule | ours | `convert` / `onMatch` |
|---|---|---|
| `EnumerableTableScanRule` | `ClrEnumerableTableScanRule` | identical, `canHandle` predicate and the expression check both |
| `EnumerableValuesRule` | `ClrEnumerableValuesRule` | identical: create, then copy onto the logical node's traits |
| `EnumerableProjectRule` | `ClrEnumerableProjectRule` | identical, `matches` included |
| `EnumerableFilterRule` | `ClrEnumerableFilterRule` | identical |
| `EnumerableCalcRule` | `ClrEnumerableCalcRule` | identical |
| `EnumerableAggregateRule` | `ClrEnumerableAggregateRule` | identical, `InvalidRelException` included |
| `EnumerableSortRule` | `ClrEnumerableSortRule` | identical |
| `EnumerableLimitRule` | `ClrEnumerableLimitRule` | identical, Calcite's own operand taken from its config |
| `EnumerableLimitSortRule` | `ClrEnumerableLimitSortRule` | identical, and out of the default list on both sides; lives in `ClrEnumerableCollectRules.cs` for no reason worth keeping |
| `EnumerableJoinRule` | `ClrEnumerableJoinRule` | identical: one rule, hash or nested loop, condition rearranged equi-first |
| `EnumerableAsofJoinRule` | `ClrEnumerableAsofJoinRule` | identical |
| `EnumerableCorrelateRule` | `ClrEnumerableCorrelateRule` | identical |
| `EnumerableConditionalCorrelateRule` | `ClrEnumerableConditionalCorrelateRule` | identical; 1.42 |
| `EnumerableCombineRule` | `ClrEnumerableCombineRule` | identical; 1.42 |
| `EnumerableUnionRule` | `ClrEnumerableUnionRule` | identical, including the cluster's trait set rather than the union's |
| `EnumerableIntersectRule` | `ClrEnumerableIntersectRule` | identical, `convertList` included |
| `EnumerableMinusRule` | `ClrEnumerableMinusRule` | identical |
| `EnumerableRepeatUnionRule` | `ClrEnumerableRepeatUnionRule` | identical |
| `EnumerableTableSpoolRule` | `ClrEnumerableTableSpoolRule` | identical, the spool's own read and write types passed through |
| `EnumerableWindowRule` | `ClrEnumerableWindowRule` | identical |
| `EnumerableTableFunctionScanRule` | `ClrEnumerableTableFunctionScanRule` | `convert` identical; ours adds `matches`, refusing a window TVF — 5.2 |
| `EnumerableCollectRule` | `ClrEnumerableCollectRule` | identical |
| `EnumerableUncollectRule` | `ClrEnumerableUncollectRule` | identical |
| `EnumerableFilterToCalcRule` | `ClrEnumerableFilterToCalcRule` | present, operand re-pointed at this convention's filter |
| `EnumerableProjectToCalcRule` | `ClrEnumerableProjectToCalcRule` | present, operand re-pointed at this convention's project |
| `EnumerableMergeJoinRule` | `ClrEnumerableMergeJoinRule` | identical |
| `EnumerableMergeUnionRule` | `ClrEnumerableMergeUnionRule` | identical, `matches` and the pushed-down limit included |
| `EnumerableSortedAggregateRule` | `ClrEnumerableSortedAggregateRule` | one refusal added; 6.11 |
| `EnumerableBatchNestedLoopJoinRule` | `ClrEnumerableBatchNestedLoopJoinRule` | identical, `matches` included. Its two `RexShuttle`s are anonymous in Calcite and named `FirstCondition` and `OtherCondition` here, after the locals Calcite assigns them to — C# has no anonymous class, so there is no name to match |
| `EnumerableMatchRule` | **—** | blocked; 5.1 and 6.14 |
| `EnumerableTableModifyRule` | **—** | the convention is read-only; 5.1 |
| `EnumerableInterpreterRule` | `ClrEnumerableInterpreterRule` | a field a caller registers, as `TO_INTERPRETER` is: Calcite's comes from `RelOptUtil.registerDefaultRules`, which registers Calcite's own |
| `EnumerableBindable.EnumerableToBindableConverterRule` | **—** | its node cannot be written in C#; 6.17 |

### 3.2 The rule classes are all accounted for

All 33 rule classes 1.42 has in the package: 32 files plus the one nested in `EnumerableBindable`.

### 3.3 Rule sets

`EnumerableRules.ENUMERABLE_RULES` is **26** at 1.42: the 24 of 1.41 — the merge union rule among them —
plus `ENUMERABLE_CONDITIONAL_CORRELATE_RULE` and `ENUMERABLE_COMBINE_RULE`. Three rule fields sit outside
the list — `ENUMERABLE_LIMIT_SORT_RULE`, `ENUMERABLE_SORTED_AGGREGATE_RULE` and
`ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE` — and nothing in core registers any of the three; a caller turns
them on.

`ClrEnumerableRules.Rules()` is **26**: those 26 less the match and table modify rules, which is 24, plus
the two converters. It was 27 for part of 2026-08-05, while the table modify node existed — 6.18. The three rules Calcite leaves out of its list are left out of this one. **There is no
membership difference**; the limit-sort rule used to be in this list and 9.18 is why it is not. Re-counted
2026-08-05 from both sources: 26 against 26.

`ClrEnumerableRules.EnumerableRuleList`'s own remark still says "its 24 less the match and table modify
rules", which is 9.1's superseded count wearing 9.20's correction — the membership it describes is right and
the number is two short. A comment, not a difference, and noted here because §3.3 is where anyone would come
to check it.

### 3.4 Calc rules

`CalcRules()` is the counterpart of `RelOptRules.CALC_RULES` — rule for rule and in Calcite's order, less
`Bindables.FROM_NONE_RULE` — plus nothing. It is run as a second pass, which is what `Programs.standard`
does with Calcite's.

---

## 4. Reused rather than ported

Calcite's, used directly, and the reason the two conventions agree on what a row is.

### 4.1 Named in this project's code

`PhysType`, `PhysTypeImpl`, `JavaRowFormat`, `RexToLixTranslator`, `RexImpTable`, `AggContext`,
`AggImpState`, `AggregateLambdaFactory`, `BasicAggregateLambdaFactory`, `BasicLazyAccumulator`,
`LazyAggregateLambdaFactory`, `SourceSorter`, and four of the `enumerable.impl` classes —
`AggAddContextImpl`, `AggResultContextImpl`, `WinAggAddContextImpl`, `WinAggResultContextImpl`.

### 4.2 Reached through those rather than named

`JavaRelImplementor`, `NestedBlockBuilder(Impl)`, `NullPolicy`, `CallImplementor`, `NotNullImplementor`,
`ReflectiveCallNotNullImplementor`, `TableFunctionCallImplementor`, `AggImplementor`, `AggAddContext`,
`AggResetContext`, `AggResultContext`, `StrictAggImplementor`, `StrictWinAggImplementor`, the seven
`WinAgg*` interfaces, and `AggResetContextImpl`. Nothing here is ported and nothing here is a gap; they are
what `RexImpTable` and the contexts are made of.

### 4.3 Neither

`MatchImplementor` and `MatchUtils` serve `EnumerableMatch` alone, which is not written — 5.1.

### 4.4 `EnumUtils`, half reused and half ported, because half of it is package private

| `EnumUtils` | `ClrEnumUtils` |
|---|---|
| `javaClass`, `fieldTypes`, `fieldRowTypes` ×2 | `JavaClass`, `FieldTypes`, `FieldRowTypes` ×2 |
| `joinSelector`, `generatePredicate` ×2 | `JoinSelector`, `GeneratePredicate` ×2 |
| `markJoinSelector` | `MarkJoinSelector` | 1.42 |
| `toLinq4jJoinType` | `ToLinq4jJoinType` |
| every `RexToLixTranslator.translate` overload | `Translate`, by way of the public `translateList` |
| `joinSelectorCompact`, `shouldGenerateCompactCode` | **—** 6.7 |
| `tumblingWindowSelector` | reused, and never named | it is reached inside `translateTableFunction`, which is public; 5.2 |
| `overridingMethodDecl`, `toInternal` ×3, `fromInternal` ×4, `internalTypes`, `convertAssignableTypes`, `convertAssignableType`, `matchMethodParameterTypes`, `matchMethodParameterType`, `isA`, `representAsInternalType`, `hopWindows` | **—** they are the private inside of `convert` and `call`, which are public and reused whole |
| `convert`, `evaluate`, `call`, `numberToBigDecimal`, `generateCollatorExpression`, `toExternal`, `sessionize`, `hopping`, `tumbling` | public — reused as they are |
| — | `BoxRows` | CLR only: a join's selector and predicate take boxed rows because linq4j's `Function2` erases to `Object`, and a delegate is typed where that interface was not — 6.12 |

### 4.5 `ClrEnumerableDefaults`, and the types with no counterpart

`ClrEnumerableDefaults` is the counterpart of linq4j's `EnumerableDefaults`, not of anything in this
package: 39 public operators over typed delegates where linq4j's are over `Function1` and `Function2`. Where
an operator's output order is a collection's — group by, distinct, union, intersect, except, and the
unmatched tail of a hash join — it holds the rows in the same Java collection linq4j does, because the order
is part of the answer (6.10). `EnumerableDefaults.Wrapped` is ported as `JavaWrapped` for the same reason.

`ClrPhysTypes`, `ExpressionTranslator`, `MethodResolver`, `TypeResolver`, `FieldResolver`, `JavaCast`,
`SamAdapters`, `SyntheticRecordEmitter` and the `Delegate*` runtime types have no Calcite counterpart at
all: they are what an expression tree costs where Calcite has Java source.

---

## 5. Differences outstanding, with no justification

Everything here is a difference from `EnumerableConvention` that nothing yet argues for. The list is the
work; an entry leaves it by being resolved, or by moving to §6 with the argument written down.

**5.1 Nodes not written.** Two, where it was four, and neither is undone work. `EnumerableInterpreter` is written —
`ClrEnumerableInterpreter`, with its rule, as of 2026-08-05 — so a scan only the interpreter can run lands
in this convention instead of Calcite's under a converter. What is left: `EnumerableMatch`, argued in 6.14, though a
MATCH_RECOGNIZE **query** now runs across the converter, which is a different thing from the node;
`EnumerableBindable`, which turns out to be **unwritable in C#** rather than undone — 6.17; and
`EnumerableTableModify`, which is **out of scope by decision** — 6.18.

**5.2 — resolved.** The window table function path was refused in the rule, because TUMBLE ran by neither
route: through the node, translating it left an `Object[] _input` referenced from no scope that declared it,
and four explanations had been tried and disproved. The fifth is the one `CLAUDE.md` already had a name for.

`RexImpTable.TumbleImplementor` builds the watermark column against a `ParameterExpression` it makes itself,
named `_input`; `EnumUtils.tumblingWindowSelector` makes the lambda's parameter separately, also named
`_input`, and puts the first expression inside the second's body. **Two objects, one name, and Janino
resolves the name** — the lambda's parameter shadows the enclosing local, exactly as Java says. Keyed by
reference there are two variables and one of them is free.

`ExpressionTranslator` now gives a lambda's parameters a lexical scope by name, consulted when a mention
resolves to nothing else — for a `FunctionExpression` and for an anonymous class's method alike.
`ClrEnumerableTableFunctionScan.TvfImplementorBasedImplement` is the counterpart of
`tvfImplementorBasedImplement`, and `isImplementorDefined` is `IsImplementorDefined`.
`RexToLixTranslator.translateTableFunction` is public and takes a linq4j expression yielding an
`Enumerable`, so the sequence crosses into Java for the length of that call and comes back — the rule this
convention holds everywhere.

Six tests: TUMBLE, TUMBLE with an offset, HOP, SESSION, an aggregate over a TUMBLE, and one asserting that
the plan really is `ClrEnumerableTableFunctionScan` rather than Calcite's node under a converter.

**5.3 — resolved.** `Rules()` registered the limit-sort rule and `ENUMERABLE_RULES` does not. The rule is
out of the default list now, and 9.18 records what dropping it uncovered. The number is kept and not reused.

**5.4 — resolved.** `ImplementRoot` did not wrap a failing node. It does now, and 1.6 says how the two
exception models line up. `ShouldNameThePlanWhenANodeCannotImplementItself` holds it.

**5.5 — resolved.** `ToBindable` dropped the `CalcitePrepare.SparkHandler` parameter. It takes all four
arguments in Calcite's order now, and refuses the Spark branch rather than ignoring it — 6.16.
`ShouldRefuseASparkHandler` holds it.

**5.1 is what is left of §5, with 5.6**: two nodes that cannot be written for reasons given in §6, and one
that waits on the convention doing writes at all. Nothing in it is undone work. Until 2026-08-05 they were also the whole
of §5 — the re-derivation of §1 to §4 added five more, of which four were wrong answers rather than absent
features: a query that ran, in a node marked as ported, giving rows `EnumerableConvention` does not give.
All five are resolved; they are kept below because a defect that was measured, fixed and tested is worth
more as a record than as a deletion.

**5.6 `ClrEnumerablePrepare` cannot share Calcite's internal-parameter map.**
`CalcitePreparingStmt.internalParameters` is private with no accessor, and it is the map
`CalcitePrepareImpl` hands to the `CalciteSignature` and thence to the `DataContext` at bind time. Ours uses
its own, which carries the conformance — all this convention reads from it — so code generation is right.
What it cannot carry is a value *Calcite's* implementor stashes: `EnumerableRelImplementor.stash` writes
into that map and the generated Java reads it back through `root.get(name)`, which would find nothing.

**It needs `ClrEnumerableToEnumerableConverter` on the plan — a sub-plan of ours under a node of Calcite's —
and the planner does not choose that shape.** Measured with the one query that forces Calcite's convention
into a plan rooted in this one, a MATCH_RECOGNIZE: the planner put the *whole* subtree in
`EnumerableConvention`, `EnumerableMatch` over `EnumerableTableScan`, and converted once at the top with
`EnumerableToClrEnumerableConverter`. Converting back and forth costs more than staying, so it stays.

So the loss is real but **not reachable through anything measured**, and one query is not a proof of
unreachability. It stays here rather than moving to §6: an entry earns §6 by being forced, and this one is
only unreached.

**5.7 — resolved.** The hash join, the semi join and the ASOF join extracted their keys with
`generateAccessor`, which no release of Calcite uses in any of the three: 1.42 keys the first two on
`generateNullAwareAccessor(keys, joinInfo.nullExclusionFlags)` and the ASOF join on
`generateAccessorWithoutNulls(keys)`, and 1.41 used `generateAccessorWithoutNulls` for all three. Ours
matched neither release, and `ClrEnumerableHashJoin.Accessor`'s comment said 1.41 had `generateAccessor`
alone, which is wrong — 9.23. All three call Calcite's accessor now; `Accessor` remains for the mark join's
null-safe lookup, which is the one place Calcite does call it.

`ShouldAgreeOnANullSafeJoinKey` holds it: a null-safe key of one field is a list holding null under the
null-aware accessor, which is not null and matches another one, so `a."AMOUNT" IS NOT DISTINCT FROM
b."AMOUNT"` joins `5|5` as Calcite does. `ShouldAgreeOnASemiJoinOnANullSafeKey` and
`ShouldAgreeOnAnAntiJoinOnANullSafeKey` hold the other two entry points.

**A key of two fields is measured too**, and it took taking the merge join away from both sides to reach the
node at all: an equi-join on `REGION` and `AMOUNT` plans as a merge join in both conventions (8.6), so
`SameHashJoin` drops `ENUMERABLE_MERGE_JOIN_RULE` and `ClrEnumerableMergeJoinRule` together and
`ShouldPlanAHashJoinWithoutTheMergeJoinRule` asserts that what is left really is a hash join.
`ShouldAgreeOnAHashJoinOnTwoKeysOneNullable` and `ShouldAgreeOnARightHashJoinOnTwoKeysOneNullable` then hold
the rows.

**The test was checked against the defect**, by putting `generateAccessor` back and running it: the CLR side
returns `5|5` and Calcite does not, because a `FlatList` holding a null equals another one and a null key
does not. That is the wrong row this entry predicted, and it is the only way to know a test discriminates.

**5.8 — resolved.** `ClrEnumerableDefaults.HashJoin` skipped a build-side row whose key was null;
`EnumerableDefaults.hashEquiJoin_` keeps one under a null key, where nothing probes it — the outer side
skips a null key too — but where the unmatched tail of a RIGHT or a FULL join still finds it. A
`java.util.HashMap` takes a null key, so the fix is to stop skipping. `ShouldAgreeOnARightJoinOnANullableKey`
and `ShouldAgreeOnAFullJoinOnANullableKey` hold it, and both would have passed with 5.7 unfixed: the key is
null by either accessor and what differed is what the loop did with it.

The existing `ShouldAgreeOnARightJoinsOwnOrder` and `ShouldAgreeOnAFullJoinsOwnOrder` join on `REGION` and
`LABEL`, which are not nullable — which is how 136 differential tests covered every part of this but the
intersection of *nullable key* and *outer join on the build side*.

**5.9 — resolved.** `ClrEnumerableRepeatUnion` and `ClrEnumerableTableSpool` were written, their rules were
registered, and no test had ever reached either; the first query that did failed in `Implement`. Four things
were wrong at once, which is what never running a node costs:

- **The pass-through format.** Both declared `pref.Prefer(inputFormat)`, so a spool over a one-column
  `VALUES` declared `Object[]` while its sequence was `IEnumerable<int>`. They declare the input's format
  now, as `ClrEnumerableSort` and `ClrEnumerableLimit` already did — the same point as 6.9, in the two nodes
  that had not had it applied.
- **A stashed variable arrived free.** `EnumerableInterpreter` under the converter stashes its `RelNode`, and
  `EnumerableRelImplementor.implementRoot` declares those at the top of the method it generates by reading
  them back out of the `DataContext`. A sub-plan translated on its own never sees that declaration. The map
  is shared between the two implementors precisely so a value can cross, so `ExpressionTranslator` now
  answers such a variable with the object itself — the same answer `ClrEnumerableRelImplementor.Stash` gives
  on this side. 9.28.
- **The spool wrote CLR values into a Java collection.** `LazyCollectionSpool` added each row to the table's
  collection as it stood; the interpreter reads that collection back and `SqlFunctions.toInt` refused a
  `System.Int32`. It converts now. That is the invariant `CLAUDE.md` states, broken for the fifth time and
  in the one node that hands rows to a Java reader without a converter in between. 9.27.
- **`unwrap(Table.class)` gave null**, so the plan added a null table to the schema. Calcite unwraps
  `TransientTable` and `requireNonNull`s it; ours does both now.

`ShouldAgreeOnARecursiveQuery` and `ShouldAgreeOnARecursiveQueryOfSeveralColumns` hold it. Neither
convention's scan will touch a transient table — `EnumerableTableScan` refuses one under CALCITE-3673 and so
does ours — so both sides read it through the interpreter, and this is the only differential test where the
two plans differ in more than their node names.

The plan-time constant is gone too: the spool looks its table up in the root schema of the `DataContext` the
plan is bound with, as Calcite looks it up, rather than reading the collection when the plan is built.

**5.10 — resolved.** `EnumerableAggregate.implement` has four branches and ours had two. The
grouping-sets branch is written now — a key selector per grouping set from the three-argument
`generateSelector`, `ClrEnumerableDefaults.GroupByMultiple` against `EnumerableDefaults.groupByMultiple`, and
a result row whose group columns are `condition(indicator, null, box(ref))` rather than the bare field
reference. `ShouldAgreeOnGroupingSets`, `ShouldAgreeOnARollup`, `ShouldAgreeOnACube`,
`ShouldAgreeOnGroupingSetsOwnOrder` and `ShouldAgreeOnTheGroupingFunction` hold it.

Both failure modes it had are gone with it: `ROLLUP` silently dropped its roll-up rows, and `GROUPING SETS`
threw an `InvalidCastException` because the key selector produced a bare field where the key physical type
carried indicators.

**5.11 — resolved.** The distinct-only branch — `aggCalls.isEmpty()` and a group set covering every input
field short-circuits to `Enumerable.distinct` rather than grouping — is written. It measured the same rows
before and after, which is why it was the one of the five that was a missing shortcut rather than a wrong
answer. `ShouldAgreeOnADistinctOverEveryColumn` holds it.

**Nothing is outstanding but 5.1 and 5.6.** 5.7 to 5.11 were found by the re-derivation of 2026-08-05 and
closed the same day; 5.2 had stood since the file was written and closed with them, because the same reading
that found the five explained it — 9.31. What closing them uncovered is 9.27 to 9.30.

---

## 6. Differences with a justification

Each of these departs from Calcite deliberately. The rule is the one `TODO.md` states, and it is stronger
than "argue for it": **copy Calcite, and invent something only where copying cannot possibly be done.** An
entry belongs here only if copying is impossible — an expression tree cannot express it, Calcite's own
member is unreachable, or Calcite's text is demonstrably wrong. "Ours is better", "ours is simpler" and
"ours would be equivalent" are not reasons, and every one of them found here so far has been a defect.

And **a defect of ours that Calcite does not have means we copied Calcite wrongly**, not that Calcite is
wrong. 6.10 and 6.12 used to sit here claiming otherwise; they are now what they always were — places where
the port was incomplete and has been completed. They stay only as the record of that.

**6.1 A node's plan is an `Expression`, not a `BlockStatement`.** *An expression tree cannot express it.*
Calcite generates one Java method per plan, so a node contributes statements and a parent appends. There is
no method to append to here, so a parent composes; `Expression.Block` covers what needs statements.

**6.2 Loops are owned, not generated.** *An expression tree cannot express it.* `ClrEnumerableDefaults.Calc`,
`.Window`, `.MergeJoin`, `.AsofJoin`, `.HashJoin` and the rest are the loops Calcite writes into generated
source — written once, over typed delegates, rather than boxing through `Function1`. Owning the loop does
**not** license changing what it does: each is the linq4j operator statement for statement, and where one was
not, it was a defect (9.15).

**6.3 A window's aggregate state is a synthetic record — the one Calcite already builds for an aggregate.**
*A consequence of 6.2.* Calcite declares a window aggregate's state and the variable holding its last result
as locals of the generated method, and the loop that mutates them is generated into that same method. Ours is
not: `ClrEnumerableDefaults.Window` is written once and is generic in `TAccumulator`, so it has to be able to
make the state and thread it through reset, add and result — which is the `Func<TAccumulator>` initializer
and the three `Func<WindowFrame, TAccumulator, TAccumulator>` in its signature. The state's shape is per
query, so the type is `typeFactory.createSyntheticType(stateTypes)` — Calcite's own `SyntheticRecordType`,
emitted as a CLR type by `SyntheticRecordEmitter` — and `declareParentAccumulator` fills it. That is exactly
what `EnumerableAggregate.implement` does with an aggregate's accumulator. Nothing is invented: the window is
given the aggregate's answer, Calcite's own, because the window's answer needs a generated method to hold
locals in.

The reason first written here was that no lambda can carry mutable state, and that is false. One closed over
an `Expression.Variable` of an enclosing block can, and `ClrEnumerableWindow.Hoist` does exactly that for the
comparator, the key selector and the key comparator, in the same method. What such a variable cannot do is
have the right lifetime: it belongs to the one call of `plan(dataContext)` and would be shared by every
enumerator of the sequence that call returns, where `Window` is an iterator and takes a fresh accumulator per
`GetEnumerator()`. `WindowFrame` is the contrast, in the same signature: the loop variables the implementors
read are a hand-written class, because the loop knows their shape and there is one of them.

**6.4 A class of several methods becomes an object of delegates.** *An expression tree cannot express it.*
`DelegateEnumerator` and friends, for what `ExpressionTranslator.Anonymous` meets in a generated block.

**6.5 Every package private member is ported rather than reused.** *Calcite's own member is unreachable.*
`ClrEnumerableTraitsUtils`, most of `ClrEnumUtils`, the nine private members of `EnumerableWindow`, the four
of `EnumerableAggregateBase`, `EnumerableMergeJoin`'s three, `PhysTypeImpl.of(typeFactory, javaRowType)`.
Each is listed against its original in §1 and §2.

**6.6 `ClrEnumerablePrefer` is this convention's own enum.** *An expression tree cannot express it*, narrowly:
`EnumerableRel.Prefer` is a Java enum carrying methods, which C# has no equivalent of — and what a node asks
its input for belongs to the convention asking. Converted at the two converters and nowhere else.

**6.7 There is one join selector, not two.** *An expression tree cannot express it — in reverse.*
`EnumUtils.joinSelector` branches to `joinSelectorCompact` when the output has at least
`JOIN_SELECTOR_COMPACT_CODE_THRESHOLD` fields, because the generated method would otherwise exceed what a
Java class file allows (CALCITE-3094). It builds the row by copying into an `Object[]` through
`JavaRowFormat.copy` instead of naming every field. An expression tree has no such limit, so
`ClrEnumUtils.JoinSelector` is the uncompacted form alone — the one Calcite uses everywhere below the
threshold. `shouldGenerateCompactCode` has no counterpart for the same reason.

**6.8 `ClrEnumerableMergeJoin.passThroughTraits` and `ClrEnumerableSortedAggregate.passThroughTraits` refuse
a foreign convention.** *Copying the line is impossible, and Calcite's own assertion says why.*
`EnumerableMergeJoin` returns `Pair.of(required, …)` — the trait set it was handed, convention and all — and
`PhysicalNode.passThrough` copies the node onto it. `TopDownRuleDriver.convert` asserts that a pass-through
preserves the node's convention, so Calcite means for the returned trait set to keep it; this method does
not, and only assertions being off lets that through. Calcite never notices because it runs one convention.
We run two, and copying the line literally gives a `ClrEnumerableMergeJoin` wearing ENUMERABLE, which the
planner refuses to register. Ours returns null for a convention that is not its own, and every other line of
both methods is the port. Measured again in 8.4.

**6.9 A pass-through node does not re-optimise its input's row format.** *A defect in Calcite,
demonstrated — in Calcite's own convention, on the same plan.* Sort, limit,
limit-sort, spool and repeat union yield their input's rows unchanged, and Calcite builds their physical
type with `PhysTypeImpl.of(typeFactory, rowType, format)` — the overload that optimises, and
`JavaRowFormat.optimize` returns SCALAR for any one-field row type. The rows keep their shape, so the
physical type says the row *is* the value while the sequence still yields `Object[]`, and a parent reads
field 0 as the row itself. Those five nodes use the four-argument overload with optimisation off.

What makes this Calcite's rather than ours is the pair of decisions on either side of it, both in Calcite's
text. `EnumerableTableScan.toRows` *reshapes* to match the optimised type — `slice0` when the format came
out SCALAR and the table yields `Object[]` — so a scan is consistent. `EnumerableTableFunctionScan` cannot
reshape, so it passes `optimize = false` and keeps an honest ARRAY. A pass-through node above it then
optimises that ARRAY away without touching the rows. A one-column non-queryable table function is the only
producer of that shape in the package, and it is exactly what our fixture is; `IsQueryable` is a faithful
port and returns false for it, as Calcite's would.

**The demonstration.** `Smalls.fibonacciTableWithLimit100` is a one-column table function written in Java,
so Janino can name it — every table function of this project's own is a CLR class, which is why this could
not be measured before. `ClrEnumerableRowFormatTests` joins it to itself with the hash join rule removed, so
the merge join is the only way to join and a sort lands over the table function. **Both conventions choose
the same plan** — a merge join over two sorts over the table function scan — and then:

- this convention answers the query;
- `EnumerableConvention` throws `Unable to cast object of type 'System.Object[]' to type 'java.lang.Long'`.

The sort declared its physical type SCALAR, so its Java row type is `long`, while yielding the table
function's `Object[]` rows unchanged; the merge join's key accessor then read the row as the value. Calcite
has not noticed because with the hash join rule in place the planner hashes and the shape never arises.

**And the divergence is necessary, measured the other way round too**: flipping those five nodes back to the
optimising overload makes `ShouldRunATableFunctionInAJoin` fail, in `ClrEnumUtils.BoxRows`. The test that
shows Calcite failing is the canary — **when it starts passing, Calcite has fixed this and the divergence
should go**.

**6.10 Six operators hold their rows in Calcite's collection, not the CLR's.** *Not a divergence at all — the
port of `EnumerableDefaults`, finished. Kept here as the record.* The order of a query that asks for none is
the order of the collection the operator held its rows in. Group by, distinct, union, intersect, except and
the unmatched tail of a hash join each held them in a `Dictionary` or a `HashSet`, and answered a query
differently from `EnumerableConvention` for that reason alone. Each now holds them in the Java collection
linq4j holds them in — a `java.util.HashMap` for group by and the hash join's lookup, a `java.util.HashSet`
for distinct and union, and a `HashSet` or a Guava `HashMultiset` for intersect and except depending on ALL
— and `JavaWrapped`, the port of `EnumerableDefaults.Wrapped`, is how a comparer reaches those collections.
Nine unordered differential tests hold it. Reproducible because of 8.2.

**6.11 `ClrEnumerableSortedAggregateRule` refuses an empty group set.** *A defect in Calcite, demonstrated —
in Calcite's own convention.* The node tells one group from the next with a comparator built from the
collation it carries, and for `GROUP BY ()` that collation is empty. Calcite's rule builds the node anyway:
register `ENUMERABLE_SORTED_AGGREGATE_RULE` and run `SELECT COUNT(*) FROM t` and the planner chooses it,
then `implementRoot` throws "Unable to implement EnumerableSortedAggregate(group=[{}] …)". A global
aggregate has nothing to sort by, so ours leaves it to `ClrEnumerableAggregate`. Both rules refuse a
non-simple group by returning null from `convert`, which is Calcite's own idiom; the empty-group-set line is
the added one.

**6.12 A correlate boxes its rows.** *Not a divergence — the port of `joinSelector`, finished. Kept here as
the record.* `EnumUtils.joinSelector` boxes both of its parameter types, because linq4j's `Function2` erases
to `Object`. Every other join here boxes its sequences to match; the correlate did not, and an EXISTS
sub-query — whose right side is one primitive boolean column — is where the selector and the call disagreed.

**6.13 A record of no fields gets one constructor.** *An expression tree cannot express it.* Calcite
generates both the empty constructor and the all-fields one, and for a record of no fields those are the
same signature; two of them in one emitted type cannot be told apart by `Type.GetConstructor`. A semi join
whose right input projects nothing is one.

**6.14 Match is not written, and cannot be as a node — and a MATCH_RECOGNIZE runs anyway.** *Calcite's own
type is unreachable.* `EnumerableMatch.PassedRowsInputGetter` and `PrevInputGetter` are package private
*types* that Calcite's own translator casts to by name; IKVM compiles them internal, so C# can name them and
cannot construct them. Reflection is not an acceptable way in.

**The query is a different question from the node, and the answer changed on 2026-08-05.** A MATCH_RECOGNIZE
now runs in a plan rooted in this convention: the planner leaves the whole subtree in `EnumerableConvention`
and `EnumerableToClrEnumerableConverter` carries the rows, and `ShouldAgreeOnMatchRecognize` compares them
against Calcite's own answer. Three things had to be true at once and each had been a blocker of its own —
`TODO.md` has them in full, and 9.32 has the one that was ours.

**Two shapes of it still do not run, and neither is ours.** `EnumerableMatch.implementPattern` takes a
symbol or a concatenation and nothing else, so `PATTERN (STRT UP+)` throws `unknown kind:
PATTERN_QUANTIFIER` out of Calcite's node in either convention; and a `PARTITION BY` of one column gives the
key a SCALAR physical type, so `Expressions.new_` on it emits `new Integer()` and Janino refuses it —
measured on `EnumerableConvention` alone, which is the only way to attribute it. Same defect as the
`new Object[]()` that an ARRAY-format input still gives.

**6.18 Modification is out of scope: this convention reads.** *Not a difference anybody is waiting on.*
`EnumerableTableModify` has no counterpart and its rule is not in `Rules()`, so a plan that writes is left to
`EnumerableConvention` and the converters carry it — which is what happens today and what the ADO.NET surface
already does.

**It was written, measured and taken out again on 2026-08-05**, so the cost of the next person wondering is
this paragraph rather than a day: `ClrEnumerableTableModify` did INSERT and DELETE against 1.42's node and
UPDATE against 1.43's, with six differential tests, five hand-asserted ones and a plan assertion, all green.
Two things worth keeping from it, because they are expensive to find and cheap to write down:

- **1.43's UPDATE needs nothing 1.42 lacks.** Its five private helpers call `getUpdateColumnList`,
  `EnumerableTableScan.deduceFormat`, `PhysTypeImpl.of`, `fieldReference`, `getJavaFieldType`,
  `Primitive.box`, `SINGLETON_ENUMERABLE`, `SELECT`, `INTO`, and otherwise `java.util` and the linq4j
  basics — every one of them in 1.42. Checked one by one. So the reference version is not what stands in
  the way of UPDATE, and never was.
- **A modifiable fixture has to be a `QueryableTable` whose element type is `Object[]`.**
  `Schemas.getTableExpression` answers a *scannable* table with an `Enumerable` whatever class was asked
  for, and `EnumerableTableModify` then fails its own precondition; and a queryable table's expression casts
  to its element type by name, which Janino cannot resolve for a CLR class. Calcite's own
  `MutableArrayTable` is exactly that shape and is private to its test kit.

**And one Calcite defect, measured on `EnumerableConvention` alone while this existed**: an INSERT whose
source scans the table it writes into runs away — the scan is lazy and the insert appends to the list it is
reading, four to five minutes before returning 429496730. SQL says the source is evaluated as of the start
of the statement. Worth reporting upstream whether or not this convention ever writes.

**6.17 The bindable node cannot be written in C#, and it is a bridge method that stops it.** *Calcite's own
interface is unimplementable.* `BindableRel` extends `ArrayBindable`, and `ArrayBindable` **narrows**
`Typed.getElementType()` from `java.lang.reflect.Type` to `Class<Object[]>`. IKVM renders that covariant
override as two members: the narrowed one, and a bridge named `<bridge>getElementType()`. Explicit interface
implementation answers the first two — `Typed.getElementType` and `ArrayBindable.getElementType` are
separate members in C# and may differ in return type — and the bridge cannot be answered at all, because
`<` and `>` are not valid in a C# identifier and there is no escape for them. Written out in full, the class
compiles down to exactly one error, and that error names a member whose name cannot be typed.

This is the sibling of 6.14 and worth stating separately: 6.14 is a package-private *type* Calcite casts to,
this is a *public* interface that C# cannot implement. Reflection and a `java.lang.reflect.Proxy` are both
the answer 6.14 already refuses. Subclassing `EnumerableBindable`, whose Java base satisfies the bridge, is
the other way in and is worse: the node would claim to be Calcite's while its input is one of ours.

**What it costs is one converter.** A caller who wants a bindable plan gets Calcite's `EnumerableBindable`
over `ClrEnumerableToEnumerableConverter` over this convention's plan, and the rows are the same;
`ClrEnumerableInterpretable.ToBindable` remains the way a plan of this convention is compiled and bound
without any of that.

**6.16 A Spark handler is refused, not ignored.** *Calcite's branch cannot be copied.*
`EnumerableInterpretable.toBindable` hands the generated `ClassDeclaration` and its source text to
`SparkHandler.compile` when one is enabled, and compiles with Janino otherwise. There is no generated class
here and no source text — the plan is an expression tree — so there is nothing to hand it. The parameter is
taken anyway, in Calcite's position, and `ToBindable` throws where a handler says it is enabled. Dropping
the parameter, which is what this used to do, would have made a caller's Spark configuration silently
ignored; taking it and refusing says which of the two conventions can honour it.

**6.15 What compiles a plan has nothing to port to.** *Calcite's member exists to feed Janino.*
`EnumerableInterpretable.box`, `getBindable`, `compileToBindable`, and `EnumerableRelImplementor.classDecl`,
`TypeFinder`, `TypeRegistrar` all serve a generated class. There is no generated class here;
`SyntheticRecordEmitter` emits the record types at runtime instead. This is about those members, not about
the nodes named for them: `EnumerableInterpreter` is written (2.21) and `EnumerableBindable` cannot be
(6.17).

---

## 7. Not in 1.41, and correctly absent

This section said "not in 1.41, and correctly absent" and listed seven classes as one group. That was read
off `D:\calcite`'s working tree and never checked against a tag, and it was wrong in two ways at once: the
tree is 1.43.0-SNAPSHOT rather than 1.42.0-SNAPSHOT, and the seven do not belong to one release.

**7.1 Was 1.42, and is now written.** `EnumerableCombine`, `EnumerableCombineRule`,
`EnumerableConditionalCorrelate`, `EnumerableConditionalCorrelateRule`; and, one level down,
`EnumUtils.markJoinSelector` with every mark-join path that calls it — in `EnumerableHashJoin` and
`EnumerableNestedLoopJoin` — plus `PhysType.generateNullAwareAccessor` and `JoinInfo.nullExclusionFlags`.
They were "correctly absent" only while 1.41 was the reference. They are 2.5, 2.6, 2.19 and 2.20 now.

**7.2 In 1.43, which is unreleased, and correctly absent.** `FetchOffsetRoundingPolicy`,
`RexImplementorTable`, `RexImplementorTables`, `org.apache.calcite.rel.core.Asof`, and
`EnumerableTableModify`'s five private helpers — the UPDATE/DELETE/INSERT rewrite, CALCITE-7510. 1.42's copy
of `EnumerableTableModify` is byte-identical to 1.41's, so nothing about that node changed between the
version this file compares against and the version the projects reference.

**`rel.core.Asof` is 1.43, not 1.42.** `rel.core.AsofJoin` — the one that matters — is 1.41 and always was.

---

## 8. Measured on the way, and worth keeping

**8.1 252 tests pass**, measured 2026-08-05, against **calcite-core 1.42.0**: 217 as of 2026-08-04, plus the
sixteen that 5.7 to 5.11 left behind — five joins on a null key, three more on a hash join reached by taking
the merge join away, six aggregates over a grouping set or a distinct, and two recursive queries — plus six
window table functions from 5.2, eight over a CUSTOM-format table (8.7), two MATCH_RECOGNIZE with the plan
they run under, three for `ClrEnumerableInterpreter` (2.21) — two comparing rows and one asserting that
registering its rule is what moves the node from `EnumerableConvention` into this one —. The twelve for the table modify are
gone with the node — 6.18. The breakdown that follows is the 2026-08-04 one and what it counts has
not changed in kind: 136 differential, of which 87
compare rows with the default planner, 11 with top-down optimisation on, 5 with the sorted aggregate rule
on, 5 with the batch nested loop join rule on, 5 with the limit-sort rule on, 8 with the mark-join sub-query
rules on, 5 assert rows by hand because `EnumerableConvention` cannot run the query at all, and 7 assert
which node the planner chose. Two of the rest hold what a failure looks like — the plan named when a node
cannot implement itself, and the Spark refusal.

**8.5 The probes of 5.7 to 5.11 are in the suite now, and were not when they were probes.** They were
written into `ClrEnumerableDifferentialTests` for the re-derivation, run, and **reverted** — a test that is
red on purpose is indistinguishable from one that broke — and they went back in one at a time as each defect
closed. Thirteen became the thirteen listed under 5.7 to 5.11: a null-safe join key and its semi and anti
forms, a right join and a full join on a nullable key, `GROUPING SETS`, `ROLLUP`, `CUBE`, a grouping set's
own order, `GROUPING(…)`, a distinct over every column, and two recursive queries.

**The rule that produced them is worth more than the tests.** Each came from reading one Calcite `implement`
against ours and asking what the difference would show; none came from a failing test, because no failing
test existed. `EnumerableConvention` will answer any question put to it — the cost of asking is one
`Same(...)` — and five defects sat in nodes marked as ported because nobody asked.

**8.7 Eight tests over a CUSTOM-format table passed the day they were written, and that is the finding.**
Every fixture in this harness but one is a `ScannableTable` of `Object[]` rows, so 239 tests had exercised
the ARRAY branch of `PhysType.record`, `fieldReference` and the join selector and left the CUSTOM branch to
the two hand-asserted query tests. `HR.emps` — `ReflectiveSchema` over `calcite-testkit`'s `HrSchema`, whose
rows are instances of a Java class — closes that: scan, filter, nullable column, aggregate, join, join on a
nullable key, window and distinct all agree.

It has to be a **Java** class for the reason `MY_SUM` does: Janino cannot name a CLR class, so a CLR-backed
table leaves `EnumerableConvention` with no plan to compare against. It was added for MATCH_RECOGNIZE, which
needs CUSTOM format to compile at all (6.14), and the coverage is what it was worth on its own.

**8.6 A join of two keys over `SALES` is a merge join in both conventions**, not a hash join: both scans are
sorted and the merge join wins on cost. Printed from `PlanOf` on both sides. It is recorded because it is
why one of the four probes that agreed proved less than it appeared to — 5.7 — and because the same trap
applies to any future probe aimed at `ClrEnumerableHashJoin`: **check the plan before believing the rows.**

`Run` takes `excludeMergeJoin` for it, which drops the rule from **both** conventions — Calcite's through
`DefaultRulesProgram` and ours by leaving it out of what the harness registers. Dropping it from one side
only would compare two different plans, which is what 9.18 records the cost of.

**8.2 Java specifies `String.hashCode`, and IKVM implements it.** Measured in `JavaHashingTests`, in two
processes: the Java-side hash of `"EAST"` is 2120701 both times, the CLR's is a different number each time,
and a `java.util.HashMap` iterates identically. So an order that comes from a Java collection is
reproducible, and one that comes from a CLR collection keyed by strings is not comparable to Calcite's.
This is what makes 6.10 reproducible rather than lucky.

**8.3 Trait derivation is dead unless the planner optimises top down.** `passThroughTraits`, `deriveTraits`
and `getDeriveMode` are called only by `TopDownRuleDriver`, and `CalciteSystemProperty.TOPDOWN_OPT` is false
by default. Eleven differential tests plan both conventions with it on, which is what proves those methods
do anything at all.

**8.4 `EnumerableMergeJoin.passThroughTraits` returns the trait set it was handed**, convention and all, and
`PhysicalNode.passThrough` then copies the node onto it. With both conventions in one planner and top-down
optimisation on, a CLR_ENUMERABLE subset asks it to pass through and gets an `EnumerableMergeJoin` wearing
CLR_ENUMERABLE, which the planner refuses. `TopDownRuleDriver.convert` asserts that a pass-through preserves
the convention, so Calcite means for it to; only assertions being off lets it through. Every trait method
here builds from `getTraitSet()`, so nothing of ours can do this — 6.8.

---

## 9. What rebuilding this file corrected

Kept so the same mistakes are not made twice. 9.1 to 9.13 were claims written from memory of the code rather
than from the code, and each hid a real difference behind a row that read as accounted for. 9.14 to 9.17
came from auditing §6 against the rule in its own header. 9.23 to 9.26 came from re-deriving §1 to §4
against `calcite-1.42.0` on 2026-08-05, which is what 9.20 said was still owed, and they are all one
mistake: **a row that names a member is not a row that has compared the two bodies.**

**9.1 `Rules()` was said to be 24 and to add the merge union rule.** It is 25, and
`ENUMERABLE_MERGE_UNION_RULE` is in `ENUMERABLE_RULES`. The rule ours actually adds is the limit-sort one,
which Calcite leaves out — now 5.3.

**9.2 `EnumUtils.markJoinSelector` was listed as an outstanding gap.** It does not exist in 1.41. It was read
off the 1.42 working tree, which is the trap the header of this file warns about. Now 7.2.

**9.3 `joinSelectorCompact` and `shouldGenerateCompactCode` were not mentioned at all.** They are a real
branch of the one `EnumUtils` method we do port, deliberately not taken. Now 6.7.

**9.4 `ClrEnumerableMergeJoin`'s extra members were given as `Predicate` and `Cartesian`.** There is no
`Cartesian`; they are `Predicate`, `AllLessThan` and `AllAtLeast`. Now 2.7.

**9.5 `ClrEnumerableSortedAggregate` was said to add two guards.** It adds one. The second was the rule's,
already listed a line away. Now 2.15 and 3.1.

**9.6 Both aggregate constructors were said to throw `InvalidRelException` and refuse the same three
things.** Only `EnumerableAggregate`'s does; `EnumerableSortedAggregate`'s asserts the convention and nothing
more. Now 2.14 and 2.15.

**9.7 `EnumerableAggregateBase`'s helpers were said never to read `this`.** Three do not;
`createAggStateTypes` does, through the inner `AggContextImpl`, which is why ours has three parameters
Calcite's does not. Now 2.13.

**9.8 `EnumerableRelImplementor`'s table was missing five members**, including `getRexBuilder`, and did not
record that `ImplementRoot` drops Calcite's "Unable to implement" wrap. Now 1.6 and 5.4.

**9.9 `EnumerableWindow`'s ported private members were counted as five and are nine**; the file named the
anonymous `WinAggFrameResultContext` without naming the method that builds it, and left `ImplementGroup` off
the ours-only list. Now 2.16.

**9.10 §4 listed sixteen classes as "reused" that this project never names**, two of which
(`MatchImplementor`, `MatchUtils`) serve only a node that is not written. Reused now means one of two things
and the file says which — 4.1, 4.2, 4.3.

**9.11 `ClrEnumerableInterpretable.ToBindable` drops a parameter** and nothing said so. Now 1.7 and 5.5.

**9.12 `ClrEnumerableDefaults` was given as 33 operators.** It is 34. Now 4.5.

**9.13 "The six `WinAgg*` interfaces"** — there are seven. Now 4.2.

**9.14 The window's synthetic accumulator was justified as "no lambda can carry that".** A lambda closed over
a block variable carries mutable state, and `ClrEnumerableWindow.Hoist` does it in the same method. The
record is there for lifetime, not expressiveness, and the type is Calcite's own `createSyntheticType` — the
same one `EnumerableAggregate` uses. Rewritten as 6.3.

**9.15 `CorrelateBatchJoin` read the whole right input up front**, justified as "an expression tree cannot
express it, narrowly". Nothing about an expression tree was involved: `ClrEnumerableDefaults` is hand-written
C# and can do whatever linq4j does. **Fixed** — the batch's first left row now pulls from the right input and
caches as it goes, every row after it reads the cache, and a semi or anti join finishes reading before it
stops, which is `EnumerableDefaults.correlateBatchJoin` statement for statement. The cost the old note
claimed — "a full pass where a batch of one matched early" — was wrong in both directions: Calcite drains
there too. What laziness actually buys is a consumer that stops early, a LIMIT above the join. Every test
still passed. The entry is gone from §6; 2.8 records what the code now does.

**9.16 Two entries were filed as defects and were ours.** "Six operators hold their rows in Calcite's
collection" and "a correlate boxes its rows" — a defect of ours that Calcite does not have means the port was
incomplete, not that Calcite is wrong. Both are finished ports and are labelled as such: 6.10 and 6.12.
Neither is a divergence from `EnumerableConvention` and neither ever was.

**9.18 Dropping the limit-sort rule from `Rules()` uncovered a defect it had been hiding.** Resolving 5.3
is one line — take the rule out of the default list, as Calcite takes it out of `ENUMERABLE_RULES` — and
`ShouldAgreeOnLimitAndOffset` then failed with `InvalidCastException: Unable to cast object of type
'java.lang.Integer' to type 'System.Int32'`. With the rule gone the whole plan lands in
`EnumerableConvention` and `EnumerableToClrEnumerableConverter` carries it; the sub-plan is one column of
`INTEGER NOT NULL`, so its physical type is SCALAR and its Java row type is `int`, while linq4j erases its
element type and yields `java.lang.Integer`. `JavaSequences.FromJava` **cast** where it should have
**converted**. It now goes through `JavaValues.As`, which is the adapter this port has for exactly that, and
which costs nothing for the `Object[]` rows that are nearly all of them.

This is the fourth time the same invariant has been broken and the second time in an adapter — see the note
in `CLAUDE.md`. It is also the second time a claim that "the converters carry it" was untrue and unmeasured:
the converter had never met a one-column primitive result, because the extra rule meant this convention
planned the query itself. Five differential tests now run with the limit-sort rule on **on both sides**,
which is the first time `EnumerableLimitSort` has ever been the oracle for `ClrEnumerableLimitSort` —
before, Calcite could not plan the node at all. Two more assert which node each side chose, with and without
the rule.

**9.32 A lexical scope has to beat the binding, not follow it.** 9.31 gave a lambda's parameters a scope by
name, consulted when a mention resolved to nothing else — which was enough for the window table function and
not for MATCH_RECOGNIZE. `EnumerableMatch` translates a pattern definition against a `row_` that is the input
row, and then declares the predicate method's parameter as a `row_` that is the `MemoryFactory.Memory`
around it. The first `row_` is already bound by the time the method body is translated, so the reference hit
won and the body called `Memory.get` on an `Employee`.

Java has no way to express what the old order assumed. **Inside a method, the parameter shadows the outer
variable, and the source carries nothing but the name** — an outer variable of that name is not merely
lower-priority, it is unreachable. The scope is consulted first now. Nothing regressed, which is the
expected result: the only trees that could change are the ones where two objects share a name inside a
lambda, and those were broken.

**9.31 The window table function was blocked on a rule this project had already written down.**
`CLAUDE.md` states it: "Java resolves a *name*, so a method's parameter shadows an outer variable Calcite
deliberately gave the same name". 5.2 is that sentence and nothing else — `TumbleImplementor` and
`tumblingWindowSelector` each make a `ParameterExpression` called `_input`, and the source Janino compiles
says `_input` in both places. Four explanations had been tried and disproved before this one, and the rule
that explains it was in the file the whole time.

**The same sentence claimed a mechanism that did not exist.** It said `Anonymous` "gives an anonymous
method's parameters a lexical scope for exactly that", and no such scope was in `ExpressionTranslator`:
`Lambda(MethodDeclaration)` bound its parameters by reference like everything else. The rule was right, the
implementation was aspirational, and nothing had reached the case. Both a lambda and an anonymous method
carry the scope now. **A rule written in a file is not a rule the code holds**, and this is the second time
that gap has cost something — 9.23 is the first.

**9.30 A linq4j tree Janino compiles has been optimised, and one we translate had not been.**
`PhysType.generateNullAwareAccessor` writes `field == null ? null : List1(field)` for every key, null-safe or
not. Where the field is a primitive that comparison is not Java — `int == null` does not compile — and
`OptimizeShuttle` folds it to `false` before Janino ever sees it. Its class comment says so in as many
words: "the optimizations are essential, not mere tweaks. Without optimization, expressions such as
`false == null` will be left in, which are invalid to Janino (because it does not automatically box
primitives)." Every tree Calcite compiles has been through it, because a node hands its tree to
`BlockBuilder.append`, which runs it.

Ours had not, and the CLR is stricter than Janino: it did not reject the comparison, it converted a null to
an `int` and threw at run time. `ExpressionTranslator` now runs `OptimizeShuttle` over an expression
arriving from outside — statements excepted, because a statement the shuttle rewrites away becomes
`EMPTY_STATEMENT`, which `BlockBuilder` filters and a bare block does not.

**This is a general rule and it belongs with the ones in `CLAUDE.md`**, where it now is: what a linq4j tree
means is what it means *after* the pass that always runs before Janino. It was invisible for as long as
every tree this convention translated directly happened to be one the shuttle would not have changed, and
5.7 was the first that was not.

**9.29 The aggregate's four branches were two, and the file called it "2 ctors, `copy`, `implement`".**
Nothing about that row was false. `implement` is one member on both sides, and inside it Calcite chooses
between a grouping set, an empty group key, a distinct, and a plain group by; ours chose between two of
those and converted a `ROLLUP` anyway, because `EnumerableAggregate`'s constructor refuses three things and
a non-simple group is not one of them — Calcite's implements it, so its constructor has no reason to. §2 now
gives `implement`'s branches as rows of their own wherever the two sides differ inside one member, which is
9.25's lesson applied rather than restated.

**9.28 A converter hands over a sub-plan, and a sub-plan is not a plan.** Fixing 5.9 turned up a stashed
variable — `EnumerableInterpreter` stashes its `RelNode`, and the declaration that binds it is emitted by
`implementRoot`, which a converted sub-plan never reaches. The value was on the map the whole time, and the
map is shared between the two implementors for exactly this reason, so the translator answers such a
variable with the object. **This is the second thing found by running a node rather than reading it**, and
both were in the same query: 5.6 argues that a value Calcite's implementor stashes cannot reach a plan of
ours, and this is that argument's other half — it can, whenever the shared map holds it.

**And the oracle was broken in the same place.** `TestDataContext.get` returned null for every name, so
Calcite's generated `bind` read null back for its own stashed table and threw. A recursive query could not
have run on *either* side of this harness, which is the other reason nothing had ever tested one. It serves
the parameter map now — which is what a real `DataContext` does, and what makes `EnumerableConvention` an
oracle for these two nodes at all.

**9.27 The spool handed CLR values to a Java reader, which is the invariant this port exists to keep.**
`LazyCollectionSpool` wrote its rows into the table's `java.util.Collection` as they stood, and the
interpreter that reads that collection back met a `System.Int32` where `SqlFunctions.toInt` wanted a
`java.lang.Integer`. `CLAUDE.md` states the rule — every boundary where a value crosses between the two
runtimes is an adapter, and if it casts it is wrong — and lists the times it has leaked. This is the next
one, and it is the first that was not a cast but an omission: nothing converted at all. It was invisible
because no test had ever written to a spool.

**9.26 Re-deriving §1 to §4 found five divergences, four of them wrong answers, and 9.21 had reported that
the same move "changed less than the size of the diff suggested".** Both statements are true and the second
is what made the first take three months to notice. 9.21 moved the baseline by diffing `calcite-1.41.0`
against `calcite-1.42.0` and re-deriving only what the diff touched, and concluded that eleven of the
seventeen changed files are classes this convention reuses rather than ports. What it did not do is read the
six that are ports **line by line**: `EnumerableHashJoin` is one of them, the diff did touch it, and the
attention went to the method 1.42 added rather than to the two calls it changed inside the methods that were
already there. 5.7 is those two calls. **A file being in the diff is not the same as a body being read**, and
the row that said so ("the mark-join path is 1.42") read as though the rest of the class had been checked.

**9.25 Four rows read as verified and were not reachable.** 2.11 gave `EnumerableRepeatUnion` as "the same
three" and `EnumerableTableSpool` as "all four, the refusal included"; 2.14 gave `EnumerableAggregate` as
"2 ctors, `copy`, `implement`". Member for member those rows are right — and the recursion pair throws on
the first query that reaches it (5.9) and the aggregate answers a `ROLLUP` with the wrong rows (5.10).
**Counting members is not comparing bodies.** `implement` is one member on both sides and Calcite's has four
branches; the row that said "`implement` → `Implement`" hid three missing ones. Every §2 row that names
`implement` now names what is inside it where the two differ.

**9.24 Four suspicions measured clean, and one of the four proves less than it looks.** Recorded because a
list of only the confirmed ones would make this pass look better calibrated than it was. `SELECT DISTINCT`
over every column agreed despite the missing shortcut, which is why 5.11 was the one of the five that cost
no rows; an equi-join on two keys, and its `EXISTS` and `NOT EXISTS` forms, agree — but the plan for all
three is a **merge join**, so they never tested what they were aimed at (8.6). Three clean, one void.

**9.23 The port's own comment was the reason 5.7 went unseen, and it is wrong about 1.41 too.**
`ClrEnumerableHashJoin.Accessor` says "1.41 has `generateAccessor` alone. The null aware variant … arrives
in 1.42; until the reference moves, a null key is excluded by the join itself rather than by the accessor."
`calcite-1.41.0`'s `PhysType` declares `generateAccessorWithoutNulls`, and 1.41's hash join, semi join and
ASOF join all call it. So the comment was wrong when it was written, stale afterwards, and read as an
explanation on both counts. This file has recorded the same shape twice before — 9.2 and 9.20, where the
version was read off a working tree instead of a tag. **A claim about which release has what is checkable in
one command, and a comment asserting it without one is a defect.**

**9.22 "RelBuilder.create wants a JDBC connection IKVM cannot give it" was an excuse, not a diagnosis.** It
does open a `jdbc:calcite:` connection, for the reason 2.20 now gives. It failed in the Linq test project
because that project had never registered calcite-core on IKVM's boot class path, so the driver's reflective
`Class.forName` could not find `CalciteJdbc41Factory` — and `AdoQueryTests` had been doing exactly that, in
one line, the whole time. The fix was that line. `ShouldCombineTwoQueries` builds a real two-query combine
now rather than the same input twice, which is what made the "shorter query contributes null" behaviour
testable at all — the weaker test could not have caught it.

Cheapest-conclusion-first, again, and the third time in this file: 9.14, 9.15 and now this.

**9.21 Moving the baseline to 1.42 changed less than the size of the diff suggested.** 21 files, 985
insertions — and of the seventeen changed files, eleven are classes this convention reuses rather than ports
(`RexImpTable`, `RexToLixTranslator`, `NullPolicy`, `MatchUtils`, `PhysType`, `PhysTypeImpl` and the rest),
so their changes arrived free with the assembly. Six ported nodes needed four deltas between them, and two
of those nodes needed nothing: `EnumerableCollect`, `EnumerableIntersect` and `EnumerableMinus` had already
been written against the 1.42 tree, which is why they carried the ARRAY/SCALAR branch and omitted the
`pref = pref.of(result.format)` line 1.41 has. **That was an unrecorded deviation from the 1.41 baseline
this file claimed** — correct against the version now referenced, and luck rather than judgement.

The real work was the four files 1.42 added and the `LEFT_MARK` join type behind them, which is 2.5, 2.6,
2.19 and 2.20 and about six hundred lines. `Combine` is the one node in this file that no query can reach.

**9.20 The whole file was pinned to the wrong baseline, and §7 to the wrong release.** This document opened
with "against Calcite 1.41.0 — the version the projects reference". The projects reference 1.42.0 now, and
§7's seven classes were called "not in 1.41" as though that made them future work: four of them are in 1.42
and so are `markJoinSelector`, `generateNullAwareAccessor` and `nullExclusionFlags`. Both errors have the
same cause as 9.2, which this file already records: the working tree was read instead of a tag, and the tree
had moved to 1.43 without anyone noticing. `EnumerableTableModify`'s rewrite was called a 1.42 feature on
the same evidence, and 1.42's copy of that file is byte-identical to 1.41's.

The header now carries the warning, 3.3 carries the rule count that follows from it (26, not 24), and §7 is
split by release. What is *not* done is re-deriving §1 to §4 against `calcite-1.42.0` — 21 files, 985
insertions — and that is the next parity task rather than something this entry quietly implies.

*Done 2026-08-05, and it found five divergences: 5.7 to 5.11, and 9.26 on why this pass did not.*

**9.19 `ImplementRoot` and `ToBindable` are Calcite's again.** 5.4 and 5.5 were both "copying is possible
and we did not", which is the one thing §6 does not admit. `ImplementRoot` wraps a failing node, so the
failure names the plan; `ToBindable` takes the Spark handler in Calcite's argument position and refuses it,
so a caller's configuration is answered rather than dropped. The refusal is the only part that could not be
copied, and it is 6.16. Two tests, one each: a `ClrEnumerableProject` built by hand is the node that refuses
to implement itself by design, which is what makes the wrap testable at all.

**9.17 "A pass-through node does not re-optimise its input's row format" was filed as demonstrated.** It is
demonstrated in *this* convention only. Running Calcite's is blocked by the CLR table function Janino cannot
name, which is the same blocker the whole table-function comparison has. The argument from Calcite's source
is now written out — scan reshapes, table function scan refuses to optimise, pass-through re-optimises anyway
— and the missing measurement is on `TODO.md`. Now 6.9.
