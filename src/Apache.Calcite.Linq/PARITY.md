# `ClrEnumerableConvention` against `EnumerableConvention`

Class by class and member by member, against **Calcite 1.41.0** — the version the projects reference. The
source read is `git archive calcite-1.41.0 core/src/main/java/org/apache/calcite/adapter/enumerable` out of
`D:\calcite`, not that repository's working tree, which is 1.42.0-SNAPSHOT and differs. The package is 105
files plus seven in `enumerable/impl`.

**Read the tag, then check the assembly.** They are not the same. `RelOptUtil.registerDefaultRules` registers
`EnumerableRules.ENUMERABLE_RULES` in the compiled 1.41.0 — measured, by counting the planner's rules before
and after the call — and the tag's text of that method gives no sign of it until its last third.

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
| `implementRoot` | `ImplementRoot` | returns a `LambdaExpression`, not a `ClassDeclaration`. Two differences beyond that: it does not walk the block for the `GotoStatement` carrying the value, because the value is the expression; and **it does not wrap a failing node in `IllegalStateException("Unable to implement …")`**, which Calcite's does — 5.4 |
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
| `toBindable(parameters, spark, rel, prefer)` | `ToBindable(parameters, rel, prefer)` | the `CalcitePrepare.SparkHandler` parameter is dropped; 5.5 |
| `getBindable`, `compileToBindable`, `StaticFieldDetector`, `BINDABLE_CACHE` | **—** | Janino compilation and its cache; there is nothing to port; 6.15 |
| `box`, `BoxEnumerable`, `BoxEnumerator` | **—** | wraps each row in a one-element `Object[]` for the interpreter, which is not ported |
| `EnumerableNode`, and the class being an `InterpretableRel` (ctor, `copy`, `implement(InterpreterImplementor)`) | **—** | the interpreter is not ported; ours is a static helper |
| — | `ClrBindable` | ours: the compiled delegate as a `Bindable` and `Typed`, which is what Janino's generated class was |

---

## 2. Nodes

`EnumerableX` → `ClrEnumerableX` throughout. Calcite's 32 node classes are all accounted for: 27 in 2.1 to
2.17, `EnumerableInterpretable` in 1.7, and `EnumerableMatch`, `EnumerableTableModify`,
`EnumerableInterpreter` and `EnumerableBindable` in 5.1.

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
| `implementHashJoin`, `implementHashSemiJoin` | `ImplementHashJoin`, `ImplementSemiJoin` | 1.41 has these two and no more; the mark-join path is 1.42, 7.2 |
| `computeSelfCost`, `passThroughTraits`, `deriveTraits`, `getDeriveMode` | all four | |
| — | `Accessor`, `Predicate` | ours, factored out of the two bodies |

#### 2.6 `EnumerableNestedLoopJoin`

| Calcite | ours | |
|---|---|---|
| 2 ctors, `create`, `copy`, `implement`, `computeSelfCost`, `passThroughTraits`, `deriveTraits`, `getDeriveMode` | 1 ctor and the rest | |

#### 2.7 `EnumerableMergeJoin`

| Calcite | ours | |
|---|---|---|
| ctor, with both sanity checks and the join-type refusal | ctor, all three | |
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
| ctor, `create`, `copy(traitSet, condition, …)` (throws), `copy(traitSet, inputs)`, `implement` | all five | |
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
| `EnumerableMergeUnion`: ctor (with both checks), `create`, `copy`, `implement` | all four | extends the union node, as Calcite's extends `EnumerableUnion` |
| `EnumerableDefaults.mergeUnion`, `MergeUnionEnumerator` | `ClrEnumerableDefaults.MergeUnion` | the k-way merge as an iterator, duplicate set cleared per key as Calcite clears it |
| `EnumerableRepeatUnion`: ctor, `copy`, `implement` | the same three | ours is public where Calcite's ctor is package private; one argument differs, 6.9 |
| `EnumerableTableSpool`: ctor, `create`, `copy`, `implement` (refuses anything but LAZY) | all four, the refusal included | one argument differs; 6.9 |

### 2.12 Collect, uncollect, table function

| Calcite | ours | |
|---|---|---|
| `EnumerableCollect`: 2 ctors, `create`, `copy`, `implement` | 1 ctor and the rest | |
| `EnumerableUncollect`: 3 ctors, `create`, `copy`, `implement` | 1 ctor and the rest | the two extra ctors are deprecated or take the item aliases Calcite's `create` leaves empty |
| `EnumerableTableFunctionScan`: ctor, `copy`, `implement`, `defaultTableFunctionImplement`, `isQueryable` | the same five, `defaultTableFunctionImplement` as `ImplementTableFunction` | `isQueryable` is a faithful port and returns false for a `ScannableTable`, which is what makes 6.9 reachable |
| `isImplementorDefined`, `tvfImplementorBasedImplement` | **—** | the window-TVF path, refused in the rule's `matches`; 5.2 |
| — | `Format` | ours |

### Aggregate

Two classes on both sides.

#### 2.13 `EnumerableAggregateBase` → `ClrEnumerableAggregateBase`

| Calcite | ours | |
|---|---|---|
| ctor | ctor | |
| `hasOrderedCall` | `HasOrderedCall` | |
| `declareParentAccumulator` | `DeclareParentAccumulator` | |
| `implementLambdaFactory` | `ImplementLambdaFactory` | |
| `createAccumulatorAdders` | `CreateAccumulatorAdders` | |
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
| `EnumerableBatchNestedLoopJoinRule` | `ClrEnumerableBatchNestedLoopJoinRule` | identical, `matches` included |
| `EnumerableMatchRule` | **—** | blocked; 5.1 and 6.14 |
| `EnumerableTableModifyRule` | **—** | the convention is read-only; 5.1 |
| `EnumerableInterpreterRule`, `EnumerableBindable.EnumerableToBindableConverterRule` | **—** | not started; 5.1 |

### 3.2 The rule classes are all accounted for

All 31 rule classes 1.41 has in the package: 30 files plus the one nested in `EnumerableBindable`.

### 3.3 Rule sets

`EnumerableRules.ENUMERABLE_RULES` is **24** at 1.41, and the merge union rule **is one of them**. Three rule
fields sit outside the list — `ENUMERABLE_LIMIT_SORT_RULE`, `ENUMERABLE_SORTED_AGGREGATE_RULE` and
`ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE` — and nothing in core registers any of the three; a caller turns
them on.

`ClrEnumerableRules.Rules()` is **24**: those 24 less match and table modify, which is 22, plus the two
converters. The three rules Calcite leaves out of its list are left out of this one. There is no membership
difference left; the limit-sort rule used to be in this list and 9.18 is why it is not.

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
| `joinSelector`, `generatePredicate` | `JoinSelector`, `GeneratePredicate` |
| `toLinq4jJoinType` | `ToLinq4jJoinType` |
| every `RexToLixTranslator.translate` overload | `Translate`, by way of the public `translateList` |
| `joinSelectorCompact`, `shouldGenerateCompactCode` | **—** 6.7 |
| `tumblingWindowSelector` | **—** the TVF path is refused; 5.2 |
| `overridingMethodDecl`, `toInternal` ×3, `fromInternal` ×4, `internalTypes`, `convertAssignableTypes`, `convertAssignableType`, `matchMethodParameterTypes`, `matchMethodParameterType`, `isA`, `representAsInternalType`, `hopWindows` | **—** they are the private inside of `convert` and `call`, which are public and reused whole |
| `convert`, `evaluate`, `call`, `numberToBigDecimal`, `generateCollatorExpression`, `toExternal`, `sessionize`, `hopping`, `tumbling` | public — reused as they are |
| — | `BoxRows` |

### 4.5 `ClrEnumerableDefaults`, and the types with no counterpart

`ClrEnumerableDefaults` is the counterpart of linq4j's `EnumerableDefaults`, not of anything in this
package: 34 public operators over typed delegates where linq4j's are over `Function1` and `Function2`. Where
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

**5.1 Nodes not written.** Four, each with its rule: `EnumerableMatch`, `EnumerableTableModify`,
`EnumerableInterpreter`, `EnumerableBindable`. Match is argued in 6.14; `TableModify` waits on the convention
being more than read-only; the interpreter and bindable are simply not done.

**5.2 The window table function path is refused.** `isImplementorDefined`, `tvfImplementorBasedImplement` and
`EnumUtils.tumblingWindowSelector` have no counterpart, and TUMBLE runs by neither route. Two failures are
recorded in `TODO.md` and neither is understood; until one is, this is outstanding rather than justified.

**5.3 — resolved.** `Rules()` registered the limit-sort rule and `ENUMERABLE_RULES` does not. The rule is
out of the default list now, and 9.18 records what dropping it uncovered. The number is kept and not reused.

**5.4 `ClrEnumerableRelImplementor.ImplementRoot` does not wrap a failing node.** Calcite catches a
`RuntimeException` out of `implement` and rethrows `IllegalStateException("Unable to implement " + <the
plan>)` with the original suppressed, so a node that cannot implement itself names the plan that reached it.
Ours lets the original out. One `try`/`catch`, and the diagnostic is the reason the defect in 6.11 was
legible at all.

**5.5 `ClrEnumerableInterpretable.ToBindable` drops the `CalcitePrepare.SparkHandler` parameter.** Calcite's
`toBindable` hands the generated class to the spark handler when one is enabled and compiles it with Janino
otherwise. There is no generated class here, so the parameter has nothing to do — but dropping it from the
signature is a decision, not a consequence, and the call sites would read the same if it were ignored.

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

**6.9 A pass-through node does not re-optimise its input's row format.** *A defect in Calcite — argued from
its source, and **not** demonstrated by running it. Read the argument before relying on it.* Sort, limit,
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

**The demonstration in `EnumerableConvention` is missing, and is blocked by the thing that blocks every
table-function comparison**: our table function is a CLR class, Janino cannot name it, so Calcite has no
plan to run. Until a Java-visible one-column table function is put under a sort in Calcite's own convention,
this is an argument from reading, not a measurement. `TODO.md` item 1b carries the experiment.

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

**6.14 Match is not written, and cannot be as a node.** *Calcite's own type is unreachable.*
`EnumerableMatch.PassedRowsInputGetter` and `PrevInputGetter` are package private *types* that Calcite's own
translator casts to by name; IKVM compiles them internal, so C# can name them and cannot construct them.
Reflection is not an acceptable way in. `TODO.md` has the three blockers in full.

**6.15 The interpreter and bindable have nothing to port to.** *Calcite's member exists to feed Janino.*
`EnumerableInterpretable.box`, `getBindable`, `compileToBindable`, and `EnumerableRelImplementor.classDecl`,
`TypeFinder`, `TypeRegistrar` all serve a generated class. There is no generated class here;
`SyntheticRecordEmitter` emits the record types at runtime instead.

---

## 7. Not in 1.41, and correctly absent

**7.1 Classes.** `EnumerableCombine`, `EnumerableCombineRule`, `EnumerableConditionalCorrelate`,
`EnumerableConditionalCorrelateRule`, `FetchOffsetRoundingPolicy`, `RexImplementorTable`,
`RexImplementorTables`. Do not port against `D:\calcite`'s working tree, which has all seven.

**7.2 Members, and the same trap one level down.** `EnumUtils.markJoinSelector` is 1.42 only, and so is
every mark-join path that calls it — in `EnumerableHashJoin`, `EnumerableNestedLoopJoin` and
`EnumerableConditionalCorrelate`. So are `PhysType.generateNullAwareAccessor` and
`JoinInfo.nullExclusionFlags`. Nothing of ours is missing on their account.

---

## 8. Measured on the way, and worth keeping

**8.1 195 tests pass**, measured 2026-08-04: 120 differential, of which 87 compare rows with the default
planner, 11 with top-down optimisation on, 5 with the sorted aggregate rule on, 5 with the batch nested loop
join rule on, 5 with the limit-sort rule on, 5 assert rows by hand because `EnumerableConvention` cannot run
the query at all, and 2 assert which node the planner chose.

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
came from auditing §6 against the rule in its own header.

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

**9.17 "A pass-through node does not re-optimise its input's row format" was filed as demonstrated.** It is
demonstrated in *this* convention only. Running Calcite's is blocked by the CLR table function Janino cannot
name, which is the same blocker the whole table-function comparison has. The argument from Calcite's source
is now written out — scan reshapes, table function scan refuses to optimise, pass-through re-optimises anyway
— and the missing measurement is on `TODO.md`. Now 6.9.
