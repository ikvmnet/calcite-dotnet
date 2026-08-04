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

---

## 1. The convention, the interface, the implementor

### `EnumerableConvention` → `ClrEnumerableConvention`

| Calcite | ours | |
|---|---|---|
| `INSTANCE` (enum constant) | `Instance` (static field, private ctor) | C# has no enum implementing an interface |
| `COST_MULTIPLIER = 1.0d` | `CostMultiplier = 1.0d` | read by `ClrEnumerableWindow.computeSelfCost` and one converter, as Calcite's is |
| `toString`, `getInterface`, `getName`, `getTraitDef`, `satisfies`, `register` | `Convention.Impl` base, given `"CLR_ENUMERABLE"` and `typeof(ClrEnumerableRel)` | equivalent |
| `enforce` | `enforce` | statement for statement |
| `canConvertConvention` | `canConvertConvention` | |
| `useAbstractConvertersForConversion` | `useAbstractConvertersForConversion` | |
| `getRelFactories` | `getRelFactories` | same four factories, from `ClrEnumerableRelFactories` |

### `EnumerableRelFactories` → `ClrEnumerableRelFactories`

| Calcite | ours | |
|---|---|---|
| `ENUMERABLE_TABLE_SCAN_FACTORY` / `TableScanFactoryImpl.createScan` | `ClrEnumerableTableScanFactory` / `TableScanFactoryImpl.createScan` | |
| `ENUMERABLE_PROJECT_FACTORY` / `ProjectFactoryImpl.createProject` | same shape | plus the deprecated 4-argument overload, which C# must implement and Java defaults |
| `ENUMERABLE_FILTER_FACTORY` / `FilterFactoryImpl.createFilter` | same shape | plus the deprecated 2-argument overload |
| `ENUMERABLE_SORT_FACTORY` / `SortFactoryImpl.createSort` | same shape | plus the deprecated 5-argument overload |

### `EnumerableRel` → `ClrEnumerableRel`

| Calcite | ours | |
|---|---|---|
| `implement(EnumerableRelImplementor, Prefer)` | `Implement(ClrEnumerableRelImplementor, ClrEnumerablePrefer)` | returns `ClrEnumerableResult`, whose payload is an `Expression` rather than a `BlockStatement` |
| `Result` (nested class) | `ClrEnumerableResult` | same three fields |
| `Prefer` (nested enum) | `ClrEnumerablePrefer` | own type, §1 below |
| `passThroughTraits` default → null | same | |
| `deriveTraits` default → null | same | |
| `getDeriveMode` default → `LEFT_FIRST` | same | |
| — | `passThrough`, `derive` ×2 forwarded to `PhysicalNode.__DefaultMethods` | C# does not inherit the defaults of an interface IKVM compiled |

### `EnumerableRel.Prefer` → `ClrEnumerablePrefer`

| Calcite | ours | |
|---|---|---|
| `ARRAY`, `ARRAY_NICE`, `CUSTOM`, `CUSTOM_NICE`, `ANY` | `Array`, `ArrayNice`, `Custom`, `CustomNice`, `Any` | |
| `preferCustom`, `preferArray`, `prefer`, `of` | `PreferCustom`, `PreferArray`, `Prefer`, `Of` | extension methods: a C# enum carries no methods |
| — | `ToCalcite`, `FromCalcite` | one per converter, and nowhere else |

### `EnumerableRelImplementor` → `ClrEnumerableRelImplementor`

| Calcite | ours | |
|---|---|---|
| `visitChild` | `VisitChild` | |
| `implementRoot` | `ImplementRoot` | returns a `LambdaExpression`, not a `ClassDeclaration` |
| `classDecl`, `TypeFinder`, `TypeRegistrar` | **—** | they exist to declare synthetic types as members of the generated class; `SyntheticRecordEmitter` emits them at runtime instead |
| `stash` | `Stash` | ours returns a constant: an expression tree can hold the object Janino could not name |
| `registerCorrelVariable` / `clearCorrelVariable` / `getCorrelVariableGetter` | `RegisterCorrelVariable` / `ClearCorrelVariable` / `GetCorrelVariableGetter` | |
| `result` | `Result` | |
| `getConformance` | `Conformance` | |
| `getTypeFactory` (from `JavaRelImplementor`) | `TypeFactory` | |
| — | `BoxScalars` | CLR only: a one-column result of a primitive would otherwise be boxed the CLR way. Not `EnumerableInterpretable.box`, which is something else — see below |

### `EnumerableInterpretable` → `ClrEnumerableInterpretable`

| Calcite | ours | |
|---|---|---|
| `toBindable` | `ToBindable` | |
| `getBindable`, `compileToBindable`, `StaticFieldDetector` | **—** | Janino compilation; there is nothing to port |
| `box`, `BoxEnumerable`, `BoxEnumerator` | **—** | wraps each row in a one-element `Object[]` for the interpreter, which is not ported |
| `EnumerableNode`, and the class being an `InterpretableRel` (`copy`, `implement(InterpreterImplementor)`) | **—** | the interpreter is not ported; ours is a static helper |

---

## 2. Nodes

`EnumerableX` → `ClrEnumerableX` throughout.

### Scan and values

| `EnumerableTableScan` | ours | |
|---|---|---|
| ctor, asserting the convention and `canHandle` | ctor, both assertions kept | |
| `create` | `Create` | statement for statement |
| `canHandle(Table)`, `canHandle(RelOptTable)` | `CanHandle(Table)`, `CanHandle(RelOptTable)` | |
| `deduceElementType` | reused from Calcite (public static) | |
| `deduceFormat` | `DeduceFormat` | |
| `getExpression(PhysType)` | inlined in `Implement` | |
| `toEnumerable`, `toRows`, `fieldExpression`, `format`, `hasCollectionField` | `ToEnumerable`, `ToRows`, `FieldExpression`, `Format`, `HasCollectionField` | |
| `passThrough`, `getDeriveMode` | `passThrough`, `getDeriveMode` | |
| `copy`, `implement` | `copy`, `Implement` | |

| `EnumerableValues` | ours | |
|---|---|---|
| ctor, `create`, `copy`, `implement` | same four | |
| `passThrough` (the tuples must already be in that order), `getDeriveMode` | `passThrough`, `getDeriveMode` | |

### Project, filter, calc

| Calcite | ours | |
|---|---|---|
| `EnumerableProject`: 2 ctors, `create`, `copy`, `implement`, `passThroughTraits`, `deriveTraits` | 1 ctor and the rest | the second ctor is deprecated in Calcite |
| `EnumerableFilter`: ctor, `create`, `copy`, `implement`, `passThroughTraits`, `deriveTraits` | all six | |
| `EnumerableCalc`: 2 ctors, `create`, `copy`, `implement`, `getProgram`, `passThroughTraits`, `deriveTraits` | 1 ctor and the rest, plus `Exps` | `getProgram` is `Calc`'s either way; `Exps` is the local Calcite writes twice inline |

`ClrEnumerableProject.Implement` and `ClrEnumerableFilter.Implement` throw, as Calcite's do: the calc rules
run afterwards and a calc is never worse.

### Sort and limit

| Calcite | ours | |
|---|---|---|
| `EnumerableSort`: ctor, `create`, `copy`, `implement` | all four | |
| `EnumerableLimit`: ctor, `create`, `copy`, `explainTerms`, `implement`, `getExpression` | all six, `getExpression` as `Count` | |
| `EnumerableLimitSort`: ctor, `create`, `copy`, `implement` | all four | both call the one `Count`, as both call the one `getExpression` |

### Joins

| `EnumerableHashJoin` | ours | |
|---|---|---|
| 2 ctors, `create`, `copy`, `implement` | 1 ctor and the rest | |
| `implementHashJoin`, `implementHashSemiJoin` | `ImplementHashJoin`, `ImplementSemiJoin` | 1.41 has these two; the mark-join path is 1.42 |
| `computeSelfCost`, `passThroughTraits`, `deriveTraits`, `getDeriveMode` | all four | |
| — | `Accessor`, `Predicate` | ours, factored out of the two bodies |

| `EnumerableNestedLoopJoin` | ours | |
|---|---|---|
| 2 ctors, `create`, `copy`, `implement`, `computeSelfCost`, `passThroughTraits`, `deriveTraits`, `getDeriveMode` | 1 ctor and the rest | |

| `EnumerableMergeJoin` | ours | |
|---|---|---|
| ctor, with both sanity checks and the join-type refusal | ctor, all three | |
| `isMergeJoinSupported`, `getCollation`, `getCollations` | `IsMergeJoinSupported`, `GetCollation`, `GetCollations` | |
| `create`, `copy`, `implement`, `computeSelfCost`, `getDeriveMode` | all five | |
| `passThroughTraits` (six cases), `deriveTraits` | both, and the six cases | one guard added; §5 |
| `buildMapping`, `extendCollation`, `intersectCollationAndJoinKey` | `BuildMapping`, `ExtendCollation`, `IntersectCollationAndJoinKey` | private there, ported |
| `EnumerableDefaults.mergeJoin`, `isMergeJoinSupported`, `compareNullsLastForMergeJoin` | `ClrEnumerableDefaults.MergeJoin`, `IsMergeJoinSupported`, `CompareNullsLastForMergeJoin` | the algorithm over typed delegates, as an iterator rather than an enumerator with a state machine |
| — | `Predicate`, `Cartesian` | ours, factored out |

| `EnumerableAsofJoin` | ours | |
|---|---|---|
| ctor, `create`, `copy(traitSet, condition, …)` (throws), `copy(traitSet, inputs)`, `implement` | all five | |
| `computeSelfCost`, `passThroughTraits`, `deriveTraits` | all three | |
| `generateTimestampComparator`, `getTimestampFieldIndex` | `GenerateTimestampComparator`, `GetTimestampFieldIndex` | |
| `EnumerableDefaults.asofJoin` | `ClrEnumerableDefaults.AsofJoin` | the algorithm reimplemented over typed delegates, as the hash join's is. The index is a `java.util.HashMap`, because the emitted order is that map's |

| `EnumerableCorrelate` | ours | |
|---|---|---|
| ctor, `create`, `copy`, `implement`, `passThroughTraits`, `deriveTraits`, `getDeriveMode` | all seven | |

### Set operations and recursion

| Calcite | ours | |
|---|---|---|
| `EnumerableUnion` / `EnumerableIntersect` / `EnumerableMinus`: ctor, `copy`, `implement` | the same three each | |
| `EnumerableRepeatUnion`: ctor, `copy`, `implement` | the same three | ours is public where Calcite's ctor is package private |
| `EnumerableTableSpool`: ctor, `create`, `copy`, `implement` (refuses anything but LAZY) | all four, the refusal included | |

### Collect, uncollect, table function

| Calcite | ours | |
|---|---|---|
| `EnumerableCollect`: 2 ctors, `create`, `copy`, `implement` | 1 ctor and the rest | |
| `EnumerableUncollect`: 3 ctors, `create`, `copy`, `implement` | 1 ctor and the rest | the two extra ctors are deprecated or take the item aliases Calcite's `create` leaves empty |
| `EnumerableTableFunctionScan`: ctor, `copy`, `implement`, `defaultTableFunctionImplement`, `isQueryable` | the same five, `defaultTableFunctionImplement` as `ImplementTableFunction` | |
| `isImplementorDefined`, `tvfImplementorBasedImplement` | **—** | the window-TVF path, refused in the rule's `matches`. See `TODO.md` |
| — | `Format` | ours |

### Aggregate

Two classes on both sides.

| `EnumerableAggregateBase` | `ClrEnumerableAggregateBase` | |
|---|---|---|
| ctor | ctor | |
| `hasOrderedCall` | `HasOrderedCall` | |
| `declareParentAccumulator` | `DeclareParentAccumulator` | |
| `implementLambdaFactory` | `ImplementLambdaFactory` | |
| `createAccumulatorAdders` | `CreateAccumulatorAdders` | |
| `createAggStateTypes` | `CreateAggStateTypes` | |
| `AggContextImpl` (private) | `ClrAggContext` | ported, being unreachable |
| `AggAddContextImpl` (public, `impl`) | `ClrAggAddContext` **extends it** | reused |
| `PhysTypeImpl.of(typeFactory, javaRowType)`, called from `EnumerableAggregate.implement` | `AccumulatorPhysType` | package private there; this is the public route to the same thing |
| — | `Function0Of`, `Function1Of`, `Function2Of` | a delegate has to be wrapped where Java passed a lambda to a functional interface |

Calcite's helpers are instance methods that never read `this`; ours are static, because
`ClrEnumerableWindow` needs two of them and is not an aggregate — see the Window note below.

| `EnumerableAggregate` | `ClrEnumerableAggregate` | |
|---|---|---|
| 2 ctors, `copy`, `implement` | 1 ctor, `copy`, `Implement` | |

Both constructors refuse what the node cannot implement, by throwing `InvalidRelException` for the rule to
catch. Ours refuses one thing more: an aggregate call carrying its own ordering, which Calcite answers with
`LazyAggregateLambdaFactory` and a `SourceSorter` per call. Refused where the node is built, never in
`Implement`; the query then plans in `EnumerableConvention` and the rows cross a converter.

### Window

| Calcite | ours | |
|---|---|---|
| ctor, `copy(traitSet, inputs)`, `copy(constants)`, `implement`, `computeSelfCost` | all five | |
| `translateBound`, `declareAndResetState`, `implementResult`, `WindowRelInputGetter`, the anonymous `WinAggFrameResultContext` | `TranslateBound`, `DeclareAndResetState`, `ImplementResult`, `WindowRelInputGetter`, `ClrWinAggFrameResultContext` | five private members, ported |
| `getPartitionIterator`, `getRowCollationKey`, `implementAdd`, `buildExcludeGuard` | `ClrEnumerableDefaults.Window` and `WindowLoop` | the loop is owned rather than generated — the one forced divergence, and it is why the aggregate's accumulator helpers are shared |
| `sampleOfTheGeneratedWindowedAggregate` | **—** | a documentation method |
| `WinAggAddContextImpl`, `WinAggResultContextImpl` (public, `impl`) | `ClrWinAggAddContext`, `ClrWinAggResultContext` **extend them** | reused |
| `WinAggContext` (interface) | `ClrWinAggContext` implements it | |
| — | `Hoist`, `PartitionSelector`, `WithReturn`, `RexArguments` | ours |

### Traits

| `EnumerableTraitsUtils` | `ClrEnumerableTraitsUtils` | |
|---|---|---|
| `isCollationOnTrivialExpr` | `IsCollationOnTrivialExpr` | |
| `passThroughTraitsForProject`, `deriveTraitsForProject` | `PassThroughTraitsForProject`, `DeriveTraitsForProject` | |
| `passThroughTraitsForJoin`, `deriveTraitsForJoin` | `PassThroughTraitsForJoin`, `DeriveTraitsForJoin` | |

Package private in Calcite, so ported. One substitution: Calcite writes `collation.apply(mapping)`, whose
implementation is `RexUtil.apply(mapping, this)`; the interface method is a generic default that IKVM
erases, so the public static is called instead. It is the same call.

### Converters — ours only

`ClrEnumerableToEnumerableConverter` and `EnumerableToClrEnumerableConverter`, with a rule each, are what
lets one plan hold both conventions. They have no Calcite counterpart. Both override `computeSelfCost` with
the multiplier of the convention they produce.

---

## 3. Rules

| Calcite rule | ours | `convert` / `onMatch` |
|---|---|---|
| `EnumerableTableScanRule` | `ClrEnumerableTableScanRule` | identical, `canHandle` predicate and the expression check both |
| `EnumerableValuesRule` | `ClrEnumerableValuesRule` | identical: create, then copy onto the logical node's traits |
| `EnumerableProjectRule` | `ClrEnumerableProjectRule` | identical, `matches` included |
| `EnumerableFilterRule` | `ClrEnumerableFilterRule` | identical |
| `EnumerableCalcRule` | `ClrEnumerableCalcRule` | identical |
| `EnumerableAggregateRule` | `ClrEnumerableAggregateRule` | identical, `InvalidRelException` included |
| `EnumerableSortRule` | `ClrEnumerableSortRule` | identical |
| `EnumerableLimitRule` | `ClrEnumerableLimitRule` | identical |
| `EnumerableLimitSortRule` | `ClrEnumerableLimitSortRule` | identical |
| `EnumerableJoinRule` | `ClrEnumerableJoinRule` | identical: one rule, hash or nested loop, condition rearranged equi-first |
| `EnumerableAsofJoinRule` | `ClrEnumerableAsofJoinRule` | identical |
| `EnumerableCorrelateRule` | `ClrEnumerableCorrelateRule` | identical |
| `EnumerableUnionRule` | `ClrEnumerableUnionRule` | identical, including the cluster's trait set rather than the union's |
| `EnumerableIntersectRule` | `ClrEnumerableIntersectRule` | identical, `convertList` included |
| `EnumerableMinusRule` | `ClrEnumerableMinusRule` | identical |
| `EnumerableRepeatUnionRule` | `ClrEnumerableRepeatUnionRule` | identical |
| `EnumerableTableSpoolRule` | `ClrEnumerableTableSpoolRule` | identical, the spool's own read and write types passed through |
| `EnumerableWindowRule` | `ClrEnumerableWindowRule` | identical |
| `EnumerableTableFunctionScanRule` | `ClrEnumerableTableFunctionScanRule` | `convert` identical; ours adds `matches`, refusing a window TVF |
| `EnumerableCollectRule` | `ClrEnumerableCollectRule` | identical |
| `EnumerableUncollectRule` | `ClrEnumerableUncollectRule` | identical |
| `EnumerableFilterToCalcRule` | `ClrEnumerableFilterToCalcRule` | present |
| `EnumerableProjectToCalcRule` | `ClrEnumerableProjectToCalcRule` | present |
| `EnumerableMergeJoinRule` | `ClrEnumerableMergeJoinRule` | identical |
| `EnumerableMergeUnionRule` | **—** | node not written |
| `EnumerableSortedAggregateRule` | **—** | node not written |
| `EnumerableBatchNestedLoopJoinRule` | **—** | node not written |
| `EnumerableMatchRule` | **—** | blocked; see `TODO.md` |
| `EnumerableTableModifyRule` | **—** | the convention is read-only |
| `EnumerableInterpreterRule`, `EnumerableBindable.EnumerableToBindableConverterRule` | **—** | not started |

**Rule sets.** `EnumerableRules.ENUMERABLE_RULES` is 24 at 1.41. `ClrEnumerableRules.Rules()` is 23 of ours —
the 24 less match — plus the two converters. `CalcRules()` is the counterpart of
`RelOptRules.CALC_RULES` plus this convention's two calc rules, run as a second pass, which is what
`Programs.standard` does with Calcite's.

---

## 4. Reused rather than ported

Calcite's, used directly, and the reason the two conventions agree on what a row is: `PhysType`,
`PhysTypeImpl`, `JavaRowFormat`, `RexToLixTranslator`, `RexImpTable`, `JavaRelImplementor`,
`NestedBlockBuilder(Impl)`, `NullPolicy`, `CallImplementor`, `NotNullImplementor`,
`ReflectiveCallNotNullImplementor`, `MatchImplementor`, `MatchUtils`, `TableFunctionCallImplementor`,
`AggContext`, `AggImplementor`, `AggImpState`, `AggAddContext`, `AggResetContext`, `AggResultContext`,
`StrictAggImplementor`, `StrictWinAggImplementor`, the six `WinAgg*` interfaces, `AggregateLambdaFactory`,
`BasicAggregateLambdaFactory`, `BasicLazyAccumulator`, `LazyAggregateLambdaFactory`, `SourceSorter`, and the
whole `enumerable.impl` package.

`EnumUtils` is half reused and half ported, because half of it is package private:

| `EnumUtils` | `ClrEnumUtils` |
|---|---|
| `javaClass`, `fieldTypes`, `fieldRowTypes` ×2 | `JavaClass`, `FieldTypes`, `FieldRowTypes` ×2 |
| `joinSelector`, `generatePredicate` | `JoinSelector`, `GeneratePredicate` |
| `toLinq4jJoinType` | `ToLinq4jJoinType` |
| every `RexToLixTranslator.translate` overload | `Translate`, by way of the public `translateList` |
| `markJoinSelector`, `tumblingWindowSelector` | **—** (mark joins are 1.42; the TVF path is refused) |
| `convert`, `evaluate`, `call`, `numberToBigDecimal`, `generateCollatorExpression`, `toExternal`, `sessionize`, `hopping`, `tumbling` | public — reused as they are |
| — | `BoxRows` |

`ClrEnumerableDefaults` is the counterpart of linq4j's `EnumerableDefaults`, not of anything in this package: 29
operators over typed delegates where linq4j's are over `Function1` and `Function2`. `ClrPhysTypes`,
`ExpressionTranslator`, `MethodResolver`, `TypeResolver`, `FieldResolver`, `JavaCast`, `SamAdapters`,
`SyntheticRecordEmitter` and the `Delegate*` runtime types have no Calcite counterpart at all: they are what
an expression tree costs where Calcite has Java source.

---

## 5. Differences outstanding, with no justification

Everything here is a difference from `EnumerableConvention` that nothing yet argues for. The list is the
work; an entry leaves it by being resolved, or by moving to §6 with the argument written down.

**Nodes not written.** Seven, each with its rule: `EnumerableMergeUnion`, `EnumerableSortedAggregate`,
`EnumerableBatchNestedLoopJoin`, `EnumerableMatch`, `EnumerableTableModify`, `EnumerableInterpreter`,
`EnumerableBindable`. Three of them have an open question in front of the code — why `MergeUnionRule` never
matches, whether `SortedAggregate` can be chosen on a row-count-only cost model, what `BatchNestedLoopJoin`
needs — and `TODO.md` holds those. Match is argued in §6; the other six are simply not done.

**An aggregate call with its own ordering is refused, not implemented.** `HasOrderedCall` is ported and
nothing reads it. Answering the query needs `LazyAggregateLambdaFactory` and a `SourceSorter` per call.
Where the refusal is placed is justified in §6; that it refuses at all is not.

**`EnumUtils.markJoinSelector` has no counterpart.** Nothing needs it while the mark-join paths are 1.42
only, but that is a version accident rather than a reason.

**The window table function path is refused.** `isImplementorDefined` and `tvfImplementorBasedImplement`
have no counterpart, and TUMBLE runs by neither route. Two failures are recorded in `TODO.md` and neither is
understood; until one is, this is outstanding rather than justified.

**`ClrEnumerableTableFunctionScan.Format` is ours alone.** Calcite decides the row format inline in
`defaultTableFunctionImplement`. The method has no counterpart and no argument for being one.

---

## 6. Differences with a justification

Each of these departs from Calcite deliberately. The rule is the one `TODO.md` states: an expression tree
cannot express it, Calcite's own member is unreachable, or it is a defect — demonstrated, never asserted.

**A node's plan is an `Expression`, not a `BlockStatement`.** *An expression tree cannot express it.*
Calcite generates one Java method per plan, so a node contributes statements and a parent appends. There is
no method to append to here, so a parent composes; `Expression.Block` covers what needs statements.

**Loops are owned, not generated.** *An expression tree cannot express it.* `ClrEnumerableDefaults.Calc`,
`.Window`, `.MergeJoin`, `.AsofJoin`, `.HashJoin` and the rest are the loops Calcite writes into generated
source — written once, over typed delegates, rather than boxing through `Function1`.

**A window's aggregate state is a synthetic record.** *An expression tree cannot express it.* Calcite
declares it as locals of the generated method and mutates them in place; no lambda can carry that.

**A class of several methods becomes an object of delegates.** *An expression tree cannot express it.*
`DelegateEnumerator` and friends, for what `ExpressionTranslator.Anonymous` meets in a generated block.

**Every package private member is ported rather than reused.** *Calcite's own member is unreachable.*
`ClrEnumerableTraitsUtils`, most of `ClrEnumUtils`, the five private members of `EnumerableWindow`, the four
of `EnumerableAggregateBase`, `EnumerableMergeJoin`'s three, `PhysTypeImpl.of(typeFactory, javaRowType)`.
Each is listed against its original in §1 and §2.

**`ClrEnumerablePrefer` is this convention's own enum.** *An expression tree cannot express it*, narrowly:
`EnumerableRel.Prefer` is a Java enum carrying methods, which C# has no equivalent of — and what a node asks
its input for belongs to the convention asking. Converted at the two converters and nowhere else.

**`ClrEnumerableMergeJoin.passThroughTraits` refuses a foreign convention.** *A defect in Calcite,
demonstrated.* `EnumerableMergeJoin` returns `Pair.of(required, …)` — the trait set it was handed, convention
and all — and `PhysicalNode.passThrough` copies the node onto it. With both conventions in one planner and
top-down optimisation on, a CLR_ENUMERABLE subset asking Calcite's node to pass through gets an
`EnumerableMergeJoin` wearing CLR_ENUMERABLE, which the planner then refuses to register.
`TopDownRuleDriver.convert` asserts that a pass-through preserves the convention, so Calcite means for it
to; only assertions being off lets it through. Ours returns null for a convention that is not its own, and
every other line of the method is the port.

**A pass-through node does not re-optimise its input's row format.** *A defect, demonstrated.* Sort, limit,
limit-sort, spool and repeat union yield their input's rows unchanged, and Calcite builds their physical
type with `PhysTypeImpl.of(typeFactory, rowType, format)` — the overload that optimises, which turns ARRAY
into SCALAR for a one-column row. The rows keep their shape, so the physical type then says the row *is* the
value while the sequence still yields `Object[]`, and a parent reads field 0 as the row itself. Java erases
the element type and cannot see it; ours is typed, and a merge join over a one-column table function is
where it came out. Those five nodes use the four-argument overload with optimisation off.

**A correlate boxes its rows.** *A defect, demonstrated.* `EnumUtils.joinSelector` boxes both of its
parameter types, because linq4j's `Function2` erases to `Object`. Every other join here boxes its sequences
to match; the correlate did not, and an EXISTS sub-query — whose right side is one primitive boolean column
— is where the selector and the call disagreed.

**A record of no fields gets one constructor.** *An expression tree cannot express it.* Calcite generates
both the empty constructor and the all-fields one, and for a record of no fields those are the same
signature; two of them in one emitted type cannot be told apart by `Type.GetConstructor`. A semi join whose
right input projects nothing is one.

**The refusal of an ordered aggregate call is placed where Calcite places a refusal.** *Calcite's own
mechanism.* `InvalidRelException` from the constructor, which the rule catches, so the query plans in
`EnumerableConvention` and the rows cross a converter. That it refuses at all is §5.

**Match is not written, and cannot be as a node.** *Calcite's own type is unreachable.*
`EnumerableMatch.PassedRowsInputGetter` and `PrevInputGetter` are package private *types* that Calcite's own
translator casts to by name; IKVM compiles them internal, so C# can name them and cannot construct them.
Reflection is not an acceptable way in. `TODO.md` has the three blockers in full.

**The interpreter and bindable have nothing to port to.** *Calcite's member exists to feed Janino.*
`EnumerableInterpretable.box`, `getBindable`, `compileToBindable`, and `EnumerableRelImplementor.classDecl`,
`TypeFinder`, `TypeRegistrar` all serve a generated class. There is no generated class here;
`SyntheticRecordEmitter` emits the record types at runtime instead.

---

## 7. Not in 1.41, and correctly absent

`EnumerableCombine`, `EnumerableCombineRule`, `EnumerableConditionalCorrelate`,
`EnumerableConditionalCorrelateRule`, `FetchOffsetRoundingPolicy`, `RexImplementorTable`,
`RexImplementorTables`. Do not port against `D:\calcite`'s working tree, which has all seven.

---

## 8. Measured on the way, and worth keeping

**Trait derivation is dead unless the planner optimises top down.** `passThroughTraits`, `deriveTraits` and
`getDeriveMode` are called only by `TopDownRuleDriver`, and `CalciteSystemProperty.TOPDOWN_OPT` is false by
default. Eleven differential tests now plan both conventions with it on, which is what proves those methods
do anything at all.

**`EnumerableMergeJoin.passThroughTraits` returns the trait set it was handed**, convention and all, and
`PhysicalNode.passThrough` then copies the node onto it. With both conventions in one planner and top-down
optimisation on, a CLR_ENUMERABLE subset asks it to pass through and gets an `EnumerableMergeJoin` wearing
CLR_ENUMERABLE, which the planner refuses. `TopDownRuleDriver.convert` asserts that a pass-through preserves
the convention, so Calcite means for it to; only assertions being off lets it through. Every trait method
here builds from `getTraitSet()`, so nothing of ours can do this — and a merge join written here must not
follow Calcite's text on that line.
