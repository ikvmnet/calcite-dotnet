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
