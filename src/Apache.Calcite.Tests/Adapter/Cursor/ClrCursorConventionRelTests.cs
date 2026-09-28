using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.rules;
using org.apache.calcite.sql;
using org.apache.calcite.sql.fun;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Runs a plan built against a <see cref="org.apache.calcite.tools.RelBuilder"/> through this convention
    /// and through Calcite's, and requires the same rows.
    /// </summary>
    /// <remarks>
    /// The same comparison as <see cref="ClrCursorConventionDifferentialTests"/>, for plans that cannot be
    /// written as SQL: <c>Combine</c> has no syntax, the POSIX regex operators are not in the core parser, a
    /// recursive query over a transient table is built with <c>transientScan</c> and <c>repeatUnion</c>, and a
    /// column with its own collation can only be typed by hand. Each test ports one of Calcite's own, which
    /// build these plans through <c>CalciteAssert.withRel</c>; the section headings name the Calcite test
    /// class.
    /// </remarks>
    public class ClrCursorConventionRelTests
    {

        static java.lang.Integer I(int value) => java.lang.Integer.valueOf(value);

        // ------------------------------------------------------------------ EnumerableCombineTest
        //
        // A combine puts one query per column and one row per position, so a shorter query contributes null.

        [Fact]
        public void ShouldAgreeOnCombiningTwoQueries() =>
            ClrCursorConventionDifferentialTests.SameRel(builder =>
            {
                builder.scan("HR", "emps").filter(builder.equals(builder.field("deptno"), builder.literal(I(10)))).project(builder.field("name"));
                builder.scan("HR", "depts").project(builder.field("name"));

                return builder.combine(2).build();
            });

        [Fact]
        public void ShouldAgreeOnCombiningQueriesOfDifferentLengths() =>
            ClrCursorConventionDifferentialTests.SameRel(builder =>
            {
                builder.scan("HR", "emps").project(builder.field("name"));
                builder.scan("HR", "depts").project(builder.field("name"));

                return builder.combine(2).build();
            });

        [Fact]
        public void ShouldAgreeOnCombiningQueriesOfSeveralColumns() =>
            ClrCursorConventionDifferentialTests.SameRel(builder =>
            {
                builder.scan("HR", "emps").filter(builder.equals(builder.field("deptno"), builder.literal(I(10)))).project(builder.field("empid"), builder.field("name"));
                builder.scan("HR", "depts").project(builder.field("deptno"), builder.field("name"));

                return builder.combine(2).build();
            });

        [Fact]
        public void ShouldAgreeOnCombiningQueriesOfDifferentColumnCounts() =>
            ClrCursorConventionDifferentialTests.SameRel(builder =>
            {
                builder.scan("HR", "depts").project(builder.field("name"));
                builder.scan("HR", "emps").filter(builder.equals(builder.field("deptno"), builder.literal(I(10))))
                    .project(builder.field("empid"), builder.field("name"), builder.field("deptno"));

                return builder.combine(2).build();
            });

        // ------------------------------------------------------------------ EnumerableCalcTest

        /// <summary>
        /// A COALESCE of a nullable field and a literal, whose null handling the implementor decides.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnCoalesce() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .scan("HR", "emps")
                .project(builder.call(SqlStdOperatorTable.COALESCE, builder.field("commission"), builder.literal(I(0))))
                .sort(0)
                .build());

        [Fact]
        public void ShouldAgreeOnAPosixRegexThatMatches() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE, "E..c");

        [Fact]
        public void ShouldAgreeOnAPosixRegexThatDoesNot() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE, "e..c");

        [Fact]
        public void ShouldAgreeOnACaseInsensitivePosixRegexThatMatches() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE, "E..c");

        [Fact]
        public void ShouldAgreeOnACaseInsensitivePosixRegexOfTheOtherCase() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE, "e..c");

        [Fact]
        public void ShouldAgreeOnANegatedPosixRegex() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE, "E..c");

        [Fact]
        public void ShouldAgreeOnANegatedPosixRegexOfTheOtherCase() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE, "e..c");

        [Fact]
        public void ShouldAgreeOnANegatedCaseInsensitivePosixRegex() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE, "E..c");

        [Fact]
        public void ShouldAgreeOnANegatedCaseInsensitivePosixRegexOfTheOtherCase() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE, "e..c");

        /// <summary>
        /// Filters on a POSIX regex operator, which has no syntax in the core parser and is reachable only
        /// through a builder.
        /// </summary>
        /// <param name="op">The POSIX regex operator, case sensitive or not and negated or not.</param>
        /// <param name="pattern">The regular expression the employees' names are matched against.</param>
        static void ShouldAgreeOnPosixRegex(SqlOperator op, string pattern) =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .scan("HR", "emps")
                .filter(builder.call(op, builder.field("name"), builder.literal(pattern)))
                .project(builder.field("empid"), builder.field("name"))
                .sort(0)
                .build());

        /// <summary>
        /// IS EMPTY over a nullable collection.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnIsEmptyOverACollection() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .scan("CATCHALL", "everyTypes")
                .project(builder.call(SqlStdOperatorTable.IS_EMPTY, builder.field("list")))
                .sort(0)
                .build());

        /// <summary>
        /// IS DISTINCT FROM as a projection rather than as a join key.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnIsDistinctFromAsAProjection() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .scan("HR", "emps")
                .project(
                    builder.field("commission"),
                    builder.call(SqlStdOperatorTable.IS_DISTINCT_FROM, builder.field("commission"), builder.literal(null)))
                .sort(0, 1)
                .build());

        /// <summary>
        /// IS [NOT] DISTINCT FROM over two DECIMALs of different scales treats 1.10 and 1.1 as equal.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>RelBuilderTest.testIsNotDistinctFromDecimal</c>. Only a call built by hand reaches
        /// <c>DistinctFromImplementor</c>: the parser and <c>RelBuilder.isNotDistinctFrom</c> both expand the
        /// operator into <c>IS NULL</c> and <c>=</c> over operands cast to a common type. <c>BigDecimal.equals</c>
        /// is sensitive to scale, so a correct implementation compares by value. The rows are checked against
        /// SQL's answer as well as Calcite's, because both conventions translate through Calcite's
        /// <c>RexImpTable</c> and would agree on a shared defect.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnIsNotDistinctFromOverDecimalsOfDifferentScales()
        {
            static org.apache.calcite.rel.RelNode Build(org.apache.calcite.tools.RelBuilder builder) => builder
                .values(["a", "b"], new java.math.BigDecimal("1.10"), new java.math.BigDecimal("1.1"))
                .project(
                    builder.field("a"),
                    builder.field("b"),
                    builder.alias(builder.equals(builder.field("a"), builder.field("b")), "eq"),
                    builder.alias(builder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, builder.field("a"), builder.field("b")), "indf"),
                    builder.alias(builder.call(SqlStdOperatorTable.IS_DISTINCT_FROM, builder.field("a"), builder.field("b")), "idf"))
                .build();

            ClrCursorConventionDifferentialTests.SameRelThrough("ClrCursorCalc", Build);

            Assert.Equal(["1.10|1.1|true|true|false"], ClrCursorConventionDifferentialTests.RunRel(Build, true));
        }

        // ------------------------------------------------------------------ EnumerableCorrelateTest
        //
        // A correlate is chosen only when the join rules are removed and JOIN_TO_CORRELATE is added, as
        // Calcite's own tests do from Hook.PLANNER. Both conventions are planned with the same change, so each
        // plans a correlate.

        static readonly RelOptRule[] AddJoinToCorrelate = [CoreRules.JOIN_TO_CORRELATE];

        static readonly RelOptRule[] RemoveTheJoins =
        [
            EnumerableRules.ENUMERABLE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
            ClrCursorRules.ClrCursorJoinRule,
            ClrCursorRules.ClrCursorMergeJoinRule,
        ];

        /// <summary>
        /// A left outer join run as a correlate.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnALeftOuterJoinRunAsACorrelate() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .scan("HR", "emps").@as("e")
                .scan("HR", "depts").@as("d")
                .join(JoinRelType.LEFT, builder.equals(builder.field(2, "e", "deptno"), builder.field(2, "d", "deptno")))
                .project(builder.field("e", "empid"), builder.field("e", "name"), builder.field("d", "name"))
                .sort(0)
                .build(),
                add: AddJoinToCorrelate, remove: RemoveTheJoins);

        /// <summary>
        /// A semi join run as a correlate.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnASemiJoinRunAsACorrelate() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .scan("HR", "emps").@as("e")
                .scan("HR", "depts").@as("d")
                .semiJoin(builder.equals(builder.field(2, "e", "deptno"), builder.field(2, "d", "deptno")))
                .project(builder.field("empid"), builder.field("name"))
                .sort(0)
                .build(),
                add: AddJoinToCorrelate, remove: RemoveTheJoins);

        /// <summary>
        /// An anti join run as a correlate.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnAnAntiJoinRunAsACorrelate() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .scan("HR", "depts").@as("d")
                .scan("HR", "emps").@as("e")
                .antiJoin(builder.equals(builder.field(2, "d", "deptno"), builder.field(2, "e", "deptno")))
                .project(builder.field("deptno"), builder.field("name"))
                .sort(0)
                .build(),
                add: AddJoinToCorrelate, remove: RemoveTheJoins);

        /// <summary>
        /// An anti join on more than an equality, run as a correlate.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnANonEquiAntiJoinRunAsACorrelate() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .scan("HR", "emps").@as("e")
                .scan("HR", "emps").@as("e2")
                .antiJoin(
                    builder.and(
                        builder.equals(builder.field(2, "e", "deptno"), builder.field(2, "e2", "deptno")),
                        builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field(2, "e2", "salary"), builder.field(2, "e", "salary"))))
                .project(builder.field("name"), builder.field("salary"))
                .sort(0)
                .build(),
                add: AddJoinToCorrelate, remove: RemoveTheJoins);

        /// <summary>
        /// An anti join, run as a correlate, whose key is null on one side; it has NOT EXISTS semantics, not NOT
        /// IN.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnAnAntiJoinOverANullKeyRunAsACorrelate() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .scan("HR", "emps").@as("empOther")
                .filter(builder.notEquals(builder.field("empOther", "deptno"), builder.literal(I(10))))
                .scan("HR", "emps").@as("empSales")
                .filter(builder.equals(builder.field("empSales", "deptno"), builder.literal(I(10))))
                .antiJoin(builder.equals(builder.field(2, "empOther", "commission"), builder.field(2, "empSales", "commission")))
                .project(builder.field("empid"), builder.field("name"))
                .sort(0)
                .build(),
                add: AddJoinToCorrelate, remove: RemoveTheJoins);

        // ------------------------------------------------------------------ EnumerableRepeatUnionTest

        /// <summary>
        /// A recursive query of two columns, only one of which advances.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnARecursiveQueryOverTwoColumns() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .values(["i", "j"], I(0), I(0))
                .transientScan("AUX")
                .filter(builder.call(SqlStdOperatorTable.LESS_THAN, builder.field(0), builder.literal(I(10))))
                .project(
                    builder.call(SqlStdOperatorTable.MOD,
                        builder.call(SqlStdOperatorTable.PLUS, builder.field(0), builder.literal(I(1))),
                        builder.literal(I(10))),
                    builder.field(1))
                .repeatUnion("AUX", false)
                .build());

        /// <summary>
        /// A recursive query whose step multiplies, so each iteration reads the row the last one wrote.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnARecursiveFactorial() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .values(["n", "fact"], I(0), I(1))
                .transientScan("D")
                .filter(builder.call(SqlStdOperatorTable.LESS_THAN, builder.field("n"), builder.literal(I(7))))
                .project(
                    java.util.Arrays.asList([
                        builder.call(SqlStdOperatorTable.PLUS, builder.field("n"), builder.literal(I(1))),
                        builder.call(SqlStdOperatorTable.MULTIPLY,
                            builder.call(SqlStdOperatorTable.PLUS, builder.field("n"), builder.literal(I(1))),
                            builder.field("fact"))]),
                    java.util.Arrays.asList(["n", "fact"]))
                .repeatUnion("D", true)
                .build());

        /// <summary>
        /// One recursive query inside another, so two spools are live at once.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnNestedRecursion() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .values(["n"], I(1))
                .transientScan("T_IN")
                .filter(builder.call(SqlStdOperatorTable.LESS_THAN, builder.field("n"), builder.literal(I(9))))
                .project(builder.call(SqlStdOperatorTable.PLUS, builder.field("n"), builder.literal(I(1))))
                .repeatUnion("T_IN", true)
                .transientScan("T_OUT")
                .filter(builder.call(SqlStdOperatorTable.LESS_THAN, builder.field("n"), builder.literal(I(100))))
                .project(builder.call(SqlStdOperatorTable.MULTIPLY, builder.field("n"), builder.literal(I(10))))
                .repeatUnion("T_OUT", true)
                .build());

        /// <summary>
        /// A recursive query whose seed holds a null, which the transient table must store.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnARecursiveQueryOverANull() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .values(["i"], I(1), I(2), null, I(3))
                .transientScan("DELTA")
                .filter(builder.call(SqlStdOperatorTable.LESS_THAN, builder.field(0), builder.literal(I(3))))
                .project(builder.call(SqlStdOperatorTable.PLUS, builder.field(0), builder.literal(I(1))))
                .repeatUnion("DELTA", true)
                .build());

        /// <summary>
        /// A recursive query whose step aggregates the working table rather than reading it row by row.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableDefaults.repeatUnion</c> stops on an empty round only where its <c>current</c> field
        /// still holds the <c>DUMMY</c> sentinel, and it does not restore the sentinel between the seed and the
        /// first round. So after a seed that emitted a row, the first empty round does not stop the sequence
        /// and the iterative part runs once more. A step that reads the working table row by row cannot show
        /// this, because an empty table gives an empty round either way; <c>COUNT(*)</c> yields a row over no
        /// rows, so the extra round appears in the result.
        ///
        /// <para>The query must be a UNION rather than a UNION ALL to terminate. The spool is cleared by a round
        /// that wrote nothing, so the step oscillates: a round that counts one is empty and empties the table,
        /// and the next counts zero and emits 99 again. Under UNION ALL that never ends, under Calcite as well.
        /// With deduplication the second 99 is a row already returned, so that round adds nothing, the
        /// sentinel survives it, and the query ends with rows 1 and 99.</para>
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnARecursiveQueryWhoseStepAggregates() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .values(["i"], I(1))
                .transientScan("EMPTY_FIRST")
                .aggregate(builder.groupKey(), builder.count(false, "C"))
                .filter(builder.equals(builder.field(0), builder.literal(java.lang.Long.valueOf(0))))
                .project(builder.literal(I(99)))
                .repeatUnion("EMPTY_FIRST", false)
                .build());

        /// <summary>
        /// A repeat union whose step is a correlate with the transient scan as its right input.
        /// </summary>
        /// <remarks>
        /// The transient table is read from inside the correlate's inner loop while the spool above the step
        /// writes to it.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnARecursiveQueryWhoseStepIsACorrelate() =>
            ClrCursorConventionDifferentialTests.SameRel(builder =>
            {
                builder
                    .scan("HIER", "emps")
                    .filter(builder.equals(builder.field("empid"), builder.literal(I(2))))
                    .project(builder.field("emps", "empid"), builder.field("emps", "name"))
                    .transientScan("#DELTA#");

                // popped, so that it can be pushed back as the join's right input and be read from inside the
                // correlate's inner loop
                var transientScan = builder.build();

                return builder
                    .scan("HIER", "hierarchies")
                    .push(transientScan)
                    .join(JoinRelType.INNER, builder.equals(builder.field(2, "#DELTA#", "empid"), builder.field(2, "hierarchies", "managerid")))
                    .scan("HIER", "emps")
                    .join(JoinRelType.INNER, builder.equals(builder.field(2, "hierarchies", "subordinateid"), builder.field(2, "emps", "empid")))
                    .project(builder.field("emps", "empid"), builder.field("emps", "name"))
                    .repeatUnion("#DELTA#", true)
                    .sort(0)
                    .build();
            },
                add: AddJoinToCorrelate,
                remove: [.. RemoveTheJoins, JoinCommuteRule.Config.DEFAULT.toRule()]);

        // ------------------------------------------------------------------ EnumerableJoinTest

        /// <summary>
        /// An anti join whose key is null on one side, run as a hash anti join rather than as a correlate.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnAnAntiJoinOverANullKey() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .scan("HR", "emps").@as("empOther")
                .filter(builder.notEquals(builder.field("empOther", "deptno"), builder.literal(I(10))))
                .scan("HR", "emps").@as("empSales")
                .filter(builder.equals(builder.field("empSales", "deptno"), builder.literal(I(10))))
                .antiJoin(builder.equals(builder.field(2, "empOther", "commission"), builder.field(2, "empSales", "commission")))
                .project(builder.field("empid"), builder.field("name"))
                .sort(0)
                .build());

        /// <summary>
        /// An anti join with a second condition that reads only the left input, which cannot be pushed down
        /// onto it.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnAnAntiJoinWhoseConditionReadsOnlyTheLeft() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .scan("HR", "emps")
                .scan("HR", "depts")
                .antiJoin(
                    builder.equals(builder.field(2, 0, "deptno"), builder.field(2, 1, "deptno")),
                    builder.equals(builder.field(2, 0, "name"), builder.literal("ddd")))
                .project(builder.field(0))
                .sort(0)
                .build());

        /// <summary>
        /// A recursive query whose step is two merge joins, so each round sorts what the last one spooled.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnARecursiveQueryWhoseStepIsAMergeJoin() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .scan("HIER", "emps")
                .filter(builder.equals(builder.field("empid"), builder.literal(I(2))))
                .project(builder.field("emps", "empid"), builder.field("emps", "name"))
                .transientScan("#DELTA#")
                .sort(builder.field("empid"))
                .scan("HIER", "hierarchies")
                .sort(builder.field("managerid"))
                .join(JoinRelType.INNER, builder.equals(builder.field(2, "#DELTA#", "empid"), builder.field(2, "hierarchies", "managerid")))
                .sort(builder.field("subordinateid"))
                .scan("HIER", "emps")
                .sort(builder.field("empid"))
                .join(JoinRelType.INNER, builder.equals(builder.field(2, "hierarchies", "subordinateid"), builder.field(2, "emps", "empid")))
                .project(builder.field("emps", "empid"), builder.field("emps", "name"))
                .repeatUnion("#DELTA#", true)
                .sort(0)
                .build(),
                add: [org.apache.calcite.interpreter.Bindables.BINDABLE_TABLE_SCAN_RULE],
                remove: [EnumerableRules.ENUMERABLE_JOIN_RULE, ClrCursorRules.ClrCursorJoinRule]);

        // ------------------------------------------------------------------ EnumerableRepeatUnionHierarchyTest
        //
        // One recursive query walked up and down the hierarchy, from one start row and from two, with and
        // without a depth limit, distinct and not. As in Calcite's parameterised test, the order of the rows is
        // compared, since it follows from the algorithm.

        [Theory]
        [InlineData(true, "1", true, -1)]
        [InlineData(true, "2", true, -2)]
        [InlineData(true, "3", true, -1)]
        [InlineData(true, "4", true, -5)]
        [InlineData(true, "5", true, -1)]
        [InlineData(true, "3", true, 0)]
        [InlineData(true, "3", true, 1)]
        [InlineData(true, "3", true, 2)]
        [InlineData(true, "3", true, 10)]
        [InlineData(true, "1", false, -1)]
        [InlineData(true, "2", false, -10)]
        [InlineData(true, "3", false, -100)]
        [InlineData(true, "4", false, -1)]
        [InlineData(true, "1", false, 0)]
        [InlineData(true, "1", false, 1)]
        [InlineData(true, "1", false, 2)]
        [InlineData(true, "1", false, 20)]
        [InlineData(true, "3,5", true, -1)]
        [InlineData(false, "3,5", true, -1)]
        [InlineData(true, "3,5", true, 0)]
        [InlineData(false, "3,5", true, 0)]
        [InlineData(true, "3,5", true, 1)]
        [InlineData(false, "3,5", true, 1)]
        [InlineData(true, "1,3", false, -1)]
        [InlineData(false, "1,3", false, -1)]
        public void ShouldAgreeOnWalkingAHierarchy(bool all, string startIds, bool ascendant, int maxDepth)
        {
            var fromField = ascendant ? "subordinateid" : "managerid";
            var toField = ascendant ? "managerid" : "subordinateid";

            ClrCursorConventionDifferentialTests.SameRel(builder =>
            {
                builder.scan("HIER", "emps");

                var filters = new java.util.ArrayList();
                foreach (var startId in startIds.Split(','))
                    filters.add(builder.equals(builder.field("empid"), builder.literal(I(int.Parse(startId)))));

                builder
                    .filter(builder.or(filters))
                    .project(builder.field("emps", "empid"), builder.field("emps", "name"))
                    .transientScan("#DELTA#")
                    .scan("HIER", "hierarchies")
                    .join(JoinRelType.INNER, builder.equals(builder.field(2, "#DELTA#", "empid"), builder.field(2, "hierarchies", fromField)))
                    .scan("HIER", "emps")
                    .join(JoinRelType.INNER, builder.equals(builder.field(2, "hierarchies", toField), builder.field(2, "emps", "empid")))
                    .project(builder.field("emps", "empid"), builder.field("emps", "name"))
                    .repeatUnion("#DELTA#", all, maxDepth);

                return builder.build();
            });
        }

        // ------------------------------------------------------------------ EnumerableStringComparisonTest
        //
        // A VARCHAR with its own collation compares by that collation rather than by the string's natural
        // order. There is no syntax for giving a column one, so these tests build the row type by hand, as
        // Calcite's do.

        static readonly org.apache.calcite.sql.SqlCollation Primary = Collation(java.text.Collator.PRIMARY);
        static readonly org.apache.calcite.sql.SqlCollation Secondary = Collation(java.text.Collator.SECONDARY);
        static readonly org.apache.calcite.sql.SqlCollation Tertiary = Collation(java.text.Collator.TERTIARY);
        static readonly org.apache.calcite.sql.SqlCollation Identical = Collation(java.text.Collator.IDENTICAL);

        static org.apache.calcite.sql.SqlCollation Collation(int strength) =>
            new org.apache.calcite.jdbc.JavaCollation(
                org.apache.calcite.sql.SqlCollation.Coercibility.IMPLICIT,
                java.util.Locale.US,
                org.apache.calcite.util.Util.getDefaultCharset(),
                strength);

        static org.apache.calcite.rel.type.RelDataType Collated(org.apache.calcite.tools.RelBuilder builder, org.apache.calcite.sql.SqlCollation collation) =>
            builder.getTypeFactory().createTypeWithCharsetAndCollation(
                builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                builder.getTypeFactory().getDefaultCharset(),
                collation);

        static org.apache.calcite.rel.type.RelDataType CollatedRow(org.apache.calcite.tools.RelBuilder builder) =>
            builder.getTypeFactory().builder().add("name", Collated(builder, Tertiary)).build();

        static org.apache.calcite.rel.type.RelDataType PlainRow(org.apache.calcite.tools.RelBuilder builder) =>
            builder.getTypeFactory().builder().add("name", builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR)).build();

        [Fact]
        public void ShouldAgreeOnSortingStringsByTheDefaultCollation() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .values(PlainRow(builder), "Legal", "presales", "hr", "Administration", "MARKETING")
                .sort(builder.field(1, 0, "name"))
                .build());

        [Fact]
        public void ShouldAgreeOnSortingStringsByTheirOwnCollation() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .values(CollatedRow(builder), "Legal", "presales", "hr", "Administration", "MARKETING")
                .sort(builder.field(1, 0, "name"))
                .build());

        /// <summary>
        /// An equality on a collated column, which compares by the column's collation.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnFilteringStringsByTheirOwnCollation() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .values(CollatedRow(builder), "Legal", "presales", "hr", "Administration", "MARKETING")
                .filter(builder.equals(builder.field(1, 0, "name"), builder.literal("MARKETING")))
                .build());

        [Fact]
        public void ShouldAgreeOnAMergeJoinOverACollatedString() =>
            ClrCursorConventionDifferentialTests.SameRel(builder => builder
                .values(CollatedRow(builder), "Legal", "presales", "HR", "Administration", "Marketing").@as("v1")
                .values(CollatedRow(builder), "Marketing", "bureaucracy", "Sales", "HR").@as("v2")
                .join(JoinRelType.INNER, builder.equals(builder.field(2, 0, "name"), builder.field(2, 1, "name")))
                .project(builder.field("v1", "name"), builder.field("v2", "name"))
                .build(),
                remove: [EnumerableRules.ENUMERABLE_JOIN_RULE, ClrCursorRules.ClrCursorJoinRule]);

        /// <summary>
        /// A merge union of two inputs whose string collations differ.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnAMergeUnionOverTwoCollations() =>
            ClrCursorConventionDifferentialTests.SameRel(b =>
            {
                var builder = b.transform(new DelegateUnaryOperator(c => ((org.apache.calcite.tools.RelBuilder.Config)c).withSimplifyValues(false)));

                return builder
                    .values(PlainRow(builder), "facilities", "HR", "administration", "Marketing")
                    .values(CollatedRow(builder), "Marketing", "administration", "presales", "HR")
                    .union(false)
                    .sort(0)
                    .build();
            },
            remove: [EnumerableRules.ENUMERABLE_UNION_RULE, ClrCursorRules.ClrCursorUnionRule]);

        [Fact]
        public void ShouldAgreeOnEveryCollatedComparison()
        {
            foreach (var (left, right, op, collation) in Comparisons())
                ClrCursorConventionDifferentialTests.SameRel(builder => builder
                    .values(["aux"], java.lang.Boolean.FALSE)
                    .project(
                        java.util.Collections.singletonList(
                            builder.call(op,
                                builder.getRexBuilder().makeLiteral(left, Collated(builder, collation)),
                                builder.getRexBuilder().makeLiteral(right, Collated(builder, collation)))))
                    .build());
        }

        /// <summary>
        /// The comparisons <c>EnumerableStringComparisonTest.testStringComparison</c> makes.
        /// </summary>
        /// <returns>Each comparison as its two operands, the operator and the collation it is made under.</returns>
        static IEnumerable<(string Left, string Right, SqlOperator Op, org.apache.calcite.sql.SqlCollation Collation)> Comparisons()
        {
            var lt = SqlStdOperatorTable.LESS_THAN;
            var gt = SqlStdOperatorTable.GREATER_THAN;
            var eq = SqlStdOperatorTable.EQUALS;
            var ne = SqlStdOperatorTable.NOT_EQUALS;

            foreach (var op in new[] { lt, gt })
                foreach (var (l, r) in new[] { ("a", "A"), ("A", "a"), ("a", "b"), ("A", "B"), ("a", "B"), ("A", "b"), ("b", "a"), ("B", "A"), ("B", "a"), ("b", "A") })
                    yield return (l, r, op, Tertiary);

            foreach (var op in new[] { eq, ne })
                foreach (var (l, r) in new[] { ("aaa", "AAA"), ("AAA", "AAA"), ("AAA", "BBB") })
                    yield return (l, r, op, Tertiary);

            foreach (var collation in new[] { Primary, Secondary, Tertiary, Identical })
                foreach (var (l, r) in new[] { ("ABC", "ABC"), ("abc", "ÀBC"), ("abc", "ABC"), ("", "") })
                    yield return (l, r, eq, collation);
        }

        /// <summary>
        /// A <see cref="java.util.function.UnaryOperator"/> over a delegate, so that a builder's configuration
        /// can be changed from C#.
        /// </summary>
        /// <param name="transform">The function <c>apply</c> calls.</param>
        sealed class DelegateUnaryOperator(Func<object, object> transform) : java.util.function.UnaryOperator
        {

            /// <inheritdoc />
            public object apply(object value) => transform(value);

            /// <inheritdoc />
            public java.util.function.Function andThen(java.util.function.Function after) => throw new NotSupportedException();

            /// <inheritdoc />
            public java.util.function.Function compose(java.util.function.Function before) => throw new NotSupportedException();

        }

    }

}
