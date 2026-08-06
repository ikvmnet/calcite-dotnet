using System;
using System.Collections.Generic;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.plan;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.rules;
using org.apache.calcite.sql;
using org.apache.calcite.sql.fun;
using org.apache.calcite.sql.type;
using org.apache.calcite.adapter.enumerable;

namespace Apache.Calcite.Linq.Tests
{

    /// <summary>
    /// Runs a plan built against a <see cref="org.apache.calcite.tools.RelBuilder"/> through this convention
    /// and through Calcite's, and requires the same rows.
    /// </summary>
    /// <remarks>
    /// The same oracle as <see cref="ClrEnumerableDifferentialTests"/>, for the plans that cannot be written
    /// as SQL. <c>Combine</c> has no syntax; the POSIX regex operators are not in the core parser; a
    /// recursive query over a transient table is built with <c>transientScan</c> and <c>repeatUnion</c>; and
    /// a column with a collation of its own can only be given a type by hand. Calcite reaches all four
    /// through <c>CalciteAssert.withRel</c>, and every test here is one of its own ported.
    /// </remarks>
    [TestClass]
    public class ClrEnumerableRelTests
    {

        static java.lang.Integer I(int value) => java.lang.Integer.valueOf(value);

        // ------------------------------------------------------------------ EnumerableCombineTest
        //
        // A combine puts one query per column and one row per position, so a shorter query contributes null.
        // There is no SQL for it, which is why the only test this convention had was written by hand.

        [TestMethod]
        public void ShouldAgreeOnCombiningTwoQueries() =>
            ClrEnumerableDifferentialTests.SameRel(builder =>
            {
                builder.scan("HR", "emps").filter(builder.equals(builder.field("deptno"), builder.literal(I(10)))).project(builder.field("name"));
                builder.scan("HR", "depts").project(builder.field("name"));

                return builder.combine(2).build();
            });

        [TestMethod]
        public void ShouldAgreeOnCombiningQueriesOfDifferentLengths() =>
            ClrEnumerableDifferentialTests.SameRel(builder =>
            {
                builder.scan("HR", "emps").project(builder.field("name"));
                builder.scan("HR", "depts").project(builder.field("name"));

                return builder.combine(2).build();
            });

        [TestMethod]
        public void ShouldAgreeOnCombiningQueriesOfSeveralColumns() =>
            ClrEnumerableDifferentialTests.SameRel(builder =>
            {
                builder.scan("HR", "emps").filter(builder.equals(builder.field("deptno"), builder.literal(I(10)))).project(builder.field("empid"), builder.field("name"));
                builder.scan("HR", "depts").project(builder.field("deptno"), builder.field("name"));

                return builder.combine(2).build();
            });

        [TestMethod]
        public void ShouldAgreeOnCombiningQueriesOfDifferentColumnCounts() =>
            ClrEnumerableDifferentialTests.SameRel(builder =>
            {
                builder.scan("HR", "depts").project(builder.field("name"));
                builder.scan("HR", "emps").filter(builder.equals(builder.field("deptno"), builder.literal(I(10))))
                    .project(builder.field("empid"), builder.field("name"), builder.field("deptno"));

                return builder.combine(2).build();
            });

        // ------------------------------------------------------------------ EnumerableCalcTest

        /// <summary>
        /// CALCITE-3536: a COALESCE of a nullable field and a literal, whose null strategy the implementor
        /// gets to decide.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnCoalesce() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
                .scan("HR", "emps")
                .project(builder.call(SqlStdOperatorTable.COALESCE, builder.field("commission"), builder.literal(I(0))))
                .sort(0)
                .build());

        [TestMethod]
        public void ShouldAgreeOnAPosixRegexThatMatches() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE, "E..c");

        [TestMethod]
        public void ShouldAgreeOnAPosixRegexThatDoesNot() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE, "e..c");

        [TestMethod]
        public void ShouldAgreeOnACaseInsensitivePosixRegexThatMatches() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE, "E..c");

        [TestMethod]
        public void ShouldAgreeOnACaseInsensitivePosixRegexOfTheOtherCase() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE, "e..c");

        [TestMethod]
        public void ShouldAgreeOnANegatedPosixRegex() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE, "E..c");

        [TestMethod]
        public void ShouldAgreeOnANegatedPosixRegexOfTheOtherCase() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE, "e..c");

        [TestMethod]
        public void ShouldAgreeOnANegatedCaseInsensitivePosixRegex() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE, "E..c");

        [TestMethod]
        public void ShouldAgreeOnANegatedCaseInsensitivePosixRegexOfTheOtherCase() => ShouldAgreeOnPosixRegex(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE, "e..c");

        /// <summary>
        /// CALCITE-4419: the POSIX regex operators have no syntax in the core parser and are only reachable
        /// through a builder.
        /// </summary>
        /// <param name="op"></param>
        /// <param name="pattern"></param>
        static void ShouldAgreeOnPosixRegex(SqlOperator op, string pattern) =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
                .scan("HR", "emps")
                .filter(builder.call(op, builder.field("name"), builder.literal(pattern)))
                .project(builder.field("empid"), builder.field("name"))
                .sort(0)
                .build());

        /// <summary>
        /// CALCITE-6680: IS EMPTY over a nullable collection, which answers on the null rather than reading it.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnIsEmptyOverACollection() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
                .scan("CATCHALL", "everyTypes")
                .project(builder.call(SqlStdOperatorTable.IS_EMPTY, builder.field("list")))
                .sort(0)
                .build());

        /// <summary>
        /// CALCITE-7357: IS DISTINCT FROM as a projection rather than as a join key.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnIsDistinctFromAsAProjection() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
                .scan("HR", "emps")
                .project(
                    builder.field("commission"),
                    builder.call(SqlStdOperatorTable.IS_DISTINCT_FROM, builder.field("commission"), builder.literal(null)))
                .sort(0, 1)
                .build());

        // ------------------------------------------------------------------ EnumerableCorrelateTest
        //
        // A correlate is only chosen when the joins are taken away and JOIN_TO_CORRELATE is put in, which is
        // what Calcite's own tests do from Hook.PLANNER. Both sides get the same treatment here, so a
        // correlate is what each of them plans.

        static readonly RelOptRule[] AddJoinToCorrelate = [CoreRules.JOIN_TO_CORRELATE];

        static readonly RelOptRule[] RemoveTheJoins =
        [
            EnumerableRules.ENUMERABLE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
            ClrEnumerableRules.ClrEnumerableJoinRule,
            ClrEnumerableRules.ClrEnumerableMergeJoinRule,
        ];

        /// <summary>
        /// CALCITE-2605: a left outer join run as a correlate.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnALeftOuterJoinRunAsACorrelate() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
                .scan("HR", "emps").@as("e")
                .scan("HR", "depts").@as("d")
                .join(JoinRelType.LEFT, builder.equals(builder.field(2, "e", "deptno"), builder.field(2, "d", "deptno")))
                .project(builder.field("e", "empid"), builder.field("e", "name"), builder.field("d", "name"))
                .sort(0)
                .build(),
                add: AddJoinToCorrelate, remove: RemoveTheJoins);

        /// <summary>
        /// CALCITE-2621: a semi join run as a correlate.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnASemiJoinRunAsACorrelate() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
                .scan("HR", "emps").@as("e")
                .scan("HR", "depts").@as("d")
                .semiJoin(builder.equals(builder.field(2, "e", "deptno"), builder.field(2, "d", "deptno")))
                .project(builder.field("empid"), builder.field("name"))
                .sort(0)
                .build(),
                add: AddJoinToCorrelate, remove: RemoveTheJoins);

        /// <summary>
        /// CALCITE-2920: an anti join run as a correlate.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnAnAntiJoinRunAsACorrelate() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
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
        [TestMethod]
        public void ShouldAgreeOnANonEquiAntiJoinRunAsACorrelate() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
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
        /// CALCITE-2920: an anti join whose key is null on one side, which is NOT EXISTS and not NOT IN.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnAnAntiJoinOverANullKeyRunAsACorrelate() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
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
        [TestMethod]
        public void ShouldAgreeOnARecursiveQueryOverTwoColumns() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
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
        [TestMethod]
        public void ShouldAgreeOnARecursiveFactorial() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
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
        [TestMethod]
        public void ShouldAgreeOnNestedRecursion() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
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
        /// CALCITE-4139: a recursive query whose seed holds a null, which the transient table has to store.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnARecursiveQueryOverANull() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
                .values(["i"], I(1), I(2), null, I(3))
                .transientScan("DELTA")
                .filter(builder.call(SqlStdOperatorTable.LESS_THAN, builder.field(0), builder.literal(I(3))))
                .project(builder.call(SqlStdOperatorTable.PLUS, builder.field(0), builder.literal(I(1))))
                .repeatUnion("DELTA", true)
                .build());

        /// <summary>
        /// CALCITE-4054: a repeat union whose step is a correlate with the transient scan on its right.
        /// </summary>
        /// <remarks>
        /// The spool is written while the correlate is reading it, which is the one shape that exercises
        /// <c>ClrEnumerableDefaults.LazyCollectionSpool</c> as a write. That method converted nothing for a
        /// while and no test had ever written to a spool.
        /// </remarks>
        [TestMethod]
        public void ShouldAgreeOnARecursiveQueryWhoseStepIsACorrelate() =>
            ClrEnumerableDifferentialTests.SameRel(builder =>
            {
                builder
                    .scan("HIER", "emps")
                    .filter(builder.equals(builder.field("empid"), builder.literal(I(2))))
                    .project(builder.field("emps", "empid"), builder.field("emps", "name"))
                    .transientScan("#DELTA#");

                // popped, so that it can be pushed back as the join's *right* input, which is the whole point
                // of CALCITE-4054: the spool is read from inside the correlate's inner loop
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
        /// CALCITE-2920: an anti join whose key is null on one side, run as a hash anti join rather than as a
        /// correlate.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnAnAntiJoinOverANullKey() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
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
        [TestMethod]
        public void ShouldAgreeOnAnAntiJoinWhoseConditionReadsOnlyTheLeft() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
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
        [TestMethod]
        public void ShouldAgreeOnARecursiveQueryWhoseStepIsAMergeJoin() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
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
                remove: [EnumerableRules.ENUMERABLE_JOIN_RULE, ClrEnumerableRules.ClrEnumerableJoinRule]);

        // ------------------------------------------------------------------ EnumerableRepeatUnionHierarchyTest
        //
        // One recursive query walked every way it can be: up the hierarchy and down it, from one start row
        // and from two, with and without a depth limit, distinct and not. Calcite runs it as one parameterised
        // test and asserts the order, which is the algorithm's; so does this.

        [TestMethod]
        [DataRow(true, "1", true, -1)]
        [DataRow(true, "2", true, -2)]
        [DataRow(true, "3", true, -1)]
        [DataRow(true, "4", true, -5)]
        [DataRow(true, "5", true, -1)]
        [DataRow(true, "3", true, 0)]
        [DataRow(true, "3", true, 1)]
        [DataRow(true, "3", true, 2)]
        [DataRow(true, "3", true, 10)]
        [DataRow(true, "1", false, -1)]
        [DataRow(true, "2", false, -10)]
        [DataRow(true, "3", false, -100)]
        [DataRow(true, "4", false, -1)]
        [DataRow(true, "1", false, 0)]
        [DataRow(true, "1", false, 1)]
        [DataRow(true, "1", false, 2)]
        [DataRow(true, "1", false, 20)]
        [DataRow(true, "3,5", true, -1)]
        [DataRow(false, "3,5", true, -1)]
        [DataRow(true, "3,5", true, 0)]
        [DataRow(false, "3,5", true, 0)]
        [DataRow(true, "3,5", true, 1)]
        [DataRow(false, "3,5", true, 1)]
        [DataRow(true, "1,3", false, -1)]
        [DataRow(false, "1,3", false, -1)]
        public void ShouldAgreeOnWalkingAHierarchy(bool all, string startIds, bool ascendant, int maxDepth)
        {
            var fromField = ascendant ? "subordinateid" : "managerid";
            var toField = ascendant ? "managerid" : "subordinateid";

            ClrEnumerableDifferentialTests.SameRel(builder =>
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
        // CALCITE-3951: a VARCHAR with a collation of its own compares by that collation and not by the
        // string's own order. There is no syntax for giving a column one, so every test here builds the row
        // type by hand, as Calcite's do.

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

        [TestMethod]
        public void ShouldAgreeOnSortingStringsByTheDefaultCollation() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
                .values(PlainRow(builder), "Legal", "presales", "hr", "Administration", "MARKETING")
                .sort(builder.field(1, 0, "name"))
                .build());

        [TestMethod]
        public void ShouldAgreeOnSortingStringsByTheirOwnCollation() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
                .values(CollatedRow(builder), "Legal", "presales", "hr", "Administration", "MARKETING")
                .sort(builder.field(1, 0, "name"))
                .build());

        /// <summary>
        /// CALCITE-5967: an equality on a collated column needs a comparator of its own inside the operator.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnFilteringStringsByTheirOwnCollation() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
                .values(CollatedRow(builder), "Legal", "presales", "hr", "Administration", "MARKETING")
                .filter(builder.equals(builder.field(1, 0, "name"), builder.literal("MARKETING")))
                .build());

        [TestMethod]
        public void ShouldAgreeOnAMergeJoinOverACollatedString() =>
            ClrEnumerableDifferentialTests.SameRel(builder => builder
                .values(CollatedRow(builder), "Legal", "presales", "HR", "Administration", "Marketing").@as("v1")
                .values(CollatedRow(builder), "Marketing", "bureaucracy", "Sales", "HR").@as("v2")
                .join(JoinRelType.INNER, builder.equals(builder.field(2, 0, "name"), builder.field(2, 1, "name")))
                .project(builder.field("v1", "name"), builder.field("v2", "name"))
                .build(),
                remove: [EnumerableRules.ENUMERABLE_JOIN_RULE, ClrEnumerableRules.ClrEnumerableJoinRule]);

        /// <summary>
        /// CALCITE-5003: a merge union of two inputs whose collations differ.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnAMergeUnionOverTwoCollations() =>
            ClrEnumerableDifferentialTests.SameRel(b =>
            {
                var builder = b.transform(new DelegateUnaryOperator(c => ((org.apache.calcite.tools.RelBuilder.Config)c).withSimplifyValues(false)));

                return builder
                    .values(PlainRow(builder), "facilities", "HR", "administration", "Marketing")
                    .values(CollatedRow(builder), "Marketing", "administration", "presales", "HR")
                    .union(false)
                    .sort(0)
                    .build();
            },
            remove: [EnumerableRules.ENUMERABLE_UNION_RULE, ClrEnumerableRules.ClrEnumerableUnionRule]);

        [TestMethod]
        public void ShouldAgreeOnEveryCollatedComparison()
        {
            foreach (var (left, right, op, collation) in Comparisons())
                ClrEnumerableDifferentialTests.SameRel(builder => builder
                    .values(["aux"], java.lang.Boolean.FALSE)
                    .project(
                        java.util.Collections.singletonList(
                            builder.call(op,
                                builder.getRexBuilder().makeLiteral(left, Collated(builder, collation)),
                                builder.getRexBuilder().makeLiteral(right, Collated(builder, collation)))))
                    .build());
        }

        /// <summary>
        /// The comparisons <c>EnumerableStringComparisonTest.testStringComparison</c> makes, which are one
        /// test there and one row each here.
        /// </summary>
        /// <returns></returns>
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
        /// A <see cref="java.util.function.UnaryOperator"/> reading a delegate, so that a builder's
        /// configuration can be changed from C#.
        /// </summary>
        /// <param name="transform"></param>
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
