using System;
using System.Collections.Generic;
using System.Linq;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Tests;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.linq4j;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Runs the same query through this convention and through Calcite's <c>EnumerableConvention</c>, and
    /// requires the same rows however the cursor is opened and advanced.
    /// </summary>
    /// <remarks>
    /// The expected answer is whatever Calcite returns, so a query is tested by adding it here rather than by
    /// writing its result by hand. This matters most for nodes such as Window, where a wrong detail gives a
    /// wrong answer rather than a failure.
    /// </remarks>
    public class ClrCursorConventionDifferentialTests
    {

        /// <summary>
        /// Puts calcite-testkit and Calcite's JDBC assembly on the boot class path.
        /// </summary>
        /// <remarks>
        /// Janino resolves the names in the source Calcite generates through its parent class loader, so an
        /// assembly the generated code mentions must be on IKVM's boot class path. FIB is
        /// <c>Smalls.fibonacciTableWithLimit100</c>, which is in calcite-testkit.
        /// </remarks>
        static ClrCursorConventionDifferentialTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.util.Smalls).Assembly);

            // RelBuilder.create opens a Calcite connection to get a prepare context, and the driver loads its
            // factory by name. Class.forName finds it only if calcite-core is on the boot class path.
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// A table with partitions, ties and a null, so that windows and aggregates have cases to get wrong.
        /// </summary>
        sealed class SalesTable : AbstractTable, ScannableTable
        {

            static readonly object?[][] Rows =
            [
                [java.lang.Integer.valueOf(1), "EAST", java.lang.Integer.valueOf(10), "A"],
                [java.lang.Integer.valueOf(2), "EAST", java.lang.Integer.valueOf(20), "B"],
                [java.lang.Integer.valueOf(3), "EAST", java.lang.Integer.valueOf(20), "C"],
                [java.lang.Integer.valueOf(4), "WEST", java.lang.Integer.valueOf(30), "D"],
                [java.lang.Integer.valueOf(5), "WEST", null, "E"],
                [java.lang.Integer.valueOf(6), "WEST", java.lang.Integer.valueOf(5), "F"],
            ];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("REGION", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .add("AMOUNT", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true))
                    .add("LABEL", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                foreach (var row in Rows)
                    list.add(row);

                return org.apache.calcite.linq4j.Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// A table that declares its rows sorted by their first field.
        /// </summary>
        /// <remarks>
        /// A merge join, a merge union and a sorted aggregate are chosen over their hash and buffering
        /// counterparts only when the input already carries a collation, which a scan takes from
        /// <c>getStatistic().getCollations()</c>. <c>SALES</c> declares none, so over it neither convention
        /// reaches those three nodes.
        /// </remarks>
        sealed class SortedTable : AbstractTable, ScannableTable
        {

            static readonly object?[][] Rows =
            [
                [java.lang.Integer.valueOf(1), "A"],
                [java.lang.Integer.valueOf(2), "B"],
                [java.lang.Integer.valueOf(2), "C"],
                [java.lang.Integer.valueOf(4), "D"],
            ];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("K", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("V", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .build();
            }

            /// <inheritdoc />
            public override Statistic getStatistic()
            {
                return Statistics.of(Rows.Length,
                    new java.util.ArrayList(),
                    com.google.common.collect.ImmutableList.of(RelCollations.of(0)));
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                foreach (var row in Rows)
                    list.add(row);

                return org.apache.calcite.linq4j.Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// A table of one NOT NULL INTEGER column, whose rows are scalars of a primitive physical type.
        /// </summary>
        /// <remarks>
        /// One NOT NULL column makes <c>JavaRowFormat.optimize</c> choose <c>SCALAR</c>, and
        /// <c>SCALAR.javaRowClass</c> is then <c>int</c> rather than <c>java.lang.Integer</c>. The rows are
        /// still boxed, since Java has no <c>Enumerable&lt;int&gt;</c>, so a node that instantiates its operator
        /// over the physical row class instead of the boxed one builds a tree that does not compile. The other
        /// fixtures' set operations are over VARCHAR columns and cannot show this.
        ///
        /// <para>The table declares a collation so that the merge union and the sorted aggregate can be reached
        /// over it too.</para>
        /// </remarks>
        sealed class ScalarsTable : AbstractTable, ScannableTable
        {

            static readonly int[] Rows = [1, 2, 2, 4];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("N", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .build();
            }

            /// <inheritdoc />
            public override Statistic getStatistic()
            {
                return Statistics.of(Rows.Length,
                    new java.util.ArrayList(),
                    com.google.common.collect.ImmutableList.of(RelCollations.of(0)));
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                foreach (var n in Rows)
                    list.add(new object[] { java.lang.Integer.valueOf(n) });

                return org.apache.calcite.linq4j.Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// A table with a column of type ANY, whose Java class is <c>Object</c> and whose values therefore
        /// carry no type the plan can read.
        /// </summary>
        /// <remarks>
        /// A provider type the ADO.NET adapter has no <c>SqlTypeName</c> for arrives as ANY, so this is the shape
        /// of a column of an unmapped type.
        ///
        /// <para><c>ID</c> is an ordinary INTEGER, so that a window over this table has something to order by
        /// that is not ANY. <c>V</c> mixes <c>java.lang.Integer</c> with <c>java.lang.Double</c>, as a document
        /// store does and as a comparison through <c>Comparable.compareTo</c> throws on; <c>S</c> holds strings,
        /// which can be ordered but not added; both have a null for an aggregate to skip.</para>
        /// </remarks>
        sealed class AnysTable : AbstractTable, ScannableTable
        {

            static readonly object?[][] Rows =
            [
                [java.lang.Integer.valueOf(1), "EAST", java.lang.Integer.valueOf(10), "b"],
                [java.lang.Integer.valueOf(2), "EAST", java.lang.Double.valueOf(20.5), "a"],
                [java.lang.Integer.valueOf(3), "WEST", java.lang.Integer.valueOf(30), "d"],
                [java.lang.Integer.valueOf(4), "WEST", null, null],
                [java.lang.Integer.valueOf(5), "WEST", java.lang.Integer.valueOf(5), "c"],
            ];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("K", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .add("V", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true))
                    .add("S", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true))
                    .build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                foreach (var row in Rows)
                    list.add(row);

                return org.apache.calcite.linq4j.Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// A table whose ANY columns hold collections, as a document store returns for a JSON array.
        /// </summary>
        /// <remarks>
        /// A separate table from <c>ANYS</c>, because these values cannot be aggregated and <c>ANYS</c> is read
        /// by the aggregate tests.
        ///
        /// <para><c>TAGS</c> holds strings and has a null, the row an inner UNNEST drops and an outer one keeps;
        /// <c>NUMS</c> holds two numeric classes and has an empty list, the other row that produces nothing.
        /// The values are <c>java.util.List</c> because that is how <c>SqlFunctions.flatProduct</c> reads a
        /// collection, an array column's value included.</para>
        /// </remarks>
        sealed class DocsTable : AbstractTable, ScannableTable
        {

            static java.util.List List(params object?[] items)
            {
                var list = new java.util.ArrayList();
                foreach (var item in items)
                    list.add(item);

                return list;
            }

            static readonly object?[][] Rows =
            [
                [java.lang.Integer.valueOf(1), List("red", "green"), List(java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2))],
                [java.lang.Integer.valueOf(2), List("blue"), List()],
                [java.lang.Integer.valueOf(3), null, List(java.lang.Integer.valueOf(3), java.lang.Double.valueOf(4.5))],
            ];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                RelDataType Any() => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true);

                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("TAGS", Any())
                    .add("NUMS", Any())
                    .build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                foreach (var row in Rows)
                    list.add(row);

                return org.apache.calcite.linq4j.Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// A table whose ANY columns hold the values a document store returns: a GUID, a timestamp and a
        /// number, each written as JSON writes it.
        /// </summary>
        /// <remarks>
        /// These test what a <c>CAST</c> of an ANY value does. <c>RexToLixTranslator.getConvertExpression</c>
        /// switches on the target type and then on the source, and ANY matches no source branch, so each cast
        /// ends at <c>EnumUtils.convert(operand, typeFactory.getJavaClass(targetType))</c>: a Java conversion
        /// between two classes, whose result depends on the target's class:
        ///
        /// <list type="bullet">
        /// <item>a primitive or <c>BigDecimal</c> target reaches <c>SqlFunctions.toInt</c> and similar, which
        /// convert, from a string too;</item>
        /// <item><c>VARCHAR</c> reaches <c>toString()</c>;</item>
        /// <item><c>TIMESTAMP</c> and <c>DATE</c> are <c>long</c> and <c>int</c>, so the cast yields the internal
        /// value (epoch milliseconds and epoch days) rather than parsing a date;</item>
        /// <item><c>UUID</c> maps to <c>org.apache.calcite.util.UuidValue</c>, and converting a string to it
        /// this way throws.</item>
        /// </list>
        ///
        /// <para>This is Calcite's generator, reached identically by both conventions. A caller wanting a
        /// conversion casts through <c>VARCHAR</c> first, a source branch every target has:
        /// <c>CAST(CAST(x AS VARCHAR) AS UUID)</c> reaches <c>SqlFunctions.uuidFromString</c> and
        /// <c>... AS TIMESTAMP</c> reaches the string parser.</para>
        /// </remarks>
        sealed class CastsTable : AbstractTable, ScannableTable
        {

            static readonly object?[][] Rows =
            [
                [java.lang.Integer.valueOf(1), "11111111-1111-1111-1111-111111111111", "2026-01-01 00:00:00", java.lang.Long.valueOf(1767225600000L), "42"],
                [java.lang.Integer.valueOf(2), "22222222-2222-2222-2222-222222222222", "2025-06-15 12:30:45", java.lang.Long.valueOf(0L), "7"],
            ];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                RelDataType Any() => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true);

                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("G", Any())
                    .add("T", Any())
                    .add("M", Any())
                    .add("N", Any())
                    .build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                foreach (var row in Rows)
                    list.add(row);

                return org.apache.calcite.linq4j.Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// A table of twelve distinct keys: a row count at which the order of a hash join's unmatched build rows
        /// depends on which collection they are read from.
        /// </summary>
        /// <remarks>
        /// A RIGHT or FULL join ends by emitting the build rows nothing matched, and <c>hashEquiJoin_</c> reads
        /// them by copying the lookup's key set into a <c>java.util.HashSet</c> and iterating the copy.
        /// <c>HashSet(Collection)</c> sizes its table as <c>tableSizeFor(max((int) (n / 0.75f) + 1, 16))</c>,
        /// while a map grown by insertion has the smallest power of two, at least 16, with
        /// <c>n &lt;= 0.75 * cap</c>. The two differ where <c>n = 0.75 * 2^k</c> (12, 24, 48); at twelve keys
        /// the map has 16 buckets and the copy 32, so their iteration orders differ. Over <c>SALES</c>'s six
        /// rows both have 16.
        /// </remarks>
        sealed class WideTable : AbstractTable, ScannableTable
        {

            static readonly object?[][] Rows = BuildRows();

            static object[][] BuildRows()
            {
                var rows = new object[12][];
                for (var i = 0; i < rows.Length; i++)
                    rows[i] = [string.Format("K{0:D2}", i + 1), java.lang.Integer.valueOf(i + 1)];

                return rows;
            }

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("K", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .add("N", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                foreach (var row in Rows)
                    list.add(row);

                return org.apache.calcite.linq4j.Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// A table with a TIMESTAMP column, for window table functions.
        /// </summary>
        /// <remarks>
        /// A TIMESTAMP value is held as the millisecond count the type factory's Java class for it expects.
        /// </remarks>
        sealed class EventsTable : AbstractTable, ScannableTable
        {

            const long Hour = 3600000L;
            const long Base = 1704067200000L;

            static readonly object?[][] Rows =
            [
                [java.lang.Long.valueOf(Base), java.lang.Integer.valueOf(1)],
                [java.lang.Long.valueOf(Base + (Hour / 6)), java.lang.Integer.valueOf(2)],
                [java.lang.Long.valueOf(Base + Hour), java.lang.Integer.valueOf(3)],
                [java.lang.Long.valueOf(Base + (Hour * 2) + (Hour / 2)), java.lang.Integer.valueOf(4)],
            ];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ROWTIME", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                foreach (var row in Rows)
                    list.add(row);

                return org.apache.calcite.linq4j.Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// The context a plan is bound with.
        /// </summary>
        /// <param name="rootSchema">The schema the plan was planned against.</param>
        /// <param name="parameters">The map both implementors stash values into, which <c>get</c> answers from.</param>
        /// <remarks>
        /// <c>parameters</c> is the map both implementors stash values into, and <c>get</c> must answer from it:
        /// Calcite's generated <c>bind</c> reads each stashed value back with <c>root.get(name)</c>, including
        /// <c>EnumerableRepeatUnion</c>'s transient table.
        /// </remarks>
        sealed class TestDataContext(SchemaPlus rootSchema, java.util.Map parameters) : DataContext
        {

            /// <inheritdoc />
            public SchemaPlus getRootSchema() => rootSchema;

            /// <inheritdoc />
            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            /// <inheritdoc />
            public QueryProvider getQueryProvider() => null!;

            /// <inheritdoc />
            public object get(string name) => parameters.get(name);

        }

        /// <summary>
        /// Builds the schema every query here is planned against.
        /// </summary>
        /// <returns>A new root schema holding every table and function the suite's queries name.</returns>
        internal static SchemaPlus Schema()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SALES", new SalesTable());
            rootSchema.add("MY_SUM", AggregateFunctionImpl.create((java.lang.Class)typeof(SumAggregate)));
            rootSchema.add("NUMBERS", TableFunctionImpl.create((java.lang.Class)typeof(NumbersTableFunction), "eval"));
            rootSchema.add("EVENTS", new EventsTable());
            rootSchema.add("SORTED", new SortedTable());
            rootSchema.add("SCALARS", new ScalarsTable());
            rootSchema.add("WIDE", new WideTable());
            rootSchema.add("ANYS", new AnysTable());
            rootSchema.add("CASTS", new CastsTable());
            rootSchema.add("DOCS", new DocsTable());
            rootSchema.add("FIB", org.apache.calcite.schema.impl.TableFunctionImpl.create(org.apache.calcite.util.Smalls.FIBONACCI_LIMIT_100_TABLE_METHOD));

            // A CUSTOM-format fixture, unlike every other table here. HrSchema's rows are instances of a Java
            // class, so a scan of it yields a synthetic record rather than an Object[], and PhysType.record,
            // fieldReference and the join selector take their CUSTOM branches.
            rootSchema.add("HR", new org.apache.calcite.adapter.java.ReflectiveSchema(new org.apache.calcite.test.schemata.hr.HrSchema()));

            // the hierarchy Calcite's EnumerableRepeatUnionHierarchyTest walks, including a recursive query
            // whose step is a correlate over the transient table. Calcite's ReflectiveSchemaWithoutRowCount
            // wrapper keeps the planner from costing the scan out of the plan.
            rootSchema.add("HIER", new org.apache.calcite.test.ReflectiveSchemaWithoutRowCount(new org.apache.calcite.test.schemata.hr.HierarchySchema()));

            // a column of every type Calcite has an implementor for, including a list for IS EMPTY, as
            // Calcite's EnumerableCalcTest uses it
            rootSchema.add("CATCHALL", new org.apache.calcite.adapter.java.ReflectiveSchema(new org.apache.calcite.test.schemata.catchall.CatchallSchema()));

            return rootSchema;
        }

        /// <summary>
        /// Runs a query in one convention and returns its rows rendered as text; in this convention, the rows
        /// are read four ways by <see cref="CursorRows"/>.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <param name="clr">Whether to plan into this convention or into Calcite's.</param>
        /// <param name="topDown">Whether the planner optimises top down, which asks a node to pass a trait down
        /// to its inputs or derive one from them.</param>
        /// <param name="planOnly">Whether to return the plan's text instead of its rows.</param>
        /// <param name="sortedAggregate">Whether to add the convention's sorted aggregate rule.</param>
        /// <param name="batchNestedLoopJoin">Whether to add the convention's batch nested loop join rule.</param>
        /// <param name="limitSort">Whether to add the convention's limit sort rule.</param>
        /// <param name="markJoin">Whether to rewrite sub-queries into mark correlates.</param>
        /// <param name="excludeHashJoin">Whether to remove both conventions' hash join rules.</param>
        /// <param name="excludeMergeJoin">Whether to remove both conventions' merge join rules.</param>
        /// <param name="interpreter">Whether to add this convention's interpreter rule.</param>
        /// <param name="add">Rules to register alongside the defaults.</param>
        /// <param name="remove">Rules to remove once everything is registered.</param>
        /// <returns>The rows rendered as text, or a single element holding the plan's text when
        /// <paramref name="planOnly"/> is set.</returns>
        static List<string> Run(string sql, bool clr, bool topDown = false, bool planOnly = false, bool sortedAggregate = false, bool batchNestedLoopJoin = false, bool limitSort = false, bool markJoin = false, bool excludeHashJoin = false, bool excludeMergeJoin = false, bool interpreter = false, RelOptRule[]? add = null, RelOptRule[]? remove = null)
        {
            // planned afresh for every reading, because a plan holding a transient table keeps its rows: a
            // recursive query that ended by deduplication leaves its last row in that table, and a second
            // reading of the same plan starts from it
            (RelNode Physical, java.util.Map Parameters, DataContext Context) Plan()
            {
                var rootSchema = Schema();

                var rules = new java.util.ArrayList();
                var calcRules = new java.util.ArrayList();

                if (clr)
                {
                    foreach (var rule in ClrCursorRules.Rules())
                    {
                        // removed on both sides alike, so that both plan the same shape: DefaultRulesProgram
                        // removes Calcite's and this removes this convention's
                        if (excludeMergeJoin && rule == ClrCursorRules.ClrCursorMergeJoinRule)
                            continue;

                        // likewise the hash join
                        if (excludeHashJoin && rule == ClrCursorRules.ClrCursorJoinRule)
                            continue;

                        rules.add(rule);
                    }

                    foreach (var rule in ClrCursorRules.CalcRules())
                        calcRules.add(rule);
                }

                // Calcite's own rules are registered by DefaultRulesProgram, because RelOptUtil.registerDefaultRules
                // registers ENUMERABLE_RULES. Where this convention has no node, the planner takes Calcite's and a
                // converter carries the rows across.

                // the sorted aggregate, batch nested loop join and limit sort rules are declared by both
                // conventions but left out of their default lists, so a test that wants one asks for it and each
                // side registers its own
                if (sortedAggregate)
                    rules.add(clr ? ClrCursorRules.ClrCursorSortedAggregateRule : EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE);

                if (batchNestedLoopJoin)
                    rules.add(clr ? ClrCursorRules.ClrCursorBatchNestedLoopJoinRule : EnumerableRules.ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE);

                if (limitSort)
                    rules.add(clr ? ClrCursorRules.ClrCursorLimitSortRule : EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);

                // Calcite registers TO_INTERPRETER from RelOptUtil.registerDefaultRules, so its side always has
                // it; this convention's counterpart is not in its default list and a caller adds it
                if (interpreter && clr)
                    rules.add(ClrCursorRules.ClrCursorInterpreterRule);

                // AVG has no implementor; this rule, from RelOptRules.BASE_RULES rather than any convention's
                // rules, rewrites it in terms of SUM and COUNT
                rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);

                // both conventions refuse a project holding an OVER; this rule, also outside any convention's
                // rules, rewrites it into a LogicalWindow
                rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
                foreach (var rule in RelOptRules.CALC_RULES.toArray())
                    calcRules.add(rule);

                var config = Frameworks.newConfigBuilder()
                    .defaultSchema(rootSchema)
                    .programs(
                        markJoin ? MarkJoinSubQueryProgram(Provider(clr)) : Programs.subQuery(Provider(clr)),
                        new DefaultRulesProgram(rules, topDown, (clr && topDown) || excludeMergeJoin, excludeHashJoin, add, remove),
                        Programs.hep(calcRules, true, Provider(clr)))
                    .build();

                var planner = Frameworks.getPlanner(config);
                var logical = planner.rel(planner.validate(planner.parse(sql))).project();
                var expanded = planner.transform(0, logical.getTraitSet(), logical);

                var convention = clr ? (Convention)ClrCursorConvention.Instance : EnumerableConvention.INSTANCE;
                // as Prepare.getDesiredRootTraitSet: the root's own traits with the convention replaced, then
                // simplified. simplify() collapses the composite collation a VALUES of several rows carries,
                // which the planner otherwise fails casting to a single RelCollation. The collation is kept
                // rather than requesting an empty trait set, or SortRemoveRule, one of Calcite's rules, drops an
                // ORDER BY.
                var chosen = planner.transform(1, expanded.getTraitSet().replace(convention).simplify(), expanded);
                var physical = planner.transform(2, chosen.getTraitSet(), chosen);

                var parameters = new java.util.HashMap();
                return (physical, parameters, new TestDataContext(rootSchema, parameters));
            }

            var (physical, parameters, context) = Plan();

            if (planOnly)
                return [org.apache.calcite.plan.RelOptUtil.toString(physical)];

            if (physical is ClrCursorRel)
                return CursorRows(() => { var (p, m, c) = Plan(); return ((ClrCursorRel)p, m, c); });

            var rows = new List<string>();
            foreach (var row in TestRows.Of(EnumerableInterpretable.toBindable(parameters, null, (EnumerableRel)physical, EnumerableRel.Prefer.ARRAY), context))
                rows.Add(Render(row));

            return rows;
        }


        /// <summary>
        /// Reads a cursor plan four ways, requires the four readings to agree, and returns one of them rendered
        /// as text.
        /// </summary>
        /// <param name="plan">Plans the query afresh and returns the physical root, the parameter map its implementor
        /// stashes into, and the context to bind it with; called once per reading.</param>
        /// <returns>The rows of the synchronous reading, each rendered as text.</returns>
        /// <remarks>
        /// The four readings are: opened synchronously and advanced with <c>Read</c>; opened with await and
        /// advanced with <c>ReadAsync</c>; opened with await and advanced with <c>Read</c>; and opened
        /// synchronously with the two advances alternating. So every query in the suite compares all four with
        /// Calcite. The awaiting readings run on the thread pool, because the test thread may have a
        /// synchronization context, and blocking on an await under one can deadlock. Each reading plans afresh,
        /// for the reason given in <see cref="Run"/>; reopening one factory is tested in
        /// <c>ClrCursorReadModeTests</c>.
        /// </remarks>
        static List<string> CursorRows(Func<(ClrCursorRel Node, java.util.Map Parameters, DataContext Context)> plan)
        {
            static (ClrCursorFactory Factory, DataContext Context) Open(Func<(ClrCursorRel Node, java.util.Map Parameters, DataContext Context)> plan)
            {
                var (node, parameters, context) = plan();
                var implementor = new ClrCursorRelImplementor(node.getCluster().getRexBuilder(), parameters);

                return (implementor.ImplementRoot(node, ClrCursorPrefer.Array), context);
            }

            var read = new List<string>();
            {
                var (factory, context) = Open(plan);
                using var cursor = factory.Open(context);
                while (cursor.Read())
                    read.Add(Render(cursor.Current));
            }

            var awaited = System.Threading.Tasks.Task.Run(async () =>
            {
                var rows = new List<string>();
                var (factory, context) = Open(plan);
                await using var cursor = await factory.OpenAsync(context, System.Threading.CancellationToken.None);
                while (await cursor.ReadAsync(System.Threading.CancellationToken.None))
                    rows.Add(Render(cursor.Current));
                return rows;
            }).GetAwaiter().GetResult();

            var awaitedThenRead = System.Threading.Tasks.Task.Run(async () =>
            {
                var rows = new List<string>();
                var (factory, context) = Open(plan);
                using var cursor = await factory.OpenAsync(context, System.Threading.CancellationToken.None);
                while (cursor.Read())
                    rows.Add(Render(cursor.Current));
                return rows;
            }).GetAwaiter().GetResult();

            var alternating = System.Threading.Tasks.Task.Run(async () =>
            {
                var rows = new List<string>();
                var (factory, context) = Open(plan);
                await using var cursor = factory.Open(context);
                for (var i = 0; ; i++)
                {
                    var moved = i % 2 == 0 ? cursor.Read() : await cursor.ReadAsync(System.Threading.CancellationToken.None);
                    if (moved == false)
                        break;

                    rows.Add(Render(cursor.Current));
                }
                return rows;
            }).GetAwaiter().GetResult();

            awaited.Should().Equal(read, "opened with await and read with await, the rows are the rows read synchronously");
            awaitedThenRead.Should().Equal(read, "opened with await and read synchronously, the rows are the rows read synchronously");
            alternating.Should().Equal(read, "alternating the two advances reads every row once, in order");

            return read;
        }

        /// <summary>
        /// Renders a row as text, so that rows from the two conventions compare by value.
        /// </summary>
        /// <param name="row">A row, an <c>object[]</c> for a multi-column result or the value itself for one column.</param>
        /// <returns>The fields' text joined with <c>|</c>, with a null written as <c>&lt;null&gt;</c>.</returns>
        static string Render(object row)
        {
            if (row is object[] array)
                return string.Join("|", array.Select(Render));

            return row?.ToString() ?? "<null>";
        }

        /// <summary>
        /// Runs a plan built against a <see cref="RelBuilder"/> in one convention and returns its rows
        /// rendered as text.
        /// </summary>
        /// <param name="build">Builds the logical plan.</param>
        /// <param name="clr">Whether to plan into this convention or into Calcite's.</param>
        /// <param name="planOnly">Whether to return the plan's text instead of its rows.</param>
        /// <param name="add">Rules to register alongside Calcite's.</param>
        /// <param name="remove">Rules to remove once everything is registered.</param>
        /// <returns>The rows rendered as text, or a single element holding the plan's text when
        /// <paramref name="planOnly"/> is set.</returns>
        /// <remarks>
        /// Some plans <c>EnumerableConvention</c>'s own tests reach cannot be written as SQL: <c>Combine</c> has
        /// no syntax, the POSIX regex operators are not in the core parser, and a recursive query over a
        /// transient table is built with <c>transientScan</c> and <c>repeatUnion</c>. Calcite tests these
        /// through <c>CalciteAssert.withRel</c>; this runs them in either convention.
        ///
        /// <para>The programs are those of <see cref="Run"/> without the sub-query pass, which has nothing to
        /// rewrite in a built plan.</para>
        /// </remarks>
        internal static List<string> RunRel(Func<RelBuilder, RelNode> build, bool clr, bool planOnly = false, RelOptRule[]? add = null, RelOptRule[]? remove = null)
        {
            // planned afresh for every reading, because a plan holding a transient table keeps its rows: a
            // recursive query that ended by deduplication leaves its last row in that table, and a second
            // reading of the same plan starts from it
            (RelNode Physical, java.util.Map Parameters, DataContext Context) Plan()
            {
                var rootSchema = Schema();

                var rules = new java.util.ArrayList();
                if (clr)
                {
                    foreach (var rule in ClrCursorRules.Rules())
                        rules.add(rule);
                }

                var calcRules = new java.util.ArrayList();
                if (clr)
                {
                    foreach (var rule in ClrCursorRules.CalcRules())
                        if (calcRules.contains(rule) == false)
                            calcRules.add(rule);
                }
                foreach (var rule in RelOptRules.CALC_RULES.toArray())
                    calcRules.add(rule);

                var config = Frameworks.newConfigBuilder().defaultSchema(rootSchema).build();
                var logical = build(RelBuilder.create(config));

                var planner = (org.apache.calcite.plan.volcano.VolcanoPlanner)logical.getCluster().getPlanner();
                planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
                planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

                var convention = clr ? (Convention)ClrCursorConvention.Instance : EnumerableConvention.INSTANCE;
                var empty = new java.util.ArrayList();

                var chosen = new DefaultRulesProgram(rules, false, false, false, add, remove)
                    .run(planner, logical, logical.getTraitSet().replace(convention).simplify(), empty, empty);

                var physical = Programs.hep(calcRules, true, Provider(clr))
                    .run(planner, chosen, chosen.getTraitSet(), empty, empty);

                var parameters = new java.util.HashMap();
                return (physical, parameters, new TestDataContext(rootSchema, parameters));
            }

            var (physical, parameters, context) = Plan();

            if (planOnly)
                return [org.apache.calcite.plan.RelOptUtil.toString(physical)];

            if (physical is ClrCursorRel)
                return CursorRows(() => { var (p, m, c) = Plan(); return ((ClrCursorRel)p, m, c); });

            var rows = new List<string>();
            foreach (var row in TestRows.Of(EnumerableInterpretable.toBindable(parameters, null, (EnumerableRel)physical, EnumerableRel.Prefer.ARRAY), context))
                rows.Add(Render(row));

            return rows;
        }

        /// <summary>
        /// Returns the text of the plan chosen for a query, to check which nodes it reaches.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <param name="clr">Whether to plan into this convention or into Calcite's.</param>
        /// <param name="sortedAggregate">Whether to add the convention's sorted aggregate rule.</param>
        /// <param name="batchNestedLoopJoin">Whether to add the convention's batch nested loop join rule.</param>
        /// <param name="limitSort">Whether to add the convention's limit sort rule.</param>
        /// <param name="markJoin">Whether to rewrite sub-queries into mark correlates.</param>
        /// <param name="excludeMergeJoin">Whether to remove both conventions' merge join rules.</param>
        /// <param name="interpreter">Whether to add this convention's interpreter rule.</param>
        /// <returns>The physical plan as <c>RelOptUtil.toString</c> writes it.</returns>
        internal static string PlanOf(string sql, bool clr, bool sortedAggregate = false, bool batchNestedLoopJoin = false, bool limitSort = false, bool markJoin = false, bool excludeMergeJoin = false, bool interpreter = false)
        {
            return Run(sql, clr, false, true, sortedAggregate, batchNestedLoopJoin, limitSort, markJoin, false, excludeMergeJoin, interpreter)[0];
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions, with this convention's interpreter
        /// rule added.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <remarks>
        /// Calcite's <c>TO_INTERPRETER</c> is always registered by <c>registerDefaultRules</c>, so adding this
        /// convention's rule gives the planner a choice of which convention hosts the interpreted node; the rows
        /// must be the same whichever it chooses.
        /// </remarks>
        static void SameInterpreted(string sql)
        {
            var mine = Run(sql, true, false, false, false, false, false, false, false, false, true);
            var calcite = Run(sql, false, false, false, false, false, false, false, false, false, true);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions, with neither convention's merge join
        /// rule registered.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <remarks>
        /// Both planners may choose a merge join for an equi-join over <c>SALES</c>, so a test aimed at the hash
        /// join removes the merge join from both sides; otherwise agreeing rows say nothing about the hash
        /// join.
        /// </remarks>
        static void SameHashJoin(string sql)
        {
            var mine = Run(sql, true, false, false, false, false, false, false, false, true);
            var calcite = Run(sql, false, false, false, false, false, false, false, false, true);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        /// <summary>
        /// Returns the metadata provider each side plans with: Calcite's own, or for this convention Calcite's
        /// with handlers for this convention's nodes added in front, as <c>ClrPrepare.GetProgram</c> uses.
        /// </summary>
        /// <param name="clr">Whether the provider is for this convention's side.</param>
        /// <returns><c>ClrCursorRelMetadata.Provider</c> for this convention, otherwise
        /// <c>DefaultRelMetadataProvider.INSTANCE</c>.</returns>
        static org.apache.calcite.rel.metadata.RelMetadataProvider Provider(bool clr) =>
            clr ? Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider : org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE;

        /// <summary>
        /// Returns the text of the plan chosen for a query, optionally with both hash join rules removed.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <param name="clr">Whether to plan into this convention or into Calcite's.</param>
        /// <param name="excludeHashJoin">Whether to remove both conventions' hash join rules.</param>
        /// <returns>The physical plan as <c>RelOptUtil.toString</c> writes it.</returns>
        internal static string PlanOfFib(string sql, bool clr, bool excludeHashJoin = false) => Run(sql, clr, false, true, false, false, false, false, excludeHashJoin)[0];

        /// <summary>
        /// Runs a query in one convention, optionally with both hash join rules removed.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <param name="clr">Whether to plan into this convention or into Calcite's.</param>
        /// <param name="excludeHashJoin">Whether to remove both conventions' hash join rules.</param>
        /// <returns>The rows rendered as text.</returns>
        internal static List<string> RunFib(string sql, bool clr, bool excludeHashJoin = false) => Run(sql, clr, false, false, false, false, false, false, excludeHashJoin);

        /// <summary>
        /// Requires that a query gives the same rows in both conventions.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <param name="limitSort">Whether to add each side's limit sort rule.</param>
        static void Same(string sql, bool limitSort = false)
        {
            var mine = Run(sql, true, limitSort: limitSort);
            var calcite = Run(sql, false, limitSort: limitSort);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        /// <summary>
        /// Requires that a query fails with the same innermost exception type and message in both conventions.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <param name="message">Part of the message Calcite's failure must contain.</param>
        /// <remarks>
        /// A query Calcite refuses must be refused here too. Comparing the innermost exception's type and
        /// message, rather than only that both throw, catches the two sides failing for different reasons.
        /// </remarks>
        static void SameFailure(string sql, string message)
        {
            static string Failure(string sql, bool clr)
            {
                try
                {
                    Run(sql, clr);
                    return "<no failure>";
                }
                catch (Exception e)
                {
                    while (e.InnerException is not null)
                        e = e.InnerException;

                    return $"{e.GetType().Name}: {e.Message}";
                }
            }

            var mine = Failure(sql, true);
            var calcite = Failure(sql, false);

            calcite.Should().Contain(message, "'{0}' should fail this way under EnumerableConvention", sql);
            mine.Should().Be(calcite, "'{0}' should fail the way EnumerableConvention fails", sql);
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions, and that this convention really
        /// planned the node it was aimed at.
        /// </summary>
        /// <param name="node">The node this convention's plan must contain.</param>
        /// <param name="sql">The query.</param>
        /// <param name="remove">Rules removed from this convention's run only, so that the planner has nothing
        /// it prefers to <paramref name="node"/>.</param>
        /// <param name="sortedAggregate">Whether to add each side's sorted aggregate rule.</param>
        /// <param name="batchNestedLoopJoin">Whether to add each side's batch nested loop join rule.</param>
        /// <param name="limitSort">Whether to add each side's limit sort rule.</param>
        /// <param name="add">Rules to register alongside the defaults, on both sides.</param>
        /// <remarks>
        /// Both conventions' rules are in one planner, and <c>VolcanoCost</c> compares row counts only, so a
        /// node of Calcite's and the same node of this convention cost the same and the planner keeps the one it
        /// registered first, which is Calcite's. Comparing rows alone could then compare Calcite with itself;
        /// the plan assertion rules that out, and the removed rules make the intended plan reachable.
        /// Calcite's side is planned without removals.
        /// </remarks>
        static void SameThrough(string node, string sql, RelOptRule[]? remove = null, bool sortedAggregate = false, bool batchNestedLoopJoin = false, bool limitSort = false, RelOptRule[]? add = null)
        {
            Run(sql, true, planOnly: true, sortedAggregate: sortedAggregate, batchNestedLoopJoin: batchNestedLoopJoin, limitSort: limitSort, add: add, remove: remove)[0]
                .Should().Contain(node, "'{0}' should be planned through {1}", sql, node);

            var mine = Run(sql, true, sortedAggregate: sortedAggregate, batchNestedLoopJoin: batchNestedLoopJoin, limitSort: limitSort, add: add, remove: remove);
            var calcite = Run(sql, false, sortedAggregate: sortedAggregate, batchNestedLoopJoin: batchNestedLoopJoin, limitSort: limitSort, add: add);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        /// <summary>
        /// Requires that a plan built against a <see cref="RelBuilder"/> gives the same rows in both
        /// conventions.
        /// </summary>
        /// <param name="build">Builds the logical plan; it is called once for each side.</param>
        /// <param name="add">Rules to register alongside the defaults, on both sides.</param>
        /// <param name="remove">Rules to remove once everything is registered, on both sides.</param>
        internal static void SameRel(Func<RelBuilder, RelNode> build, RelOptRule[]? add = null, RelOptRule[]? remove = null)
        {
            var mine = RunRel(build, true, add: add, remove: remove);
            var calcite = RunRel(build, false, add: add, remove: remove);

            mine.Should().Equal(calcite, "the plan should give what EnumerableConvention gives");
        }

        /// <summary>
        /// Requires that a plan built against a <see cref="RelBuilder"/> gives the same rows in both
        /// conventions, and that this convention's plan contains the node named by <paramref name="node"/>.
        /// </summary>
        /// <param name="node">The node this convention's plan must contain.</param>
        /// <param name="build">Builds the logical plan; it is called once for each side and once more for the plan text.</param>
        /// <param name="add">Rules to register alongside the defaults, on both sides.</param>
        /// <param name="remove">Rules to remove once everything is registered, on both sides.</param>
        internal static void SameRelThrough(string node, Func<RelBuilder, RelNode> build, RelOptRule[]? add = null, RelOptRule[]? remove = null)
        {
            RunRel(build, true, planOnly: true, add: add, remove: remove)[0]
                .Should().Contain(node, "the plan should be planned through {0}", node);

            var mine = RunRel(build, true, add: add, remove: remove);
            var calcite = RunRel(build, false, add: add, remove: remove);

            mine.Should().Equal(calcite, "the plan should give what EnumerableConvention gives");
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions, with each side's sorted aggregate rule
        /// added.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <remarks>
        /// Neither convention registers the rule by default, as Calcite does not.
        /// </remarks>
        static void SameSortedAggregate(string sql)
        {
            var mine = Run(sql, true, false, false, true);
            var calcite = Run(sql, false, false, false, true);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions, with each side's batch nested loop
        /// join rule added.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <remarks>
        /// Neither convention registers the rule by default, as Calcite does not. Both use Calcite's batch size
        /// of 100.
        /// </remarks>
        static void SameBatchNestedLoopJoin(string sql)
        {
            var mine = Run(sql, true, false, false, false, true);
            var calcite = Run(sql, false, false, false, false, true);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions, with each side's limit sort rule
        /// added.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <remarks>
        /// Neither convention registers the rule by default: Calcite's is a field of <c>EnumerableRules</c> left
        /// out of <c>ENUMERABLE_RULES</c>, like the sorted aggregate and batch nested loop join rules.
        /// </remarks>
        static void SameLimitSort(string sql)
        {
            var mine = Run(sql, true, false, false, false, false, true);
            var calcite = Run(sql, false, false, false, false, false, true);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        /// <summary>
        /// Builds the sub-query pass that rewrites EXISTS, IN and SOME into a LEFT MARK join rather than a
        /// correlate.
        /// </summary>
        /// <param name="provider">The metadata provider the hep pass costs with.</param>
        /// <returns>A hep program applying Calcite's mark-correlate sub-query rules.</returns>
        /// <remarks>
        /// <c>Programs.subQuery</c> chooses between two rule sets on
        /// <c>CalciteConnectionConfig.topDownGeneralDecorrelationEnabled</c>, which is off by default, so it does
        /// not reach the mark-join rules. This builds that second rule set directly.
        /// </remarks>
        static Program MarkJoinSubQueryProgram(org.apache.calcite.rel.metadata.RelMetadataProvider provider)
        {
            var rules = new java.util.ArrayList();
            rules.add(org.apache.calcite.rel.rules.CoreRules.FILTER_SUB_QUERY_TO_MARK_CORRELATE);
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_SUB_QUERY_TO_MARK_CORRELATE);
            rules.add(org.apache.calcite.rel.rules.CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_OVER_SUM_TO_SUM0_RULE);

            var builder = org.apache.calcite.plan.hep.HepProgram.builder();
            builder.addRuleCollection(rules);

            return Programs.of(builder.build(), true, provider);
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions, with sub-queries rewritten by
        /// <see cref="MarkJoinSubQueryProgram"/>.
        /// </summary>
        /// <param name="sql">The query.</param>
        static void SameMarkJoin(string sql)
        {
            var mine = Run(sql, true, false, false, false, false, false, true);
            var calcite = Run(sql, false, false, false, false, false, false, true);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions when the planner optimises top down.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <remarks>
        /// Only top-down optimisation calls <c>passThroughTraits</c>, <c>deriveTraits</c> and
        /// <c>getDeriveMode</c>, and Calcite leaves it off by default, so this is what compares this
        /// convention's implementations of them with <c>EnumerableConvention</c>'s.
        /// </remarks>
        static void SameTopDown(string sql)
        {
            var mine = Run(sql, true, true);
            var calcite = Run(sql, false, true);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives, planned top down", sql);
        }

        /// <summary>
        /// Requires that a query gives the stated rows in this convention.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <param name="expected">The rows, each rendered as <see cref="Render"/> writes it, in the order the query returns them.</param>
        /// <remarks>
        /// Only for queries <c>EnumerableConvention</c> cannot run. Prefer <see cref="Same"/> otherwise: a
        /// hand-written expectation can be wrong in the same way as the code under test.
        /// </remarks>
        static void Gives(string sql, params string[] expected)
        {
            Run(sql, true).Should().Equal(expected, "'{0}' should give the rows SQL says it does", sql);
        }

        [Fact]
        public void ShouldAgreeOnAScan() => Same("SELECT \"ID\", \"REGION\" FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnAFilterAndProjection() => Same("SELECT \"ID\", \"AMOUNT\" + 1 FROM \"SALES\" WHERE \"AMOUNT\" > 10 ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnANullableExpression() => Same("SELECT \"ID\", \"AMOUNT\" + \"ID\" FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnSortingWithNulls() => Same("SELECT \"ID\" FROM \"SALES\" ORDER BY \"AMOUNT\", \"ID\"");

        [Fact]
        public void ShouldAgreeOnSortingDescendingWithNulls() => Same("SELECT \"ID\" FROM \"SALES\" ORDER BY \"AMOUNT\" DESC, \"ID\"");

        [Fact]
        public void ShouldAgreeOnAGroupBy() => Same("SELECT \"REGION\", COUNT(*), SUM(\"AMOUNT\"), MIN(\"AMOUNT\"), MAX(\"AMOUNT\") FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"");

        /// <summary>
        /// A GROUP BY with no ORDER BY, where the two conventions must agree on the order of the groups; both
        /// group in a <c>java.util.HashMap</c>.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnAGroupBysOwnOrder() => Same("SELECT \"REGION\", COUNT(*) FROM \"SALES\" GROUP BY \"REGION\"");

        // MIN, MAX, SUM and AVG over a column of type ANY, whose Java class is Object.
        //
        // Calcite cannot run these, so the answers are asserted by hand with Gives. ShouldStillBeBeyondCalcite
        // below fails once Calcite can run them, at which point they should be compared with Same.
        //
        // The values are BigDecimal wherever SqlFunctions.plusAny and divideAny have produced them, as
        // Calcite's ANY arithmetic does for a scalar + too.

        [Fact]
        public void ShouldAggregateAnAnyColumn() => Gives("SELECT MIN(\"V\"), MAX(\"V\"), SUM(\"V\"), AVG(\"V\") FROM \"ANYS\"", "5|30|65.5|16.375");

        [Fact]
        public void ShouldGroupAnAggregateOverAnAnyColumn() => Gives("SELECT \"K\", MIN(\"V\"), MAX(\"V\"), SUM(\"V\"), AVG(\"V\") FROM \"ANYS\" GROUP BY \"K\" ORDER BY \"K\"", "EAST|10|20.5|30.5|15.25", "WEST|5|30|35|17.5");

        /// <summary>
        /// MIN and MAX over an ANY column holding two numeric classes.
        /// </summary>
        /// <remarks>
        /// <c>SqlFunctions.lesser</c>, which <c>RexImpTable.MinMaxImplementor</c> calls, compares through
        /// <c>Comparable.compareTo</c> and throws on an <c>Integer</c> against a <c>Double</c>; <c>ltAny</c>
        /// compares them as BigDecimal, as a scalar <c>&lt;</c> over ANY does. In a document store one path
        /// commonly holds both.
        /// </remarks>
        [Fact]
        public void ShouldAggregateAnAnyColumnOfMixedNumericTypes() => Gives("SELECT MIN(\"V\"), MAX(\"V\") FROM \"ANYS\" WHERE \"K\" = 'EAST'", "10|20.5");

        /// <summary>
        /// MIN and MAX over an ANY column holding strings.
        /// </summary>
        /// <remarks>
        /// IKVM gives <see cref="string"/> <c>java.lang.Comparable</c> as a ghost interface, which a CLR cast
        /// does not see. The comparison here does not cast to <c>Comparable</c>, because <c>ltAny</c> takes two
        /// <c>Object</c>s.
        /// </remarks>
        [Fact]
        public void ShouldAggregateAnAnyColumnOfStrings() => Gives("SELECT MIN(\"S\"), MAX(\"S\") FROM \"ANYS\"", "a|d");

        /// <summary>
        /// An aggregate over an ANY column of a group with no rows in it.
        /// </summary>
        /// <remarks>
        /// <c>StrictAggImplementor</c> decides this and the ANY implementors do not override it: SUM, MIN and MAX
        /// return null over an empty set. A null accumulator is also what MIN reads as "no row yet".
        /// </remarks>
        [Fact]
        public void ShouldAggregateAnEmptyAnyColumn() => Gives("SELECT MIN(\"V\"), MAX(\"V\"), SUM(\"V\"), AVG(\"V\") FROM \"ANYS\" WHERE \"K\" = 'NORTH'", "<null>|<null>|<null>|<null>");

        /// <summary>
        /// SUM over an ANY column holding something that cannot be added.
        /// </summary>
        /// <remarks>
        /// <c>plusAny</c> throws for anything but two numbers, and the accumulator reaches it as a scalar
        /// <c>+</c> does.
        /// </remarks>
        [Fact]
        public void ShouldRefuseToSumAnAnyColumnOfStrings()
        {
            var act = () => Run("SELECT SUM(\"S\") FROM \"ANYS\"", true);

            act.Should().Throw<java.lang.RuntimeException>().WithMessage("*arithmetic*");
        }

        /// <summary>
        /// MIN, MAX and SUM over an ANY column in a window.
        /// </summary>
        /// <remarks>
        /// <c>RexImpTable</c> uses the regular implementor in a window for a function with no window implementor,
        /// which none of these three has, so <c>ClrCursorWindow</c> also receives the ANY implementors, through
        /// its own code path rather than the aggregate's.
        /// </remarks>
        [Fact]
        public void ShouldWindowAnAggregateOverAnAnyColumn()
        {
            Gives("SELECT \"ID\", MIN(\"V\") OVER (PARTITION BY \"K\"), MAX(\"V\") OVER (PARTITION BY \"K\"), SUM(\"V\") OVER (PARTITION BY \"K\") FROM \"ANYS\" ORDER BY \"ID\"",
                "1|10|20.5|30.5",
                "2|10|20.5|30.5",
                "3|5|30|35",
                "4|5|30|35",
                "5|5|30|35");
        }

        [Fact]
        public void ShouldRunARunningTotalOverAnAnyColumn()
        {
            Gives("SELECT \"ID\", SUM(\"V\") OVER (ORDER BY \"ID\") FROM \"ANYS\" ORDER BY \"ID\"",
                "1|10",
                "2|30.5",
                "3|60.5",
                "4|60.5",
                "5|65.5");
        }

        /// <summary>
        /// ANY_VALUE over an ANY column.
        /// </summary>
        /// <remarks>
        /// <c>RexImpTable</c> implements ANY_VALUE with <c>MinMaxImplementor</c>, which takes the MAX branch for
        /// any kind other than MIN, so the value is the largest. This follows Calcite.
        /// </remarks>
        [Fact]
        public void ShouldTakeAnyValueOfAnAnyColumn() => Gives("SELECT ANY_VALUE(\"V\"), ANY_VALUE(\"S\") FROM \"ANYS\"", "30|d");

        /// <summary>
        /// The deviations and the variances over an ANY column.
        /// </summary>
        /// <remarks>
        /// None of these has an implementor. <c>AGGREGATE_REDUCE_FUNCTIONS</c> rewrites each in terms of sums of
        /// the value and of its square and a count, so they work over ANY only as long as SUM does.
        /// </remarks>
        [Fact]
        public void ShouldDeviateOverAnAnyColumn() => Gives("SELECT VAR_POP(\"V\"), VAR_SAMP(\"V\") FROM \"ANYS\"", "93.171875|124.2291666666667");

        /// <summary>
        /// An aggregate over an ANY column carrying a FILTER.
        /// </summary>
        /// <remarks>
        /// <c>StrictAggImplementor</c> handles the filter, folding it into the same condition as the null check,
        /// rather than the ANY implementors.
        /// </remarks>
        [Fact]
        public void ShouldFilterAnAggregateOverAnAnyColumn() => Gives("SELECT MIN(\"V\") FILTER (WHERE \"ID\" > 1), SUM(\"V\") FILTER (WHERE \"K\" = 'EAST') FROM \"ANYS\"", "5|30.5");

        /// <summary>
        /// A DISTINCT aggregate over an ANY column.
        /// </summary>
        /// <remarks>
        /// Both conventions' aggregates refuse a distinct call, and <c>AGGREGATE_EXPAND_DISTINCT_AGGREGATES</c>
        /// rewrites the DISTINCT away first, so this tests that rule over an ANY column rather than the
        /// implementors.
        /// </remarks>
        [Fact]
        public void ShouldAggregateDistinctlyOverAnAnyColumn() => Gives("SELECT COUNT(DISTINCT \"V\"), SUM(DISTINCT \"V\") FROM \"ANYS\"", "4|65.5");

        // UNNEST over a column of type ANY. EnumerableUncollect asks NonNullableAccessors.getComponentTypeOrThrow
        // for an element type, which ANY lacks, and throws before a row is read, so Calcite plans these but
        // cannot run them. As for the aggregates above, the answers are asserted by hand and
        // ShouldStillBeBeyondCalcite fails once Calcite can run them.
        //
        // Each plan is a correlate whose right input is the uncollect, because decorrelation cannot remove an
        // UNNEST of a correlation variable; this is the shape a document store's array traversal reaches.

        [Fact]
        public void ShouldUncollectAnAnyColumn() =>
            Gives("SELECT d.\"ID\", t.\"X\" FROM \"DOCS\" d, UNNEST(d.\"TAGS\") AS t(\"X\")", "1|red", "1|green", "2|blue");

        /// <summary>
        /// UNNEST over an ANY column whose lists hold two numeric classes.
        /// </summary>
        /// <remarks>
        /// No element is converted: <c>SqlFunctions.flatProduct</c> reads a single SCALAR field with
        /// <c>LIST_AS_ENUMERABLE</c>, which enumerates the list as it is, so an <c>Integer</c> and a
        /// <c>Double</c> in one column come through unchanged.
        /// </remarks>
        [Fact]
        public void ShouldUncollectAnAnyColumnOfMixedNumericTypes() =>
            Gives("SELECT d.\"ID\", t.\"X\" FROM \"DOCS\" d, UNNEST(d.\"NUMS\") AS t(\"X\")", "1|1", "1|2", "3|3", "3|4.5");

        /// <summary>
        /// An outer UNNEST over an ANY column keeps a row whose collection is null, against a null.
        /// </summary>
        /// <remarks>
        /// A null and an empty list behave alike, as for an array column: <c>LIST_AS_ENUMERABLE</c> treats a null
        /// as the empty sequence, and an inner correlate emits nothing for an outer row whose right side is
        /// empty. So an inner UNNEST drops the row (row 3 has a null <c>TAGS</c>, row 2 an empty <c>NUMS</c>) and
        /// an outer one keeps it.
        /// </remarks>
        [Fact]
        public void ShouldOuterUncollectANullAnyColumn() =>
            Gives("SELECT d.\"ID\", t.\"X\" FROM \"DOCS\" d LEFT JOIN UNNEST(d.\"TAGS\") AS t(\"X\") ON TRUE",
                "1|red", "1|green", "2|blue", "3|<null>");

        /// <summary>
        /// WITH ORDINALITY over an ANY column, which numbers each element from one.
        /// </summary>
        /// <remarks>
        /// <c>SqlUnnestOperator.inferReturnType</c> and <c>Uncollect.deriveUncollectRowType</c> append the
        /// ORDINALITY column in their ANY branch as in the others, and the node passes <c>withOrdinality</c> to
        /// <c>FLAT_ZIP</c> as <c>EnumerableUncollect</c> does.
        /// </remarks>
        [Fact]
        public void ShouldNumberAnUncollectedAnyColumn() =>
            Gives("SELECT d.\"ID\", t.\"X\", t.\"O\" FROM \"DOCS\" d, UNNEST(d.\"TAGS\") WITH ORDINALITY AS t(\"X\", \"O\")",
                "1|red|1", "1|green|2", "2|blue|1");

        /// <summary>
        /// An aggregate over the column an UNNEST of an ANY column produces, which is itself ANY.
        /// </summary>
        /// <remarks>
        /// The uncollect does not know the element type, so the column it produces is ANY, and MIN and MAX over
        /// it are implemented by <see cref="ClrAnyAggImplementors"/>.
        /// </remarks>
        [Fact]
        public void ShouldAggregateOverAnUncollectedAnyColumn() =>
            Gives("SELECT d.\"ID\", COUNT(*), MIN(t.\"X\"), MAX(t.\"X\") FROM \"DOCS\" d, UNNEST(d.\"NUMS\") AS t(\"X\") GROUP BY d.\"ID\" ORDER BY 1",
                "1|2|1|2", "3|2|3|4.5");

        /// <summary>
        /// A filter on the outer row of an UNNEST over an ANY column.
        /// </summary>
        /// <remarks>
        /// The filter is planned under the correlate rather than over the uncollect, so the correlate's left
        /// input is a calc rather than a bare scan.
        /// </remarks>
        [Fact]
        public void ShouldFilterTheOuterRowOfAnUncollectedAnyColumn() =>
            Gives("SELECT t.\"X\" FROM \"DOCS\" d, UNNEST(d.\"TAGS\") AS t(\"X\") WHERE d.\"ID\" = 1", "red", "green");

        /// <summary>
        /// Requires that Calcite still cannot run a query.
        /// </summary>
        /// <param name="sql">A query tested with <see cref="Gives"/>.</param>
        /// <remarks>
        /// The queries tested with <see cref="Gives"/> have no expected answer from Calcite only while
        /// <c>EnumerableConvention</c> cannot run them. If Calcite can, this fails, and those tests should compare
        /// the two conventions instead. It does not assert that Calcite ought to fail.
        /// </remarks>
        static void StillBeyondCalcite(string sql)
        {
            Failure(() => Run(sql, false)).Should().NotBeNull(
                "Calcite still cannot implement '{0}'; now that it can, the answers this convention gives should be compared against its own", sql);
        }

        /// <summary>
        /// Runs a query and returns what it threw, or null if it did not throw.
        /// </summary>
        /// <param name="run">Runs the query.</param>
        /// <returns>The exception <paramref name="run"/> threw, or null.</returns>
        static Exception? Failure(Func<List<string>> run)
        {
            try
            {
                run();
                return null;
            }
            catch (Exception e)
            {
                return e;
            }
        }

        [Fact]
        public void ShouldStillBeBeyondCalcite()
        {
            StillBeyondCalcite("SELECT MIN(\"V\") FROM \"ANYS\"");
            StillBeyondCalcite("SELECT MAX(\"V\") FROM \"ANYS\"");
            StillBeyondCalcite("SELECT SUM(\"V\") FROM \"ANYS\"");
            StillBeyondCalcite("SELECT AVG(\"V\") FROM \"ANYS\"");
            StillBeyondCalcite("SELECT ANY_VALUE(\"V\") FROM \"ANYS\"");
            StillBeyondCalcite("SELECT VAR_POP(\"V\") FROM \"ANYS\"");
            StillBeyondCalcite("SELECT MIN(\"V\") FILTER (WHERE \"ID\" > 1) FROM \"ANYS\"");
            StillBeyondCalcite("SELECT \"K\", MIN(\"V\"), SUM(\"V\") FROM \"ANYS\" GROUP BY \"K\"");

            StillBeyondCalcite("SELECT d.\"ID\", t.\"X\" FROM \"DOCS\" d, UNNEST(d.\"TAGS\") AS t(\"X\")");
            StillBeyondCalcite("SELECT d.\"ID\", t.\"X\" FROM \"DOCS\" d LEFT JOIN UNNEST(d.\"TAGS\") AS t(\"X\") ON TRUE");
            StillBeyondCalcite("SELECT d.\"ID\", t.\"X\", t.\"O\" FROM \"DOCS\" d, UNNEST(d.\"TAGS\") WITH ORDINALITY AS t(\"X\", \"O\")");
        }

        // the same columns read without an ANY aggregate implementor, which Calcite can run, so that a failure
        // above can be attributed to the aggregate rather than to the column, the fixture or the scan

        [Fact]
        public void ShouldAgreeOnScanningAnAnyColumn() => Same("SELECT \"K\", \"V\", \"S\" FROM \"ANYS\"");

        [Fact]
        public void ShouldAgreeOnCountingAnAnyColumn() => Same("SELECT \"K\", COUNT(\"V\"), COUNT(*) FROM \"ANYS\" GROUP BY \"K\" ORDER BY \"K\"");

        [Fact]
        public void ShouldAgreeOnAggregatingACastAnyColumn() => Same("SELECT MIN(CAST(\"V\" AS INTEGER)), MAX(CAST(\"V\" AS INTEGER)), SUM(CAST(\"V\" AS INTEGER)), AVG(CAST(\"V\" AS INTEGER)) FROM \"ANYS\"");

        [Fact]
        public void ShouldAgreeOnAGroupedAggregateOverACastAnyColumn() => Same("SELECT \"K\", MIN(CAST(\"V\" AS INTEGER)), SUM(CAST(\"V\" AS INTEGER)) FROM \"ANYS\" GROUP BY \"K\" ORDER BY \"K\"");

        [Fact]
        public void ShouldAgreeOnCastingAnAnyColumnToVarchar() => Same("SELECT \"ID\", CAST(\"G\" AS VARCHAR) FROM \"CASTS\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnCastingAnAnyColumnToANumber() => Same("SELECT \"ID\", CAST(\"N\" AS INTEGER), CAST(\"N\" AS DECIMAL(10, 2)) FROM \"CASTS\" ORDER BY \"ID\"");

        /// <summary>
        /// A cast of an ANY value to TIMESTAMP reads it as the internal representation, epoch milliseconds,
        /// rather than parsing it, in both conventions.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnCastingAnAnyColumnOfMillisToATimestamp() => Same("SELECT \"ID\", CAST(\"M\" AS TIMESTAMP) FROM \"CASTS\" ORDER BY \"ID\"");

        /// <summary>
        /// The same cast over a timestamp written as text fails in both conventions, because the text is passed
        /// to <c>Long.parseLong</c>.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnRefusingATimestampCastOfAnAnyColumnOfText() => SameFailure("SELECT CAST(\"T\" AS TIMESTAMP) FROM \"CASTS\"", "For input string: \"2026-01-01 00:00:00\"");

        /// <summary>
        /// A cast of an ANY value to UUID fails in both conventions, because the conversion is a Java
        /// conversion between classes rather than a parse, and a string is not a <c>UuidValue</c>.
        /// </summary>
        /// <remarks>
        /// <c>RexToLixTranslator.getConvertExpression</c> matches no source branch for ANY, so the cast ends at
        /// <c>EnumUtils.convert(operand, typeFactory.getJavaClass(targetType))</c>. <c>getJavaClass</c> maps UUID
        /// to <c>org.apache.calcite.util.UuidValue</c>, Calcite's runtime representation of a UUID (it orders as
        /// an unsigned 128-bit value, where <c>UUID.compareTo</c> compares signed halves), and converting a
        /// string to it throws. The convention reproduces Calcite; parsing would need a UUID source branch in
        /// <c>getConvertExpression</c>.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnRefusingAUuidCastOfAnAnyColumn() =>
            SameFailure("SELECT \"ID\", CAST(\"G\" AS UUID) FROM \"CASTS\" ORDER BY \"ID\"", "to type 'org.apache.calcite.util.UuidValue'");

        /// <summary>
        /// Casting through VARCHAR first converts, because VARCHAR is a source branch every target has.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnCastingAnAnyColumnThroughVarcharToUuid() => Same("SELECT \"ID\", CAST(CAST(\"G\" AS VARCHAR) AS UUID) FROM \"CASTS\" ORDER BY \"ID\"");

        /// <summary>
        /// Casting through VARCHAR to TIMESTAMP reaches the string parser, which expects SQL's literal format
        /// rather than ISO-8601.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnCastingAnAnyColumnThroughVarcharToATimestamp() => Same("SELECT \"ID\", CAST(CAST(\"T\" AS VARCHAR) AS TIMESTAMP) FROM \"CASTS\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnAGlobalAggregate() => Same("SELECT COUNT(*), SUM(\"AMOUNT\"), AVG(\"AMOUNT\") FROM \"SALES\"");

        // The three forms of aggregate that are not a plain GROUP BY. A grouping set folds every row into one
        // group per set in a single pass, and the group columns a set does not group by come out null, as the
        // key's indicator field decides.

        [Fact]
        public void ShouldAgreeOnGroupingSets() =>
            Same("SELECT \"REGION\", COUNT(*) FROM \"SALES\" GROUP BY GROUPING SETS ((\"REGION\"), ()) ORDER BY 1, 2");

        [Fact]
        public void ShouldAgreeOnARollup() =>
            Same("SELECT \"REGION\", \"LABEL\", COUNT(*) FROM \"SALES\" GROUP BY ROLLUP(\"REGION\", \"LABEL\") ORDER BY 1, 2, 3");

        [Fact]
        public void ShouldAgreeOnACube() =>
            Same("SELECT \"REGION\", \"LABEL\", COUNT(*) FROM \"SALES\" GROUP BY CUBE(\"REGION\", \"LABEL\") ORDER BY 1, 2, 3");

        [Fact]
        public void ShouldAgreeOnGroupingSetsOwnOrder() =>
            Same("SELECT \"REGION\", COUNT(*) FROM \"SALES\" GROUP BY GROUPING SETS ((\"REGION\"), ())");

        // A grouping set over four columns keys on eight fields, one per column and one indicator per column.
        // FlatLists.copyOf builds a key of more than six fields over an array whose element type Calcite names
        // as Comparable, the ghost interface IKVM gives a string, so only at this arity does a VARCHAR group
        // column reach it.

        [Fact]
        public void ShouldAgreeOnARollupOverEveryColumn() =>
            Same("SELECT \"ID\", \"REGION\", \"AMOUNT\", \"LABEL\", COUNT(*) FROM \"SALES\" GROUP BY ROLLUP(\"ID\", \"REGION\", \"AMOUNT\", \"LABEL\") ORDER BY 1, 2, 3, 4, 5");

        [Fact]
        public void ShouldAgreeOnTheGroupingFunction() =>
            Same("SELECT \"REGION\", GROUPING(\"REGION\"), COUNT(*) FROM \"SALES\" GROUP BY ROLLUP(\"REGION\") ORDER BY 1, 2");

        [Fact]
        public void ShouldAgreeOnADistinctOverEveryColumn() =>
            Same("SELECT DISTINCT \"ID\", \"REGION\", \"AMOUNT\", \"LABEL\" FROM \"SALES\" ORDER BY 1");

        // An aggregate call with its own ordering holds a group's rows and folds them once the ordering is
        // applied: LazyAggregateLambdaFactory, with a SourceSorter per ordered call and a BasicLazyAccumulator
        // per unordered one. These cover one ordered call, a global aggregate, an ordered and an unordered
        // call in one aggregate, and an ordering on a nullable column.

        [Fact]
        public void ShouldAgreeOnAnOrderedAggregateCall() =>
            Same("SELECT \"REGION\", LISTAGG(\"LABEL\", ',') WITHIN GROUP (ORDER BY \"ID\" DESC) FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"");

        [Fact]
        public void ShouldAgreeOnAGlobalOrderedAggregateCall() =>
            Same("SELECT LISTAGG(\"LABEL\", ',') WITHIN GROUP (ORDER BY \"ID\" DESC) FROM \"SALES\"");

        [Fact]
        public void ShouldAgreeOnAnOrderedAndAnUnorderedCallTogether() =>
            Same("SELECT \"REGION\", COUNT(*), LISTAGG(\"LABEL\", ',') WITHIN GROUP (ORDER BY \"ID\") FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"");

        [Fact]
        public void ShouldAgreeOnAnAggregateCallOrderedByANullableColumn() =>
            Same("SELECT \"REGION\", LISTAGG(\"LABEL\", ',') WITHIN GROUP (ORDER BY \"AMOUNT\") FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"");

        [Fact]
        public void ShouldAgreeOnAnInnerJoin() => Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [Fact]
        public void ShouldAgreeOnALeftJoin() => Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT JOIN (SELECT * FROM \"SALES\" WHERE \"AMOUNT\" > 25) b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        // A batch nested loop join, whose rule must be added. The right input becomes a filter over a
        // disjunction of the batch's conditions, so one pass of it serves up to a hundred left rows.

        [Fact]
        public void ShouldAgreeOnABatchNestedLoopJoin() =>
            SameBatchNestedLoopJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [Fact]
        public void ShouldAgreeOnABatchNestedLoopLeftJoin() =>
            SameBatchNestedLoopJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT JOIN (SELECT * FROM \"SALES\" WHERE \"ID\" > 4) b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [Fact]
        public void ShouldAgreeOnABatchNestedLoopJoinWithAnInequality() =>
            SameBatchNestedLoopJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" < b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [Fact]
        public void ShouldAgreeOnABatchNestedLoopSemiJoin() =>
            SameBatchNestedLoopJoin("SELECT \"ID\" FROM \"SALES\" a WHERE EXISTS (SELECT 1 FROM \"SALES\" b WHERE b.\"REGION\" = a.\"REGION\" AND b.\"ID\" > a.\"ID\") ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnABatchNestedLoopAntiJoin() =>
            SameBatchNestedLoopJoin("SELECT \"ID\" FROM \"SALES\" a WHERE NOT EXISTS (SELECT 1 FROM \"SALES\" b WHERE b.\"REGION\" = a.\"REGION\" AND b.\"ID\" > a.\"ID\") ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnAJoinWithAnInequality() => Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" < b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [Fact]
        public void ShouldAgreeOnAnAsofJoin() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a ASOF JOIN \"SALES\" b MATCH_CONDITION b.\"ID\" <= a.\"ID\" ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\"");

        [Fact]
        public void ShouldAgreeOnALeftAsofJoin() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT ASOF JOIN (SELECT * FROM \"SALES\" WHERE \"ID\" > 3) b MATCH_CONDITION b.\"ID\" <= a.\"ID\" ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\"");

        [Fact]
        public void ShouldAgreeOnAnAsofJoinLookingForward() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a ASOF JOIN \"SALES\" b MATCH_CONDITION b.\"ID\" > a.\"ID\" ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\"");

        /// <summary>
        /// An ASOF join emits rows in the order of the map it indexes its left input by, so with no ORDER BY
        /// this compares that order with linq4j's.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnAnAsofJoinsOwnOrder() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a ASOF JOIN \"SALES\" b MATCH_CONDITION b.\"ID\" <= a.\"ID\" ON a.\"REGION\" = b.\"REGION\"");

        [Fact]
        public void ShouldAgreeOnAnAsofJoinOnASeveralFieldKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a ASOF JOIN \"SALES\" b MATCH_CONDITION b.\"ID\" <= a.\"ID\" ON a.\"REGION\" = b.\"REGION\" AND a.\"LABEL\" = b.\"LABEL\" ORDER BY a.\"ID\"");

        [Fact]
        public void ShouldAgreeOnALeftAsofJoinWithANullKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT ASOF JOIN \"SALES\" b MATCH_CONDITION b.\"ID\" <= a.\"ID\" ON a.\"AMOUNT\" = b.\"AMOUNT\" ORDER BY a.\"ID\"");

        // A right and a full join with no ORDER BY: the rows of the right input that matched nothing come out
        // at the end, in the order of the lookup the join built.

        [Fact]
        public void ShouldAgreeOnARightJoinsOwnOrder() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM (SELECT * FROM \"SALES\" WHERE \"ID\" < 3) a RIGHT JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"LABEL\" = b.\"LABEL\"");

        /// <summary>
        /// The same order at a build-side size where it depends on which collection the unmatched rows are read
        /// from.
        /// </summary>
        /// <remarks>
        /// <c>WIDE</c> has twelve keys, so the lookup has 16 buckets and the <c>HashSet</c> copied from its key
        /// set has 32, and their orders differ. <see cref="ShouldAgreeOnARightJoinsOwnOrder"/> is over six keys,
        /// where both have 16.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnARightJoinsOwnOrderOverTwelveKeys() =>
            SameThrough("ClrCursorHashJoin", "SELECT a.\"N\", b.\"K\" FROM (SELECT * FROM \"WIDE\" WHERE \"N\" < 3) a RIGHT JOIN \"WIDE\" b ON a.\"K\" = b.\"K\"");

        [Fact]
        public void ShouldAgreeOnAFullJoinsOwnOrderOverTwelveKeys() =>
            SameThrough("ClrCursorHashJoin", "SELECT a.\"N\", b.\"K\" FROM (SELECT * FROM \"WIDE\" WHERE \"N\" < 3) a FULL JOIN \"WIDE\" b ON a.\"K\" = b.\"K\"");

        [Fact]
        public void ShouldAgreeOnAFullJoinsOwnOrder() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM (SELECT * FROM \"SALES\" WHERE \"ID\" < 3) a FULL JOIN (SELECT * FROM \"SALES\" WHERE \"ID\" > 1) b ON a.\"LABEL\" = b.\"LABEL\"");

        // Set operations with no ORDER BY: as for the GROUP BY above, the rows come out in the order of the
        // collection the operator holds them in, which in Calcite is a java.util.HashSet or a HashMultiset.

        [Fact]
        public void ShouldAgreeOnUnionsOwnOrder() => Same("SELECT \"REGION\" FROM \"SALES\" UNION SELECT \"LABEL\" FROM \"SALES\"");

        [Fact]
        public void ShouldAgreeOnIntersectsOwnOrder() => Same("SELECT \"LABEL\" FROM \"SALES\" INTERSECT SELECT \"LABEL\" FROM \"SALES\" WHERE \"ID\" < 5");

        [Fact]
        public void ShouldAgreeOnIntersectAllsOwnOrder() => Same("SELECT \"REGION\" FROM \"SALES\" INTERSECT ALL SELECT \"REGION\" FROM \"SALES\" WHERE \"ID\" < 5");

        [Fact]
        public void ShouldAgreeOnExceptsOwnOrder() => Same("SELECT \"LABEL\" FROM \"SALES\" EXCEPT SELECT \"LABEL\" FROM \"SALES\" WHERE \"ID\" > 4");

        [Fact]
        public void ShouldAgreeOnExceptAllsOwnOrder() => Same("SELECT \"REGION\" FROM \"SALES\" EXCEPT ALL SELECT \"REGION\" FROM \"SALES\" WHERE \"ID\" > 4");

        [Fact]
        public void ShouldAgreeOnDistinctsOwnOrder() => Same("SELECT DISTINCT \"REGION\" FROM \"SALES\"");

        [Fact]
        public void ShouldAgreeOnUnionAll() => Same("SELECT \"ID\" FROM \"SALES\" UNION ALL SELECT \"ID\" FROM \"SALES\" ORDER BY 1");

        /// <summary>
        /// A union of many inputs is implemented as one concat over all of them, not a pairwise fold.
        /// </summary>
        /// <remarks>
        /// A pairwise fold would nest each step inside both opens of the next, doubling the compiled plan with
        /// every input, so twenty-four inputs would exhaust memory. An <c>IN</c> list of many dynamic parameters
        /// becomes such a union.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnAUnionAllOfManyInputs() =>
            Same(string.Join(" UNION ALL ", System.Linq.Enumerable.Range(1, 24).Select(i => $"SELECT \"ID\" FROM \"SALES\" WHERE \"ID\" = {i % 6}")) + " ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnUnionDistinct() => Same("SELECT \"REGION\" FROM \"SALES\" UNION SELECT \"REGION\" FROM \"SALES\" ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnIntersect() => Same("SELECT \"REGION\" FROM \"SALES\" INTERSECT SELECT \"REGION\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnExcept() => Same("SELECT \"REGION\" FROM \"SALES\" EXCEPT SELECT \"REGION\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnLimitAndOffset() => Same("SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY");

        [Fact]
        public void ShouldAgreeOnALimitSort() =>
            SameLimitSort("SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" FETCH NEXT 3 ROWS ONLY");

        [Fact]
        public void ShouldAgreeOnALimitSortWithAnOffset() =>
            SameLimitSort("SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY");

        [Fact]
        public void ShouldAgreeOnALimitSortWithAnOffsetAndNoFetch() =>
            SameLimitSort("SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" OFFSET 4 ROWS");

        [Fact]
        public void ShouldAgreeOnALimitSortOverANullableKey() =>
            SameLimitSort("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" FETCH NEXT 4 ROWS ONLY");

        [Fact]
        public void ShouldAgreeOnALimitSortPastTheEnd() =>
            SameLimitSort("SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" DESC OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY");

        /// <summary>
        /// A FETCH and an OFFSET wider than an <c>int</c>.
        /// </summary>
        /// <remarks>
        /// Calcite reads the counts as <c>BigDecimal</c> rather than <c>int</c>, so both conventions return rows
        /// rather than an overflow error.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnALimitWiderThanAnInt() =>
            Same("SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" OFFSET 2500000000 ROWS FETCH NEXT 3000000000 ROWS ONLY");

        /// <summary>
        /// A FETCH wider than an <c>int</c>, over the bounded sort.
        /// </summary>
        /// <remarks>
        /// The limit sort adds the offset to the fetch to bound its map, where a count too wide for an
        /// <c>int</c> could be mistakenly narrowed.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnALimitSortWiderThanAnInt() =>
            SameLimitSort("SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" OFFSET 2 ROWS FETCH NEXT 3000000000 ROWS ONLY");

        /// <summary>
        /// A FETCH and an OFFSET that are not whole numbers.
        /// </summary>
        /// <remarks>
        /// A count is a <c>BigDecimal</c>, and <c>rowsRequired</c> rounds it up to the row it reaches into rather
        /// than truncating: OFFSET 1.5 skips two rows and FETCH 2.5 takes three. The rows are also compared with
        /// the whole-number query, because both conventions would agree on a truncating answer too.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnAFractionalLimit()
        {
            const string fractional = "SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" OFFSET 1.5 ROWS FETCH NEXT 2.5 ROWS ONLY";
            const string rounded = "SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY";

            SameLimitSort(fractional);

            var rows = Run(fractional, true, limitSort: true);
            rows.Should().HaveCount(3);
            rows.Should().Equal(Run(rounded, true, limitSort: true));
        }

        /// <summary>
        /// Both conventions plan a limit sort for the queries above, rather than one of them planning a limit
        /// over a sort.
        /// </summary>
        /// <remarks>
        /// The limit sort tests above compare rows only; this checks that both sides actually plan a limit sort,
        /// so that the node is compared with Calcite's.
        /// </remarks>
        [Fact]
        public void ShouldPlanALimitSortInBothConventions()
        {
            const string sql = "SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY";

            PlanOf(sql, true, limitSort: true).Should().Contain("ClrCursorLimitSort");
            PlanOf(sql, false, limitSort: true).Should().Contain("EnumerableLimitSort");
        }

        // The limit sort's edge cases. linq4j keeps at most offset + fetch rows and evicts as it reads, so
        // eviction, ties and an offset past the end are where a bounded sort can go wrong without an ordinary
        // query showing it.

        [Fact]
        public void ShouldAgreeOnALimitSortWithTiesAcrossTheBoundary() =>
            SameThrough("ClrCursorLimitSort", "SELECT \"K\", \"V\" FROM \"SORTED\" ORDER BY \"K\" FETCH NEXT 2 ROWS ONLY", limitSort: true);

        // descending, because SORTED is already ascending by K, and with an offset Calcite then plans a bare
        // limit over the scan rather than a limit sort
        [Fact]
        public void ShouldAgreeOnALimitSortWithAnOffsetInsideATie() =>
            SameThrough("ClrCursorLimitSort", "SELECT \"K\", \"V\" FROM \"SORTED\" ORDER BY \"K\" DESC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY", limitSort: true);

        /// <summary>
        /// A limit sort whose offset is past the last row gives no rows on either side.
        /// </summary>
        /// <remarks>
        /// No node assertion: an offset past the end lets the planner prune the plan, so there is no limit sort
        /// to find. Both sides must return no rows.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnALimitSortWithAnOffsetPastTheEnd() =>
            Same("SELECT \"K\" FROM \"SORTED\" ORDER BY \"K\" OFFSET 10 ROWS FETCH NEXT 2 ROWS ONLY", limitSort: true);

        [Fact]
        public void ShouldAgreeOnALimitSortTakingEverything() =>
            SameThrough("ClrCursorLimitSort", "SELECT \"K\" FROM \"SORTED\" ORDER BY \"K\" FETCH NEXT 100 ROWS ONLY", limitSort: true);

        [Fact]
        public void ShouldAgreeOnALimitSortOverNulls() =>
            SameThrough("ClrCursorLimitSort", "SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" OFFSET 1 ROWS FETCH NEXT 3 ROWS ONLY", limitSort: true);

        [Fact]
        public void ShouldAgreeOnALimitSortDescending() =>
            SameThrough("ClrCursorLimitSort", "SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" DESC OFFSET 1 ROWS FETCH NEXT 3 ROWS ONLY", limitSort: true);

        /// <summary>
        /// Without the limit sort rule, this convention plans a limit over a sort rather than a limit sort.
        /// </summary>
        [Fact]
        public void ShouldPlanALimitOverASortWithoutTheRule() =>
            PlanOf("SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY", true)
                .Should().NotContain("LimitSort");

        [Fact]
        public void ShouldAgreeOnAMarkJoinFromExists() =>
            SameMarkJoin("SELECT \"ID\" FROM \"SALES\" WHERE EXISTS (SELECT 1 FROM \"SALES\" \"S2\" WHERE \"S2\".\"ID\" > 4) ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnAMarkJoinFromAnEmptyExists() =>
            SameMarkJoin("SELECT \"ID\" FROM \"SALES\" WHERE EXISTS (SELECT 1 FROM \"SALES\" \"S2\" WHERE \"S2\".\"ID\" > 99) ORDER BY \"ID\"");

        [Fact]
        public void ShouldPlanANestedLoopMarkJoin() =>
            PlanOf("SELECT \"ID\" FROM \"SALES\" WHERE EXISTS (SELECT 1 FROM \"SALES\" \"S2\" WHERE \"S2\".\"ID\" > 4)", true, markJoin: true)
                .Should().Contain("ClrCursorNestedLoopJoin").And.Contain("left_mark");

        [Fact]
        public void ShouldAgreeOnAMarkJoinFromIn() =>
            SameMarkJoin("SELECT \"ID\" FROM \"SALES\" WHERE \"AMOUNT\" IN (SELECT \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" > 3) ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnAMarkJoinFromInProjected() =>
            SameMarkJoin("SELECT \"ID\", \"AMOUNT\" IN (SELECT \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" > 3) AS \"M\" FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnAMarkJoinOverANullKey() =>
            SameMarkJoin("SELECT \"ID\", \"AMOUNT\" IN (SELECT \"AMOUNT\" FROM \"SALES\") AS \"M\" FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnAMarkJoinOverAnEmptyRight() =>
            SameMarkJoin("SELECT \"ID\", \"AMOUNT\" IN (SELECT \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" > 99) AS \"M\" FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnANotInMarkJoin() =>
            SameMarkJoin("SELECT \"ID\" FROM \"SALES\" WHERE \"AMOUNT\" NOT IN (SELECT \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" > 3) ORDER BY \"ID\"");

        [Fact]
        public void ShouldPlanAHashMarkJoin() =>
            PlanOf("SELECT \"ID\" FROM \"SALES\" WHERE \"AMOUNT\" IN (SELECT \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" > 3)", true, markJoin: true)
                .Should().Contain("ClrCursorHashJoin").And.Contain("left_mark");

        [Fact]
        public void ShouldAgreeOnACorrelatedMarkJoinFromExists() =>
            SameMarkJoin("SELECT \"ID\" FROM \"SALES\" \"S1\" WHERE EXISTS (SELECT 1 FROM \"SALES\" \"S2\" WHERE \"S2\".\"REGION\" = \"S1\".\"REGION\" AND \"S2\".\"ID\" > 3) ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnACorrelatedMarkJoinFromIn() =>
            SameMarkJoin("SELECT \"ID\" FROM \"SALES\" \"S1\" WHERE \"AMOUNT\" IN (SELECT \"AMOUNT\" FROM \"SALES\" \"S2\" WHERE \"S2\".\"REGION\" = \"S1\".\"REGION\") ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnACorrelatedMarkJoinProjected() =>
            SameMarkJoin("SELECT \"ID\", EXISTS (SELECT 1 FROM \"SALES\" \"S2\" WHERE \"S2\".\"REGION\" = \"S1\".\"REGION\" AND \"S2\".\"ID\" > 3) AS \"E\" FROM \"SALES\" \"S1\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldPlanAConditionalCorrelate() =>
            PlanOf("SELECT \"ID\" FROM \"SALES\" \"S1\" WHERE EXISTS (SELECT 1 FROM \"SALES\" \"S2\" WHERE \"S2\".\"REGION\" = \"S1\".\"REGION\" AND \"S2\".\"ID\" > 3)", true, markJoin: true)
                .Should().Contain("ClrCursorConditionalCorrelate").And.Contain("left_mark");

        [Fact]
        public void ShouldAgreeOnValues() => Same("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y)");

        // Recursive queries, which reach ClrCursorRepeatUnion and ClrCursorTableSpool. Neither convention's
        // table scan reads a TransientTable, so both sides read it through the interpreter.

        [Fact]
        public void ShouldAgreeOnARecursiveQuery() =>
            SameThrough("ClrCursorRepeatUnion", "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 4) SELECT n FROM t ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnARecursiveQueryOfSeveralColumns() =>
            SameThrough("ClrCursorTableSpool", "WITH RECURSIVE t(n, m) AS (VALUES (1, 10) UNION ALL SELECT n + 1, m + 10 FROM t WHERE n < 4) SELECT n, m FROM t ORDER BY 1");

        // The interpreter, through which either convention reads a transient table. Without this convention's
        // interpreter rule the node is Calcite's under a converter; with it the node is this convention's and
        // there is no converter. The first two tests require the same rows either way.

        [Fact]
        public void ShouldAgreeOnARecursiveQueryInterpretedHere() =>
            SameInterpreted("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 4) SELECT n FROM t ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnARecursiveQueryOfSeveralColumnsInterpretedHere() =>
            SameInterpreted("WITH RECURSIVE t(n, m) AS (VALUES (1, 10) UNION ALL SELECT n + 1, m + 10 FROM t WHERE n < 4) SELECT n, m FROM t ORDER BY 1");

        [Fact]
        public void ShouldPlanTheInterpreterInThisConvention()
        {
            var sql = "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 4) SELECT n FROM t ORDER BY 1";

            PlanOf(sql, true).Should().Contain("EnumerableInterpreter").And.NotContain("ClrCursorInterpreter");
            PlanOf(sql, true, interpreter: true).Should().Contain("ClrCursorInterpreter");
        }

        [Fact]
        public void ShouldAgreeOnACorrelatedSubQuery() => Same("SELECT \"ID\" FROM \"SALES\" a WHERE \"AMOUNT\" = (SELECT MAX(\"AMOUNT\") FROM \"SALES\" b WHERE b.\"REGION\" = a.\"REGION\") ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnAScalarSubQuery() => Same("SELECT \"ID\", (SELECT COUNT(*) FROM \"SALES\") FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnCaseAndNullHandling() => Same("SELECT \"ID\", CASE WHEN \"AMOUNT\" IS NULL THEN -1 ELSE \"AMOUNT\" END FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnRowNumber() => Same("SELECT \"ID\", ROW_NUMBER() OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnRankOverTies() => Same("SELECT \"ID\", RANK() OVER (ORDER BY \"AMOUNT\"), DENSE_RANK() OVER (ORDER BY \"AMOUNT\") FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnAPartitionedWindow() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (PARTITION BY \"REGION\") FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnARunningTotal() => SameThrough("ClrCursorWindow", "SELECT \"ID\", SUM(\"AMOUNT\") OVER (PARTITION BY \"REGION\" ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnARowsFrame() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\" ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnARangeFrame() => Same("SELECT \"ID\", COUNT(*) OVER (ORDER BY \"AMOUNT\" RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnSeveralWindows() => Same("SELECT \"ID\", ROW_NUMBER() OVER (ORDER BY \"ID\"), SUM(\"AMOUNT\") OVER (PARTITION BY \"REGION\") FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnAWindowOverEverything() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER () FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnSeveralPartitionKeys() => Same("SELECT \"ID\", COUNT(*) OVER (PARTITION BY \"REGION\", \"AMOUNT\") FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnANullPartitionKey() => Same("SELECT \"ID\", COUNT(*) OVER (PARTITION BY \"AMOUNT\") FROM \"SALES\" ORDER BY \"ID\"");

        /// <summary>
        /// FIRST_VALUE and LAST_VALUE carrying IGNORE NULLS.
        /// </summary>
        /// <remarks>
        /// IGNORE NULLS is supported for these two functions only. The implementor Calcite supplies reads the
        /// flag through <c>WinAggContext.ignoreNulls</c>.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnFirstValueIgnoringNulls() =>
            Same("SELECT \"ID\", \"AMOUNT\", FIRST_VALUE(\"AMOUNT\") IGNORE NULLS OVER (ORDER BY \"ID\" ROWS 2 PRECEDING) FROM \"SALES\" ORDER BY \"ID\"");

        /// <inheritdoc cref="ShouldAgreeOnFirstValueIgnoringNulls" />
        [Fact]
        public void ShouldAgreeOnLastValueIgnoringNulls() =>
            Same("SELECT \"ID\", \"AMOUNT\", LAST_VALUE(\"AMOUNT\") IGNORE NULLS OVER (ORDER BY \"ID\" ROWS 2 PRECEDING) FROM \"SALES\" ORDER BY \"ID\"");

        /// <inheritdoc cref="ShouldAgreeOnFirstValueIgnoringNulls" />
        [Fact]
        public void ShouldAgreeOnFirstValueRespectingNulls() =>
            Same("SELECT \"ID\", \"AMOUNT\", FIRST_VALUE(\"AMOUNT\") RESPECT NULLS OVER (ORDER BY \"ID\" ROWS 2 PRECEDING) FROM \"SALES\" ORDER BY \"ID\"");

        /// <summary>
        /// A window aggregate carrying a FILTER.
        /// </summary>
        /// <remarks>
        /// <c>SqlToRelConverter</c> turns the FILTER into a <c>CASE</c> in the calc below the window, so the
        /// window's <c>AggregateCall</c> carries no filter argument and <c>WinAggAddContext.rexFilterArgument</c>
        /// is not exercised. These check that both conventions answer the query alike.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnAFilteredWindowCount() =>
            Same("SELECT \"ID\", COUNT(*) FILTER (WHERE \"AMOUNT\" > 15) OVER (PARTITION BY \"REGION\") FROM \"SALES\" ORDER BY \"ID\"");

        /// <inheritdoc cref="ShouldAgreeOnAFilteredWindowCount" />
        [Fact]
        public void ShouldAgreeOnAFilteredWindowSum() =>
            Same("SELECT \"ID\", SUM(\"AMOUNT\") FILTER (WHERE \"AMOUNT\" IS NOT NULL) OVER (PARTITION BY \"REGION\") FROM \"SALES\" ORDER BY \"ID\"");

        /// <inheritdoc cref="ShouldAgreeOnAFilteredWindowCount" />
        [Fact]
        public void ShouldAgreeOnTwoFilteredWindowAggregates() =>
            Same("SELECT \"ID\", COUNT(*) FILTER (WHERE \"AMOUNT\" > 15) OVER (PARTITION BY \"REGION\"), SUM(\"AMOUNT\") FILTER (WHERE \"AMOUNT\" <= 15) OVER (PARTITION BY \"REGION\") FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnAnEmptyFrame() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\" ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING) FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnARangeFrameWithAnOffset() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\" RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) FROM \"SALES\" ORDER BY \"ID\"");

        /// <summary>
        /// A RANGE bound with an offset over a nullable order key fails, here as in Calcite.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableWindow.translateBound</c> boxes the key type only where the bound has no offset --
        /// <c>if (bound.getOffset() == null) desiredKeyType = Primitive.box(desiredKeyType)</c> -- so with an
        /// offset the key stays whatever the type factory gave, which for a nullable column is
        /// <c>java.lang.Integer</c>. The <c>subtract</c> built on it then unboxes, and a null key is a
        /// <c>NullPointerException</c>.
        ///
        /// <para>This convention uses the same translation and fails the same way: IKVM maps the Java exception
        /// onto <see cref="NullReferenceException"/>, which is also what the expression tree's unboxing raises.
        /// Both sides are asserted, so that if Calcite changes this the test fails and this convention should
        /// follow.</para>
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnFailingARangeFrameWithAnOffsetOverANullableKey()
        {
            const string sql = "SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"AMOUNT\" RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) FROM \"SALES\" ORDER BY \"ID\"";

            var calcite = () => Run(sql, false);
            var mine = () => Run(sql, true);

            calcite.Should().Throw<NullReferenceException>("Calcite unboxes a null order key");
            mine.Should().Throw<NullReferenceException>("and so do we, from the same translation");
        }

        [Fact]
        public void ShouldAgreeOnLeadAndLag() => Same("SELECT \"ID\", LAG(\"AMOUNT\") OVER (ORDER BY \"ID\"), LEAD(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnFirstAndLastValue() => Same("SELECT \"ID\", FIRST_VALUE(\"AMOUNT\") OVER (PARTITION BY \"REGION\" ORDER BY \"ID\"), LAST_VALUE(\"AMOUNT\") OVER (PARTITION BY \"REGION\" ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // a running frame rather than the whole partition, because it distinguishes the three exclusions;
        // over an unbounded frame Calcite excludes nothing after the first row (see below)
        [Fact]
        public void ShouldAgreeOnExcludingTheCurrentRow() => Same("SELECT \"ID\", COUNT(\"AMOUNT\") OVER (ORDER BY \"AMOUNT\" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnExcludingTies() => Same("SELECT \"ID\", COUNT(\"AMOUNT\") OVER (ORDER BY \"AMOUNT\" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnExcludingTheGroup() => Same("SELECT \"ID\", COUNT(\"AMOUNT\") OVER (ORDER BY \"AMOUNT\" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) FROM \"SALES\" ORDER BY \"ID\"");

        /// <summary>
        /// An EXCLUDE over a frame whose bounds never move.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableWindow</c> recomputes a frame only when its bounds move, and the exclusion is not part of
        /// that test, so over UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING the frame is computed for a partition's
        /// first row only. The exclusion depends on the current row, so after row 0 nothing is excluded and every
        /// row but the first counts the whole partition. <c>ClrCursorDefaults.Window</c> reproduces this Calcite
        /// defect; the expected answer is Calcite's, not SQL's.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnExcludingTheCurrentRowOverAnUnboundedFrame() => Same("SELECT \"ID\", COUNT(\"AMOUNT\") OVER (PARTITION BY \"REGION\" ORDER BY \"AMOUNT\" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnNtile() => Same("SELECT \"ID\", NTILE(2) OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnNthValue() => Same("SELECT \"ID\", NTH_VALUE(\"AMOUNT\", 2) OVER (PARTITION BY \"REGION\" ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // one aggregate whose value survives an intact frame and one that does not, in the same window, so
        // both result lambdas run over the same accumulator
        [Fact]
        public void ShouldAgreeOnACachedAndAnUncachedAggregateTogether() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\"), LAG(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // a RANGE frame ending at the current row with more than one ordering key, which is the only shape
        // that reaches the five-argument binary search
        [Fact]
        public void ShouldAgreeOnARangeFrameOverSeveralOrderKeys() => Same("SELECT \"ID\", COUNT(*) OVER (ORDER BY \"REGION\", \"AMOUNT\" RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM \"SALES\" ORDER BY \"ID\"");

        // no ORDER BY, so the rows come in partition order, which is a hash map's. Under IKVM a String hashes
        // as .NET does, randomised per process, so the order varies between runs; it can be compared only
        // because both conventions use the same kind of map in the same process.
        [Fact]
        public void ShouldAgreeOnThePartitionOrder() => Same("SELECT \"REGION\", \"ID\", COUNT(*) OVER (PARTITION BY \"REGION\") FROM \"SALES\"");

        // a primitive key, which must be boxed as the type factory says before a map holds it
        [Fact]
        public void ShouldAgreeOnAPrimitivePartitionKey() => Same("SELECT \"ID\", COUNT(*) OVER (PARTITION BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // two calls on one implementor instance, which keeps state of its own between getStateType and
        // implementAdd: COUNT(*) takes the frame's row count and COUNT of a nullable column accumulates
        [Fact]
        public void ShouldAgreeOnTwoCountsInOneWindow() => Same("SELECT \"ID\", COUNT(*) OVER (ORDER BY \"ID\"), COUNT(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnAFrameEntirelyFollowing() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\" ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnMinAndMax() => Same("SELECT \"ID\", MIN(\"AMOUNT\") OVER (PARTITION BY \"REGION\"), MAX(\"AMOUNT\") OVER (PARTITION BY \"REGION\") FROM \"SALES\" ORDER BY \"ID\"");

        // AVG has no implementor, so this reaches a window only after it is rewritten in terms of SUM and COUNT
        [Fact]
        public void ShouldAgreeOnAnAverageOverAWindow() => Same("SELECT \"ID\", AVG(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnAWindowOverNoRows() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (PARTITION BY \"REGION\") FROM \"SALES\" WHERE \"ID\" < 0 ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnAWindowOverAFilteredInput() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" WHERE \"REGION\" = 'EAST' ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnRowNumberWithoutAnOrder() => Same("SELECT \"ID\", ROW_NUMBER() OVER (PARTITION BY \"REGION\") FROM \"SALES\" ORDER BY \"ID\"");

        // an offset and a default, so the window carries more than one constant past its input's own fields
        [Fact]
        public void ShouldAgreeOnLagWithAnOffsetAndDefault() => Same("SELECT \"ID\", LAG(\"AMOUNT\", 2, -1) OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // A user-defined aggregate written in C#. EnumerableConvention writes its IKVM name
        // (cli.Apache.Calcite.Tests.SumAggregate) into generated Java source, which Janino resolves through
        // the class loader IKVM.Maven.Sdk stamps on calcite-core; this requires IKVM 8.16.0 or later. The
        // hand-written rows are asserted as well, as SQL's answer independent of either convention.

        // running sum over ORDER BY ID, which the default RANGE frame makes a prefix, with the null skipped
        [Fact]
        public void ShouldRunAUserDefinedWindowAggregate()
        {
            const string sql = "SELECT \"ID\", MY_SUM(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"";

            Same(sql);
            Gives(sql, "1|10", "2|30", "3|50", "4|80", "5|80", "6|85");
        }

        // EAST is 10 + 20 + 20 and WEST is 30 + 5, the null contributing nothing
        [Fact]
        public void ShouldRunAUserDefinedAggregate()
        {
            const string sql = "SELECT \"REGION\", MY_SUM(\"AMOUNT\") FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"";

            Same(sql);
            Gives(sql, "EAST|50", "WEST|35");
        }

        // SORTED declares a collation, so both conventions may plan these with a merge join where the same
        // query over SALES gets a hash join.
        [Fact]
        public void ShouldAgreeOnAJoinOverSortedInputs() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" JOIN \"SORTED\" \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" ORDER BY 1, 2");

        [Fact]
        public void ShouldAgreeOnALeftJoinOverSortedInputs() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" LEFT JOIN \"SORTED\" \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" AND \"S2\".\"V\" <> 'B' ORDER BY 1, 2");

        // A sorted aggregate, whose rule must be added, is chosen where the output is ordered by the group key
        // over an input with that collation. This convention's rule refuses a global aggregate: Calcite builds
        // the node for one and then cannot implement it, because the collation that separates groups is empty.

        [Fact]
        public void ShouldAgreeOnASortedAggregate() =>
            SameSortedAggregate("SELECT \"K\", COUNT(*) FROM \"SORTED\" GROUP BY \"K\" ORDER BY \"K\"");

        [Fact]
        public void ShouldAgreeOnASortedAggregateOfSeveralCalls() =>
            SameSortedAggregate("SELECT \"K\", COUNT(*), MIN(\"V\"), MAX(\"V\") FROM \"SORTED\" GROUP BY \"K\" ORDER BY \"K\"");

        [Fact]
        public void ShouldAgreeOnASortedAggregateOverAFilteredInput() =>
            SameSortedAggregate("SELECT \"K\", COUNT(*) FROM \"SORTED\" WHERE \"V\" <> 'C' GROUP BY \"K\" ORDER BY \"K\"");

        [Fact]
        public void ShouldAgreeOnAGlobalAggregateWithTheSortedRuleOn() =>
            SameSortedAggregate("SELECT COUNT(*), MIN(\"V\") FROM \"SORTED\"");

        [Fact]
        public void ShouldAgreeOnAnUnorderedGroupByWithTheSortedRuleOn() =>
            SameSortedAggregate("SELECT \"K\", COUNT(*) FROM \"SORTED\" GROUP BY \"K\"");

        [Fact]
        public void ShouldAgreeOnAGroupByOverASortedInput() =>
            Same("SELECT \"K\", COUNT(*) FROM \"SORTED\" GROUP BY \"K\" ORDER BY 1");

        // A merge union: an ORDER BY directly over a UNION, the shape its rule requires. Naming the columns
        // instead of SELECT * puts a projection between the two and the rule does not fire.

        [Fact]
        public void ShouldAgreeOnAMergeUnionAll() =>
            Same("SELECT * FROM \"SORTED\" UNION ALL SELECT * FROM \"SORTED\" ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnAMergeUnionDistinct() =>
            Same("SELECT * FROM \"SORTED\" UNION SELECT * FROM \"SORTED\" ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnAMergeUnionWithALimit() =>
            Same("SELECT * FROM \"SORTED\" UNION ALL SELECT * FROM \"SORTED\" ORDER BY 1 FETCH FIRST 3 ROWS ONLY");

        [Fact]
        public void ShouldAgreeOnAMergeUnionWithAnOffsetAndALimit() =>
            Same("SELECT * FROM \"SORTED\" UNION ALL SELECT * FROM \"SORTED\" ORDER BY 1 OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY");

        [Fact]
        public void ShouldAgreeOnAMergeUnionOfThreeInputs() =>
            Same("SELECT * FROM \"SORTED\" UNION ALL SELECT * FROM \"SORTED\" UNION ALL SELECT * FROM \"SORTED\" ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnAUnionOverSortedInputs() =>
            Same("SELECT \"K\", \"V\" FROM \"SORTED\" UNION SELECT \"K\", \"V\" FROM \"SORTED\" ORDER BY 1, 2");

        // Merge joins over inputs that declare a collation, covering the join types and the paths the algorithm
        // treats separately: a run of equal keys on both sides, a key missing from one side, several keys, an
        // extra condition that is not an equality, and a null key, which the comparator never matches.

        [Fact]
        public void ShouldAgreeOnASemiJoinOverSortedInputs() =>
            Same("SELECT \"K\", \"V\" FROM \"SORTED\" \"S1\" WHERE EXISTS (SELECT 1 FROM \"SORTED\" \"S2\" WHERE \"S2\".\"K\" = \"S1\".\"K\") ORDER BY 1, 2");

        [Fact]
        public void ShouldAgreeOnAnAntiJoinOverSortedInputs() =>
            Same("SELECT \"K\", \"V\" FROM \"SORTED\" \"S1\" WHERE NOT EXISTS (SELECT 1 FROM \"SORTED\" \"S2\" WHERE \"S2\".\"K\" = \"S1\".\"K\" AND \"S2\".\"V\" = 'A') ORDER BY 1, 2");

        [Fact]
        public void ShouldAgreeOnAJoinOverSortedInputsMissingKeys() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" JOIN (SELECT * FROM \"SORTED\" WHERE \"K\" <> 2) \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" ORDER BY 1, 2");

        [Fact]
        public void ShouldAgreeOnALeftJoinOverSortedInputsMissingKeys() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" LEFT JOIN (SELECT * FROM \"SORTED\" WHERE \"K\" > 2) \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" ORDER BY 1, 2");

        [Fact]
        public void ShouldAgreeOnAJoinOverSortedInputsOnSeveralKeys() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" JOIN \"SORTED\" \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" AND \"S1\".\"V\" = \"S2\".\"V\" ORDER BY 1, 2");

        [Fact]
        public void ShouldAgreeOnAJoinOverSortedInputsWithAnExtraCondition() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" JOIN \"SORTED\" \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" AND \"S1\".\"V\" < \"S2\".\"V\" ORDER BY 1, 2");

        [Fact]
        public void ShouldAgreeOnAJoinOnANullableKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" = b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [Fact]
        public void ShouldAgreeOnALeftJoinOnANullableKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT JOIN \"SALES\" b ON a.\"AMOUNT\" = b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        // A null key on the build side of a hash join. INNER and LEFT joins never read the unmatched right
        // rows, and a plain equality never matches a null; ShouldAgreeOnARightJoinsOwnOrder and
        // ShouldAgreeOnAFullJoinsOwnOrder read them but join on columns that are not nullable. These cover a
        // nullable key together with an outer join on the build side, and a null-safe key.

        [Fact]
        public void ShouldAgreeOnANullSafeJoinKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" IS NOT DISTINCT FROM b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [Fact]
        public void ShouldAgreeOnARightJoinOnANullableKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM (SELECT * FROM \"SALES\" WHERE \"ID\" < 3) a RIGHT JOIN \"SALES\" b ON a.\"AMOUNT\" = b.\"AMOUNT\"");

        [Fact]
        public void ShouldAgreeOnAFullJoinOnANullableKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM (SELECT * FROM \"SALES\" WHERE \"ID\" < 3) a FULL JOIN \"SALES\" b ON a.\"AMOUNT\" = b.\"AMOUNT\"");

        // A hash join on a two-field key with one nullable field: the null-aware accessor makes the whole key
        // null, where a plain accessor would build a list holding a null that matches another such list. Both
        // conventions plan a merge join for this query, so the merge join rule is removed from both sides.

        [Fact]
        public void ShouldAgreeOnAHashJoinOnTwoKeysOneNullable() =>
            SameHashJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"AMOUNT\" = b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [Fact]
        public void ShouldAgreeOnARightHashJoinOnTwoKeysOneNullable() =>
            SameHashJoin("SELECT a.\"ID\", b.\"ID\" FROM (SELECT * FROM \"SALES\" WHERE \"ID\" < 3) a RIGHT JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"AMOUNT\" = b.\"AMOUNT\"");

        [Fact]
        public void ShouldPlanAHashJoinWithoutTheMergeJoinRule()
        {
            var sql = "SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"AMOUNT\" = b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"";

            PlanOf(sql, false, excludeMergeJoin: true).Should().Contain("EnumerableHashJoin");
            PlanOf(sql, true, excludeMergeJoin: true).Should().Contain("ClrCursorHashJoin");
        }

        [Fact]
        public void ShouldAgreeOnASemiJoinOnANullSafeKey() =>
            Same("SELECT a.\"ID\" FROM \"SALES\" a WHERE EXISTS (SELECT 1 FROM \"SALES\" b WHERE a.\"AMOUNT\" IS NOT DISTINCT FROM b.\"AMOUNT\") ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnAnAntiJoinOnANullSafeKey() =>
            Same("SELECT a.\"ID\" FROM \"SALES\" a WHERE NOT EXISTS (SELECT 1 FROM \"SALES\" b WHERE a.\"AMOUNT\" IS NOT DISTINCT FROM b.\"AMOUNT\") ORDER BY 1");

        // The CUSTOM row format, over HR.emps. The tests above run over Object[] rows, so these take the CUSTOM
        // branches of the physical type.

        [Fact]
        public void ShouldAgreeOnACustomFormatScan() =>
            Same("SELECT \"empid\", \"name\" FROM \"HR\".\"emps\" ORDER BY \"empid\"");

        [Fact]
        public void ShouldAgreeOnACustomFormatFilterAndProjection() =>
            Same("SELECT \"empid\", \"salary\" + 1 FROM \"HR\".\"emps\" WHERE \"deptno\" = 10 ORDER BY \"empid\"");

        [Fact]
        public void ShouldAgreeOnACustomFormatNullableColumn() =>
            Same("SELECT \"empid\", \"commission\" FROM \"HR\".\"emps\" ORDER BY \"empid\"");

        [Fact]
        public void ShouldAgreeOnACustomFormatAggregate() =>
            Same("SELECT \"deptno\", COUNT(*), SUM(\"salary\"), MIN(\"commission\") FROM \"HR\".\"emps\" GROUP BY \"deptno\" ORDER BY \"deptno\"");

        [Fact]
        public void ShouldAgreeOnACustomFormatJoin() =>
            Same("SELECT a.\"empid\", b.\"empid\" FROM \"HR\".\"emps\" a JOIN \"HR\".\"emps\" b ON a.\"deptno\" = b.\"deptno\" ORDER BY a.\"empid\", b.\"empid\"");

        [Fact]
        public void ShouldAgreeOnACustomFormatJoinOnANullableKey() =>
            Same("SELECT a.\"empid\", b.\"empid\" FROM \"HR\".\"emps\" a JOIN \"HR\".\"emps\" b ON a.\"commission\" = b.\"commission\" ORDER BY a.\"empid\", b.\"empid\"");

        [Fact]
        public void ShouldAgreeOnACustomFormatWindow() =>
            Same("SELECT \"empid\", SUM(\"salary\") OVER (PARTITION BY \"deptno\" ORDER BY \"empid\") FROM \"HR\".\"emps\" ORDER BY \"empid\"");

        [Fact]
        public void ShouldAgreeOnACustomFormatDistinct() =>
            Same("SELECT DISTINCT \"deptno\" FROM \"HR\".\"emps\" ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnStringFunctions() => Same("SELECT UPPER(\"LABEL\") || '-' || LOWER(\"REGION\") FROM \"SALES\" ORDER BY 1");

        // Planned top down, the only mode that calls passThroughTraits, deriveTraits and getDeriveMode. These
        // cover each node whose trait derivation differs from the default: a project and a calc (permutation
        // and cast), a filter, a hash join, a nested loop join, a correlate, a scan and a VALUES, each with a
        // collation to push down or derive.

        [Fact]
        public void ShouldAgreeOnAProjectionSortedTopDown() =>
            SameTopDown("SELECT \"REGION\", \"ID\" FROM \"SALES\" ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnACastInASortedProjectionTopDown() =>
            SameTopDown("SELECT CAST(\"ID\" AS BIGINT), \"REGION\" FROM \"SALES\" ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnAFilterUnderASortTopDown() =>
            SameTopDown("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" WHERE \"AMOUNT\" > 5 ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnASortedJoinTopDown() =>
            SameTopDown("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [Fact]
        public void ShouldAgreeOnASortedLeftJoinTopDown() =>
            SameTopDown("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [Fact]
        public void ShouldAgreeOnASortedNestedLoopJoinTopDown() =>
            SameTopDown("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" < b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [Fact]
        public void ShouldAgreeOnAJoinOverSortedInputsTopDown() =>
            SameTopDown("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" JOIN \"SORTED\" \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" ORDER BY 1, 2");

        [Fact]
        public void ShouldAgreeOnACorrelatedSubQueryTopDown() =>
            SameTopDown("SELECT \"ID\" FROM \"SALES\" a WHERE \"AMOUNT\" > (SELECT MIN(\"AMOUNT\") FROM \"SALES\" b WHERE b.\"REGION\" = a.\"REGION\") ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnValuesTopDown() =>
            SameTopDown("SELECT * FROM (VALUES (1, 'A'), (2, 'B'), (3, 'C')) AS t(\"N\", \"L\") ORDER BY \"N\"");

        [Fact]
        public void ShouldAgreeOnAnAggregateTopDown() =>
            SameTopDown("SELECT \"REGION\", SUM(\"AMOUNT\") FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"");

        [Fact]
        public void ShouldAgreeOnAWindowTopDown() =>
            SameTopDown("SELECT \"ID\", SUM(\"AMOUNT\") OVER (PARTITION BY \"REGION\" ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // A table function written in C#, which, as for MY_SUM, EnumerableConvention names by its IKVM class
        // name (requires IKVM 8.16.0 or later). The function yields one to n, which the hand-written rows also
        // assert.
        [Fact]
        public void ShouldRunATableFunction()
        {
            const string sql = "SELECT * FROM TABLE(NUMBERS(3))";

            Same(sql);
            Gives(sql, "1", "2", "3");
        }

        // A merge join over a one-column table function puts a sort on it, which hits EnumerableSort's defect:
        // it optimises the scan's ARRAY format to SCALAR and passes the Object[] rows on unchanged. This
        // convention reproduces Calcite and refuses the plan; ClrCursorSortTests has the detail. When
        // EnumerableSort is fixed, expect the rows "1", "2" instead. The hash join rule is removed on both
        // sides, because otherwise the planner hashes this join and there is no sort.
        [Fact]
        public void ShouldRefuseATableFunctionInAJoin()
        {
            const string sql = "SELECT \"S\".\"ID\" FROM \"SALES\" AS \"S\", TABLE(NUMBERS(2)) AS \"N\" WHERE \"S\".\"ID\" = \"N\".\"N\" ORDER BY 1";

            Run(sql, true, planOnly: true, excludeHashJoin: true)[0].Should().Contain("ClrCursorMergeJoin");

            var act = () => Run(sql, true, excludeHashJoin: true);

            act.Should().Throw<java.lang.IllegalStateException>()
                .WithInnerException<java.lang.IllegalStateException>()
                .WithMessage("*ClrCursorSort handed up an open of System.Object[] where its row type is java.lang.Integer*");
        }

        // the same join as Calcite plans it, hashed, with no sort under it
        [Fact]
        public void ShouldAgreeOnATableFunctionInAHashJoin()
        {
            const string sql = "SELECT \"S\".\"ID\" FROM \"SALES\" AS \"S\", TABLE(NUMBERS(2)) AS \"N\" WHERE \"S\".\"ID\" = \"N\".\"N\" ORDER BY 1";

            Run(sql, true, planOnly: true)[0].Should().Contain("ClrCursorHashJoin");
            Same(sql);
            Gives(sql, "1", "2");
        }

        // The window table functions, which RexImpTable implements rather than the schema.
        // TumbleImplementor and tumblingWindowSelector each name a parameter _input, so the translation must
        // resolve parameters by lexical scope, as Java source compiled by Janino does.

        [Fact]
        public void ShouldAgreeOnTumble() =>
            Same("SELECT \"ROWTIME\", \"ID\", \"window_start\", \"window_end\" FROM TABLE(TUMBLE(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), INTERVAL '1' HOUR)) ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnTumbleWithAnOffset() =>
            Same("SELECT \"ID\", \"window_start\" FROM TABLE(TUMBLE(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), INTERVAL '1' HOUR, INTERVAL '10' MINUTE)) ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnHop() =>
            Same("SELECT \"ID\", \"window_start\", \"window_end\" FROM TABLE(HOP(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), INTERVAL '30' MINUTE, INTERVAL '1' HOUR)) ORDER BY \"ID\", \"window_start\"");

        [Fact]
        public void ShouldAgreeOnSession() =>
            Same("SELECT \"ID\", \"window_start\", \"window_end\" FROM TABLE(SESSION(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), DESCRIPTOR(\"ID\"), INTERVAL '1' HOUR)) ORDER BY \"ID\"");

        [Fact]
        public void ShouldAgreeOnAnAggregateOverTumble() =>
            Same("SELECT \"window_start\", COUNT(*) FROM TABLE(TUMBLE(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), INTERVAL '1' HOUR)) GROUP BY \"window_start\" ORDER BY 1");

        [Fact]
        public void ShouldPlanTumbleInThisConvention() =>
            PlanOf("SELECT \"ID\", \"window_start\" FROM TABLE(TUMBLE(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), INTERVAL '1' HOUR))", true)
                .Should().Contain("ClrCursorTableFunctionScan");

        [Fact]
        public void ShouldRunATableFunctionUnderAnAggregate() =>
            Gives("SELECT COUNT(*), SUM(\"N\") FROM TABLE(NUMBERS(4))", "4|10");



        // MATCH_RECOGNIZE in a plan rooted in this convention: the whole subtree stays in EnumerableConvention
        // under one converter. This convention has no node for it, because Calcite casts its input getter to
        // two package-private types.
        //
        // The query is shaped to run in Calcite. The measures row is built with Expressions.new_ on the row's
        // Java type, so an ARRAY-format input would give "new Object[]()", which is not Java; HR.emps is
        // CUSTOM, so a record constructor is emitted instead. The predicate's parameter (a Memory around the
        // row) and the row it was translated against are both named row_, so translation must resolve names
        // by lexical scope. And EnumerableMatch.implementPattern accepts only a symbol or a concatenation, so
        // a quantified pattern such as (STRT UP+) throws "unknown kind: PATTERN_QUANTIFIER" in either
        // convention; the pattern here is a fixed sequence.

        [Fact]
        public void ShouldAgreeOnMatchRecognize() =>
            Same("SELECT * FROM \"HR\".\"emps\" MATCH_RECOGNIZE (ORDER BY \"empid\" MEASURES STRT.\"empid\" AS \"s\", UP.\"empid\" AS \"e\" PATTERN (STRT UP) DEFINE UP AS UP.\"salary\" > PREV(UP.\"salary\")) AS T");

        // PARTITION BY has no test because it runs in neither convention. A one-column partition key has a
        // SCALAR physical type, and EnumerableMatch builds the key with Expressions.new_ on its Java row type,
        // emitting "new Integer()", which Janino rejects. This is a Calcite defect, the same as the
        // "new Object[]()" one above.

        [Fact]
        public void ShouldPlanMatchRecognizeUnderAConverter() =>
            PlanOf("SELECT * FROM \"HR\".\"emps\" MATCH_RECOGNIZE (ORDER BY \"empid\" MEASURES STRT.\"empid\" AS \"s\" PATTERN (STRT UP) DEFINE UP AS UP.\"salary\" > PREV(UP.\"salary\")) AS T", true)
                .Should().StartWith("EnumerableToClrCursorConverter");

        // ------------------------------------------------------------------ a row that is one primitive
        //
        // SCALARS is one NOT NULL INTEGER column, so its physical row type is int while its rows are
        // java.lang.Integer. Each node below instantiates an operator over the row type and must use the boxed
        // one. Tests that name a node remove the rules that would otherwise let the planner choose Calcite's.

        [Fact]
        public void ShouldAgreeOnAScalarRowScan() => Same("SELECT \"N\" FROM \"SCALARS\" ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnAScalarRowProjection() => Same("SELECT \"N\" + 1 FROM \"SCALARS\" ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnAScalarRowDistinct() => Same("SELECT DISTINCT \"N\" FROM \"SCALARS\" ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnAScalarRowAggregate() => Same("SELECT \"N\" FROM \"SCALARS\" GROUP BY \"N\" ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnAScalarRowUnionAll() =>
            SameThrough("ClrCursorUnion", "SELECT \"N\" FROM \"SCALARS\" UNION ALL SELECT \"N\" FROM \"SCALARS\" WHERE \"N\" < 3 ORDER BY 1",
                remove: [ClrCursorRules.ClrCursorMergeUnionRule]);

        [Fact]
        public void ShouldAgreeOnAScalarRowUnionDistinct() =>
            SameThrough("ClrCursorUnion", "SELECT \"N\" FROM \"SCALARS\" UNION SELECT \"N\" FROM \"SCALARS\" WHERE \"N\" < 3 ORDER BY 1",
                remove: [EnumerableRules.ENUMERABLE_MERGE_UNION_RULE, ClrCursorRules.ClrCursorMergeUnionRule]);

        // INTERSECT without ALL is rewritten to an aggregate over a union and never reaches the node; only
        // INTERSECT ALL does
        [Fact]
        public void ShouldAgreeOnAScalarRowIntersectAll() =>
            SameThrough("ClrCursorIntersect", "SELECT \"N\" FROM \"SCALARS\" INTERSECT ALL SELECT \"N\" FROM \"SCALARS\" WHERE \"N\" < 3 ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnAScalarRowExceptAll() =>
            SameThrough("ClrCursorMinus", "SELECT \"N\" FROM \"SCALARS\" EXCEPT ALL SELECT \"N\" FROM \"SCALARS\" WHERE \"N\" < 3 ORDER BY 1");

        [Fact]
        public void ShouldAgreeOnAScalarRowLimit() =>
            SameThrough("ClrCursorLimit", "SELECT \"N\" FROM \"SCALARS\" ORDER BY \"N\" OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
                remove: [EnumerableRules.ENUMERABLE_LIMIT_RULE]);

        [Fact]
        public void ShouldAgreeOnAScalarRowLimitSort() =>
            SameThrough("ClrCursorLimitSort", "SELECT \"N\" FROM \"SCALARS\" ORDER BY \"N\" FETCH NEXT 2 ROWS ONLY",
                remove: [EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE, EnumerableRules.ENUMERABLE_LIMIT_RULE],
                limitSort: true);

        [Fact]
        public void ShouldAgreeOnAScalarRowMergeUnion() =>
            SameThrough("ClrCursorMergeUnion", "SELECT \"N\" FROM \"SCALARS\" UNION SELECT \"N\" FROM \"SCALARS\" ORDER BY 1",
                remove: [EnumerableRules.ENUMERABLE_MERGE_UNION_RULE, EnumerableRules.ENUMERABLE_UNION_RULE, EnumerableRules.ENUMERABLE_SORT_RULE]);

        [Fact]
        public void ShouldAgreeOnAScalarRowSortedAggregate() =>
            SameThrough("ClrCursorSortedAggregate", "SELECT \"N\" FROM \"SCALARS\" GROUP BY \"N\" ORDER BY 1",
                remove: [EnumerableRules.ENUMERABLE_AGGREGATE_RULE, EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE, ClrCursorRules.ClrCursorAggregateRule],
                sortedAggregate: true);

        [Fact]
        public void ShouldAgreeOnAScalarRowCollectedIntoAnArray() =>
            SameThrough("ClrCursorCollect", "SELECT ARRAY(SELECT \"N\" FROM \"SCALARS\") FROM (VALUES (1))",
                remove: [EnumerableRules.ENUMERABLE_COLLECT_RULE]);

        [Fact]
        public void ShouldAgreeOnAScalarRowCollectedIntoAMultiset() =>
            SameThrough("ClrCursorCollect", "SELECT MULTISET(SELECT \"N\" FROM \"SCALARS\") FROM (VALUES (1))",
                remove: [EnumerableRules.ENUMERABLE_COLLECT_RULE]);

        [Fact]
        public void ShouldAgreeOnAScalarRowUncollected() =>
            SameThrough("ClrCursorUncollect", "SELECT * FROM UNNEST(ARRAY[1, 2, 3])",
                remove: [EnumerableRules.ENUMERABLE_UNCOLLECT_RULE]);

        // ------------------------------------------------------------------ EnumerableIEJoinTest
        //
        // A join whose condition is two cross-input inequalities. Each test names the node and removes
        // Calcite's rule, because registerDefaultRules registers it and the planner keeps the equal-cost node
        // it registered first, which is Calcite's under a converter.

        static readonly RelOptRule[] TheirIeJoin = [EnumerableRules.ENUMERABLE_IE_JOIN_RULE];

        [Fact]
        public void ShouldAgreeOnAnIeJoin() =>
            SameThrough("ClrCursorIEJoin", "SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"ID\" < b.\"ID\" AND a.\"AMOUNT\" > b.\"AMOUNT\" ORDER BY 1, 2",
                remove: TheirIeJoin);

        /// <summary>
        /// An IE join's row order comes from its two sorts, so with no ORDER BY this compares that order with
        /// linq4j's.
        /// </summary>
        /// <remarks>
        /// This does not test the sorts' stability: <c>SALES</c> gives twelve entries, and .NET's introsort
        /// insertion-sorts sixteen or fewer, which is stable. <c>ClrCursorDefaultsTests.ShouldHoldTheInputOrderOfEqualIeJoinKeys</c>
        /// tests stability over enough entries.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnAnIeJoinsOwnOrder() =>
            SameThrough("ClrCursorIEJoin", "SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"ID\" < b.\"ID\" AND a.\"AMOUNT\" > b.\"AMOUNT\"",
                remove: TheirIeJoin);

        // The strictness of each operator decides how entries with equal keys break ties, and so whether a
        // pair of equal keys is in the result. These cover all four combinations.

        [Fact]
        public void ShouldAgreeOnAnIeJoinOfTwoNonStrictInequalities() =>
            SameThrough("ClrCursorIEJoin", "SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" <= b.\"AMOUNT\" AND a.\"ID\" >= b.\"ID\" ORDER BY 1, 2",
                remove: TheirIeJoin);

        [Fact]
        public void ShouldAgreeOnAnIeJoinOfAStrictAndANonStrictInequality() =>
            SameThrough("ClrCursorIEJoin", "SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" < b.\"AMOUNT\" AND a.\"ID\" >= b.\"ID\" ORDER BY 1, 2",
                remove: TheirIeJoin);

        [Fact]
        public void ShouldAgreeOnAnIeJoinOfANonStrictAndAStrictInequality() =>
            SameThrough("ClrCursorIEJoin", "SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" <= b.\"AMOUNT\" AND a.\"ID\" > b.\"ID\" ORDER BY 1, 2",
                remove: TheirIeJoin);

        // Both descending, which sorts both orders the other way round.

        [Fact]
        public void ShouldAgreeOnAnIeJoinOfTwoDescendingInequalities() =>
            SameThrough("ClrCursorIEJoin", "SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"ID\" > b.\"ID\" AND a.\"AMOUNT\" > b.\"AMOUNT\" ORDER BY 1, 2",
                remove: TheirIeJoin);

        /// <summary>
        /// A condition written right-to-left is the same join, reversed by the rule.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnAnIeJoinWrittenRightToLeft() =>
            SameThrough("ClrCursorIEJoin", "SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON b.\"ID\" > a.\"ID\" AND b.\"AMOUNT\" < a.\"AMOUNT\" ORDER BY 1, 2",
                remove: TheirIeJoin);

        /// <summary>
        /// A row whose key is null matches nothing, on either side.
        /// </summary>
        /// <remarks>
        /// <c>SALES</c> has one row with a null <c>AMOUNT</c>, and both keys here are that column, so that
        /// row is dropped from both inputs before either sort.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnAnIeJoinOverANullableKeyOnBothSides() =>
            SameThrough("ClrCursorIEJoin", "SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" < b.\"AMOUNT\" AND a.\"AMOUNT\" > b.\"ID\" ORDER BY 1, 2",
                remove: TheirIeJoin);

        [Fact]
        public void ShouldAgreeOnAnIeJoinOverACharacterKey() =>
            SameThrough("ClrCursorIEJoin", "SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"LABEL\" < b.\"LABEL\" AND a.\"REGION\" >= b.\"REGION\" ORDER BY 1, 2",
                remove: TheirIeJoin);

        /// <summary>
        /// The first two inequalities drive the join and the rest are a calc's predicate above it.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnAnIeJoinWithAResidualInequality() =>
            SameThrough("ClrCursorIEJoin", "SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"ID\" < b.\"ID\" AND a.\"AMOUNT\" > b.\"AMOUNT\" AND a.\"LABEL\" < b.\"LABEL\" ORDER BY 1, 2",
                remove: TheirIeJoin);

        /// <summary>
        /// Two contradictory inequalities still plan as an IE join, and return no rows.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnAnIeJoinThatMatchesNothing() =>
            SameThrough("ClrCursorIEJoin", "SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"ID\" < b.\"ID\" AND a.\"ID\" > b.\"ID\" ORDER BY 1, 2",
                remove: TheirIeJoin);

        // ------------------------------------------------------------------ EnumerableUncollectTest
        //
        // The shapes of UNNEST that Calcite's EnumerableUncollectTest covers. Each names the node and removes
        // Calcite's uncollect rule, because otherwise the planner prefers Calcite's node.

        static readonly RelOptRule[] TheirUncollect = [EnumerableRules.ENUMERABLE_UNCOLLECT_RULE];

        [Fact]
        public void ShouldAgreeOnUnnestingAnArray() =>
            SameThrough("ClrCursorUncollect", "SELECT * FROM UNNEST(ARRAY[3, 4]) AS T2(y)", remove: TheirUncollect);

        [Fact]
        public void ShouldAgreeOnUnnestingANullArray() =>
            SameThrough("ClrCursorUncollect", "SELECT * FROM UNNEST(CAST(NULL AS INTEGER ARRAY))", remove: TheirUncollect);

        [Fact]
        public void ShouldAgreeOnUnnestingAnArrayOfArrays() =>
            SameThrough("ClrCursorUncollect", "SELECT * FROM UNNEST(ARRAY[ARRAY[3], ARRAY[4]]) AS T2(y)", remove: TheirUncollect);

        [Fact]
        public void ShouldAgreeOnUnnestingAnArrayOfLongerArrays() =>
            SameThrough("ClrCursorUncollect", "SELECT * FROM UNNEST(ARRAY[ARRAY[3, 4], ARRAY[4, 5]]) AS T2(y)", remove: TheirUncollect);

        [Fact]
        public void ShouldAgreeOnUnnestingAnArrayOfArraysOfArrays() =>
            SameThrough("ClrCursorUncollect",
                "SELECT * FROM UNNEST(ARRAY[ARRAY[ARRAY[3, 4], ARRAY[4, 5]], ARRAY[ARRAY[7, 8], ARRAY[9, 10]]]) AS T2(y)",
                remove: TheirUncollect);

        // one field that is a struct of one item, and no ordinality, so the result is the item itself rather
        // than a list holding it; the node has a separate branch for this
        [Fact]
        public void ShouldAgreeOnUnnestingAnArrayOfOneFieldRows() =>
            SameThrough("ClrCursorUncollect", "SELECT * FROM UNNEST(ARRAY[ROW(3), ROW(4)]) AS T2(y)", remove: TheirUncollect);

        [Fact]
        public void ShouldAgreeOnUnnestingAnArrayOfTwoFieldRows() =>
            SameThrough("ClrCursorUncollect", "SELECT * FROM UNNEST(ARRAY[ROW(3, 5), ROW(4, 6)]) AS T2(y, z)", remove: TheirUncollect);

        [Fact]
        public void ShouldAgreeOnUnnestingWithOrdinality() =>
            SameThrough("ClrCursorUncollect", "SELECT * FROM UNNEST(ARRAY[ROW(3), ROW(4)]) WITH ORDINALITY AS T2(y, o)", remove: TheirUncollect);

        // UNNEST(ARRAY[ROW(1, ROW(5, 10)), ROW(2, ROW(6, 12))]) has no test because it fails before planning:
        // RelStructuredTypeFlattener throws NoSuchElementException from SqlToRelConverter.flattenTypes, which
        // PlannerImpl.rel calls. Both sides share that conversion. A one-field row holding a row, the next
        // test, does run.

        [Fact]
        public void ShouldAgreeOnUnnestingAnArrayOfOneFieldRowsHoldingRows() =>
            SameThrough("ClrCursorUncollect", "SELECT * FROM UNNEST(ARRAY[ROW(ROW(3)), ROW(ROW(4))]) AS T2(y)", remove: TheirUncollect);

        [Fact]
        public void ShouldAgreeOnUnnestingAlongsideAnotherInput() =>
            SameThrough("ClrCursorUncollect",
                "SELECT * FROM (VALUES (1), (2)) T1(x), UNNEST(ARRAY[3, 4]) AS T2(y) ORDER BY 1, 2",
                remove: TheirUncollect);

        [Fact]
        public void ShouldAgreeOnUnnestingArraysAlongsideAnotherInput() =>
            SameThrough("ClrCursorUncollect",
                "SELECT * FROM (VALUES (1), (2)) T1(x), UNNEST(ARRAY[ARRAY[3, 4], ARRAY[4, 5]]) AS T2(y) ORDER BY 1",
                remove: TheirUncollect);

        [Fact]
        public void ShouldAgreeOnUnnestingRowsAlongsideAnotherInput() =>
            SameThrough("ClrCursorUncollect",
                "SELECT * FROM (VALUES (1), (2)) T1(x), UNNEST(ARRAY[ROW(3, 5), ROW(4, 6)]) AS T2(y, z) ORDER BY 1, 2",
                remove: TheirUncollect);

        [Fact]
        public void ShouldAgreeOnUnnestingWithOrdinalityAlongsideAnotherInput() =>
            SameThrough("ClrCursorUncollect",
                "SELECT * FROM (VALUES (1), (2)) T1(x), UNNEST(ARRAY[ROW(3), ROW(4)]) WITH ORDINALITY AS T2(y, o) ORDER BY 1, 2",
                remove: TheirUncollect);

        // ------------------------------------------------------------------ EnumerableBatchNestedLoopJoinTest

        [Fact]
        public void ShouldAgreeOnABatchNestedLoopJoinOnAStringKey() =>
            SameBatchNestedLoopJoin("SELECT d.\"name\", e.\"salary\" FROM \"HR\".\"depts\" d JOIN \"HR\".\"emps\" e ON d.\"name\" = e.\"name\" ORDER BY 1, 2");

        [Fact]
        public void ShouldAgreeOnABatchNestedLoopJoinFromANotInSubQuery() =>
            SameBatchNestedLoopJoin("SELECT COUNT(e.\"name\") FROM \"HR\".\"emps\" e WHERE e.\"deptno\" NOT IN (SELECT d.\"deptno\" FROM \"HR\".\"depts\" d WHERE d.\"name\" = 'Sales')");

        [Fact]
        public void ShouldAgreeOnABatchNestedLoopJoinOnTwoEqualities() =>
            SameBatchNestedLoopJoin("SELECT COUNT(e.\"name\") FROM \"HR\".\"emps\" e JOIN \"HR\".\"depts\" d ON d.\"deptno\" = e.\"empid\" AND d.\"deptno\" = e.\"deptno\"");

        [Fact]
        public void ShouldAgreeOnABatchNestedLoopJoinOnAMismatchedKey() =>
            SameBatchNestedLoopJoin("SELECT COUNT(e.\"name\") FROM \"HR\".\"emps\" e JOIN \"HR\".\"depts\" d ON d.\"deptno\" = e.\"empid\"");

        // an outer hash join whose null-generating side is a CUSTOM row with a primitive field. Calcite writes
        // the selector as `right == null ? null : right.empid`, which Java types as Integer, so the translated
        // conditional must be typed as the boxed type to hold the null
        [Fact]
        public void ShouldAgreeOnAHashLeftJoinOverAPrimitiveField()
        {
            const string sql = "SELECT d.\"deptno\", e.\"empid\" FROM \"HR\".\"depts\" d LEFT JOIN \"HR\".\"emps\" e ON d.\"deptno\" = e.\"deptno\" ORDER BY 1, 2";

            Run(sql, true, planOnly: true)[0].Should().MatchRegex(@"ClrCursorHashJoin\(condition=\[[^\]]*\], joinType=\[left\]\)");
            Same(sql);
        }

        [Fact]
        public void ShouldAgreeOnABatchNestedLoopLeftJoinCount() =>
            SameBatchNestedLoopJoin("SELECT COUNT(d.\"deptno\") FROM \"HR\".\"depts\" d LEFT JOIN \"HR\".\"emps\" e ON d.\"deptno\" = e.\"deptno\"");

        // two batch joins in one plan, where Calcite's node falls back to a compact row builder to stay within
        // Java's method size limit. An expression tree has no such limit and builds one form; this checks the
        // difference does not change the result.
        [Fact]
        public void ShouldAgreeOnADoubleBatchNestedLoopJoin() =>
            SameBatchNestedLoopJoin("SELECT e.\"name\", d.\"name\", l.\"name\" FROM \"HR\".\"emps\" e JOIN \"HR\".\"depts\" d ON d.\"deptno\" <> e.\"empid\" JOIN \"HR\".\"locations\" l ON e.\"empid\" <> l.\"empid\" AND d.\"deptno\" = l.\"empid\" ORDER BY 1, 2, 3");

        // ------------------------------------------------------------------ EnumerableCorrelateTest, in SQL

        [Fact]
        public void ShouldAgreeOnACorrelateFromExists() =>
            Same("SELECT e.\"empid\", e.\"name\" FROM \"HR\".\"emps\" e WHERE EXISTS (SELECT 1 FROM \"HR\".\"depts\" d WHERE d.\"deptno\" = e.\"deptno\") ORDER BY 1");

        // the correlated condition compares against a nullable column, so the field the sub-query reads is
        // boxed rather than primitive
        [Fact]
        public void ShouldAgreeOnACorrelateOverABoxedPrimitive() =>
            Same("SELECT e.\"empid\" FROM \"HR\".\"emps\" e WHERE NOT EXISTS (SELECT 1 FROM \"HR\".\"depts\" d WHERE d.\"deptno\" = e.\"commission\") ORDER BY 1");

        /// <summary>
        /// A scalar sub-query correlated on two columns at once, in a query with its own filter.
        /// </summary>
        [Fact]
        public void ShouldAgreeOnAComplexNestedCorrelatedSubQuery() =>
            Same("SELECT \"empid\", \"deptno\", (SELECT COUNT(*) FROM \"HR\".\"emps\" AS x WHERE x.\"salary\" > \"emps\".\"salary\" AND x.\"deptno\" < \"emps\".\"deptno\") FROM \"HR\".\"emps\" WHERE \"empid\" < \"salary\" ORDER BY 1, 2, 3");

        // ------------------------------------------------------------------ EnumerableMergeUnionTest
        //
        // The order keys Calcite's own tests use: a nullable column with the nulls at either end, and a second
        // key running the other way.

        [Fact]
        public void ShouldAgreeOnAMergeUnionAllOrderedByANullableKeyNullsFirst() =>
            Same("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" UNION ALL SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY \"AMOUNT\" ASC NULLS FIRST, \"ID\" DESC");

        [Fact]
        public void ShouldAgreeOnAMergeUnionOrderedByANullableKeyNullsFirst() =>
            Same("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" UNION SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY \"AMOUNT\" ASC NULLS FIRST, \"ID\" DESC");

        [Fact]
        public void ShouldAgreeOnAMergeUnionAllOrderedByANullableKeyNullsLast() =>
            Same("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" UNION ALL SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY \"AMOUNT\" ASC NULLS LAST, \"ID\" DESC");

        [Fact]
        public void ShouldAgreeOnAMergeUnionOrderedByANullableKeyNullsLast() =>
            Same("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" UNION SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY \"AMOUNT\" ASC NULLS LAST, \"ID\" DESC");

        [Fact]
        public void ShouldAgreeOnAMergeUnionOfOneColumnOrderedByIt() =>
            Same("SELECT \"LABEL\" FROM \"SALES\" UNION SELECT \"LABEL\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY 1");

        // ------------------------------------------------------------------ EnumerableHashJoinTest

        [Fact]
        public void ShouldAgreeOnAFullJoinOnACompositeNullableKey() =>
            SameHashJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a FULL JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"AMOUNT\" = b.\"AMOUNT\" ORDER BY 1, 2");

        [Fact]
        public void ShouldAgreeOnASemiJoinOnACompositeNullableKey() =>
            SameHashJoin("SELECT a.\"ID\" FROM \"SALES\" a WHERE (a.\"REGION\", a.\"AMOUNT\") IN (SELECT b.\"REGION\", b.\"AMOUNT\" FROM \"SALES\" b WHERE b.\"ID\" < 4) ORDER BY 1");

        // an equality plus another predicate, which the hash join tests on each pair the equality matched
        [Fact]
        public void ShouldAgreeOnAHashJoinWithAnExtraPredicate() =>
            SameHashJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"ID\" < b.\"ID\" ORDER BY 1, 2");

        [Fact]
        public void ShouldAgreeOnALeftHashJoinWithAnExtraPredicate() =>
            SameHashJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"ID\" < b.\"ID\" ORDER BY 1, 2");

        [Fact]
        public void ShouldAgreeOnARightHashJoinWithAnExtraPredicate() =>
            SameHashJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a RIGHT JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"ID\" < b.\"ID\" ORDER BY 1, 2");

        [Fact]
        public void ShouldAgreeOnASemiHashJoinWithAnExtraPredicate() =>
            SameHashJoin("SELECT a.\"ID\" FROM \"SALES\" a WHERE EXISTS (SELECT 1 FROM \"SALES\" b WHERE a.\"REGION\" = b.\"REGION\" AND a.\"ID\" < b.\"ID\") ORDER BY 1");

        // ------------------------------------------------------------------ EnumerableLimitSortTest
        //
        // The order keys Calcite's own limit-sort tests use: a nullable column with the nulls at either end,
        // and a second key.

        [Fact]
        public void ShouldAgreeOnALimitSortWithNullsFirst() =>
            SameLimitSort("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" NULLS FIRST, \"ID\" FETCH NEXT 3 ROWS ONLY");

        [Fact]
        public void ShouldAgreeOnALimitSortWithNullsLast() =>
            SameLimitSort("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" NULLS LAST, \"ID\" FETCH NEXT 3 ROWS ONLY");

        [Fact]
        public void ShouldAgreeOnALimitSortWithNullsFirstAndAnOffset() =>
            SameLimitSort("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" NULLS FIRST, \"ID\" OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY");

        /// <summary>
        /// A limit sort on one nullable key with NULLS FIRST gives the same rows on both sides.
        /// </summary>
        /// <remarks>
        /// A single-column collation over a nullable column, the only shape whose sort key can itself be null:
        /// a multi-field key is a FlatLists row, which is never null even when a field in it is.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnALimitSortOnOneNullableKeyNullsFirst() =>
            SameLimitSort("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" NULLS FIRST FETCH NEXT 3 ROWS ONLY");

        [Fact]
        public void ShouldAgreeOnALimitSortOnOneNullableKeyTakingEverything() =>
            SameLimitSort("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" FETCH NEXT 100 ROWS ONLY");

        [Fact]
        public void ShouldAgreeOnALimitSortWithNullsLastAndAnOffset() =>
            SameLimitSort("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" NULLS LAST, \"ID\" OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY");

        [Fact]
        public void ShouldAgreeOnALimitSortOverSeveralKeysRunningBothWays() =>
            SameLimitSort("SELECT \"ID\", \"REGION\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"REGION\" DESC, \"AMOUNT\" NULLS LAST, \"ID\" OFFSET 1 ROWS FETCH NEXT 4 ROWS ONLY");

        /// <summary>
        /// <c>JSON_VALUE</c> with <c>RETURNING VARCHAR ARRAY</c> over an array gives the same result on both sides.
        /// </summary>
        /// <remarks>
        /// Both sides return null. The validator types the column <c>VARCHAR ARRAY</c>, then
        /// <c>convertJsonReturningFunction</c> removes the <c>RETURNING</c> operands, so the scalar
        /// <c>JsonFunctions.jsonValue</c> runs; it rejects the array, and the default <c>NULL ON ERROR</c> turns
        /// that into a null. This is Calcite's behaviour. <c>JSON_QUERY</c> is the function that reads an array.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnJsonValueReturningAnArray() =>
            Same("SELECT JSON_VALUE('{\"c\":[\"a\",\"b\",\"c\"]}', '$.c' RETURNING VARCHAR ARRAY) AS \"A\"");

        [Fact]
        public void ShouldAgreeOnJsonQueryReturningAnArray() =>
            Same("SELECT JSON_QUERY('{\"c\":[\"a\",\"b\",\"c\"]}', '$.c' RETURNING VARCHAR ARRAY) AS \"A\"");

        /// <summary>
        /// Both sides refuse an integral JSON number read through <c>RETURNING DOUBLE</c>.
        /// </summary>
        /// <remarks>
        /// <c>RETURNING</c> types the call but does not convert its value. <c>JsonFunctions.jsonValue</c> returns
        /// what Jackson parsed, an <c>Integer</c> for <c>0</c> and a <c>Double</c> for <c>-83.489548</c>, and
        /// <c>AbstractRexCallImplementor.genValueStatement</c> asks <c>EnumUtils.convert</c> for <c>Object</c> to
        /// <c>Double</c>. Neither side is statically numeric, so the conversion is a bare Java cast
        /// (<c>Expressions.convert_</c>), which fails on the <c>Integer</c> row.
        ///
        /// <para>This is Calcite's behaviour; its <c>JdbcTest.testJsonValueError</c> asserts the same cast failing
        /// for <c>RETURNING INTEGER</c> over a string. The two rows show that the value read, not the statement,
        /// decides whether it throws.</para>
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnRefusingJsonValueReturningDoubleOverAnIntegralNumber() =>
            SameFailure("SELECT JSON_VALUE(\"V\", '$.c' RETURNING DOUBLE) AS \"A\" FROM (VALUES ('{\"c\":-83.489548}'), ('{\"c\":0}')) AS \"T\"(\"V\")", "Unable to cast object of type 'java.lang.Integer' to type 'java.lang.Double'");

        /// <summary>
        /// Both sides read a fractional JSON number through <c>RETURNING DOUBLE</c>.
        /// </summary>
        /// <remarks>
        /// The control for the test above: over a number written with a decimal point the statement succeeds,
        /// because Jackson parses it as a <c>Double</c> and the cast is the identity.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnJsonValueReturningDoubleOverAFractionalNumber() =>
            Same("SELECT JSON_VALUE('{\"c\":-83.489548}', '$.c' RETURNING DOUBLE) AS \"A\"");

        /// <summary>
        /// Both sides refuse a fractional JSON number read through <c>RETURNING INTEGER</c>.
        /// </summary>
        /// <remarks>
        /// The converse: a fractional number through <c>RETURNING INTEGER</c> fails the same way, so the
        /// behaviour is not specific to DOUBLE.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnRefusingJsonValueReturningIntegerOverAFractionalNumber() =>
            SameFailure("SELECT JSON_VALUE('{\"c\":0.5}', '$.c' RETURNING INTEGER) AS \"A\"", "Unable to cast object of type 'java.lang.Double' to type 'java.lang.Integer'");

        /// <summary>
        /// Both sides read a JSON number as a DOUBLE through <c>CAST</c>, whether or not it is written with a decimal point.
        /// </summary>
        /// <remarks>
        /// A <c>CAST</c> does convert. Without <c>RETURNING</c> the call is typed <c>VARCHAR(2000)</c>, so
        /// <c>EnumUtils.convert</c> takes its <c>toType == String.class</c> branch and writes
        /// <c>x == null ? null : x.toString()</c>; the cast from VARCHAR to DOUBLE is then
        /// <c>SqlFunctions.toDouble</c>, a parse, which accepts both forms of the number. This is how a caller
        /// reads a number from a document.
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnCastingJsonValueToADoubleWhicheverWayTheNumberIsWritten() =>
            Same("SELECT CAST(JSON_VALUE(\"V\", '$.c') AS DOUBLE) AS \"A\" FROM (VALUES ('{\"c\":-83.489548}'), ('{\"c\":0}')) AS \"T\"(\"V\")");

    }

}
