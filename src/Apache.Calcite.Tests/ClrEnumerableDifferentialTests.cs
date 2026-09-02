using System;
using System.Collections.Generic;
using System.Linq;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

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
using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Interop;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Runs the same query through this convention and through Calcite's, and requires the same rows.
    /// </summary>
    /// <remarks>
    /// A node here can be wrong in a way no assertion written by hand would catch, because the expected answer
    /// is whatever Calcite says it is. Asking Calcite is cheap now that both conventions run side by side, and
    /// it is the only check that scales to a node like Window, where a detail wrong gives a wrong answer rather
    /// than a failure.
    /// </remarks>
    [TestClass]
    public class ClrEnumerableDifferentialTests
    {

        /// <summary>
        /// Initializes the static instance.
        /// </summary>
        /// <remarks>
        /// Janino resolves the names in the source Calcite generates through its parent class loader, so an
        /// assembly the generated code mentions has to be on IKVM's boot class path. FIB is
        /// <c>Smalls.fibonacciTableWithLimit100</c>, which lives in calcite-testkit.
        /// </remarks>
        static ClrEnumerableDifferentialTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.util.Smalls).Assembly);

            // RelBuilder.create opens a Calcite connection to get a prepare context, and the driver loads its
            // factory by name. Class.forName finds it only if calcite-core is on the boot class path.
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// A table with partitions, ties, nulls and an order, so that a window has something to disagree over.
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

                return Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// A table that says its rows arrive sorted by their first field.
        /// </summary>
        /// <remarks>
        /// A merge join, a merge union and a sorted aggregate are only ever chosen over their hash and
        /// buffering counterparts when the input already carries a collation, and a table is where one comes
        /// from: <c>getStatistic().getCollations()</c> is what both conventions' scans put in their trait set.
        /// <c>SALES</c> advertises none, so with it Calcite picks a hash join, a union and an aggregate, and
        /// the three sorted nodes are unreachable in either convention.
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

                return Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// A table of one NOT NULL INTEGER column, which is the row shape every other fixture here avoids.
        /// </summary>
        /// <remarks>
        /// One column and NOT NULL is what makes <c>JavaRowFormat.optimize</c> answer <c>SCALAR</c>, and
        /// <c>SCALAR.javaRowClass</c> then answers <c>int</c> rather than <c>java.lang.Integer</c>. A
        /// sequence still carries the box — Java has no <c>Enumerable&lt;int&gt;</c> to carry anything else
        /// — so a node that closes its operator over the physical row type instead of the boxed one builds a
        /// tree that will not compile. Seven did, and no test had this shape: every set operation here is
        /// over <c>REGION</c> or <c>LABEL</c>, and both are VARCHAR.
        ///
        /// <para>A collation, so that the merge union and the sorted aggregate can be reached over it as
        /// well. <c>SALES</c> advertises none and <c>SORTED</c> has two columns.</para>
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

                return Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// A table with a column of type ANY, whose Java class is <c>Object</c> and whose values therefore
        /// carry no type the plan can read.
        /// </summary>
        /// <remarks>
        /// Not a curiosity: a provider type the ADO.NET adapter has no <c>SqlTypeName</c> for arrives as
        /// ANY, so this is the shape a column of an unmapped type has. Every other fixture here declares a
        /// concrete type, so until now nothing asked an aggregate to accumulate over a value whose type is
        /// only known at run time.
        ///
        /// <para>An ordinary INTEGER key in front, so that a window over this table has something to order
        /// by that is not itself ANY. Three shapes after it, on purpose. <c>V</c> mixes a <c>java.lang.Integer</c> with a
        /// <c>java.lang.Double</c>, which is the ordinary case for a document store and the one a comparison
        /// through <c>Comparable.compareTo</c> throws on; <c>S</c> holds strings, which are orderable and not
        /// addable; and both have a null, so that an aggregate has one to skip.</para>
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

                return Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// A table whose ANY columns hold the values a document store puts behind one — a GUID, a
        /// timestamp and a number, each written the way JSON writes it.
        /// </summary>
        /// <remarks>
        /// <c>ANYS</c> asks what an aggregate does over a value of unknown type; this asks what a
        /// <c>CAST</c> does, which is a different question with a surprising answer.
        /// <c>RexToLixTranslator.getConvertExpression</c> switches on the target type and then on the
        /// source, and ANY matches no source branch anywhere, so every one of these ends at
        /// <c>EnumUtils.convert(operand, typeFactory.getJavaClass(targetType))</c> — a Java conversion
        /// between two <em>classes</em>, with no idea that a SQL cast was asked for. What that gives
        /// depends entirely on which class the target has:
        ///
        /// <list type="bullet">
        /// <item>a primitive or <c>BigDecimal</c> target reaches <c>SqlFunctions.toInt</c> and friends,
        /// which do convert — including from a string;</item>
        /// <item><c>VARCHAR</c> reaches <c>toString()</c>;</item>
        /// <item><c>TIMESTAMP</c> and <c>DATE</c> are <c>long</c> and <c>int</c>, so the cast asks for the
        /// <em>internal</em> value: epoch millis and epoch days, never a date parse;</item>
        /// <item><c>UUID</c> has no case in <c>JavaTypeFactoryImpl.getJavaClass</c> at all, so its class is
        /// <c>Object</c>, the conversion is between <c>Object</c> and <c>Object</c>, and the cast is the
        /// identity — the value comes through as whatever it already was.</item>
        /// </list>
        ///
        /// <para>None of that is this project's: it is Calcite's own generator, reached identically by
        /// both conventions, and the tests below are here to hold that it stays reached identically. Where
        /// a caller wants a conversion, the second cast is what performs it — <c>VARCHAR</c> is a source
        /// branch that every target has, so <c>CAST(CAST(x AS VARCHAR) AS UUID)</c> reaches
        /// <c>SqlFunctions.uuidFromString</c> and <c>… AS TIMESTAMP</c> reaches the string parser.</para>
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

                return Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// A table of twelve distinct keys, which is the one row count at which a hash join's leftovers can
        /// come out in the wrong order.
        /// </summary>
        /// <remarks>
        /// A RIGHT or a FULL join ends by emitting the build rows nothing probed, and <c>hashEquiJoin_</c>
        /// walks those by copying the lookup's key set into a <c>java.util.HashSet</c> and iterating
        /// <em>that</em>. The copy does not have the map's iteration order:
        /// <c>HashSet(Collection)</c> sizes its table as <c>tableSizeFor(max((int) (n / 0.75f) + 1, 16))</c>,
        /// while a map grown by insertion holds the smallest power of two at or above 16 that still leaves
        /// <c>n &lt;= 0.75 * cap</c>. The two disagree exactly where <c>n = 0.75 * 2^k</c> — 12, 24, 48 — and
        /// at twelve keys the map is a table of 16 and the copy a table of 32.
        ///
        /// <para>So this is the shape no other fixture here has. <c>SALES</c> has six rows, which puts both
        /// at 16, and a right join over it agrees whichever collection the leftovers are walked from.</para>
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

                return Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// A table with a timestamp, which is what a window table function needs and <c>SALES</c> has not.
        /// </summary>
        /// <remarks>
        /// A table of its own rather than a column added to <c>SALES</c>, so that nothing already asserted
        /// changes. A TIMESTAMP arrives as the millisecond count the type factory says it is.
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

                return Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// The context a plan is bound with.
        /// </summary>
        /// <param name="rootSchema"></param>
        /// <remarks>
        /// <paramref name="parameters"/> is the map both implementors stash into, and <c>get</c> has to serve
        /// it: Calcite's generated <c>bind</c> opens with a declaration per stashed value, reading each back
        /// with <c>root.get(name)</c>. Answering null there is answering null to
        /// <c>EnumerableRepeatUnion</c>'s scratch table, which is how a recursive query failed on
        /// <c>EnumerableConvention</c>'s side of this harness — the side that is supposed to be the oracle.
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
        /// Runs a query in one convention and returns its rows rendered as text.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="clr">Whether to plan into this convention or into Calcite's.</param>
        /// <param name="topDown">Whether the planner optimises top down, which is what asks a node to pass a
        /// trait down to its inputs or derive one from them.</param>
        /// <returns></returns>
        /// <summary>
        /// The schema every query here is planned against.
        /// </summary>
        /// <returns></returns>
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
            rootSchema.add("FIB", org.apache.calcite.schema.impl.TableFunctionImpl.create(org.apache.calcite.util.Smalls.FIBONACCI_LIMIT_100_TABLE_METHOD));

            // A CUSTOM-format fixture, which every other table here is not. HrSchema's rows are instances of
            // a Java class, so a scan of it yields a synthetic record rather than an Object[] and
            // PhysType.record, fieldReference and the join selector all take the other branch. It is Calcite's
            // own schema rather than one of this project's classes because Janino could not name a CLR class
            // under IKVM 8.14.0 or 8.15.0, which left EnumerableConvention with no plan to compare against.
            // 8.16.0 fixed that, so it is no longer a constraint on the fixture.
            rootSchema.add("HR", new org.apache.calcite.adapter.java.ReflectiveSchema(new org.apache.calcite.test.schemata.hr.HrSchema()));

            // the hierarchy CALCITE-4054 is about, which is a recursive query whose step is a correlate over
            // the transient table. ReflectiveSchemaWithoutRowCount is Calcite's own wrapper and is what keeps
            // the planner from costing the scan out of the plan.
            rootSchema.add("HIER", new org.apache.calcite.test.ReflectiveSchemaWithoutRowCount(new org.apache.calcite.test.schemata.hr.HierarchySchema()));

            // a column of every type Calcite has an implementor for, which is where IS EMPTY finds a list to
            // ask about. Calcite's own EnumerableCalcTest uses it for the same reason.
            rootSchema.add("CATCHALL", new org.apache.calcite.adapter.java.ReflectiveSchema(new org.apache.calcite.test.schemata.catchall.CatchallSchema()));

            return rootSchema;
        }

        static List<string> Run(string sql, bool clr, bool topDown = false, bool planOnly = false, bool sortedAggregate = false, bool batchNestedLoopJoin = false, bool limitSort = false, bool markJoin = false, bool excludeHashJoin = false, bool excludeMergeJoin = false, bool interpreter = false, RelOptRule[]? add = null, RelOptRule[]? remove = null)
        {
            var rootSchema = Schema();

            var rules = new java.util.ArrayList();
            var calcRules = new java.util.ArrayList();

            if (clr)
            {
                foreach (var rule in ClrEnumerableRules.Rules())
                {
                    // dropped on both sides together, or the comparison is between two different plans:
                    // DefaultRulesProgram takes Calcite's out and this takes ours
                    if (excludeMergeJoin && rule == ClrEnumerableRules.ClrEnumerableMergeJoinRule)
                        continue;

                    rules.add(rule);
                }

                foreach (var rule in ClrEnumerableRules.CalcRules())
                    calcRules.add(rule);
            }

            // Calcite's own rules are registered by DefaultRulesProgram, because RelOptUtil.registerDefaultRules
            // registers ENUMERABLE_RULES itself. Where this convention has no node for something the planner
            // takes Calcite's, and the converters carry the rows across.

            // Calcite turns the sorted aggregate on by configuration rather than putting it in
            // ENUMERABLE_RULES, and this convention does the same, so a test that wants it asks for it and
            // each side registers its own
            if (sortedAggregate)
                rules.add(clr ? ClrEnumerableRules.ClrEnumerableSortedAggregateRule : EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE);

            if (batchNestedLoopJoin)
                rules.add(clr ? ClrEnumerableRules.ClrEnumerableBatchNestedLoopJoinRule : EnumerableRules.ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE);

            // and the limit sort is the third of the three rules Calcite declares as fields and leaves out of
            // ENUMERABLE_RULES. It was in this convention's default list once, which meant Calcite could
            // never plan the node this one planned, so nothing here was comparing limit sorts at all
            if (limitSort)
                rules.add(clr ? ClrEnumerableRules.ClrEnumerableLimitSortRule : EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);

            // Calcite registers TO_INTERPRETER from RelOptUtil.registerDefaultRules, so its side always has
            // one; this convention's counterpart is a field a caller adds, exactly as the sorted aggregate is
            if (interpreter && clr)
                rules.add(ClrEnumerableRules.ClrEnumerableInterpreterRule);

            // AVG has no implementor of its own; a real program reduces it to SUM over COUNT first, and this
            // rule lives in RelOptRules.BASE_RULES rather than in any convention's set
            rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);

            // a project holding an OVER is refused by both conventions; it becomes a LogicalWindow first, and
            // that rule likewise lives outside any convention's set
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    markJoin ? MarkJoinSubQueryProgram() : Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
                    new DefaultRulesProgram(rules, topDown, (clr && topDown) || excludeMergeJoin, excludeHashJoin, add, remove),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);

            var convention = clr ? (Convention)ClrEnumerableConvention.Instance : EnumerableConvention.INSTANCE;
            // Prepare.getDesiredRootTraitSet: the root's own traits with the convention replaced, then
            // simplified. simplify() is what collapses the composite collation a VALUES of several rows
            // carries; without it the planner casts that trait to a single RelCollation and fails. The
            // collation has to be kept rather than dropped for an empty trait set, or SortRemoveRule — which
            // arrives with Calcite's abstract rules — takes an ORDER BY away as unwanted.
            var chosen = planner.transform(1, expanded.getTraitSet().replace(convention).simplify(), expanded);
            var physical = planner.transform(2, chosen.getTraitSet(), chosen);

            if (planOnly)
                return [org.apache.calcite.plan.RelOptUtil.toString(physical)];

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);
            var source = physical is ClrEnumerableRel node
                ? TestRows.Of(ClrEnumerableInterpretable.ToBindable(parameters, node, ClrEnumerablePrefer.Array), context)
                : TestRows.Of(EnumerableInterpretable.toBindable(parameters, null, (EnumerableRel)physical, EnumerableRel.Prefer.ARRAY), context);

            var rows = new List<string>();
            foreach (var row in source)
                rows.Add(Render(row));

            return rows;
        }

        /// <summary>
        /// Renders a row so that two conventions can be compared without caring which object holds a value.
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
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
        /// <param name="planOnly"></param>
        /// <param name="add">Rules to register alongside Calcite's.</param>
        /// <param name="remove">Rules to take away once everything is registered.</param>
        /// <returns></returns>
        /// <remarks>
        /// Some of what <c>EnumerableConvention</c>'s own tests reach cannot be written as SQL:
        /// <c>Combine</c> has no syntax at all, the POSIX regex operators are not in the core parser, and a
        /// recursive query over a transient table is built with <c>transientScan</c> and <c>repeatUnion</c>.
        /// Calcite tests all of those through <c>CalciteAssert.withRel</c>; this is that, against both
        /// conventions.
        ///
        /// <para>The programs are the ones <see cref="Run"/> uses, less the sub-query pass, which has nothing
        /// to rewrite in a plan that was never a query.</para>
        /// </remarks>
        internal static List<string> RunRel(Func<RelBuilder, RelNode> build, bool clr, bool planOnly = false, RelOptRule[]? add = null, RelOptRule[]? remove = null)
        {
            var rootSchema = Schema();

            var rules = new java.util.ArrayList();
            if (clr)
                foreach (var rule in ClrEnumerableRules.Rules())
                    rules.add(rule);

            var calcRules = new java.util.ArrayList();
            if (clr)
                foreach (var rule in ClrEnumerableRules.CalcRules())
                    calcRules.add(rule);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder().defaultSchema(rootSchema).build();
            var logical = build(RelBuilder.create(config));

            var planner = (org.apache.calcite.plan.volcano.VolcanoPlanner)logical.getCluster().getPlanner();
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

            var convention = clr ? (Convention)ClrEnumerableConvention.Instance : EnumerableConvention.INSTANCE;
            var empty = new java.util.ArrayList();

            var chosen = new DefaultRulesProgram(rules, false, false, false, add, remove)
                .run(planner, logical, logical.getTraitSet().replace(convention).simplify(), empty, empty);

            var physical = Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE)
                .run(planner, chosen, chosen.getTraitSet(), empty, empty);

            if (planOnly)
                return [org.apache.calcite.plan.RelOptUtil.toString(physical)];

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);
            var source = physical is ClrEnumerableRel node
                ? TestRows.Of(ClrEnumerableInterpretable.ToBindable(parameters, node, ClrEnumerablePrefer.Array), context)
                : TestRows.Of(EnumerableInterpretable.toBindable(parameters, null, (EnumerableRel)physical, EnumerableRel.Prefer.ARRAY), context);

            var rows = new List<string>();
            foreach (var row in source)
                rows.Add(Render(row));

            return rows;
        }

        /// <summary>
        /// Returns the chosen plan, for a question about which nodes a query reaches.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="clr"></param>
        /// <returns></returns>
        internal static string PlanOf(string sql, bool clr, bool sortedAggregate = false, bool batchNestedLoopJoin = false, bool limitSort = false, bool markJoin = false, bool excludeMergeJoin = false, bool interpreter = false)
        {
            return Run(sql, clr, false, true, sortedAggregate, batchNestedLoopJoin, limitSort, markJoin, false, excludeMergeJoin, interpreter)[0];
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions, with neither convention's merge
        /// join rule registered.
        /// </summary>
        /// <param name="sql"></param>
        /// <remarks>
        /// The merge join is what both planners choose for a join of two keys over <c>SALES</c> — the scans
        /// sort cheaply and it wins on cost — so a question about the hash join has to take it away, from
        /// both sides at once. Without this the rows agree and prove nothing about the node they were aimed
        /// at.
        /// </remarks>
        /// <summary>
        /// Requires that a query gives the same rows in both conventions, with this convention's interpreter
        /// rule on.
        /// </summary>
        /// <param name="sql"></param>
        /// <remarks>
        /// Both sides can interpret either way — Calcite's <c>TO_INTERPRETER</c> is registered by
        /// <c>registerDefaultRules</c> — so what this turns on is the *choice* of which convention hosts the
        /// interpreted node, and the rows have to be the same whichever wins.
        /// </remarks>
        static void SameInterpreted(string sql)
        {
            var mine = Run(sql, true, false, false, false, false, false, false, false, false, true);
            var calcite = Run(sql, false, false, false, false, false, false, false, false, false, true);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        static void SameHashJoin(string sql)
        {
            var mine = Run(sql, true, false, false, false, false, false, false, false, true);
            var calcite = Run(sql, false, false, false, false, false, false, false, false, true);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions.
        /// </summary>
        /// <param name="sql"></param>
        internal static string PlanOfFib(string sql, bool clr, bool excludeHashJoin = false) => Run(sql, clr, false, true, false, false, false, false, excludeHashJoin)[0];

        internal static List<string> RunFib(string sql, bool clr, bool excludeHashJoin = false) => Run(sql, clr, false, false, false, false, false, false, excludeHashJoin);

        static void Same(string sql, bool limitSort = false)
        {
            var mine = Run(sql, true, limitSort: limitSort);
            var calcite = Run(sql, false, limitSort: limitSort);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        /// <summary>
        /// Requires that a query fails the same way in both conventions.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="message">What the exception both sides throw has to say.</param>
        /// <remarks>
        /// A query Calcite refuses is as much a fact about Calcite as one it answers, and a convention that
        /// answered it would be the divergence. <see cref="Same"/> cannot state that: two throws are not two
        /// row lists, and a test that only ran the query would pass on a defect that made both sides fail
        /// for different reasons. The message is what pins which failure, so the java.lang exception a
        /// generated block throws is compared rather than merely counted.
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
        /// <param name="node">The node this convention must have chosen.</param>
        /// <param name="sql"></param>
        /// <param name="remove">Rules to take away from this convention's run alone, so that the planner has
        /// nothing it prefers to <paramref name="node"/>.</param>
        /// <remarks>
        /// Both conventions' rules are in one planner and <c>VolcanoCost</c> compares the row count and
        /// nothing else, so a node of Calcite's and the same node of this convention never differ in cost and
        /// the planner keeps whichever it saw first — Calcite's, which <c>registerDefaultRules</c> registers.
        /// A test that only compares rows can therefore be comparing Calcite against Calcite, and three of
        /// the ones here were: <c>ClrEnumerableLimit</c> had never run at all. The plan assertion is what
        /// makes the comparison mean something, and the rules taken away are what make the plan possible.
        ///
        /// <para>Only this convention's run loses them. Calcite's side is planned as it always is, and is the
        /// oracle.</para>
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
        /// <param name="build"></param>
        /// <param name="add"></param>
        /// <param name="remove"></param>
        internal static void SameRel(Func<RelBuilder, RelNode> build, RelOptRule[]? add = null, RelOptRule[]? remove = null)
        {
            var mine = RunRel(build, true, add: add, remove: remove);
            var calcite = RunRel(build, false, add: add, remove: remove);

            mine.Should().Equal(calcite, "the plan should give what EnumerableConvention gives");
        }

        /// <summary>
        /// Requires that a plan built against a <see cref="RelBuilder"/> gives the same rows in both
        /// conventions, and that this convention really planned the node it was aimed at.
        /// </summary>
        /// <param name="node"></param>
        /// <param name="build"></param>
        /// <param name="add"></param>
        /// <param name="remove"></param>
        internal static void SameRelThrough(string node, Func<RelBuilder, RelNode> build, RelOptRule[]? add = null, RelOptRule[]? remove = null)
        {
            RunRel(build, true, planOnly: true, add: add, remove: remove)[0]
                .Should().Contain(node, "the plan should be planned through {0}", node);

            var mine = RunRel(build, true, add: add, remove: remove);
            var calcite = RunRel(build, false, add: add, remove: remove);

            mine.Should().Equal(calcite, "the plan should give what EnumerableConvention gives");
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions, with the sorted aggregate rule on.
        /// </summary>
        /// <param name="sql"></param>
        /// <remarks>
        /// Neither convention registers that rule by default, because Calcite does not: a caller turns it on.
        /// Each side gets its own.
        /// </remarks>
        static void SameSortedAggregate(string sql)
        {
            var mine = Run(sql, true, false, false, true);
            var calcite = Run(sql, false, false, false, true);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions, with the batch nested loop join
        /// rule on.
        /// </summary>
        /// <param name="sql"></param>
        /// <remarks>
        /// Neither convention registers that rule by default, because Calcite does not. Each side gets its
        /// own, at Calcite's batch size of 100.
        /// </remarks>
        static void SameBatchNestedLoopJoin(string sql)
        {
            var mine = Run(sql, true, false, false, false, true);
            var calcite = Run(sql, false, false, false, false, true);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions, with the limit sort rule on.
        /// </summary>
        /// <param name="sql"></param>
        /// <remarks>
        /// Neither convention registers that rule by default, because Calcite does not: it is a field of
        /// <c>EnumerableRules</c> left out of <c>ENUMERABLE_RULES</c>, like the sorted aggregate and the
        /// batch nested loop join. Each side gets its own.
        /// </remarks>
        static void SameLimitSort(string sql)
        {
            var mine = Run(sql, true, false, false, false, false, true);
            var calcite = Run(sql, false, false, false, false, false, true);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        /// <summary>
        /// The sub-query pass that rewrites EXISTS/IN/SOME to a LEFT MARK join rather than to a correlate.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// <c>Programs.subQuery</c> chooses between two rule sets on
        /// <c>CalciteConnectionConfig.topDownGeneralDecorrelationEnabled</c>, which is off by default, so the
        /// mark-join rules are not reached through it. They have the same standing as the sorted aggregate
        /// rule: Calcite ships them and a caller turns them on. This is that second set, spelled out.
        /// </remarks>
        static Program MarkJoinSubQueryProgram()
        {
            var rules = new java.util.ArrayList();
            rules.add(org.apache.calcite.rel.rules.CoreRules.FILTER_SUB_QUERY_TO_MARK_CORRELATE);
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_SUB_QUERY_TO_MARK_CORRELATE);
            rules.add(org.apache.calcite.rel.rules.CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_OVER_SUM_TO_SUM0_RULE);

            var builder = org.apache.calcite.plan.hep.HepProgram.builder();
            builder.addRuleCollection(rules);

            return Programs.of(builder.build(), true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE);
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions, with the mark-join sub-query
        /// rules on.
        /// </summary>
        /// <param name="sql"></param>
        static void SameMarkJoin(string sql)
        {
            var mine = Run(sql, true, false, false, false, false, false, true);
            var calcite = Run(sql, false, false, false, false, false, false, true);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions when the planner optimises top down.
        /// </summary>
        /// <param name="sql"></param>
        /// <remarks>
        /// Top-down optimisation is the only thing that calls <c>passThroughTraits</c>, <c>deriveTraits</c>
        /// and <c>getDeriveMode</c>. Calcite leaves it off by default, so with it off those three are dead on
        /// both sides and a comparison says nothing about them; with it on, this convention's answers to them
        /// are being compared against <c>EnumerableConvention</c>'s.
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
        /// <param name="sql"></param>
        /// <param name="expected"></param>
        /// <remarks>
        /// For the queries <c>EnumerableConvention</c> cannot run at all, so there is nothing to ask. Prefer
        /// <see cref="Same"/> everywhere else: an expectation written by hand is one this convention can agree
        /// with while both of them are wrong.
        /// </remarks>
        static void Gives(string sql, params string[] expected)
        {
            Run(sql, true).Should().Equal(expected, "'{0}' should give the rows SQL says it does", sql);
        }

        [TestMethod]
        public void ShouldAgreeOnAScan() => Same("SELECT \"ID\", \"REGION\" FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAFilterAndProjection() => Same("SELECT \"ID\", \"AMOUNT\" + 1 FROM \"SALES\" WHERE \"AMOUNT\" > 10 ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnANullableExpression() => Same("SELECT \"ID\", \"AMOUNT\" + \"ID\" FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnSortingWithNulls() => Same("SELECT \"ID\" FROM \"SALES\" ORDER BY \"AMOUNT\", \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnSortingDescendingWithNulls() => Same("SELECT \"ID\" FROM \"SALES\" ORDER BY \"AMOUNT\" DESC, \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAGroupBy() => Same("SELECT \"REGION\", COUNT(*), SUM(\"AMOUNT\"), MIN(\"AMOUNT\"), MAX(\"AMOUNT\") FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"");

        /// <summary>
        /// A GROUP BY with no ORDER BY, which is where the two conventions have to agree on an order neither
        /// was asked for: ours groups in a <see cref="System.Collections.Generic.Dictionary{TKey, TValue}"/>
        /// and Calcite's in a <c>java.util.HashMap</c>.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnAGroupBysOwnOrder() => Same("SELECT \"REGION\", COUNT(*) FROM \"SALES\" GROUP BY \"REGION\"");

        // MIN, MAX, SUM and AVG over a column of type ANY, whose Java class is Object.
        //
        // Not Same: Calcite cannot run any of these, so there is no oracle to compare against and the answers
        // are asserted by hand, exactly as they are for a .NET user-defined function. What Calcite does with
        // them is held separately by ShouldStillBeBeyondCalcite below, which is the test that says when to
        // come back here — the day Calcite implements these, these answers are what its own should be
        // compared against.
        //
        // The values are BigDecimal wherever SqlFunctions.plusAny and divideAny have been through them, which
        // is what Calcite's ANY arithmetic answers for a scalar + as well.

        [TestMethod]
        public void ShouldAggregateAnAnyColumn() => Gives("SELECT MIN(\"V\"), MAX(\"V\"), SUM(\"V\"), AVG(\"V\") FROM \"ANYS\"", "5|30|65.5|16.375");

        [TestMethod]
        public void ShouldGroupAnAggregateOverAnAnyColumn() => Gives("SELECT \"K\", MIN(\"V\"), MAX(\"V\"), SUM(\"V\"), AVG(\"V\") FROM \"ANYS\" GROUP BY \"K\" ORDER BY \"K\"", "EAST|10|20.5|30.5|15.25", "WEST|5|30|35|17.5");

        /// <summary>
        /// MIN and MAX over an ANY column holding two numeric classes.
        /// </summary>
        /// <remarks>
        /// The case that decides the comparison. <c>SqlFunctions.lesser</c>, which
        /// <c>RexImpTable.MinMaxImplementor</c> calls, compares through <c>Comparable.compareTo</c> and throws
        /// on an <c>Integer</c> against a <c>Double</c>; <c>ltAny</c> compares the two as BigDecimal, which is
        /// what a scalar <c>&lt;</c> over ANY already does. A schema of ANY columns is usually a document
        /// store, where one path holding both is the ordinary case rather than the odd one.
        /// </remarks>
        [TestMethod]
        public void ShouldAggregateAnAnyColumnOfMixedNumericTypes() => Gives("SELECT MIN(\"V\"), MAX(\"V\") FROM \"ANYS\" WHERE \"K\" = 'EAST'", "10|20.5");

        /// <summary>
        /// MIN and MAX over an ANY column holding strings.
        /// </summary>
        /// <remarks>
        /// A <see cref="string"/> is what IKVM gives <c>java.lang.Comparable</c> to as a ghost, so a value of
        /// one reaching a comparison is worth a test of its own. Nothing casts to <c>Comparable</c> here —
        /// <c>ltAny</c> takes two <c>Object</c>s — which is the whole reason the ghost cannot bite.
        /// </remarks>
        [TestMethod]
        public void ShouldAggregateAnAnyColumnOfStrings() => Gives("SELECT MIN(\"S\"), MAX(\"S\") FROM \"ANYS\"", "a|d");

        /// <summary>
        /// An aggregate over an ANY column of a group with no rows in it.
        /// </summary>
        /// <remarks>
        /// <c>StrictAggImplementor</c> decides this and neither implementor here overrides it: SUM is nullable
        /// and answers null over an empty set, and MIN and MAX do the same. The accumulator being null is also
        /// what MIN reads as "no row yet", so the two meanings meet here.
        /// </remarks>
        [TestMethod]
        public void ShouldAggregateAnEmptyAnyColumn() => Gives("SELECT MIN(\"V\"), MAX(\"V\"), SUM(\"V\"), AVG(\"V\") FROM \"ANYS\" WHERE \"K\" = 'NORTH'", "<null>|<null>|<null>|<null>");

        /// <summary>
        /// SUM over an ANY column holding something that cannot be added.
        /// </summary>
        /// <remarks>
        /// Calcite's refusal, reached from the accumulator rather than from a scalar <c>+</c>:
        /// <c>plusAny</c> throws for anything but two numbers, and this convention does not soften that. A
        /// query that adds up a document path holding text should say so rather than answer.
        /// </remarks>
        [TestMethod]
        public void ShouldRefuseToSumAnAnyColumnOfStrings()
        {
            var act = () => Run("SELECT SUM(\"S\") FROM \"ANYS\"", true);

            act.Should().Throw<java.lang.RuntimeException>().WithMessage("*arithmetic*");
        }

        /// <summary>
        /// MIN, MAX and SUM over an ANY column in a window.
        /// </summary>
        /// <remarks>
        /// The same implementors, reached the other way. <c>RexImpTable</c> answers a window context with the
        /// regular implementor for any function that has no window implementor of its own, and none of these
        /// three has one, so <c>ClrEnumerableWindow</c> asks for and gets the ANY substitution — but it asks
        /// through its own code rather than through the aggregate's, which is why this is worth running.
        /// </remarks>
        [TestMethod]
        public void ShouldWindowAnAggregateOverAnAnyColumn()
        {
            Gives("SELECT \"ID\", MIN(\"V\") OVER (PARTITION BY \"K\"), MAX(\"V\") OVER (PARTITION BY \"K\"), SUM(\"V\") OVER (PARTITION BY \"K\") FROM \"ANYS\" ORDER BY \"ID\"",
                "1|10|20.5|30.5",
                "2|10|20.5|30.5",
                "3|5|30|35",
                "4|5|30|35",
                "5|5|30|35");
        }

        [TestMethod]
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
        /// The same implementor as MAX, upstream and here: <c>RexImpTable</c> answers ANY_VALUE with
        /// <c>MinMaxImplementor</c>, which asks whether the kind is MIN and takes the other branch when it is
        /// not. So the value is the largest rather than an arbitrary one, and that is Calcite's choice being
        /// followed rather than a decision made here.
        /// </remarks>
        [TestMethod]
        public void ShouldTakeAnyValueOfAnAnyColumn() => Gives("SELECT ANY_VALUE(\"V\"), ANY_VALUE(\"S\") FROM \"ANYS\"", "30|d");

        /// <summary>
        /// The deviations and the variances over an ANY column.
        /// </summary>
        /// <remarks>
        /// None of these has an implementor in any convention, in any type. <c>AGGREGATE_REDUCE_FUNCTIONS</c>
        /// rewrites each into sums of the value and of its square over a count, so they cost nothing beyond
        /// SUM working — but that means they are only reachable while it does, and a test says so rather than
        /// leaving it to be rediscovered.
        /// </remarks>
        [TestMethod]
        public void ShouldDeviateOverAnAnyColumn() => Gives("SELECT VAR_POP(\"V\"), VAR_SAMP(\"V\") FROM \"ANYS\"", "93.171875|124.2291666666667");

        /// <summary>
        /// An aggregate over an ANY column carrying a FILTER.
        /// </summary>
        /// <remarks>
        /// The filter is <c>StrictAggImplementor</c>'s business rather than an implementor's — it folds into
        /// the same condition the null check builds — so this works for the same reason the null does. Worth
        /// a row of its own because Calcite cannot run it, and so the differential suite cannot say it.
        /// </remarks>
        [TestMethod]
        public void ShouldFilterAnAggregateOverAnAnyColumn() => Gives("SELECT MIN(\"V\") FILTER (WHERE \"ID\" > 1), SUM(\"V\") FILTER (WHERE \"K\" = 'EAST') FROM \"ANYS\"", "5|30.5");

        /// <summary>
        /// A DISTINCT aggregate over an ANY column.
        /// </summary>
        /// <remarks>
        /// Both conventions refuse a distinct call outright, exactly as <c>EnumerableAggregate</c> does, and
        /// <c>AGGREGATE_EXPAND_DISTINCT_AGGREGATES</c> is what takes the DISTINCT off before either sees it.
        /// So this measures the rule reaching an ANY column rather than anything in the implementors.
        /// </remarks>
        [TestMethod]
        public void ShouldAggregateDistinctlyOverAnAnyColumn() => Gives("SELECT COUNT(DISTINCT \"V\"), SUM(DISTINCT \"V\") FROM \"ANYS\"", "4|65.5");

        /// <summary>
        /// Requires that these are still queries Calcite itself cannot run.
        /// </summary>
        /// <param name="sql"></param>
        /// <remarks>
        /// The other half of asserting an answer by hand. The four queries above have no oracle only for as
        /// long as <c>EnumerableConvention</c> cannot implement them, and if that changes this goes red and
        /// says so — which is the moment to compare the two conventions row by row rather than to discover
        /// from a user that they disagree. It is not an assertion that Calcite ought to fail.
        /// </remarks>
        static void StillBeyondCalcite(string sql)
        {
            Failure(() => Run(sql, false)).Should().NotBeNull(
                "Calcite still cannot implement '{0}'; now that it can, the answers this convention gives should be compared against its own", sql);
        }

        /// <summary>
        /// Runs a query and returns what it threw, or null if it did not throw.
        /// </summary>
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

        [TestMethod]
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
        }

        // and the same column read every way that already worked, so that a change here is known to be about
        // the aggregate rather than about the column, the fixture or the scan

        [TestMethod]
        public void ShouldAgreeOnScanningAnAnyColumn() => Same("SELECT \"K\", \"V\", \"S\" FROM \"ANYS\"");

        [TestMethod]
        public void ShouldAgreeOnCountingAnAnyColumn() => Same("SELECT \"K\", COUNT(\"V\"), COUNT(*) FROM \"ANYS\" GROUP BY \"K\" ORDER BY \"K\"");

        [TestMethod]
        public void ShouldAgreeOnAggregatingACastAnyColumn() => Same("SELECT MIN(CAST(\"V\" AS INTEGER)), MAX(CAST(\"V\" AS INTEGER)), SUM(CAST(\"V\" AS INTEGER)), AVG(CAST(\"V\" AS INTEGER)) FROM \"ANYS\"");

        [TestMethod]
        public void ShouldAgreeOnAGroupedAggregateOverACastAnyColumn() => Same("SELECT \"K\", MIN(CAST(\"V\" AS INTEGER)), SUM(CAST(\"V\" AS INTEGER)) FROM \"ANYS\" GROUP BY \"K\" ORDER BY \"K\"");

        [TestMethod]
        public void ShouldAgreeOnCastingAnAnyColumnToVarchar() => Same("SELECT \"ID\", CAST(\"G\" AS VARCHAR) FROM \"CASTS\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnCastingAnAnyColumnToANumber() => Same("SELECT \"ID\", CAST(\"N\" AS INTEGER), CAST(\"N\" AS DECIMAL(10, 2)) FROM \"CASTS\" ORDER BY \"ID\"");

        /// <summary>
        /// A TIMESTAMP whose source is ANY reads the value as the internal representation — epoch millis —
        /// rather than parsing it, and this holds that both conventions do.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnCastingAnAnyColumnOfMillisToATimestamp() => Same("SELECT \"ID\", CAST(\"M\" AS TIMESTAMP) FROM \"CASTS\" ORDER BY \"ID\"");

        /// <summary>
        /// The other half of the same fact: a timestamp written as text is not epoch millis, so the same
        /// cast over the same column asks <c>Long.parseLong</c> for a date and gets what it deserves.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnRefusingATimestampCastOfAnAnyColumnOfText() => SameFailure("SELECT CAST(\"T\" AS TIMESTAMP) FROM \"CASTS\"", "For input string: \"2026-01-01 00:00:00\"");

        /// <summary>
        /// A UUID whose source is ANY converts nothing at all: <c>JavaTypeFactoryImpl.getJavaClass</c> has
        /// no UUID case, so the target class is <c>Object</c> and the cast is the identity. The string
        /// arrives at the projection wearing a type it does not have.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnCastingAnAnyColumnToUuidChangingNothing() => Same("SELECT \"ID\", CAST(\"G\" AS UUID) FROM \"CASTS\" ORDER BY \"ID\"");

        /// <summary>
        /// And that the second cast is what converts, VARCHAR being a source branch every target has.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnCastingAnAnyColumnThroughVarcharToUuid() => Same("SELECT \"ID\", CAST(CAST(\"G\" AS VARCHAR) AS UUID) FROM \"CASTS\" ORDER BY \"ID\"");

        /// <summary>
        /// The same route to a timestamp, which reaches the string parser and so wants SQL's literal
        /// spelling rather than ISO-8601.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnCastingAnAnyColumnThroughVarcharToATimestamp() => Same("SELECT \"ID\", CAST(CAST(\"T\" AS VARCHAR) AS TIMESTAMP) FROM \"CASTS\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAGlobalAggregate() => Same("SELECT COUNT(*), SUM(\"AMOUNT\"), AVG(\"AMOUNT\") FROM \"SALES\"");

        // An aggregate call carrying its own ordering, which holds the rows of a group and folds them once
        // the call's ordering has been applied — LazyAggregateLambdaFactory over a SourceSorter per ordered
        // call and a BasicLazyAccumulator per unordered one. The four cover: one ordered call, a global
        // aggregate with no GROUP BY, an ordered and an unordered call in one aggregate, and an ordering on a
        // nullable column.

        // The three branches of the aggregate that are not a plain GROUP BY. A grouping set folds every row
        // into one group per set in a single pass, and the group columns a set does not group by come out
        // null however the row read — which is what the indicator field of the key decides.

        [TestMethod]
        public void ShouldAgreeOnGroupingSets() =>
            Same("SELECT \"REGION\", COUNT(*) FROM \"SALES\" GROUP BY GROUPING SETS ((\"REGION\"), ()) ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnARollup() =>
            Same("SELECT \"REGION\", \"LABEL\", COUNT(*) FROM \"SALES\" GROUP BY ROLLUP(\"REGION\", \"LABEL\") ORDER BY 1, 2, 3");

        [TestMethod]
        public void ShouldAgreeOnACube() =>
            Same("SELECT \"REGION\", \"LABEL\", COUNT(*) FROM \"SALES\" GROUP BY CUBE(\"REGION\", \"LABEL\") ORDER BY 1, 2, 3");

        [TestMethod]
        public void ShouldAgreeOnGroupingSetsOwnOrder() =>
            Same("SELECT \"REGION\", COUNT(*) FROM \"SALES\" GROUP BY GROUPING SETS ((\"REGION\"), ())");

        // A grouping set over four columns keys on eight fields -- one per column and one indicator per column
        // -- and a row of more than six is the only one FlatLists.copyOf builds, over an array whose element
        // type Calcite names as Comparable. That is the arity at which a VARCHAR group column reaches the
        // ghost interface, so no grouping set over three columns or fewer covers it.

        [TestMethod]
        public void ShouldAgreeOnARollupOverEveryColumn() =>
            Same("SELECT \"ID\", \"REGION\", \"AMOUNT\", \"LABEL\", COUNT(*) FROM \"SALES\" GROUP BY ROLLUP(\"ID\", \"REGION\", \"AMOUNT\", \"LABEL\") ORDER BY 1, 2, 3, 4, 5");

        [TestMethod]
        public void ShouldAgreeOnTheGroupingFunction() =>
            Same("SELECT \"REGION\", GROUPING(\"REGION\"), COUNT(*) FROM \"SALES\" GROUP BY ROLLUP(\"REGION\") ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnADistinctOverEveryColumn() =>
            Same("SELECT DISTINCT \"ID\", \"REGION\", \"AMOUNT\", \"LABEL\" FROM \"SALES\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAnOrderedAggregateCall() =>
            Same("SELECT \"REGION\", LISTAGG(\"LABEL\", ',') WITHIN GROUP (ORDER BY \"ID\" DESC) FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"");

        [TestMethod]
        public void ShouldAgreeOnAGlobalOrderedAggregateCall() =>
            Same("SELECT LISTAGG(\"LABEL\", ',') WITHIN GROUP (ORDER BY \"ID\" DESC) FROM \"SALES\"");

        [TestMethod]
        public void ShouldAgreeOnAnOrderedAndAnUnorderedCallTogether() =>
            Same("SELECT \"REGION\", COUNT(*), LISTAGG(\"LABEL\", ',') WITHIN GROUP (ORDER BY \"ID\") FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"");

        [TestMethod]
        public void ShouldAgreeOnAnAggregateCallOrderedByANullableColumn() =>
            Same("SELECT \"REGION\", LISTAGG(\"LABEL\", ',') WITHIN GROUP (ORDER BY \"AMOUNT\") FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"");

        [TestMethod]
        public void ShouldAgreeOnAnInnerJoin() => Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnALeftJoin() => Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT JOIN (SELECT * FROM \"SALES\" WHERE \"AMOUNT\" > 25) b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        // A batch nested loop join, which needs its rule turned on. The right input becomes a filter over a
        // disjunction of the batch's conditions, so one pass of it serves a hundred left rows.

        [TestMethod]
        public void ShouldAgreeOnABatchNestedLoopJoin() =>
            SameBatchNestedLoopJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnABatchNestedLoopLeftJoin() =>
            SameBatchNestedLoopJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT JOIN (SELECT * FROM \"SALES\" WHERE \"ID\" > 4) b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnABatchNestedLoopJoinWithAnInequality() =>
            SameBatchNestedLoopJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" < b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnABatchNestedLoopSemiJoin() =>
            SameBatchNestedLoopJoin("SELECT \"ID\" FROM \"SALES\" a WHERE EXISTS (SELECT 1 FROM \"SALES\" b WHERE b.\"REGION\" = a.\"REGION\" AND b.\"ID\" > a.\"ID\") ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnABatchNestedLoopAntiJoin() =>
            SameBatchNestedLoopJoin("SELECT \"ID\" FROM \"SALES\" a WHERE NOT EXISTS (SELECT 1 FROM \"SALES\" b WHERE b.\"REGION\" = a.\"REGION\" AND b.\"ID\" > a.\"ID\") ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAJoinWithAnInequality() => Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" < b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAnAsofJoin() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a ASOF JOIN \"SALES\" b MATCH_CONDITION b.\"ID\" <= a.\"ID\" ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnALeftAsofJoin() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT ASOF JOIN (SELECT * FROM \"SALES\" WHERE \"ID\" > 3) b MATCH_CONDITION b.\"ID\" <= a.\"ID\" ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAnAsofJoinLookingForward() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a ASOF JOIN \"SALES\" b MATCH_CONDITION b.\"ID\" > a.\"ID\" ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\"");

        /// <summary>
        /// The order of an ASOF join's rows is the order of the map it indexes the left input by, so a query
        /// with no ORDER BY is the one that says whether ours agrees with linq4j's.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnAnAsofJoinsOwnOrder() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a ASOF JOIN \"SALES\" b MATCH_CONDITION b.\"ID\" <= a.\"ID\" ON a.\"REGION\" = b.\"REGION\"");

        [TestMethod]
        public void ShouldAgreeOnAnAsofJoinOnASeveralFieldKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a ASOF JOIN \"SALES\" b MATCH_CONDITION b.\"ID\" <= a.\"ID\" ON a.\"REGION\" = b.\"REGION\" AND a.\"LABEL\" = b.\"LABEL\" ORDER BY a.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnALeftAsofJoinWithANullKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT ASOF JOIN \"SALES\" b MATCH_CONDITION b.\"ID\" <= a.\"ID\" ON a.\"AMOUNT\" = b.\"AMOUNT\" ORDER BY a.\"ID\"");

        // A right and a full join with no ORDER BY: the rows of the right input that matched nothing come out
        // at the end, in the order of the lookup the join built.

        [TestMethod]
        public void ShouldAgreeOnARightJoinsOwnOrder() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM (SELECT * FROM \"SALES\" WHERE \"ID\" < 3) a RIGHT JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"LABEL\" = b.\"LABEL\"");

        /// <summary>
        /// The same question at the one build-side size where the collection the leftovers are walked from
        /// decides the answer.
        /// </summary>
        /// <remarks>
        /// <c>WIDE</c> has twelve keys, so the lookup is a table of 16 and the <c>HashSet</c> copied from its
        /// key set is a table of 32; the two orders differ. <see cref="ShouldAgreeOnARightJoinsOwnOrder"/> is
        /// over six keys, where both are 16 and either collection gives the same rows.
        /// </remarks>
        [TestMethod]
        public void ShouldAgreeOnARightJoinsOwnOrderOverTwelveKeys() =>
            SameThrough("ClrEnumerableHashJoin", "SELECT a.\"N\", b.\"K\" FROM (SELECT * FROM \"WIDE\" WHERE \"N\" < 3) a RIGHT JOIN \"WIDE\" b ON a.\"K\" = b.\"K\"");

        [TestMethod]
        public void ShouldAgreeOnAFullJoinsOwnOrderOverTwelveKeys() =>
            SameThrough("ClrEnumerableHashJoin", "SELECT a.\"N\", b.\"K\" FROM (SELECT * FROM \"WIDE\" WHERE \"N\" < 3) a FULL JOIN \"WIDE\" b ON a.\"K\" = b.\"K\"");

        [TestMethod]
        public void ShouldAgreeOnAFullJoinsOwnOrder() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM (SELECT * FROM \"SALES\" WHERE \"ID\" < 3) a FULL JOIN (SELECT * FROM \"SALES\" WHERE \"ID\" > 1) b ON a.\"LABEL\" = b.\"LABEL\"");

        // A set operation with no ORDER BY, which is the same question as the GROUP BY above: the rows come
        // out in the order of the collection the operator held them in, and Calcite holds them in a
        // java.util.HashSet or a HashMultiset.

        [TestMethod]
        public void ShouldAgreeOnUnionsOwnOrder() => Same("SELECT \"REGION\" FROM \"SALES\" UNION SELECT \"LABEL\" FROM \"SALES\"");

        [TestMethod]
        public void ShouldAgreeOnIntersectsOwnOrder() => Same("SELECT \"LABEL\" FROM \"SALES\" INTERSECT SELECT \"LABEL\" FROM \"SALES\" WHERE \"ID\" < 5");

        [TestMethod]
        public void ShouldAgreeOnIntersectAllsOwnOrder() => Same("SELECT \"REGION\" FROM \"SALES\" INTERSECT ALL SELECT \"REGION\" FROM \"SALES\" WHERE \"ID\" < 5");

        [TestMethod]
        public void ShouldAgreeOnExceptsOwnOrder() => Same("SELECT \"LABEL\" FROM \"SALES\" EXCEPT SELECT \"LABEL\" FROM \"SALES\" WHERE \"ID\" > 4");

        [TestMethod]
        public void ShouldAgreeOnExceptAllsOwnOrder() => Same("SELECT \"REGION\" FROM \"SALES\" EXCEPT ALL SELECT \"REGION\" FROM \"SALES\" WHERE \"ID\" > 4");

        [TestMethod]
        public void ShouldAgreeOnDistinctsOwnOrder() => Same("SELECT DISTINCT \"REGION\" FROM \"SALES\"");

        [TestMethod]
        public void ShouldAgreeOnUnionAll() => Same("SELECT \"ID\" FROM \"SALES\" UNION ALL SELECT \"ID\" FROM \"SALES\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnUnionDistinct() => Same("SELECT \"REGION\" FROM \"SALES\" UNION SELECT \"REGION\" FROM \"SALES\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnIntersect() => Same("SELECT \"REGION\" FROM \"SALES\" INTERSECT SELECT \"REGION\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnExcept() => Same("SELECT \"REGION\" FROM \"SALES\" EXCEPT SELECT \"REGION\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnLimitAndOffset() => Same("SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY");

        [TestMethod]
        public void ShouldAgreeOnALimitSort() =>
            SameLimitSort("SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" FETCH NEXT 3 ROWS ONLY");

        [TestMethod]
        public void ShouldAgreeOnALimitSortWithAnOffset() =>
            SameLimitSort("SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY");

        [TestMethod]
        public void ShouldAgreeOnALimitSortWithAnOffsetAndNoFetch() =>
            SameLimitSort("SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" OFFSET 4 ROWS");

        [TestMethod]
        public void ShouldAgreeOnALimitSortOverANullableKey() =>
            SameLimitSort("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" FETCH NEXT 4 ROWS ONLY");

        [TestMethod]
        public void ShouldAgreeOnALimitSortPastTheEnd() =>
            SameLimitSort("SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" DESC OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY");

        /// <summary>
        /// Both conventions plan a limit sort for the queries above, rather than one of them planning a limit
        /// over a sort.
        /// </summary>
        /// <remarks>
        /// Without this the five tests above would agree for the wrong reason. The rule was in this
        /// convention's default set and never in Calcite's, so a limit sort was compared against a limit over
        /// a sort and the node had no oracle at all.
        /// </remarks>
        [TestMethod]
        public void ShouldPlanALimitSortInBothConventions()
        {
            const string sql = "SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY";

            PlanOf(sql, true, limitSort: true).Should().Contain("ClrEnumerableLimitSort");
            PlanOf(sql, false, limitSort: true).Should().Contain("EnumerableLimitSort");
        }

        // The limit sort's edges, against Calcite. OrderByWithFetchAndOffset was a full sort followed by a
        // skip and a take, where linq4j keeps at most offset + fetch rows and evicts as it reads
        // (CALCITE-3920, CALCITE-4157). Porting that algorithm properly means the eviction, the tie handling
        // and the offset-past-the-end case are all newly written code, and none of it is visible in the
        // answer to an ordinary query -- so these are the cases where a hand-rolled bound goes wrong.

        [TestMethod]
        public void ShouldAgreeOnALimitSortWithTiesAcrossTheBoundary() =>
            SameThrough("ClrEnumerableLimitSort", "SELECT \"K\", \"V\" FROM \"SORTED\" ORDER BY \"K\" FETCH NEXT 2 ROWS ONLY", limitSort: true);

        [TestMethod]
        public void ShouldAgreeOnALimitSortWithAnOffsetInsideATie() =>
            SameThrough("ClrEnumerableLimitSort", "SELECT \"K\", \"V\" FROM \"SORTED\" ORDER BY \"K\" OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY", limitSort: true);

        [TestMethod]
        /// <remarks>
        /// No node assertion: an offset past the end lets the planner prune the whole thing, so there is no
        /// limit sort in the plan to find. What is being compared is that both sides answer nothing.
        /// </remarks>
        public void ShouldAgreeOnALimitSortWithAnOffsetPastTheEnd() =>
            Same("SELECT \"K\" FROM \"SORTED\" ORDER BY \"K\" OFFSET 10 ROWS FETCH NEXT 2 ROWS ONLY", limitSort: true);

        [TestMethod]
        public void ShouldAgreeOnALimitSortTakingEverything() =>
            SameThrough("ClrEnumerableLimitSort", "SELECT \"K\" FROM \"SORTED\" ORDER BY \"K\" FETCH NEXT 100 ROWS ONLY", limitSort: true);

        [TestMethod]
        public void ShouldAgreeOnALimitSortOverNulls() =>
            SameThrough("ClrEnumerableLimitSort", "SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" OFFSET 1 ROWS FETCH NEXT 3 ROWS ONLY", limitSort: true);

        [TestMethod]
        public void ShouldAgreeOnALimitSortDescending() =>
            SameThrough("ClrEnumerableLimitSort", "SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" DESC OFFSET 1 ROWS FETCH NEXT 3 ROWS ONLY", limitSort: true);

        /// <summary>
        /// With neither side given the rule, both plan a limit over a sort — which is what carried a
        /// one-column primitive result across the converter and found the cast in <c>JavaSequences.FromJava</c>.
        /// </summary>
        [TestMethod]
        public void ShouldPlanALimitOverASortWithoutTheRule() =>
            PlanOf("SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY", true)
                .Should().NotContain("LimitSort");

        [TestMethod]
        public void ShouldAgreeOnAMarkJoinFromExists() =>
            SameMarkJoin("SELECT \"ID\" FROM \"SALES\" WHERE EXISTS (SELECT 1 FROM \"SALES\" \"S2\" WHERE \"S2\".\"ID\" > 4) ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAMarkJoinFromAnEmptyExists() =>
            SameMarkJoin("SELECT \"ID\" FROM \"SALES\" WHERE EXISTS (SELECT 1 FROM \"SALES\" \"S2\" WHERE \"S2\".\"ID\" > 99) ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldPlanANestedLoopMarkJoin() =>
            PlanOf("SELECT \"ID\" FROM \"SALES\" WHERE EXISTS (SELECT 1 FROM \"SALES\" \"S2\" WHERE \"S2\".\"ID\" > 4)", true, markJoin: true)
                .Should().Contain("ClrEnumerableNestedLoopJoin").And.Contain("left_mark");

        [TestMethod]
        public void ShouldAgreeOnAMarkJoinFromIn() =>
            SameMarkJoin("SELECT \"ID\" FROM \"SALES\" WHERE \"AMOUNT\" IN (SELECT \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" > 3) ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAMarkJoinFromInProjected() =>
            SameMarkJoin("SELECT \"ID\", \"AMOUNT\" IN (SELECT \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" > 3) AS \"M\" FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAMarkJoinOverANullKey() =>
            SameMarkJoin("SELECT \"ID\", \"AMOUNT\" IN (SELECT \"AMOUNT\" FROM \"SALES\") AS \"M\" FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAMarkJoinOverAnEmptyRight() =>
            SameMarkJoin("SELECT \"ID\", \"AMOUNT\" IN (SELECT \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" > 99) AS \"M\" FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnANotInMarkJoin() =>
            SameMarkJoin("SELECT \"ID\" FROM \"SALES\" WHERE \"AMOUNT\" NOT IN (SELECT \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" > 3) ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldPlanAHashMarkJoin() =>
            PlanOf("SELECT \"ID\" FROM \"SALES\" WHERE \"AMOUNT\" IN (SELECT \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" > 3)", true, markJoin: true)
                .Should().Contain("ClrEnumerableHashJoin").And.Contain("left_mark");

        [TestMethod]
        public void ShouldAgreeOnACorrelatedMarkJoinFromExists() =>
            SameMarkJoin("SELECT \"ID\" FROM \"SALES\" \"S1\" WHERE EXISTS (SELECT 1 FROM \"SALES\" \"S2\" WHERE \"S2\".\"REGION\" = \"S1\".\"REGION\" AND \"S2\".\"ID\" > 3) ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnACorrelatedMarkJoinFromIn() =>
            SameMarkJoin("SELECT \"ID\" FROM \"SALES\" \"S1\" WHERE \"AMOUNT\" IN (SELECT \"AMOUNT\" FROM \"SALES\" \"S2\" WHERE \"S2\".\"REGION\" = \"S1\".\"REGION\") ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnACorrelatedMarkJoinProjected() =>
            SameMarkJoin("SELECT \"ID\", EXISTS (SELECT 1 FROM \"SALES\" \"S2\" WHERE \"S2\".\"REGION\" = \"S1\".\"REGION\" AND \"S2\".\"ID\" > 3) AS \"E\" FROM \"SALES\" \"S1\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldPlanAConditionalCorrelate() =>
            PlanOf("SELECT \"ID\" FROM \"SALES\" \"S1\" WHERE EXISTS (SELECT 1 FROM \"SALES\" \"S2\" WHERE \"S2\".\"REGION\" = \"S1\".\"REGION\" AND \"S2\".\"ID\" > 3)", true, markJoin: true)
                .Should().Contain("ClrEnumerableConditionalCorrelate").And.Contain("left_mark");

        [TestMethod]
        public void ShouldAgreeOnValues() => Same("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y)");

        // The only query that reaches ClrEnumerableRepeatUnion and ClrEnumerableTableSpool. The transient
        // table is scanned by neither convention — EnumerableTableScan refuses a TransientTable
        // (CALCITE-3673) and so does ours — so both sides read it through the interpreter.

        [TestMethod]
        public void ShouldAgreeOnARecursiveQuery() =>
            Same("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 4) SELECT n FROM t ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnARecursiveQueryOfSeveralColumns() =>
            Same("WITH RECURSIVE t(n, m) AS (VALUES (1, 10) UNION ALL SELECT n + 1, m + 10 FROM t WHERE n < 4) SELECT n, m FROM t ORDER BY 1");

        // The interpreter, which is the only way either convention reads a transient table. With the rule off
        // the node is Calcite's under a converter; with it on it is this convention's and there is one
        // convention boundary fewer. The rows are the same either way, which is what the first two assert.

        [TestMethod]
        public void ShouldAgreeOnARecursiveQueryInterpretedHere() =>
            SameInterpreted("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 4) SELECT n FROM t ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnARecursiveQueryOfSeveralColumnsInterpretedHere() =>
            SameInterpreted("WITH RECURSIVE t(n, m) AS (VALUES (1, 10) UNION ALL SELECT n + 1, m + 10 FROM t WHERE n < 4) SELECT n, m FROM t ORDER BY 1");

        [TestMethod]
        public void ShouldPlanTheInterpreterInThisConvention()
        {
            var sql = "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 4) SELECT n FROM t ORDER BY 1";

            PlanOf(sql, true).Should().Contain("EnumerableInterpreter");
            PlanOf(sql, true, interpreter: true).Should().Contain("ClrEnumerableInterpreter");
        }

        [TestMethod]
        public void ShouldAgreeOnACorrelatedSubQuery() => Same("SELECT \"ID\" FROM \"SALES\" a WHERE \"AMOUNT\" = (SELECT MAX(\"AMOUNT\") FROM \"SALES\" b WHERE b.\"REGION\" = a.\"REGION\") ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAScalarSubQuery() => Same("SELECT \"ID\", (SELECT COUNT(*) FROM \"SALES\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnCaseAndNullHandling() => Same("SELECT \"ID\", CASE WHEN \"AMOUNT\" IS NULL THEN -1 ELSE \"AMOUNT\" END FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnRowNumber() => Same("SELECT \"ID\", ROW_NUMBER() OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnRankOverTies() => Same("SELECT \"ID\", RANK() OVER (ORDER BY \"AMOUNT\"), DENSE_RANK() OVER (ORDER BY \"AMOUNT\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAPartitionedWindow() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (PARTITION BY \"REGION\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnARunningTotal() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (PARTITION BY \"REGION\" ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnARowsFrame() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\" ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnARangeFrame() => Same("SELECT \"ID\", COUNT(*) OVER (ORDER BY \"AMOUNT\" RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnSeveralWindows() => Same("SELECT \"ID\", ROW_NUMBER() OVER (ORDER BY \"ID\"), SUM(\"AMOUNT\") OVER (PARTITION BY \"REGION\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAWindowOverEverything() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER () FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnSeveralPartitionKeys() => Same("SELECT \"ID\", COUNT(*) OVER (PARTITION BY \"REGION\", \"AMOUNT\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnANullPartitionKey() => Same("SELECT \"ID\", COUNT(*) OVER (PARTITION BY \"AMOUNT\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAnEmptyFrame() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\" ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING) FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
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
        /// <para>Ours is the same translation and fails the same way: IKVM maps that exception onto
        /// <see cref="NullReferenceException"/>, and the unboxing an expression tree does for the same
        /// arithmetic raises the same one. Both sides are asserted, because the point is not that ours throws
        /// -- it is that neither convention answers a query the other answers. If Calcite ever fixes this,
        /// this test fails and tells us to follow.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldAgreeOnFailingARangeFrameWithAnOffsetOverANullableKey()
        {
            const string sql = "SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"AMOUNT\" RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) FROM \"SALES\" ORDER BY \"ID\"";

            var calcite = () => Run(sql, false);
            var mine = () => Run(sql, true);

            calcite.Should().Throw<NullReferenceException>("Calcite unboxes a null order key");
            mine.Should().Throw<NullReferenceException>("and so do we, from the same translation");
        }

        [TestMethod]
        public void ShouldAgreeOnLeadAndLag() => Same("SELECT \"ID\", LAG(\"AMOUNT\") OVER (ORDER BY \"ID\"), LEAD(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnFirstAndLastValue() => Same("SELECT \"ID\", FIRST_VALUE(\"AMOUNT\") OVER (PARTITION BY \"REGION\" ORDER BY \"ID\"), LAST_VALUE(\"AMOUNT\") OVER (PARTITION BY \"REGION\" ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // a running frame rather than the whole partition, because it is the one that tells the three
        // exclusions apart: over an unbounded frame Calcite reports no row as any other's peer
        [TestMethod]
        public void ShouldAgreeOnExcludingTheCurrentRow() => Same("SELECT \"ID\", COUNT(\"AMOUNT\") OVER (ORDER BY \"AMOUNT\" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnExcludingTies() => Same("SELECT \"ID\", COUNT(\"AMOUNT\") OVER (ORDER BY \"AMOUNT\" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnExcludingTheGroup() => Same("SELECT \"ID\", COUNT(\"AMOUNT\") OVER (ORDER BY \"AMOUNT\" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnNtile() => Same("SELECT \"ID\", NTILE(2) OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnNthValue() => Same("SELECT \"ID\", NTH_VALUE(\"AMOUNT\", 2) OVER (PARTITION BY \"REGION\" ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // one aggregate whose value survives an intact frame and one that does not, in the same window, so
        // both result lambdas run over the same accumulator
        [TestMethod]
        public void ShouldAgreeOnACachedAndAnUncachedAggregateTogether() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\"), LAG(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // a RANGE frame ending at the current row with more than one ordering key, which is the only shape
        // that reaches the five-argument binary search
        [TestMethod]
        public void ShouldAgreeOnARangeFrameOverSeveralOrderKeys() => Same("SELECT \"ID\", COUNT(*) OVER (ORDER BY \"REGION\", \"AMOUNT\" RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM \"SALES\" ORDER BY \"ID\"");

        // no ORDER BY, so the rows arrive in the order the partitions do, which is a hash map's. Nothing
        // reproduces that but the map itself: under IKVM a String hashes as .NET hashes it, which is
        // randomised per process, so the order is not even the same run to run — both conventions read the
        // same map in the same process, which is the only reason this can be asserted at all.
        [TestMethod]
        public void ShouldAgreeOnThePartitionOrder() => Same("SELECT \"REGION\", \"ID\", COUNT(*) OVER (PARTITION BY \"REGION\") FROM \"SALES\"");

        // a key that is a primitive, so it has to be boxed the way the type factory says before a map holds it
        [TestMethod]
        public void ShouldAgreeOnAPrimitivePartitionKey() => Same("SELECT \"ID\", COUNT(*) OVER (PARTITION BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // two calls on one implementor instance, which keeps state of its own between getStateType and
        // implementAdd: COUNT(*) takes the frame's row count and COUNT of a nullable column accumulates
        [TestMethod]
        public void ShouldAgreeOnTwoCountsInOneWindow() => Same("SELECT \"ID\", COUNT(*) OVER (ORDER BY \"ID\"), COUNT(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAFrameEntirelyFollowing() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\" ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnMinAndMax() => Same("SELECT \"ID\", MIN(\"AMOUNT\") OVER (PARTITION BY \"REGION\"), MAX(\"AMOUNT\") OVER (PARTITION BY \"REGION\") FROM \"SALES\" ORDER BY \"ID\"");

        // AVG has no implementor, so this only reaches a window at all once it is reduced to SUM over COUNT
        [TestMethod]
        public void ShouldAgreeOnAnAverageOverAWindow() => Same("SELECT \"ID\", AVG(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAWindowOverNoRows() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (PARTITION BY \"REGION\") FROM \"SALES\" WHERE \"ID\" < 0 ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAWindowOverAFilteredInput() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" WHERE \"REGION\" = 'EAST' ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnRowNumberWithoutAnOrder() => Same("SELECT \"ID\", ROW_NUMBER() OVER (PARTITION BY \"REGION\") FROM \"SALES\" ORDER BY \"ID\"");

        // an offset and a default, so the window carries more than one constant past its input's own fields
        [TestMethod]
        public void ShouldAgreeOnLagWithAnOffsetAndDefault() => Same("SELECT \"ID\", LAG(\"AMOUNT\", 2, -1) OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // The two below were the only ones asserted by hand, because Calcite could not answer them. A
        // user-defined function written in C# is a class IKVM names cli.Apache.Calcite.Tests.SumAggregate;
        // EnumerableConvention writes that name into generated Java source, and Janino resolves it through
        // the class-loader stamp IKVM.Maven.Sdk puts on calcite-core — which IKVM 8.14.0 and 8.15.0 could
        // not read, so the plan failed to compile. This convention holds the method itself rather than its
        // name, so it ran either way. 8.16.0 reads the stamp again and Calcite runs the same query, measured
        // at one commit either side, so these are differential like the rest. The hand-written rows stay as
        // a second oracle, being SQL's answer rather than either convention's.

        // running sum over ORDER BY ID, which the default RANGE frame makes a prefix, with the null skipped
        [TestMethod]
        public void ShouldRunAUserDefinedWindowAggregate()
        {
            const string sql = "SELECT \"ID\", MY_SUM(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"";

            Same(sql);
            Gives(sql, "1|10", "2|30", "3|50", "4|80", "5|80", "6|85");
        }

        // EAST is 10 + 20 + 20 and WEST is 30 + 5, the null contributing nothing
        [TestMethod]
        public void ShouldRunAUserDefinedAggregate()
        {
            const string sql = "SELECT \"REGION\", MY_SUM(\"AMOUNT\") FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"";

            Same(sql);
            Gives(sql, "EAST|50", "WEST|35");
        }

        // SORTED advertises a collation, so Calcite plans these with EnumerableMergeJoin where it plans the
        // same query over SALES with a hash join. This convention has no merge join yet, so what these
        // compare is our hash join against Calcite's merge join — two algorithms, one answer. They become
        // the merge join's own tests the moment the node exists.
        [TestMethod]
        public void ShouldAgreeOnAJoinOverSortedInputs() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" JOIN \"SORTED\" \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnALeftJoinOverSortedInputs() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" LEFT JOIN \"SORTED\" \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" AND \"S2\".\"V\" <> 'B' ORDER BY 1, 2");

        // A sorted aggregate, which needs its rule turned on and is chosen where the query wants its output
        // ordered by the group key over an input that carries that collation. A global aggregate is refused
        // by our rule: Calcite builds the node for one and then cannot implement it, because the collation it
        // would tell groups apart with is empty.

        [TestMethod]
        public void ShouldAgreeOnASortedAggregate() =>
            SameSortedAggregate("SELECT \"K\", COUNT(*) FROM \"SORTED\" GROUP BY \"K\" ORDER BY \"K\"");

        [TestMethod]
        public void ShouldAgreeOnASortedAggregateOfSeveralCalls() =>
            SameSortedAggregate("SELECT \"K\", COUNT(*), MIN(\"V\"), MAX(\"V\") FROM \"SORTED\" GROUP BY \"K\" ORDER BY \"K\"");

        [TestMethod]
        public void ShouldAgreeOnASortedAggregateOverAFilteredInput() =>
            SameSortedAggregate("SELECT \"K\", COUNT(*) FROM \"SORTED\" WHERE \"V\" <> 'C' GROUP BY \"K\" ORDER BY \"K\"");

        [TestMethod]
        public void ShouldAgreeOnAGlobalAggregateWithTheSortedRuleOn() =>
            SameSortedAggregate("SELECT COUNT(*), MIN(\"V\") FROM \"SORTED\"");

        [TestMethod]
        public void ShouldAgreeOnAnUnorderedGroupByWithTheSortedRuleOn() =>
            SameSortedAggregate("SELECT \"K\", COUNT(*) FROM \"SORTED\" GROUP BY \"K\"");

        [TestMethod]
        public void ShouldAgreeOnAGroupByOverASortedInput() =>
            Same("SELECT \"K\", COUNT(*) FROM \"SORTED\" GROUP BY \"K\" ORDER BY 1");

        // A merge union: an ORDER BY directly over a UNION, which is the shape its rule requires. Naming the
        // columns instead of SELECT * puts a projection between the two and the rule never fires — that is
        // what made this node look unreachable for a while.

        [TestMethod]
        public void ShouldAgreeOnAMergeUnionAll() =>
            Same("SELECT * FROM \"SORTED\" UNION ALL SELECT * FROM \"SORTED\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAMergeUnionDistinct() =>
            Same("SELECT * FROM \"SORTED\" UNION SELECT * FROM \"SORTED\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAMergeUnionWithALimit() =>
            Same("SELECT * FROM \"SORTED\" UNION ALL SELECT * FROM \"SORTED\" ORDER BY 1 FETCH FIRST 3 ROWS ONLY");

        [TestMethod]
        public void ShouldAgreeOnAMergeUnionWithAnOffsetAndALimit() =>
            Same("SELECT * FROM \"SORTED\" UNION ALL SELECT * FROM \"SORTED\" ORDER BY 1 OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY");

        [TestMethod]
        public void ShouldAgreeOnAMergeUnionOfThreeInputs() =>
            Same("SELECT * FROM \"SORTED\" UNION ALL SELECT * FROM \"SORTED\" UNION ALL SELECT * FROM \"SORTED\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAUnionOverSortedInputs() =>
            Same("SELECT \"K\", \"V\" FROM \"SORTED\" UNION SELECT \"K\", \"V\" FROM \"SORTED\" ORDER BY 1, 2");

        // A merge join is what these plan to, on both sides, over an input that advertises a collation. The
        // four above reach it as well — they were written before the node existed, comparing our hash join
        // against Calcite's merge join, and they now compare the two merge joins. These add the join types
        // and the shapes the algorithm has separate paths for: a run of equal keys on both sides, a key
        // missing from one side, several keys, an extra condition that is not an equality, and a null key,
        // which is where the comparator refuses to call two nulls equal.

        [TestMethod]
        public void ShouldAgreeOnASemiJoinOverSortedInputs() =>
            Same("SELECT \"K\", \"V\" FROM \"SORTED\" \"S1\" WHERE EXISTS (SELECT 1 FROM \"SORTED\" \"S2\" WHERE \"S2\".\"K\" = \"S1\".\"K\") ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnAnAntiJoinOverSortedInputs() =>
            Same("SELECT \"K\", \"V\" FROM \"SORTED\" \"S1\" WHERE NOT EXISTS (SELECT 1 FROM \"SORTED\" \"S2\" WHERE \"S2\".\"K\" = \"S1\".\"K\" AND \"S2\".\"V\" = 'A') ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnAJoinOverSortedInputsMissingKeys() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" JOIN (SELECT * FROM \"SORTED\" WHERE \"K\" <> 2) \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnALeftJoinOverSortedInputsMissingKeys() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" LEFT JOIN (SELECT * FROM \"SORTED\" WHERE \"K\" > 2) \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnAJoinOverSortedInputsOnSeveralKeys() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" JOIN \"SORTED\" \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" AND \"S1\".\"V\" = \"S2\".\"V\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnAJoinOverSortedInputsWithAnExtraCondition() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" JOIN \"SORTED\" \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" AND \"S1\".\"V\" < \"S2\".\"V\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnAJoinOnANullableKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" = b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnALeftJoinOnANullableKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT JOIN \"SALES\" b ON a.\"AMOUNT\" = b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        // A null key, on the side the hash join builds from and under the operator that has to answer for it.
        // The two above join on a nullable key and neither reaches this: an INNER and a LEFT join never look
        // at the rows of the right input that matched nothing, and a plain equality never asks a null to
        // match. `ShouldAgreeOnARightJoinsOwnOrder` and `ShouldAgreeOnAFullJoinsOwnOrder` do look, and join
        // on REGION and LABEL, which are not nullable. The intersection of a nullable key and an outer join
        // on the build side is what these cover.

        [TestMethod]
        public void ShouldAgreeOnANullSafeJoinKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" IS NOT DISTINCT FROM b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnARightJoinOnANullableKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM (SELECT * FROM \"SALES\" WHERE \"ID\" < 3) a RIGHT JOIN \"SALES\" b ON a.\"AMOUNT\" = b.\"AMOUNT\"");

        [TestMethod]
        public void ShouldAgreeOnAFullJoinOnANullableKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM (SELECT * FROM \"SALES\" WHERE \"ID\" < 3) a FULL JOIN \"SALES\" b ON a.\"AMOUNT\" = b.\"AMOUNT\"");

        // A hash join on a key of two fields, one of them nullable, which is the case the null-aware accessor
        // nulls the whole key for and the plain accessor leaves as a list holding a null that matches another
        // one. Both conventions plan a merge join for this query, so the merge join rule comes off both sides
        // to reach the node the question is about.

        [TestMethod]
        public void ShouldAgreeOnAHashJoinOnTwoKeysOneNullable() =>
            SameHashJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"AMOUNT\" = b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnARightHashJoinOnTwoKeysOneNullable() =>
            SameHashJoin("SELECT a.\"ID\", b.\"ID\" FROM (SELECT * FROM \"SALES\" WHERE \"ID\" < 3) a RIGHT JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"AMOUNT\" = b.\"AMOUNT\"");

        [TestMethod]
        public void ShouldPlanAHashJoinWithoutTheMergeJoinRule()
        {
            var sql = "SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"AMOUNT\" = b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"";

            PlanOf(sql, false, excludeMergeJoin: true).Should().Contain("EnumerableHashJoin");
            PlanOf(sql, true, excludeMergeJoin: true).Should().Contain("ClrEnumerableHashJoin");
        }

        [TestMethod]
        public void ShouldAgreeOnASemiJoinOnANullSafeKey() =>
            Same("SELECT a.\"ID\" FROM \"SALES\" a WHERE EXISTS (SELECT 1 FROM \"SALES\" b WHERE a.\"AMOUNT\" IS NOT DISTINCT FROM b.\"AMOUNT\") ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAnAntiJoinOnANullSafeKey() =>
            Same("SELECT a.\"ID\" FROM \"SALES\" a WHERE NOT EXISTS (SELECT 1 FROM \"SALES\" b WHERE a.\"AMOUNT\" IS NOT DISTINCT FROM b.\"AMOUNT\") ORDER BY 1");

        // The CUSTOM row format, over HR.emps. Everything above runs over Object[] rows, so every one of
        // these takes a branch of PhysType that 239 tests had not.

        [TestMethod]
        public void ShouldAgreeOnACustomFormatScan() =>
            Same("SELECT \"empid\", \"name\" FROM \"HR\".\"emps\" ORDER BY \"empid\"");

        [TestMethod]
        public void ShouldAgreeOnACustomFormatFilterAndProjection() =>
            Same("SELECT \"empid\", \"salary\" + 1 FROM \"HR\".\"emps\" WHERE \"deptno\" = 10 ORDER BY \"empid\"");

        [TestMethod]
        public void ShouldAgreeOnACustomFormatNullableColumn() =>
            Same("SELECT \"empid\", \"commission\" FROM \"HR\".\"emps\" ORDER BY \"empid\"");

        [TestMethod]
        public void ShouldAgreeOnACustomFormatAggregate() =>
            Same("SELECT \"deptno\", COUNT(*), SUM(\"salary\"), MIN(\"commission\") FROM \"HR\".\"emps\" GROUP BY \"deptno\" ORDER BY \"deptno\"");

        [TestMethod]
        public void ShouldAgreeOnACustomFormatJoin() =>
            Same("SELECT a.\"empid\", b.\"empid\" FROM \"HR\".\"emps\" a JOIN \"HR\".\"emps\" b ON a.\"deptno\" = b.\"deptno\" ORDER BY a.\"empid\", b.\"empid\"");

        [TestMethod]
        public void ShouldAgreeOnACustomFormatJoinOnANullableKey() =>
            Same("SELECT a.\"empid\", b.\"empid\" FROM \"HR\".\"emps\" a JOIN \"HR\".\"emps\" b ON a.\"commission\" = b.\"commission\" ORDER BY a.\"empid\", b.\"empid\"");

        [TestMethod]
        public void ShouldAgreeOnACustomFormatWindow() =>
            Same("SELECT \"empid\", SUM(\"salary\") OVER (PARTITION BY \"deptno\" ORDER BY \"empid\") FROM \"HR\".\"emps\" ORDER BY \"empid\"");

        [TestMethod]
        public void ShouldAgreeOnACustomFormatDistinct() =>
            Same("SELECT DISTINCT \"deptno\" FROM \"HR\".\"emps\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnStringFunctions() => Same("SELECT UPPER(\"LABEL\") || '-' || LOWER(\"REGION\") FROM \"SALES\" ORDER BY 1");

        // Planned top down, which is the only thing that calls passThroughTraits, deriveTraits and
        // getDeriveMode. Every node whose trait derivation is more than the default is here: a project and a
        // calc (permutation and cast), a filter, a hash join, a nested loop join, a correlate, a scan and a
        // VALUES, each with a collation to push down or derive.

        [TestMethod]
        public void ShouldAgreeOnAProjectionSortedTopDown() =>
            SameTopDown("SELECT \"REGION\", \"ID\" FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnACastInASortedProjectionTopDown() =>
            SameTopDown("SELECT CAST(\"ID\" AS BIGINT), \"REGION\" FROM \"SALES\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAFilterUnderASortTopDown() =>
            SameTopDown("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" WHERE \"AMOUNT\" > 5 ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnASortedJoinTopDown() =>
            SameTopDown("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnASortedLeftJoinTopDown() =>
            SameTopDown("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnASortedNestedLoopJoinTopDown() =>
            SameTopDown("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" < b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAJoinOverSortedInputsTopDown() =>
            SameTopDown("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" JOIN \"SORTED\" \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnACorrelatedSubQueryTopDown() =>
            SameTopDown("SELECT \"ID\" FROM \"SALES\" a WHERE \"AMOUNT\" > (SELECT MIN(\"AMOUNT\") FROM \"SALES\" b WHERE b.\"REGION\" = a.\"REGION\") ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnValuesTopDown() =>
            SameTopDown("SELECT * FROM (VALUES (1, 'A'), (2, 'B'), (3, 'C')) AS t(\"N\", \"L\") ORDER BY \"N\"");

        [TestMethod]
        public void ShouldAgreeOnAnAggregateTopDown() =>
            SameTopDown("SELECT \"REGION\", SUM(\"AMOUNT\") FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"");

        [TestMethod]
        public void ShouldAgreeOnAWindowTopDown() =>
            SameTopDown("SELECT \"ID\", SUM(\"AMOUNT\") OVER (PARTITION BY \"REGION\" ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // MATCH_RECOGNIZE. The pattern is a plain sequence of symbols in every one of these, because that is
        // all EnumerableMatch generates: implementPattern handles a literal and a concatenation and throws on
        // anything else, so *, + and | never reach a plan in either convention.

        // A table function is a class too, so the same thing holds as for MY_SUM: Janino could not name a CLR
        // class under IKVM 8.14.0 or 8.15.0, so EnumerableConvention had no plan for these and there was
        // nothing to compare against. 8.16.0 names one, so this is differential like the rest; the function
        // yields one to n, which the hand-written rows still assert.
        [TestMethod]
        public void ShouldRunATableFunction()
        {
            const string sql = "SELECT * FROM TABLE(NUMBERS(3))";

            Same(sql);
            Gives(sql, "1", "2", "3");
        }

        // A join over a one-column table function puts a sort on it, and that is EnumerableSort's defect:
        // it optimises the scan's ARRAY to SCALAR and hands the Object[] rows on unchanged. Refused rather
        // than answered, because Calcite is wrong here in the same way and this convention does what Calcite
        // does — ClrEnumerableRowFormatTests carries the whole measurement. Restore the expected rows "1",
        // "2" when EnumerableSort is fixed.
        [TestMethod]
        public void ShouldRefuseATableFunctionInAJoin()
        {
            var act = () => Gives("SELECT \"S\".\"ID\" FROM \"SALES\" AS \"S\", TABLE(NUMBERS(2)) AS \"N\" WHERE \"S\".\"ID\" = \"N\".\"N\" ORDER BY 1", "1", "2");

            act.Should().Throw<java.lang.IllegalStateException>()
                .WithInnerException<java.lang.IllegalStateException>()
                .WithMessage("*ClrEnumerableSort handed up a sequence of System.Object[] where its row type is java.lang.Integer*");
        }

        // The window table functions, which are the path RexImpTable implements rather than the schema.
        // TumbleImplementor and tumblingWindowSelector each name a parameter `_input`, and what lines the two
        // up is the lexical scope by name that Janino gets for free.

        [TestMethod]
        public void ShouldAgreeOnTumble() =>
            Same("SELECT \"ROWTIME\", \"ID\", \"window_start\", \"window_end\" FROM TABLE(TUMBLE(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), INTERVAL '1' HOUR)) ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnTumbleWithAnOffset() =>
            Same("SELECT \"ID\", \"window_start\" FROM TABLE(TUMBLE(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), INTERVAL '1' HOUR, INTERVAL '10' MINUTE)) ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnHop() =>
            Same("SELECT \"ID\", \"window_start\", \"window_end\" FROM TABLE(HOP(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), INTERVAL '30' MINUTE, INTERVAL '1' HOUR)) ORDER BY \"ID\", \"window_start\"");

        [TestMethod]
        public void ShouldAgreeOnSession() =>
            Same("SELECT \"ID\", \"window_start\", \"window_end\" FROM TABLE(SESSION(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), DESCRIPTOR(\"ID\"), INTERVAL '1' HOUR)) ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAnAggregateOverTumble() =>
            Same("SELECT \"window_start\", COUNT(*) FROM TABLE(TUMBLE(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), INTERVAL '1' HOUR)) GROUP BY \"window_start\" ORDER BY 1");

        [TestMethod]
        public void ShouldPlanTumbleInThisConvention() =>
            PlanOf("SELECT \"ID\", \"window_start\" FROM TABLE(TUMBLE(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), INTERVAL '1' HOUR))", true)
                .Should().Contain("ClrEnumerableTableFunctionScan");

        [TestMethod]
        public void ShouldRunATableFunctionUnderAnAggregate() =>
            Gives("SELECT COUNT(*), SUM(\"N\") FROM TABLE(NUMBERS(4))", "4|10");



        // MATCH_RECOGNIZE, in a plan rooted in this convention: the whole subtree stays in
        // EnumerableConvention with one converter at the top. The node itself cannot be written here —
        // Calcite casts its input getter to two package-private *types* — and does not have to be for the
        // query to answer.
        //
        // Three things have to be true at once for this to run. The measures row is
        // built with Expressions.new_ on the row's Java type, so an ARRAY-format input gives "new Object[]()"
        // — not Java, and not completable by a translator either; HR.emps is CUSTOM, so that line emits a
        // record constructor instead. The predicate's parameter is a Memory around the row and the condition
        // was translated against the row itself, both named row_, which is the lexical scope by name. And
        // EnumerableMatch.implementPattern takes a symbol or a concatenation and nothing else, so PATTERN
        // (STRT UP+) throws "unknown kind: PATTERN_QUANTIFIER" out of Calcite's own node, in either
        // convention — a fixed pattern is what either side can run.

        [TestMethod]
        public void ShouldAgreeOnMatchRecognize() =>
            Same("SELECT * FROM \"HR\".\"emps\" MATCH_RECOGNIZE (ORDER BY \"empid\" MEASURES STRT.\"empid\" AS \"s\", UP.\"empid\" AS \"e\" PATTERN (STRT UP) DEFINE UP AS UP.\"salary\" > PREV(UP.\"salary\")) AS T");

        // PARTITION BY has no test because it does not run in either convention. The partition key of one
        // column has a SCALAR physical type, EnumerableMatch builds the key with Expressions.new_ on its Java
        // row type, and that emits "new Integer()" — Janino: "No applicable constructor/method found for zero
        // actual parameters". Measured on EnumerableConvention alone, so it is Calcite's defect, and it is the
        // same one as "new Object[]()" a few lines further on in that node. See TODO.md.

        [TestMethod]
        public void ShouldPlanMatchRecognizeUnderAConverter() =>
            PlanOf("SELECT * FROM \"HR\".\"emps\" MATCH_RECOGNIZE (ORDER BY \"empid\" MEASURES STRT.\"empid\" AS \"s\" PATTERN (STRT UP) DEFINE UP AS UP.\"salary\" > PREV(UP.\"salary\")) AS T", true)
                .Should().StartWith("EnumerableToClrEnumerableConverter");

        // ------------------------------------------------------------------ a row that is one primitive
        //
        // SCALARS is one NOT NULL INTEGER column, so its physical row type is int and its sequence carries
        // java.lang.Integer. Every node below closes an operator over that row type, and seven of them used
        // the physical one; each of these failed before the node was corrected, and every one names the node
        // it is aimed at, because for four of them the planner would otherwise have chosen Calcite's.

        [TestMethod]
        public void ShouldAgreeOnAScalarRowScan() => Same("SELECT \"N\" FROM \"SCALARS\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAScalarRowProjection() => Same("SELECT \"N\" + 1 FROM \"SCALARS\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAScalarRowDistinct() => Same("SELECT DISTINCT \"N\" FROM \"SCALARS\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAScalarRowAggregate() => Same("SELECT \"N\" FROM \"SCALARS\" GROUP BY \"N\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAScalarRowUnionAll() =>
            SameThrough("ClrEnumerableUnion", "SELECT \"N\" FROM \"SCALARS\" UNION ALL SELECT \"N\" FROM \"SCALARS\" WHERE \"N\" < 3 ORDER BY 1",
                remove: [ClrEnumerableRules.ClrEnumerableMergeUnionRule]);

        [TestMethod]
        public void ShouldAgreeOnAScalarRowUnionDistinct() =>
            SameThrough("ClrEnumerableUnion", "SELECT \"N\" FROM \"SCALARS\" UNION SELECT \"N\" FROM \"SCALARS\" WHERE \"N\" < 3 ORDER BY 1",
                remove: [EnumerableRules.ENUMERABLE_MERGE_UNION_RULE, ClrEnumerableRules.ClrEnumerableMergeUnionRule]);

        // INTERSECT without ALL is rewritten to an aggregate over a union and never reaches the node; only
        // INTERSECT ALL does, which is why no set-operation test had ever built one over a primitive row
        [TestMethod]
        public void ShouldAgreeOnAScalarRowIntersectAll() =>
            SameThrough("ClrEnumerableIntersect", "SELECT \"N\" FROM \"SCALARS\" INTERSECT ALL SELECT \"N\" FROM \"SCALARS\" WHERE \"N\" < 3 ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAScalarRowExceptAll() =>
            SameThrough("ClrEnumerableMinus", "SELECT \"N\" FROM \"SCALARS\" EXCEPT ALL SELECT \"N\" FROM \"SCALARS\" WHERE \"N\" < 3 ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAScalarRowLimit() =>
            SameThrough("ClrEnumerableLimit", "SELECT \"N\" FROM \"SCALARS\" ORDER BY \"N\" OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
                remove: [EnumerableRules.ENUMERABLE_LIMIT_RULE]);

        [TestMethod]
        public void ShouldAgreeOnAScalarRowLimitSort() =>
            SameThrough("ClrEnumerableLimitSort", "SELECT \"N\" FROM \"SCALARS\" ORDER BY \"N\" FETCH NEXT 2 ROWS ONLY",
                remove: [EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE, EnumerableRules.ENUMERABLE_LIMIT_RULE],
                limitSort: true);

        [TestMethod]
        public void ShouldAgreeOnAScalarRowMergeUnion() =>
            SameThrough("ClrEnumerableMergeUnion", "SELECT \"N\" FROM \"SCALARS\" UNION SELECT \"N\" FROM \"SCALARS\" ORDER BY 1",
                remove: [EnumerableRules.ENUMERABLE_MERGE_UNION_RULE, EnumerableRules.ENUMERABLE_UNION_RULE, EnumerableRules.ENUMERABLE_SORT_RULE]);

        [TestMethod]
        public void ShouldAgreeOnAScalarRowSortedAggregate() =>
            SameThrough("ClrEnumerableSortedAggregate", "SELECT \"N\" FROM \"SCALARS\" GROUP BY \"N\" ORDER BY 1",
                remove: [EnumerableRules.ENUMERABLE_AGGREGATE_RULE, EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE, ClrEnumerableRules.ClrEnumerableAggregateRule],
                sortedAggregate: true);

        [TestMethod]
        public void ShouldAgreeOnAScalarRowCollectedIntoAnArray() =>
            SameThrough("ClrEnumerableCollect", "SELECT ARRAY(SELECT \"N\" FROM \"SCALARS\") FROM (VALUES (1))",
                remove: [EnumerableRules.ENUMERABLE_COLLECT_RULE]);

        [TestMethod]
        public void ShouldAgreeOnAScalarRowCollectedIntoAMultiset() =>
            SameThrough("ClrEnumerableCollect", "SELECT MULTISET(SELECT \"N\" FROM \"SCALARS\") FROM (VALUES (1))",
                remove: [EnumerableRules.ENUMERABLE_COLLECT_RULE]);

        [TestMethod]
        public void ShouldAgreeOnAScalarRowUncollected() =>
            SameThrough("ClrEnumerableUncollect", "SELECT * FROM UNNEST(ARRAY[1, 2, 3])",
                remove: [EnumerableRules.ENUMERABLE_UNCOLLECT_RULE]);

        // ------------------------------------------------------------------ EnumerableUncollectTest
        //
        // Every shape UNNEST can take, which is Calcite's own list. The node had one test before this, over an
        // array of strings, and the branch CALCITE-4063 added — one field, itself a struct of one item —
        // had never been entered. Each of these names the node, because the planner prefers Calcite's.

        static readonly RelOptRule[] TheirUncollect = [EnumerableRules.ENUMERABLE_UNCOLLECT_RULE];

        [TestMethod]
        public void ShouldAgreeOnUnnestingAnArray() =>
            SameThrough("ClrEnumerableUncollect", "SELECT * FROM UNNEST(ARRAY[3, 4]) AS T2(y)", remove: TheirUncollect);

        [TestMethod]
        public void ShouldAgreeOnUnnestingANullArray() =>
            SameThrough("ClrEnumerableUncollect", "SELECT * FROM UNNEST(CAST(NULL AS INTEGER ARRAY))", remove: TheirUncollect);

        [TestMethod]
        public void ShouldAgreeOnUnnestingAnArrayOfArrays() =>
            SameThrough("ClrEnumerableUncollect", "SELECT * FROM UNNEST(ARRAY[ARRAY[3], ARRAY[4]]) AS T2(y)", remove: TheirUncollect);

        [TestMethod]
        public void ShouldAgreeOnUnnestingAnArrayOfLongerArrays() =>
            SameThrough("ClrEnumerableUncollect", "SELECT * FROM UNNEST(ARRAY[ARRAY[3, 4], ARRAY[4, 5]]) AS T2(y)", remove: TheirUncollect);

        [TestMethod]
        public void ShouldAgreeOnUnnestingAnArrayOfArraysOfArrays() =>
            SameThrough("ClrEnumerableUncollect",
                "SELECT * FROM UNNEST(ARRAY[ARRAY[ARRAY[3, 4], ARRAY[4, 5]], ARRAY[ARRAY[7, 8], ARRAY[9, 10]]]) AS T2(y)",
                remove: TheirUncollect);

        // CALCITE-4063: one field, a struct of one item, and no ordinality, so the result is the item itself
        // rather than a list holding it. That is the one branch of the node a lambda of its own stands for.
        [TestMethod]
        public void ShouldAgreeOnUnnestingAnArrayOfOneFieldRows() =>
            SameThrough("ClrEnumerableUncollect", "SELECT * FROM UNNEST(ARRAY[ROW(3), ROW(4)]) AS T2(y)", remove: TheirUncollect);

        [TestMethod]
        public void ShouldAgreeOnUnnestingAnArrayOfTwoFieldRows() =>
            SameThrough("ClrEnumerableUncollect", "SELECT * FROM UNNEST(ARRAY[ROW(3, 5), ROW(4, 6)]) AS T2(y, z)", remove: TheirUncollect);

        [TestMethod]
        public void ShouldAgreeOnUnnestingWithOrdinality() =>
            SameThrough("ClrEnumerableUncollect", "SELECT * FROM UNNEST(ARRAY[ROW(3), ROW(4)]) WITH ORDINALITY AS T2(y, o)", remove: TheirUncollect);

        // UNNEST(ARRAY[ROW(1, ROW(5, 10)), ROW(2, ROW(6, 12))]) has no test, because it does not reach a
        // convention at all: RelStructuredTypeFlattener throws NoSuchElementException out of
        // SqlToRelConverter.flattenTypes, which PlannerImpl.rel calls before any planning. Measured on the
        // Calcite side of this harness as well, and the two sides run the same converter, so it is Calcite's
        // and it is about how this harness converts rather than about either convention. A row of one field
        // holding a row is the next test and does run.

        [TestMethod]
        public void ShouldAgreeOnUnnestingAnArrayOfOneFieldRowsHoldingRows() =>
            SameThrough("ClrEnumerableUncollect", "SELECT * FROM UNNEST(ARRAY[ROW(ROW(3)), ROW(ROW(4))]) AS T2(y)", remove: TheirUncollect);

        [TestMethod]
        public void ShouldAgreeOnUnnestingAlongsideAnotherInput() =>
            SameThrough("ClrEnumerableUncollect",
                "SELECT * FROM (VALUES (1), (2)) T1(x), UNNEST(ARRAY[3, 4]) AS T2(y) ORDER BY 1, 2",
                remove: TheirUncollect);

        [TestMethod]
        public void ShouldAgreeOnUnnestingArraysAlongsideAnotherInput() =>
            SameThrough("ClrEnumerableUncollect",
                "SELECT * FROM (VALUES (1), (2)) T1(x), UNNEST(ARRAY[ARRAY[3, 4], ARRAY[4, 5]]) AS T2(y) ORDER BY 1",
                remove: TheirUncollect);

        [TestMethod]
        public void ShouldAgreeOnUnnestingRowsAlongsideAnotherInput() =>
            SameThrough("ClrEnumerableUncollect",
                "SELECT * FROM (VALUES (1), (2)) T1(x), UNNEST(ARRAY[ROW(3, 5), ROW(4, 6)]) AS T2(y, z) ORDER BY 1, 2",
                remove: TheirUncollect);

        [TestMethod]
        public void ShouldAgreeOnUnnestingWithOrdinalityAlongsideAnotherInput() =>
            SameThrough("ClrEnumerableUncollect",
                "SELECT * FROM (VALUES (1), (2)) T1(x), UNNEST(ARRAY[ROW(3), ROW(4)]) WITH ORDINALITY AS T2(y, o) ORDER BY 1, 2",
                remove: TheirUncollect);

        // ------------------------------------------------------------------ EnumerableBatchNestedLoopJoinTest

        [TestMethod]
        public void ShouldAgreeOnABatchNestedLoopJoinOnAStringKey() =>
            SameBatchNestedLoopJoin("SELECT d.\"name\", e.\"salary\" FROM \"HR\".\"depts\" d JOIN \"HR\".\"emps\" e ON d.\"name\" = e.\"name\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnABatchNestedLoopJoinFromANotInSubQuery() =>
            SameBatchNestedLoopJoin("SELECT COUNT(e.\"name\") FROM \"HR\".\"emps\" e WHERE e.\"deptno\" NOT IN (SELECT d.\"deptno\" FROM \"HR\".\"depts\" d WHERE d.\"name\" = 'Sales')");

        [TestMethod]
        public void ShouldAgreeOnABatchNestedLoopJoinOnTwoEqualities() =>
            SameBatchNestedLoopJoin("SELECT COUNT(e.\"name\") FROM \"HR\".\"emps\" e JOIN \"HR\".\"depts\" d ON d.\"deptno\" = e.\"empid\" AND d.\"deptno\" = e.\"deptno\"");

        [TestMethod]
        public void ShouldAgreeOnABatchNestedLoopJoinOnAMismatchedKey() =>
            SameBatchNestedLoopJoin("SELECT COUNT(e.\"name\") FROM \"HR\".\"emps\" e JOIN \"HR\".\"depts\" d ON d.\"deptno\" = e.\"empid\"");

        [TestMethod]
        public void ShouldAgreeOnABatchNestedLoopLeftJoinCount() =>
            SameBatchNestedLoopJoin("SELECT COUNT(d.\"deptno\") FROM \"HR\".\"depts\" d LEFT JOIN \"HR\".\"emps\" e ON d.\"deptno\" = e.\"deptno\"");

        // two batch joins in one plan, which is where Calcite's own node has to fall back to a compact row
        // builder or exceed what a Java method may hold. An expression tree has no such limit and builds the
        // one form, so this is the query that says the difference does not change the answer.
        [TestMethod]
        public void ShouldAgreeOnADoubleBatchNestedLoopJoin() =>
            SameBatchNestedLoopJoin("SELECT e.\"name\", d.\"name\", l.\"name\" FROM \"HR\".\"emps\" e JOIN \"HR\".\"depts\" d ON d.\"deptno\" <> e.\"empid\" JOIN \"HR\".\"locations\" l ON e.\"empid\" <> l.\"empid\" AND d.\"deptno\" = l.\"empid\" ORDER BY 1, 2, 3");

        // ------------------------------------------------------------------ EnumerableCorrelateTest, in SQL

        [TestMethod]
        public void ShouldAgreeOnACorrelateFromExists() =>
            Same("SELECT e.\"empid\", e.\"name\" FROM \"HR\".\"emps\" e WHERE EXISTS (SELECT 1 FROM \"HR\".\"depts\" d WHERE d.\"deptno\" = e.\"deptno\") ORDER BY 1");

        // CALCITE-2930's shape: the correlated condition compares against a nullable column, so the field the
        // sub-query reads is a box rather than a primitive
        [TestMethod]
        public void ShouldAgreeOnACorrelateOverABoxedPrimitive() =>
            Same("SELECT e.\"empid\" FROM \"HR\".\"emps\" e WHERE NOT EXISTS (SELECT 1 FROM \"HR\".\"depts\" d WHERE d.\"deptno\" = e.\"commission\") ORDER BY 1");

        /// <summary>
        /// CALCITE-5638: a scalar sub-query correlated on two columns at once, under a filter that is itself
        /// correlated.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnAComplexNestedCorrelatedSubQuery() =>
            Same("SELECT \"empid\", \"deptno\", (SELECT COUNT(*) FROM \"HR\".\"emps\" AS x WHERE x.\"salary\" > \"emps\".\"salary\" AND x.\"deptno\" < \"emps\".\"deptno\") FROM \"HR\".\"emps\" WHERE \"empid\" < \"salary\" ORDER BY 1, 2, 3");

        // ------------------------------------------------------------------ EnumerableMergeUnionTest
        //
        // The order keys Calcite's own tests use and this convention had none of: a nullable column ordered
        // with the nulls at either end, and a second key running the other way.

        [TestMethod]
        public void ShouldAgreeOnAMergeUnionAllOrderedByANullableKeyNullsFirst() =>
            Same("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" UNION ALL SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY \"AMOUNT\" ASC NULLS FIRST, \"ID\" DESC");

        [TestMethod]
        public void ShouldAgreeOnAMergeUnionOrderedByANullableKeyNullsFirst() =>
            Same("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" UNION SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY \"AMOUNT\" ASC NULLS FIRST, \"ID\" DESC");

        [TestMethod]
        public void ShouldAgreeOnAMergeUnionAllOrderedByANullableKeyNullsLast() =>
            Same("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" UNION ALL SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY \"AMOUNT\" ASC NULLS LAST, \"ID\" DESC");

        [TestMethod]
        public void ShouldAgreeOnAMergeUnionOrderedByANullableKeyNullsLast() =>
            Same("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" UNION SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY \"AMOUNT\" ASC NULLS LAST, \"ID\" DESC");

        [TestMethod]
        public void ShouldAgreeOnAMergeUnionOfOneColumnOrderedByIt() =>
            Same("SELECT \"LABEL\" FROM \"SALES\" UNION SELECT \"LABEL\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY 1");

        // ------------------------------------------------------------------ EnumerableHashJoinTest

        [TestMethod]
        public void ShouldAgreeOnAFullJoinOnACompositeNullableKey() =>
            SameHashJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a FULL JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"AMOUNT\" = b.\"AMOUNT\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnASemiJoinOnACompositeNullableKey() =>
            SameHashJoin("SELECT a.\"ID\" FROM \"SALES\" a WHERE (a.\"REGION\", a.\"AMOUNT\") IN (SELECT b.\"REGION\", b.\"AMOUNT\" FROM \"SALES\" b WHERE b.\"ID\" < 4) ORDER BY 1");

        // an equality and something else besides, which the hash join tests on the pair it has already matched
        [TestMethod]
        public void ShouldAgreeOnAHashJoinWithAnExtraPredicate() =>
            SameHashJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"ID\" < b.\"ID\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnALeftHashJoinWithAnExtraPredicate() =>
            SameHashJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"ID\" < b.\"ID\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnARightHashJoinWithAnExtraPredicate() =>
            SameHashJoin("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a RIGHT JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"ID\" < b.\"ID\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnASemiHashJoinWithAnExtraPredicate() =>
            SameHashJoin("SELECT a.\"ID\" FROM \"SALES\" a WHERE EXISTS (SELECT 1 FROM \"SALES\" b WHERE a.\"REGION\" = b.\"REGION\" AND a.\"ID\" < b.\"ID\") ORDER BY 1");

        // ------------------------------------------------------------------ EnumerableLimitSortTest
        //
        // The order keys Calcite's own limit-sort tests use: a nullable column with the nulls at either end,
        // and a second key. This convention's five limit-sort tests were all one key with the default null
        // ordering.

        [TestMethod]
        public void ShouldAgreeOnALimitSortWithNullsFirst() =>
            SameLimitSort("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" NULLS FIRST, \"ID\" FETCH NEXT 3 ROWS ONLY");

        [TestMethod]
        public void ShouldAgreeOnALimitSortWithNullsLast() =>
            SameLimitSort("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" NULLS LAST, \"ID\" FETCH NEXT 3 ROWS ONLY");

        [TestMethod]
        public void ShouldAgreeOnALimitSortWithNullsFirstAndAnOffset() =>
            SameLimitSort("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" NULLS FIRST, \"ID\" OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY");

        /// <remarks>
        /// A <em>single</em>-column collation over a nullable column, which is the only shape whose sort key
        /// can itself be null: a multi-field collation key is a FlatLists row, and a row is never null even
        /// when a field in it is. The four tests above are all two-key and therefore cannot reach it.
        /// </remarks>
        [TestMethod]
        public void ShouldAgreeOnALimitSortOnOneNullableKeyNullsFirst() =>
            SameLimitSort("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" NULLS FIRST FETCH NEXT 3 ROWS ONLY");

        [TestMethod]
        public void ShouldAgreeOnALimitSortOnOneNullableKeyTakingEverything() =>
            SameLimitSort("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" FETCH NEXT 100 ROWS ONLY");

        [TestMethod]
        public void ShouldAgreeOnALimitSortWithNullsLastAndAnOffset() =>
            SameLimitSort("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"AMOUNT\" NULLS LAST, \"ID\" OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY");

        [TestMethod]
        public void ShouldAgreeOnALimitSortOverSeveralKeysRunningBothWays() =>
            SameLimitSort("SELECT \"ID\", \"REGION\", \"AMOUNT\" FROM \"SALES\" ORDER BY \"REGION\" DESC, \"AMOUNT\" NULLS LAST, \"ID\" OFFSET 1 ROWS FETCH NEXT 4 ROWS ONLY");

    }

}
