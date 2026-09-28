using System;
using System.Collections.Generic;
using System.Linq;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Runtime;
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
    /// Runs a query end to end in the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    public class ClrCursorConventionTests
    {

        /// <summary>
        /// Puts Calcite's JDBC assembly on the boot class path.
        /// </summary>
        /// <remarks>
        /// <c>Frameworks.withPrepare</c> opens a <c>jdbc:calcite:</c> connection and loads its factory by name,
        /// so the assembly holding that factory must be on IKVM's boot class path for <c>Class.forName</c> to
        /// find it.
        /// </remarks>
        static ClrCursorConventionTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// A table of three rows implementing Calcite's <see cref="ScannableTable"/>, with one nullable column.
        /// </summary>
        sealed class PeopleTable : AbstractTable, ScannableTable
        {

            static readonly object?[][] Rows =
            [
                [java.lang.Integer.valueOf(1), "SMITH", java.lang.Integer.valueOf(30), java.lang.Integer.valueOf(5)],
                [java.lang.Integer.valueOf(2), "JONES", java.lang.Integer.valueOf(40), null],
                [java.lang.Integer.valueOf(3), "BROWN", java.lang.Integer.valueOf(20), java.lang.Integer.valueOf(7)],
            ];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .add("AGE", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("BONUS", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true))
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
        /// <param name="rootSchema">The schema the plan was planned against, which a table's expression looks itself up in.</param>
        /// <remarks>
        /// <c>DataContexts.EMPTY</c> is not enough: a table's expression looks the table up again through the
        /// root schema at run time, and an empty context has none.
        /// </remarks>
        sealed class TestDataContext(SchemaPlus rootSchema) : DataContext
        {

            /// <inheritdoc />
            public SchemaPlus getRootSchema() => rootSchema;

            /// <inheritdoc />
            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            /// <inheritdoc />
            public QueryProvider getQueryProvider() => null!;

            /// <inheritdoc />
            public object get(string name) => null!;

        }

        /// <summary>
        /// Plans a query into the convention and returns the chosen plan and the schema it was planned
        /// against.
        /// </summary>
        /// <param name="sql">The query, over the <c>PEOPLE</c> table.</param>
        /// <returns>The physical root, and the root schema the query was planned against.</returns>
        static (ClrCursorRel Plan, SchemaPlus Schema) Plan(string sql)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("PEOPLE", new PeopleTable());

            // the convention's calc rules, as a pass after the planner's
            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.CalcRules())
                calcRules.add(rule);

            // the prepare pipeline's program: Programs.standard, which still runs Calcite's calc pass, followed
            // by this convention's calc pass. A Frameworks planner carries only Calcite's default rules, so
            // this convention's rules are added first, as ClrPrepareImpl.CreatePlanner does
            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.sequence(
                        new AddRulesProgram(ClrCursorRules.Rules()),
                        Programs.standard(Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider),
                        Programs.hep(calcRules, true, Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider)))
                .build();

            var planner = Frameworks.getPlanner(config);
            var parsed = planner.parse(sql);
            var validated = planner.validate(parsed);
            var logical = planner.rel(validated).project();

            // as Prepare.getDesiredRootTraitSet: the root's own traits with the convention replaced, then
            // simplified. An empty set asks for no collation, and SortRemoveRule would then drop an ORDER BY.
            // One program, so one transform.
            var traitSet = logical.getTraitSet().replace(ClrCursorConvention.Instance).simplify();
            var physical = (ClrCursorRel)planner.transform(0, traitSet, logical);

            return (physical, rootSchema);
        }

        /// <summary>
        /// Plans a query into the convention, compiles it, and returns its rows, read through both opens.
        /// </summary>
        /// <param name="sql">The query, over the <c>PEOPLE</c> table.</param>
        /// <returns>The rows, a one-column result wrapped in a one-element array. A failure to implement the plan is
        /// rethrown as an <see cref="InvalidOperationException"/> whose message includes the plan.</returns>
        static List<object[]> Run(string sql)
        {
            var (physical, rootSchema) = Plan(sql);

            ClrCursorFactory factory;
            try
            {
                factory = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), new java.util.HashMap()).ImplementRoot(physical, ClrCursorPrefer.Array);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"{e.Message}{Environment.NewLine}{RelOptUtil.toString(physical)}", e);
            }

            return Rows(factory, new TestDataContext(rootSchema));
        }

        /// <summary>
        /// Opens the plan both ways, requires the two readings to agree, and returns one of them.
        /// </summary>
        /// <param name="factory">The implemented plan.</param>
        /// <param name="context">The context each open binds with.</param>
        /// <returns>The rows of the synchronous reading, a one-column result wrapped in a one-element array.</returns>
        static List<object[]> Rows(ClrCursorFactory factory, DataContext context)
        {
            var rows = new List<object[]>();
            using (var cursor = factory.Open(context))
                while (cursor.Read())
                    rows.Add(cursor.Current as object[] ?? [cursor.Current!]);

            var awaited = System.Threading.Tasks.Task.Run(async () =>
            {
                var read = new List<object[]>();
                await using var cursor = await factory.OpenAsync(context, System.Threading.CancellationToken.None);
                while (await cursor.ReadAsync(System.Threading.CancellationToken.None))
                    read.Add(cursor.Current as object[] ?? [cursor.Current!]);
                return read;
            }).GetAwaiter().GetResult();

            awaited.Select(r => string.Join("|", r.Select(v => v?.ToString()))).Should().Equal(rows.Select(r => string.Join("|", r.Select(v => v?.ToString()))));

            return rows;
        }

        /// <summary>
        /// A node that cannot implement itself fails with the plan that reached it named, as Calcite's
        /// <c>implementRoot</c> names it.
        /// </summary>
        /// <remarks>
        /// <see cref="ClrCursorProject"/> always refuses to implement itself, because the calc rules rewrite
        /// every project into a calc; so the refusal is reached here by building a project by hand. The
        /// <c>UnsupportedOperationException</c> it throws is wrapped in one naming the plan.
        /// </remarks>
        [Fact]
        public void ShouldNameThePlanWhenANodeCannotImplementItself()
        {
            var (physical, _) = Plan("SELECT \"ID\", \"NAME\" FROM \"PEOPLE\"");

            var identity = new java.util.ArrayList();
            for (int i = 0; i < physical.getRowType().getFieldCount(); i++)
                identity.add(physical.getCluster().getRexBuilder().makeInputRef(physical, i));

            var project = ClrCursorProject.Create(physical, identity, physical.getRowType());

            var act = () => new ClrCursorRelImplementor(project.getCluster().getRexBuilder(), new java.util.HashMap()).ImplementRoot(project, ClrCursorPrefer.Array);

            act.Should().Throw<java.lang.IllegalStateException>()
                .WithMessage("Unable to implement ClrCursorProject*")
                .WithInnerException<java.lang.UnsupportedOperationException>();
        }

        /// <summary>
        /// A combine gives one row per index, each column holding one query's values as a map, and as many rows
        /// as its largest input.
        /// </summary>
        /// <remarks>
        /// No SQL statement produces a <c>Combine</c>; it exists for multi-root optimisation, and a caller builds
        /// one with <c>RelBuilder.combine</c>, as this test does.
        /// </remarks>
        [Fact]
        public void ShouldCombineTwoQueries()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("PEOPLE", new PeopleTable());

            var config = Frameworks.newConfigBuilder().defaultSchema(rootSchema).build();
            var builder = RelBuilder.create(config);

            // three names against two ids, so the shorter query runs out and contributes null. The literal is
            // a java.lang.Integer because RelBuilder.literal takes an Object and refuses a CLR-boxed int, which
            // arrives as cli.System.Int32
            var logical = builder
                .scan("PEOPLE").project(builder.field("NAME"))
                .scan("PEOPLE")
                    .filter(builder.call(org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN, builder.field("ID"), builder.literal(java.lang.Integer.valueOf(3))))
                    .project(builder.field("ID"))
                .combine()
                .build();

            var planner = (org.apache.calcite.plan.volcano.VolcanoPlanner)logical.getCluster().getPlanner();
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            foreach (var rule in ClrCursorRules.Rules())
                planner.addRule((RelOptRule)rule);

            var traitSet = logical.getTraitSet().replace(ClrCursorConvention.Instance).simplify();
            planner.setRoot(planner.changeTraits(logical, traitSet));

            var chosen = planner.findBestExp();

            // the calc pass that follows the planner: a project cannot implement itself, and the calc rules
            // rewrite every project into a calc
            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.CalcRules())
                calcRules.add(rule);

            var physical = (ClrCursorRel)Programs
                .hep(calcRules, true, Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider)
                .run(planner, chosen, chosen.getTraitSet(), new java.util.ArrayList(), new java.util.ArrayList());

            physical.Should().BeOfType<ClrCursorCombine>();

            var factory = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), new java.util.HashMap()).ImplementRoot(physical, ClrCursorPrefer.Array);

            var rows = Rows(factory, new TestDataContext(rootSchema));

            rows.Should().HaveCount(3);
            ((java.util.Map)rows[0][0]).get("NAME").Should().Be("SMITH");
            ((java.util.Map)rows[0][1]).get("ID").Should().Be(java.lang.Integer.valueOf(1));

            // the second query has two rows, so the third has nothing to hold
            rows[2][0].Should().NotBeNull();
            rows[2][1].Should().BeNull();
        }

        [Fact]
        public void ShouldScanATable()
        {
            var rows = Run("SELECT \"ID\", \"NAME\" FROM \"PEOPLE\"");

            rows.Should().HaveCount(3);
            rows.Select(r => (string)r[1]).Should().BeEquivalentTo(["SMITH", "JONES", "BROWN"]);
        }

        [Fact]
        public void ShouldFilter()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"AGE\" > 25");

            rows.Select(r => (string)r[0]).Should().BeEquivalentTo(["SMITH", "JONES"]);
        }

        [Fact]
        public void ShouldProjectAnExpression()
        {
            var rows = Run("SELECT \"AGE\" + 1 FROM \"PEOPLE\" WHERE \"ID\" = 1");

            rows.Should().ContainSingle();
            rows[0][0].Should().Be(java.lang.Integer.valueOf(31));
        }

        [Fact]
        public void ShouldSort()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" ORDER BY \"AGE\"");

            rows.Select(r => (string)r[0]).Should().Equal("BROWN", "SMITH", "JONES");
        }

        [Fact]
        public void ShouldSortDescending()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" ORDER BY \"AGE\" DESC");

            rows.Select(r => (string)r[0]).Should().Equal("JONES", "SMITH", "BROWN");
        }

        [Fact]
        public void ShouldLimit()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" ORDER BY \"ID\" FETCH NEXT 2 ROWS ONLY");

            rows.Select(r => (string)r[0]).Should().Equal("SMITH", "JONES");
        }

        [Fact]
        public void ShouldOffsetAndLimit()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" ORDER BY \"ID\" OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY");

            rows.Select(r => (string)r[0]).Should().Equal("JONES");
        }

        [Fact]
        public void ShouldComputeOverANullableColumn()
        {
            // a nullable column is a java.lang.Integer and the arithmetic is on an int, so RexImpTable emits
            // unboxing and boxing around it
            var rows = Run("SELECT \"AGE\" + \"BONUS\" FROM \"PEOPLE\" ORDER BY \"ID\"");

            rows.Should().HaveCount(3);
            rows[0][0].Should().Be(java.lang.Integer.valueOf(35));
            rows[1][0].Should().BeNull();
            rows[2][0].Should().Be(java.lang.Integer.valueOf(27));
        }

        [Fact]
        public void ShouldCountEveryRow()
        {
            var rows = Run("SELECT COUNT(*) FROM \"PEOPLE\"");

            rows.Should().ContainSingle();
            rows[0][0].Should().Be(java.lang.Long.valueOf(3L));
        }

        [Fact]
        public void ShouldAggregateWithoutAGroup()
        {
            var rows = Run("SELECT SUM(\"AGE\"), MIN(\"AGE\"), MAX(\"AGE\") FROM \"PEOPLE\"");

            rows.Should().ContainSingle();
            rows[0][0].Should().Be(java.lang.Integer.valueOf(90));
            rows[0][1].Should().Be(java.lang.Integer.valueOf(20));
            rows[0][2].Should().Be(java.lang.Integer.valueOf(40));
        }

        /// <summary>
        /// A correlate survives the shipped program, which runs Calcite's decorrelation.
        /// </summary>
        /// <remarks>
        /// Decorrelation turns a scalar sub-query or an EXISTS into a join, but cannot decorrelate an UNNEST over
        /// a correlation variable, so this shape keeps its correlate. The test checks the plan as well as the
        /// rows, because the rows do not show which plan produced them.
        /// </remarks>
        [Fact]
        public void ShouldCorrelateThroughTheShippedProgram()
        {
            const string sql = "SELECT t.\"x\", u.\"y\" FROM (VALUES (1, ARRAY[10,20]), (2, ARRAY[30])) AS t(\"x\", \"xs\"), UNNEST(t.\"xs\") AS u(\"y\")";

            var (physical, _) = Plan(sql);
            RelOptUtil.toString(physical).Should().Contain("Correlate", "the decorrelation cannot take an UNNEST of a correlation variable apart");

            var rows = Run(sql);

            rows.Should().HaveCount(3);
            rows.Select(r => string.Join("|", r.Select(v => v?.ToString()))).Should().Equal("1|10", "1|20", "2|30");
        }

        /// <summary>
        /// A correlated sub-query becomes a join through the shipped program.
        /// </summary>
        /// <remarks>
        /// Without decorrelation this would stay a correlate, a nested loop over the outer rows, where Calcite
        /// plans a join.
        /// </remarks>
        [Fact]
        public void ShouldDecorrelateASubQueryThroughTheShippedProgram()
        {
            const string sql = "SELECT \"ID\" FROM \"PEOPLE\" a WHERE \"AGE\" = (SELECT MAX(\"AGE\") FROM \"PEOPLE\" b WHERE b.\"BONUS\" IS NULL OR a.\"ID\" = b.\"ID\")";

            var (physical, _) = Plan(sql);
            RelOptUtil.toString(physical).Should().NotContain("Correlate", "Calcite's decorrelation should have made this a join");
        }

        // The next three shapes each need a logical rewrite that belongs to no convention and that Calcite
        // registers by default; they plan only if the program keeps Calcite's rules on the planner. The
        // differential suites register these rewrites by hand, so only these tests cover the shipped program.

        /// <summary>
        /// AVG through the shipped program.
        /// </summary>
        /// <remarks>
        /// <c>RexImpTable</c> has no implementor for AVG. <c>AGGREGATE_REDUCE_FUNCTIONS</c>, which is in
        /// <c>RelOptRules.BASE_RULES</c> rather than in any convention's rules, rewrites it in terms of
        /// <c>$SUM0</c> and <c>COUNT</c>.
        /// </remarks>
        [Fact]
        public void ShouldAverageThroughTheShippedProgram()
        {
            var rows = Run("SELECT AVG(\"AGE\") FROM \"PEOPLE\"");

            rows.Should().HaveCount(1);
            rows[0][0].Should().Be(java.lang.Integer.valueOf(30));
        }

        /// <summary>
        /// A DISTINCT aggregate through the shipped program.
        /// </summary>
        /// <remarks>
        /// Both conventions' aggregates refuse a distinct call, so
        /// <c>AGGREGATE_EXPAND_DISTINCT_AGGREGATES</c> must rewrite the DISTINCT away first.
        /// </remarks>
        [Fact]
        public void ShouldCountDistinctThroughTheShippedProgram()
        {
            var rows = Run("SELECT COUNT(DISTINCT \"NAME\") FROM \"PEOPLE\"");

            rows.Should().HaveCount(1);
            rows[0][0].Should().Be(java.lang.Long.valueOf(3L));
        }

        /// <summary>
        /// A window through the shipped program.
        /// </summary>
        /// <remarks>
        /// Both conventions refuse a project holding an OVER, so <c>PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW</c> must
        /// first rewrite it into a <c>LogicalWindow</c>.
        /// </remarks>
        [Fact]
        public void ShouldWindowThroughTheShippedProgram()
        {
            var rows = Run("SELECT \"ID\", SUM(\"AGE\") OVER (ORDER BY \"ID\") FROM \"PEOPLE\" ORDER BY \"ID\"");

            rows.Should().HaveCount(3);
            rows.Select(r => r[1]).Should().Equal(
                java.lang.Integer.valueOf(30),
                java.lang.Integer.valueOf(70),
                java.lang.Integer.valueOf(90));
        }

        /// <summary>
        /// A filtered count through the shipped program.
        /// </summary>
        [Fact]
        public void ShouldFallBackToCalciteThroughTheShippedProgram()
        {
            var rows = Run("SELECT COUNT(*) FROM \"PEOPLE\" WHERE \"AGE\" > 25");

            rows.Should().HaveCount(1);
            rows[0][0].Should().Be(java.lang.Long.valueOf(2L));
        }

        [Fact]
        public void ShouldGroupBy()
        {
            var rows = Run("SELECT \"NAME\", COUNT(*) FROM \"PEOPLE\" GROUP BY \"NAME\" ORDER BY \"NAME\"");

            rows.Should().HaveCount(3);
            rows.Select(r => (string)r[0]).Should().Equal("BROWN", "JONES", "SMITH");
            rows.Select(r => r[1]).Should().AllBeEquivalentTo(java.lang.Long.valueOf(1L));
        }

        [Fact]
        public void ShouldGroupByAndSum()
        {
            var rows = Run("SELECT \"AGE\" > 25, SUM(\"AGE\") FROM \"PEOPLE\" GROUP BY \"AGE\" > 25 ORDER BY 1");

            rows.Should().HaveCount(2);
            rows[0][1].Should().Be(java.lang.Integer.valueOf(20));
            rows[1][1].Should().Be(java.lang.Integer.valueOf(70));
        }

        [Fact]
        public void ShouldAggregateOverANullableColumn()
        {
            // SUM skips a null, so this is 12 rather than null
            var rows = Run("SELECT SUM(\"BONUS\") FROM \"PEOPLE\"");

            rows.Should().ContainSingle();
            rows[0][0].Should().Be(java.lang.Integer.valueOf(12));
        }

        [Fact]
        public void ShouldInnerJoin()
        {
            var rows = Run("SELECT a.\"NAME\", b.\"AGE\" FROM \"PEOPLE\" a JOIN \"PEOPLE\" b ON a.\"ID\" = b.\"ID\" WHERE a.\"ID\" = 1");

            rows.Should().ContainSingle();
            rows[0][0].Should().Be("SMITH");
            rows[0][1].Should().Be(java.lang.Integer.valueOf(30));
        }

        [Fact]
        public void ShouldLeftJoinAndPadWithNulls()
        {
            var rows = Run("SELECT a.\"NAME\", b.\"NAME\" FROM \"PEOPLE\" a LEFT JOIN (SELECT * FROM \"PEOPLE\" WHERE \"ID\" = 1) b ON a.\"ID\" = b.\"ID\" ORDER BY a.\"ID\"");

            rows.Should().HaveCount(3);
            rows[0][1].Should().Be("SMITH");
            rows[1][1].Should().BeNull();
            rows[2][1].Should().BeNull();
        }

        [Fact]
        public void ShouldJoinOnMoreThanAnEquality()
        {
            var rows = Run("SELECT a.\"NAME\" FROM \"PEOPLE\" a JOIN \"PEOPLE\" b ON a.\"ID\" = b.\"ID\" AND a.\"AGE\" > 25");

            rows.Select(r => (string)r[0]).Should().BeEquivalentTo(["SMITH", "JONES"]);
        }

        [Fact]
        public void ShouldJoinOnAnInequalityAlone()
        {
            // no equality to build a lookup on, so the hash join rule refuses and the nested loop takes it
            var rows = Run("SELECT a.\"NAME\", b.\"NAME\" FROM \"PEOPLE\" a JOIN \"PEOPLE\" b ON a.\"AGE\" < b.\"AGE\"");

            rows.Should().HaveCount(3);
        }

        [Fact]
        public void ShouldRunACorrelatedSubQuery()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" a WHERE \"AGE\" = (SELECT MAX(\"AGE\") FROM \"PEOPLE\" b WHERE b.\"ID\" = a.\"ID\")");

            rows.Select(r => (string)r[0]).Should().BeEquivalentTo(["SMITH", "JONES", "BROWN"]);
        }

        [Fact]
        public void ShouldUnionAll()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" = 1 UNION ALL SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" = 1");

            rows.Select(r => (string)r[0]).Should().Equal("SMITH", "SMITH");
        }

        [Fact]
        public void ShouldUnionDistinct()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" = 1 UNION SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" = 1");

            rows.Select(r => (string)r[0]).Should().Equal("SMITH");
        }

        [Fact]
        public void ShouldIntersect()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"AGE\" > 25 INTERSECT SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" = 1");

            rows.Select(r => (string)r[0]).Should().Equal("SMITH");
        }

        [Fact]
        public void ShouldExcept()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" EXCEPT SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"AGE\" > 25");

            rows.Select(r => (string)r[0]).Should().Equal("BROWN");
        }

        [Fact]
        public void ShouldUnionWholeRowsRatherThanCompareArraysByReference()
        {
            // a row of JavaRowFormat.ARRAY is an array, and two equal rows are two arrays. Without the comparer
            // PhysType gives for the format, a distinct union would keep both.
            var rows = Run("SELECT \"ID\", \"NAME\" FROM \"PEOPLE\" UNION SELECT \"ID\", \"NAME\" FROM \"PEOPLE\"");

            rows.Should().HaveCount(3);
        }

        [Fact]
        public void ShouldSortAndLimitTogether()
        {
            // a sort carrying a fetch is one node, so only as many rows as are wanted are kept
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" ORDER BY \"AGE\" DESC FETCH NEXT 2 ROWS ONLY");

            rows.Select(r => (string)r[0]).Should().Equal("JONES", "SMITH");
        }

        [Fact]
        public void ShouldCollectASubQueryIntoAMultiset()
        {
            var rows = Run("SELECT MULTISET(SELECT \"NAME\" FROM \"PEOPLE\") FROM (VALUES (1))");

            rows.Should().ContainSingle();
            ((java.util.List)rows[0][0]).size().Should().Be(3);
        }

        [Fact]
        public void ShouldUncollectAnArray()
        {
            var rows = Run("SELECT * FROM UNNEST(ARRAY['a', 'b', 'c'])");

            rows.Should().HaveCount(3);
            rows.Select(r => (string)r[0]).Should().Equal("a", "b", "c");
        }

        [Fact]
        public void ShouldReadValues()
        {
            var rows = Run("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y)");

            rows.Should().HaveCount(2);
            rows.Select(r => (string)r[1]).Should().Equal("a", "b");
        }

    }

}
