using System;
using System.Collections.Generic;
using System.Linq;

using Apache.Calcite.Extensions;

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
using Apache.Calcite.Extensions.Adapter.Enumerable;

using Apache.Calcite.Extensions.Runtime;
using Apache.Calcite.Extensions.Interop;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Runs a query end to end in the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    [TestClass]
    public class ClrEnumerableQueryTests
    {

        /// <summary>
        /// Initializes the static instance.
        /// </summary>
        /// <remarks>
        /// <c>Frameworks.withPrepare</c> opens a <c>jdbc:calcite:</c> connection and reaches its factory
        /// by name, so the assembly holding that factory has to be on IKVM's boot class path or
        /// <c>Class.forName</c> cannot find it. The AdoNet tests do the same thing for the same reason.
        /// </remarks>
        static ClrEnumerableQueryTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// A table of three rows, given to Calcite the way any table is.
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

                return Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// The context a plan is bound with.
        /// </summary>
        /// <param name="rootSchema"></param>
        /// <remarks>
        /// <c>DataContexts.EMPTY</c> will not do: a table's own expression reaches the root schema to find the
        /// table again at run time, and an empty context has none.
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
        /// <param name="sql"></param>
        /// <returns></returns>
        static (ClrEnumerableRel Plan, SchemaPlus Schema) Plan(string sql)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("PEOPLE", new PeopleTable());

            // the three passes this convention needs, which the library names now rather than every caller
            // spelling them out
            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(ClrEnumerablePrograms.Standard())
                .build();

            var planner = Frameworks.getPlanner(config);
            var parsed = planner.parse(sql);
            var validated = planner.validate(parsed);
            var logical = planner.rel(validated).project();

            // the passes Programs.standard makes, less the decorrelation: a sub-query has to be expanded
            // before the planner sees it, because a filter carrying one is refused as Calcite refuses it, and
            // leaving the correlate in place is what puts ClrEnumerableCorrelate on the plan at all
            var expanded = planner.transform(0, logical.getTraitSet(), logical);

            var traitSet = ClrEnumerablePrograms.DesiredRootTraitSet(planner.getEmptyTraitSet());
            var chosen = planner.transform(1, traitSet, expanded);
            var physical = (ClrEnumerableRel)planner.transform(2, chosen.getTraitSet(), chosen);

            return (physical, rootSchema);
        }

        /// <summary>
        /// Plans a query into the convention, compiles it, and returns its rows.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        static List<object[]> Run(string sql)
        {
            var (physical, rootSchema) = Plan(sql);

            IClrBindable bindable;
            try
            {
                bindable = ClrEnumerableInterpretable.ToBindable(new java.util.HashMap(), null, physical, ClrEnumerablePrefer.Array);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"{e.Message}{Environment.NewLine}{RelOptUtil.toString(physical)}", e);
            }

            var rows = new List<object[]>();
            foreach (var current in bindable.Bind(new TestDataContext(rootSchema)))
            {
                rows.Add(current as object[] ?? [current]);
            }

            return rows;
        }

        /// <summary>
        /// A node that cannot implement itself fails with the plan that reached it named, as Calcite's
        /// <c>implementRoot</c> names it.
        /// </summary>
        /// <remarks>
        /// <see cref="ClrEnumerableProject"/> is the node to ask, because refusing to implement itself is
        /// what it is for: the calc rules rewrite every project into a calc afterwards, so the refusal is
        /// unreachable through the planner and reachable by building one by hand. Without the wrap this is an
        /// <c>UnsupportedOperationException</c> naming nothing.
        /// </remarks>
        [TestMethod]
        public void ShouldNameThePlanWhenANodeCannotImplementItself()
        {
            var (physical, _) = Plan("SELECT \"ID\", \"NAME\" FROM \"PEOPLE\"");

            var identity = new java.util.ArrayList();
            for (int i = 0; i < physical.getRowType().getFieldCount(); i++)
                identity.add(physical.getCluster().getRexBuilder().makeInputRef(physical, i));

            var project = Apache.Calcite.Extensions.Adapter.Enumerable.ClrEnumerableProject.Create(physical, identity, physical.getRowType());

            var act = () => ClrEnumerableInterpretable.ToBindable(new java.util.HashMap(), null, project, ClrEnumerablePrefer.Array);

            act.Should().Throw<java.lang.IllegalStateException>()
                .WithMessage("Unable to implement ClrEnumerableProject*")
                .WithInnerException<java.lang.UnsupportedOperationException>();
        }

        /// <summary>
        /// A Spark handler is refused rather than ignored.
        /// </summary>
        /// <remarks>
        /// Calcite hands the generated class and its source to <c>SparkHandler.compile</c>. There is neither
        /// here, so the parameter is taken — so that a caller's configuration is seen — and refused.
        /// </remarks>
        [TestMethod]
        public void ShouldRefuseASparkHandler()
        {
            var (physical, _) = Plan("SELECT \"ID\", \"NAME\" FROM \"PEOPLE\"");

            var act = () => ClrEnumerableInterpretable.ToBindable(new java.util.HashMap(), new EnabledSparkHandler(), physical, ClrEnumerablePrefer.Array);

            act.Should().Throw<java.lang.UnsupportedOperationException>().WithMessage("*Spark*");
        }

        /// <summary>
        /// A Spark handler that says it is on, which is the only thing asked of it before it is refused.
        /// </summary>
        sealed class EnabledSparkHandler : org.apache.calcite.jdbc.CalcitePrepare.SparkHandler
        {

            /// <inheritdoc />
            public bool enabled() => true;

            /// <inheritdoc />
            public org.apache.calcite.rel.RelNode flattenTypes(RelOptPlanner planner, org.apache.calcite.rel.RelNode rootRel, bool restructure) => throw new java.lang.UnsupportedOperationException();

            /// <inheritdoc />
            public void registerRules(org.apache.calcite.jdbc.CalcitePrepare.SparkHandler.RuleSetBuilder builder) => throw new java.lang.UnsupportedOperationException();

            /// <inheritdoc />
            public org.apache.calcite.runtime.ArrayBindable compile(org.apache.calcite.linq4j.tree.ClassDeclaration expr, string s) => throw new java.lang.UnsupportedOperationException();

            /// <inheritdoc />
            public object sparkContext() => throw new java.lang.UnsupportedOperationException();

        }

        /// <summary>
        /// A combine gives one row per index, each column holding one query values as a map, and the row
        /// count is the largest of the inputs.
        /// </summary>
        /// <remarks>
        /// No SQL statement produces a <c>Combine</c>: it exists for multi-root optimisation in the planner,
        /// and a caller builds one with <c>RelBuilder.combine</c>. So this is built rather than parsed —
        /// which is the only way to run the node at all, and is why it is run rather than assumed.
        /// </remarks>
        [TestMethod]
        public void ShouldCombineTwoQueries()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("PEOPLE", new PeopleTable());

            var config = Frameworks.newConfigBuilder().defaultSchema(rootSchema).build();
            var builder = RelBuilder.create(config);

            // three names against two ids, so the shorter query runs out and contributes null. The literal
            // is a java.lang.Integer: RelBuilder.literal takes an Object, and a CLR-boxed int arrives as
            // cli.System.Int32, which it refuses -- the same invariant JavaValues.From keeps everywhere else
            var logical = builder
                .scan("PEOPLE").project(builder.field("NAME"))
                .scan("PEOPLE")
                    .filter(builder.call(org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN, builder.field("ID"), builder.literal(java.lang.Integer.valueOf(3))))
                    .project(builder.field("ID"))
                .combine()
                .build();

            var planner = (org.apache.calcite.plan.volcano.VolcanoPlanner)logical.getCluster().getPlanner();
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            foreach (var rule in ClrEnumerableRules.Rules())
                planner.addRule((RelOptRule)rule);

            var traitSet = logical.getTraitSet().replace(ClrEnumerableConvention.Instance).simplify();
            planner.setRoot(planner.changeTraits(logical, traitSet));

            var chosen = planner.findBestExp();

            // the second pass Programs.standard makes: a project refuses to implement itself, and the calc
            // rules are what rewrite every one of them into a calc
            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrEnumerableRules.CalcRules())
                calcRules.add(rule);

            var physical = (ClrEnumerableRel)Programs
                .hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE)
                .run(planner, chosen, chosen.getTraitSet(), new java.util.ArrayList(), new java.util.ArrayList());

            physical.Should().BeOfType<Apache.Calcite.Extensions.Adapter.Enumerable.ClrEnumerableCombine>();

            var bindable = ClrEnumerableInterpretable.ToBindable(new java.util.HashMap(), null, physical, ClrEnumerablePrefer.Array);

            var rows = new List<object[]>();
            foreach (var current in bindable.Bind(new TestDataContext(rootSchema)))
                rows.Add(current as object[] ?? [current]);

            rows.Should().HaveCount(3);
            ((java.util.Map)rows[0][0]).get("NAME").Should().Be("SMITH");
            ((java.util.Map)rows[0][1]).get("ID").Should().Be(java.lang.Integer.valueOf(1));

            // the second query has two rows, so the third has nothing to hold
            rows[2][0].Should().NotBeNull();
            rows[2][1].Should().BeNull();
        }

        [TestMethod]
        public void ShouldScanATable()
        {
            var rows = Run("SELECT \"ID\", \"NAME\" FROM \"PEOPLE\"");

            rows.Should().HaveCount(3);
            rows.Select(r => (string)r[1]).Should().BeEquivalentTo(["SMITH", "JONES", "BROWN"]);
        }

        [TestMethod]
        public void ShouldFilter()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"AGE\" > 25");

            rows.Select(r => (string)r[0]).Should().BeEquivalentTo(["SMITH", "JONES"]);
        }

        [TestMethod]
        public void ShouldProjectAnExpression()
        {
            var rows = Run("SELECT \"AGE\" + 1 FROM \"PEOPLE\" WHERE \"ID\" = 1");

            rows.Should().ContainSingle();
            rows[0][0].Should().Be(java.lang.Integer.valueOf(31));
        }

        [TestMethod]
        public void ShouldSort()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" ORDER BY \"AGE\"");

            rows.Select(r => (string)r[0]).Should().Equal("BROWN", "SMITH", "JONES");
        }

        [TestMethod]
        public void ShouldSortDescending()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" ORDER BY \"AGE\" DESC");

            rows.Select(r => (string)r[0]).Should().Equal("JONES", "SMITH", "BROWN");
        }

        [TestMethod]
        public void ShouldLimit()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" ORDER BY \"ID\" FETCH NEXT 2 ROWS ONLY");

            rows.Select(r => (string)r[0]).Should().Equal("SMITH", "JONES");
        }

        [TestMethod]
        public void ShouldOffsetAndLimit()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" ORDER BY \"ID\" OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY");

            rows.Select(r => (string)r[0]).Should().Equal("JONES");
        }

        [TestMethod]
        public void ShouldComputeOverANullableColumn()
        {
            // a nullable column is a java.lang.Integer and the arithmetic is on an int, so this is the query
            // that makes RexImpTable emit the boxing and unboxing the tests otherwise never reach
            var rows = Run("SELECT \"AGE\" + \"BONUS\" FROM \"PEOPLE\" ORDER BY \"ID\"");

            rows.Should().HaveCount(3);
            rows[0][0].Should().Be(java.lang.Integer.valueOf(35));
            rows[1][0].Should().BeNull();
            rows[2][0].Should().Be(java.lang.Integer.valueOf(27));
        }

        [TestMethod]
        public void ShouldCountEveryRow()
        {
            var rows = Run("SELECT COUNT(*) FROM \"PEOPLE\"");

            rows.Should().ContainSingle();
            rows[0][0].Should().Be(java.lang.Long.valueOf(3L));
        }

        [TestMethod]
        public void ShouldAggregateWithoutAGroup()
        {
            var rows = Run("SELECT SUM(\"AGE\"), MIN(\"AGE\"), MAX(\"AGE\") FROM \"PEOPLE\"");

            rows.Should().ContainSingle();
            rows[0][0].Should().Be(java.lang.Integer.valueOf(90));
            rows[0][1].Should().Be(java.lang.Integer.valueOf(20));
            rows[0][2].Should().Be(java.lang.Integer.valueOf(40));
        }

        [TestMethod]
        public void ShouldGroupBy()
        {
            var rows = Run("SELECT \"NAME\", COUNT(*) FROM \"PEOPLE\" GROUP BY \"NAME\" ORDER BY \"NAME\"");

            rows.Should().HaveCount(3);
            rows.Select(r => (string)r[0]).Should().Equal("BROWN", "JONES", "SMITH");
            rows.Select(r => r[1]).Should().AllBeEquivalentTo(java.lang.Long.valueOf(1L));
        }

        [TestMethod]
        public void ShouldGroupByAndSum()
        {
            var rows = Run("SELECT \"AGE\" > 25, SUM(\"AGE\") FROM \"PEOPLE\" GROUP BY \"AGE\" > 25 ORDER BY 1");

            rows.Should().HaveCount(2);
            rows[0][1].Should().Be(java.lang.Integer.valueOf(20));
            rows[1][1].Should().Be(java.lang.Integer.valueOf(70));
        }

        [TestMethod]
        public void ShouldAggregateOverANullableColumn()
        {
            // SUM skips a null, so this is 12 rather than null
            var rows = Run("SELECT SUM(\"BONUS\") FROM \"PEOPLE\"");

            rows.Should().ContainSingle();
            rows[0][0].Should().Be(java.lang.Integer.valueOf(12));
        }

        [TestMethod]
        public void ShouldInnerJoin()
        {
            var rows = Run("SELECT a.\"NAME\", b.\"AGE\" FROM \"PEOPLE\" a JOIN \"PEOPLE\" b ON a.\"ID\" = b.\"ID\" WHERE a.\"ID\" = 1");

            rows.Should().ContainSingle();
            rows[0][0].Should().Be("SMITH");
            rows[0][1].Should().Be(java.lang.Integer.valueOf(30));
        }

        [TestMethod]
        public void ShouldLeftJoinAndPadWithNulls()
        {
            var rows = Run("SELECT a.\"NAME\", b.\"NAME\" FROM \"PEOPLE\" a LEFT JOIN (SELECT * FROM \"PEOPLE\" WHERE \"ID\" = 1) b ON a.\"ID\" = b.\"ID\" ORDER BY a.\"ID\"");

            rows.Should().HaveCount(3);
            rows[0][1].Should().Be("SMITH");
            rows[1][1].Should().BeNull();
            rows[2][1].Should().BeNull();
        }

        [TestMethod]
        public void ShouldJoinOnMoreThanAnEquality()
        {
            var rows = Run("SELECT a.\"NAME\" FROM \"PEOPLE\" a JOIN \"PEOPLE\" b ON a.\"ID\" = b.\"ID\" AND a.\"AGE\" > 25");

            rows.Select(r => (string)r[0]).Should().BeEquivalentTo(["SMITH", "JONES"]);
        }

        [TestMethod]
        public void ShouldJoinOnAnInequalityAlone()
        {
            // no equality to build a lookup on, so the hash join rule refuses and the nested loop takes it
            var rows = Run("SELECT a.\"NAME\", b.\"NAME\" FROM \"PEOPLE\" a JOIN \"PEOPLE\" b ON a.\"AGE\" < b.\"AGE\"");

            rows.Should().HaveCount(3);
        }

        [TestMethod]
        public void ShouldRunACorrelatedSubQuery()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" a WHERE \"AGE\" = (SELECT MAX(\"AGE\") FROM \"PEOPLE\" b WHERE b.\"ID\" = a.\"ID\")");

            rows.Select(r => (string)r[0]).Should().BeEquivalentTo(["SMITH", "JONES", "BROWN"]);
        }

        [TestMethod]
        public void ShouldUnionAll()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" = 1 UNION ALL SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" = 1");

            rows.Select(r => (string)r[0]).Should().Equal("SMITH", "SMITH");
        }

        [TestMethod]
        public void ShouldUnionDistinct()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" = 1 UNION SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" = 1");

            rows.Select(r => (string)r[0]).Should().Equal("SMITH");
        }

        [TestMethod]
        public void ShouldIntersect()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"AGE\" > 25 INTERSECT SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" = 1");

            rows.Select(r => (string)r[0]).Should().Equal("SMITH");
        }

        [TestMethod]
        public void ShouldExcept()
        {
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" EXCEPT SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"AGE\" > 25");

            rows.Select(r => (string)r[0]).Should().Equal("BROWN");
        }

        [TestMethod]
        public void ShouldUnionWholeRowsRatherThanCompareArraysByReference()
        {
            // a row of JavaRowFormat.ARRAY is an array, and two equal rows are two arrays. Without the comparer
            // PhysType gives for the format, a distinct union would keep both.
            var rows = Run("SELECT \"ID\", \"NAME\" FROM \"PEOPLE\" UNION SELECT \"ID\", \"NAME\" FROM \"PEOPLE\"");

            rows.Should().HaveCount(3);
        }

        [TestMethod]
        public void ShouldSortAndLimitTogether()
        {
            // a sort carrying a fetch is one node, so only as many rows as are wanted are kept
            var rows = Run("SELECT \"NAME\" FROM \"PEOPLE\" ORDER BY \"AGE\" DESC FETCH NEXT 2 ROWS ONLY");

            rows.Select(r => (string)r[0]).Should().Equal("JONES", "SMITH");
        }

        [TestMethod]
        public void ShouldCollectASubQueryIntoAMultiset()
        {
            var rows = Run("SELECT MULTISET(SELECT \"NAME\" FROM \"PEOPLE\") FROM (VALUES (1))");

            rows.Should().ContainSingle();
            ((java.util.List)rows[0][0]).size().Should().Be(3);
        }

        [TestMethod]
        public void ShouldUncollectAnArray()
        {
            var rows = Run("SELECT * FROM UNNEST(ARRAY['a', 'b', 'c'])");

            rows.Should().HaveCount(3);
            rows.Select(r => (string)r[0]).Should().Equal("a", "b", "c");
        }

        [TestMethod]
        public void ShouldReadValues()
        {
            var rows = Run("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y)");

            rows.Should().HaveCount(2);
            rows.Select(r => (string)r[1]).Should().Equal("a", "b");
        }

    }

}
