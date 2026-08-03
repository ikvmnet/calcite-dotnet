using System;
using System.Collections.Generic;
using System.Linq;

using Apache.Calcite.Linq;

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

namespace Apache.Calcite.Linq.Tests
{

    /// <summary>
    /// Runs a query end to end in the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    [TestClass]
    public class ClrEnumerableQueryTests
    {

        /// <summary>
        /// A table of three rows, given to Calcite the way any table is.
        /// </summary>
        sealed class PeopleTable : AbstractTable, ScannableTable
        {

            static readonly object[][] Rows =
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
        /// Plans a query into the convention, compiles it, and returns its rows.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        static List<object[]> Run(string sql)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("PEOPLE", new PeopleTable());

            var rules = new java.util.ArrayList();
            foreach (var rule in ClrEnumerableRules.Rules())
                rules.add(rule);

            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrEnumerableRules.CalcRules())
                calcRules.add(rule);

            // the same two passes Programs.standard makes: the planner chooses a plan, and a hep pass then
            // rewrites every project and filter in it into a calc
            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
                    Programs.ofRules(rules),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var parsed = planner.parse(sql);
            var validated = planner.validate(parsed);
            var logical = planner.rel(validated).project();

            // the passes Programs.standard makes, less the decorrelation: a sub-query has to be expanded
            // before the planner sees it, because a filter carrying one is refused as Calcite refuses it, and
            // leaving the correlate in place is what puts ClrEnumerableCorrelate on the plan at all
            var expanded = planner.transform(0, logical.getTraitSet(), logical);

            var traitSet = planner.getEmptyTraitSet().replace(ClrEnumerableConvention.Instance);
            var chosen = planner.transform(1, traitSet, expanded);
            var physical = (ClrEnumerableRel)planner.transform(2, chosen.getTraitSet(), chosen);

            org.apache.calcite.runtime.Bindable bindable;
            try
            {
                bindable = ClrInterpretable.ToBindable(new java.util.HashMap(), physical, EnumerableRel.Prefer.ARRAY);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"{e.Message}{Environment.NewLine}{RelOptUtil.toString(physical)}", e);
            }

            var rows = new List<object[]>();
            var enumerator = bindable.bind(new TestDataContext(rootSchema)).enumerator();
            while (enumerator.moveNext())
            {
                var current = enumerator.current();
                rows.Add(current as object[] ?? [current]);
            }

            return rows;
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
