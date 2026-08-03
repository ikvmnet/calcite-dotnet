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
                [java.lang.Integer.valueOf(1), "SMITH", java.lang.Integer.valueOf(30)],
                [java.lang.Integer.valueOf(2), "JONES", java.lang.Integer.valueOf(40)],
                [java.lang.Integer.valueOf(3), "BROWN", java.lang.Integer.valueOf(20)],
            ];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .add("AGE", typeFactory.createSqlType(SqlTypeName.INTEGER))
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
                    Programs.ofRules(rules),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var parsed = planner.parse(sql);
            var validated = planner.validate(parsed);
            var logical = planner.rel(validated).project();

            var traitSet = planner.getEmptyTraitSet().replace(ClrEnumerableConvention.Instance);
            var chosen = planner.transform(0, traitSet, logical);
            var physical = (ClrEnumerableRel)planner.transform(1, chosen.getTraitSet(), chosen);

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
        public void ShouldReadValues()
        {
            var rows = Run("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y)");

            rows.Should().HaveCount(2);
            rows.Select(r => (string)r[1]).Should().Equal("a", "b");
        }

    }

}
