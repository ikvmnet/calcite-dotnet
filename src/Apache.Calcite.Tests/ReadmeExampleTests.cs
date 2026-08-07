using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;
using org.apache.calcite.tools;
using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Runs the example in the project README, so that it cannot go stale without a test going red.
    /// </summary>
    [TestClass]
    public class ReadmeExampleTests
    {

        /// <summary>
        /// The table the example queries.
        /// </summary>
        class PeopleTable : AbstractTable, ScannableTable
        {

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR, 20))
                    .build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                return org.apache.calcite.linq4j.Linq4j.asEnumerable(new object[][]
                {
                    [java.lang.Integer.valueOf(1), "Alice"],
                    [java.lang.Integer.valueOf(2), "Bob"],
                });
            }

        }

        /// <summary>
        /// The context the compiled plan is bound to.
        /// </summary>
        class ExampleDataContext(SchemaPlus schema) : DataContext
        {

            /// <inheritdoc />
            public SchemaPlus getRootSchema() => schema;

            /// <inheritdoc />
            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() =>
                new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            /// <inheritdoc />
            public org.apache.calcite.linq4j.QueryProvider getQueryProvider() => null!;

            /// <inheritdoc />
            public object get(string name) => null!;

        }

        [TestMethod]
        public void ShouldRunTheExampleFromTheReadme()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("PEOPLE", new PeopleTable());

            var sql = "SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" = 2";
            var dataContext = new ExampleDataContext(rootSchema);

            // ---- README example begins ----
            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(ClrEnumerablePrograms.Standard())
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();

            // Standard() is three passes, in this order: sub-query expansion, the planner, then the calc rules.
            var expanded = planner.transform(0, logical.getTraitSet(), logical);
            var traits = ClrEnumerablePrograms.DesiredRootTraitSet(planner.getEmptyTraitSet());
            var chosen = planner.transform(1, traits, expanded);
            var physical = (ClrEnumerableRel)planner.transform(2, chosen.getTraitSet(), chosen);

            // the root is a node of this convention; build its plan and compile it
            var implementor = new ClrEnumerableRelImplementor(
                physical.getCluster().getRexBuilder(), new java.util.HashMap());
            var lambda = implementor.ImplementRoot(physical, ClrEnumerablePrefer.Array);
            var plan = (Func<DataContext, System.Collections.IEnumerable>)lambda.Compile();

            foreach (var current in plan(dataContext))
            {
                // a one-column result is the value itself, not a row of one
                var row = current as object[] ?? [current];
                Console.WriteLine(string.Join('\t', row));
            }
            // ---- README example ends ----

            // and again, collecting rather than printing, to assert what it produced
            var rows = new List<object[]>();
            foreach (var current in plan(dataContext))
                rows.Add(current as object[] ?? [current]);

            rows.Should().HaveCount(1);
            rows[0].Should().HaveCount(1);
            rows[0][0].Should().Be("Bob");
        }

    }

}
