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
            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrEnumerableRules.CalcRules())
                calcRules.add(rule);

            // Programs.standard(), with this convention's rules put on the planner in front of it -- a
            // Frameworks planner carries Calcite's alone -- and its calc rules run afterwards, which is
            // Programs.calc once more over this convention's list. standard's own calc pass still runs
            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.sequence(
                        new AddRulesProgram(ClrEnumerableRules.Rules()),
                        Programs.standard(),
                        Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE)))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();

            // one sequence, so one transform, exactly as Programs.standard is driven.
            // the logical root's own traits, not an empty set: they carry the collation the ORDER BY produced,
            // and SortRemoveRule takes the sort away as unwanted if the required traits do not ask for it
            var traits = logical.getTraitSet().replace(ClrEnumerableConvention.Instance).simplify();
            var physical = (ClrEnumerableRel)planner.transform(0, traits, logical);

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
