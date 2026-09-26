using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Enumerable;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Runs the example in the project README, so that it cannot go stale without a test going red.
    /// </summary>
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

        /// <summary>
        /// The example from the README, run as written: one factory, opened both ways.
        /// </summary>
        [Fact]
        public async System.Threading.Tasks.Task ShouldRunTheExampleFromTheReadme()
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("PEOPLE", new PeopleTable());

            var sql = "SELECT \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" = 2";
            var dataContext = new ExampleDataContext(rootSchema);
            var cancellationToken = System.Threading.CancellationToken.None;

            var calcRules = new java.util.ArrayList();
            foreach (var rule in Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRules.CalcRules())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.sequence(
                        new AddRulesProgram(Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRules.Rules()),
                        Programs.standard(),
                        Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE)))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var traits = logical.getTraitSet().replace(Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorConvention.Instance).simplify();
            var physical = planner.transform(0, traits, logical);

            // ---- README example begins ----
            var implementor = new Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRelImplementor(
                physical.getCluster().getRexBuilder(), new java.util.HashMap());
            var factory = implementor.ImplementRoot((Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRel)physical, ClrEnumerablePrefer.Array);

            // opening runs the plan's acquisition -- a sort drains, a leaf executes -- and reading reads rows
            await using var cursor = await factory.OpenAsync(dataContext, cancellationToken);
            while (await cursor.ReadAsync(cancellationToken))
                Console.WriteLine(cursor.Current);

            // or synchronously, over the same factory
            using var pulled = factory.Open(dataContext);
            while (pulled.Read())
                Console.WriteLine(pulled.Current);
            // ---- README example ends ----

            var awaited = new List<object?>();
            await using (var again = await factory.OpenAsync(dataContext, cancellationToken))
                while (await again.ReadAsync(cancellationToken))
                    awaited.Add(again.Current);

            var read = new List<object?>();
            using (var again = factory.Open(dataContext))
                while (again.Read())
                    read.Add(again.Current);

            awaited.Should().Equal(["Bob"]);
            read.Should().Equal(["Bob"]);
        }

    }

}
