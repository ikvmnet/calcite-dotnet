using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;

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
    /// Runs the code example in the <c>Apache.Calcite.Extensions</c> README.
    /// </summary>
    /// <remarks>
    /// The code between the <c>README example</c> markers appears in the README with the same tokens and
    /// comments, and the planning code before them with the same tokens; keep them in step.
    /// </remarks>
    public class ReadmeExampleTests
    {

        /// <summary>
        /// The table the example queries: two rows of <c>ID</c> and <c>NAME</c>.
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
        /// The data context the compiled plan is opened against.
        /// </summary>
        /// <param name="schema">The root schema the plan was planned against.</param>
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
        /// Runs the README example, then opens the same factory both ways again and checks the rows.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
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
                        Programs.standard(Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider),
                        Programs.hep(calcRules, true, Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider)))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var traits = logical.getTraitSet().replace(Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorConvention.Instance).simplify();
            var physical = planner.transform(0, traits, logical);

            // ---- README example begins ----
            var implementor = new Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRelImplementor(
                physical.getCluster().getRexBuilder(), new java.util.HashMap());
            var factory = implementor.ImplementRoot((Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRel)physical, ClrCursorPrefer.Array);

            // opening does the plan's up-front work (a sort drains its input, a table runs its query); reading returns rows
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
