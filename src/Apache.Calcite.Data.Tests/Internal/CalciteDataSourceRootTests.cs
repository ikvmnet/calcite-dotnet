using System;
using System.Collections.Generic;
using System.Linq;

using org.apache.calcite.jdbc;
using org.apache.calcite.schema.impl;

using Xunit;

using Apache.Calcite.Data.Tests;

namespace Apache.Calcite.Data.Internal.Tests
{

    /// <summary>
    /// What an isolated data source hands a session: a root assembled for it from the model and the builder's
    /// steps, over the schema objects the model's factories made once, and kept for the next session while
    /// nothing has altered it.
    /// </summary>
    public class CalciteDataSourceRootTests
    {

        static readonly string Factory = typeof(CountingSchemaFactory).FullName + ", " + typeof(CountingSchemaFactory).Assembly.GetName().Name;

        /// <summary>
        /// A model with a custom schema over <see cref="CountingSchemaFactory"/>, and a map schema holding a
        /// view, a type and the <c>cache</c> setting.
        /// </summary>
        static string Model(out string token)
        {
            token = Guid.NewGuid().ToString("N");
            return "Model=inline:{\"version\":\"1.0\",\"defaultSchema\":\"S\",\"schemas\":["
                + "{\"name\":\"C\",\"type\":\"custom\",\"factory\":\"" + Factory + "\",\"operand\":{\"token\":\"" + token + "\"}},"
                + "{\"name\":\"S\",\"type\":\"map\",\"cache\":false,"
                + "\"types\":[{\"name\":\"T\",\"type\":\"INTEGER\"}],"
                + "\"tables\":[{\"name\":\"V\",\"type\":\"view\",\"sql\":\"VALUES (1), (2)\"}]}]}";
        }

        static CalciteDataSourceRoot Build(TimeSpan idleLifetime, params Action<org.apache.calcite.schema.SchemaPlus>[] configure)
        {
            return CalciteDataSourceRoot.Build(new CalciteConnectionStringBuilder(Model(out _)), configure, sharing: CalciteRootSharing.Isolated, idleLifetime: idleLifetime);
        }

        [Fact]
        public void Each_lease_should_be_a_root_of_its_own_over_the_schemas_the_factories_made_once()
        {
            var root = CalciteDataSourceRoot.Build(new CalciteConnectionStringBuilder(Model(out var token) + ";Conformance=ORACLE_12"), [], sharing: CalciteRootSharing.Isolated, idleLifetime: TimeSpan.FromMinutes(5));
            var a = root.Rent().Schema;
            var b = root.Rent().Schema;

            Assert.NotSame(a, b);
            Assert.Equal(1, CountingSchemaFactory.Builds[token]);
            Assert.Same(a.getSubSchema("C", true)!.schema, b.getSubSchema("C", true)!.schema);
            Assert.Same(a, a.getSubSchema("C", true)!.root());
            Assert.Same(b, b.getSubSchema("C", true)!.root());

            foreach (var r in new[] { a, b })
            {
                Assert.Contains("DUAL", Names(r.getFunctionNames()));
                Assert.NotNull(r.getSubSchema("metadata", true));

                var s = r.getSubSchema("S", true)!;
                Assert.Contains("V", Names(s.getFunctionNames()));
                Assert.Contains("T", Names(s.getTypeNames()));
                Assert.False(s.plus().isCacheEnabled());
            }

            // a map schema is the root's own
            Assert.NotSame(a.getSubSchema("S", true)!.schema, b.getSubSchema("S", true)!.schema);
        }

        [Fact]
        public void A_returned_root_should_be_rented_again_while_nothing_altered_it()
        {
            var root = Build(TimeSpan.FromMinutes(5));
            var lease = root.Rent();

            root.Return(lease, altered: false);

            Assert.Same(lease, root.Rent());
        }

        [Fact]
        public void A_root_DDL_altered_should_not_be_rented_again()
        {
            var root = Build(TimeSpan.FromMinutes(5));
            var lease = root.Rent();

            root.Return(lease, altered: true);

            Assert.NotSame(lease, root.Rent());
        }

        /// <summary>
        /// A recursive query's working table is registered at the root while it runs; one left behind marks
        /// the root as altered even though no DDL ran.
        /// </summary>
        [Fact]
        public void A_root_with_a_table_left_at_it_should_not_be_rented_again()
        {
            var root = Build(TimeSpan.FromMinutes(5));
            var lease = root.Rent();
            lease.Schema.add("left", new EmptyTable());

            root.Return(lease, altered: false);

            var next = root.Rent();
            Assert.NotSame(lease, next);
            Assert.Null(next.Schema.getTable("left", true));
        }

        [Fact]
        public void A_root_idle_for_the_idle_lifetime_should_not_be_rented_again()
        {
            var root = Build(TimeSpan.Zero);
            var lease = root.Rent();

            root.Return(lease, altered: false);

            Assert.NotSame(lease, root.Rent());
        }

        [Fact]
        public void A_dropped_root_should_dispose_the_schemas_DDL_added_and_leave_the_data_sources_alone()
        {
            var shared = new DisposableSchema();
            var root = Build(TimeSpan.FromMinutes(5), r => r.add("D", shared));
            var lease = root.Rent();
            var added = new DisposableSchema();
            lease.Schema.add("X", added);

            root.Return(lease, altered: true);

            Assert.True(added.Disposed);
            Assert.False(shared.Disposed);
        }

        /// <summary>
        /// A step runs for every root. A schema it makes each time belongs to the root it was made for; the
        /// first root's belongs to the data source, which disposes it when its root is released.
        /// </summary>
        [Fact]
        public void A_schema_a_step_makes_for_each_root_should_be_disposed_with_that_root()
        {
            var made = new List<DisposableSchema>();
            var root = Build(TimeSpan.FromMinutes(5), r =>
            {
                var schema = new DisposableSchema();
                made.Add(schema);
                r.add("P", schema);
            });
            root.Acquire();
            var first = root.Rent();
            var second = root.Rent();
            Assert.Equal(2, made.Count);

            root.Return(second, altered: true);
            Assert.False(made[0].Disposed);
            Assert.True(made[1].Disposed);

            root.Return(first, altered: false);
            root.Retire();
            root.Release();
            Assert.True(made[0].Disposed);
        }

        [Fact]
        public void A_root_returned_after_retirement_should_not_be_kept()
        {
            var shared = new DisposableSchema();
            var root = Build(TimeSpan.FromMinutes(5), r => r.add("D", shared));
            root.Acquire();
            var lease = root.Rent();

            root.Retire();
            root.Return(lease, altered: false);
            Assert.False(shared.Disposed);

            root.Release();
            Assert.True(shared.Disposed);
        }

        [Fact]
        public void A_schema_a_factory_made_should_be_disposed_when_the_data_source_is_released()
        {
            var root = CalciteDataSourceRoot.Build(new CalciteConnectionStringBuilder(Model(out var token)), [], sharing: CalciteRootSharing.Isolated, idleLifetime: TimeSpan.FromMinutes(5));
            root.Acquire();
            root.Return(root.Rent(), altered: true);
            Assert.False(CountingSchemaFactory.Made[token].Single().Disposed);

            root.Retire();
            root.Release();
            Assert.True(CountingSchemaFactory.Made[token].Single().Disposed);
        }

        static string[] Names(java.util.Collection names)
        {
            return names.toArray().Cast<string>().ToArray();
        }

        /// <summary>
        /// A table with no columns, standing in for the working table a recursive query registers.
        /// </summary>
        sealed class EmptyTable : AbstractTable
        {

            /// <inheritdoc />
            public override org.apache.calcite.rel.type.RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder().build();
            }

        }

    }

}
