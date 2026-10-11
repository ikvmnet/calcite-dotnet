using System;
using System.Linq;

using org.apache.calcite.jdbc;
using org.apache.calcite.schema.impl;

using Xunit;

using Apache.Calcite.Data.Tests;

namespace Apache.Calcite.Data.Internal.Tests
{

    /// <summary>
    /// What an isolated root hands a session: a copy of the template's wrapper tree over the same schema
    /// objects, kept for the next session while nothing has altered it.
    /// </summary>
    public class CalciteDataSourceRootTests
    {

        /// <summary>
        /// A model whose sub-schema carries what a copy has to carry over: a view (a function), a type, and the
        /// <c>cache</c> setting, which the model applies after the schema is added.
        /// </summary>
        const string Model = "Model=inline:{\"version\":\"1.0\",\"defaultSchema\":\"S\",\"schemas\":["
            + "{\"name\":\"S\",\"type\":\"map\",\"cache\":false,"
            + "\"types\":[{\"name\":\"T\",\"type\":\"INTEGER\"}],"
            + "\"tables\":[{\"name\":\"V\",\"type\":\"view\",\"sql\":\"VALUES (1), (2)\"}]},"
            + "{\"name\":\"S2\",\"type\":\"map\"}]}";

        static CalciteDataSourceRoot Build(TimeSpan idleLifetime)
        {
            return CalciteDataSourceRoot.Build(new CalciteConnectionStringBuilder(Model), [], sharing: CalciteRootSharing.Isolated, idleLifetime: idleLifetime);
        }

        [Fact]
        public void A_copy_should_hold_the_template_entries_over_the_same_schemas()
        {
            // a conformance with a DUAL table, so that the root holds a function of its own
            var root = CalciteDataSourceRoot.Build(new CalciteConnectionStringBuilder(Model + ";Conformance=ORACLE_12"), [], sharing: CalciteRootSharing.Isolated, idleLifetime: TimeSpan.FromMinutes(5));
            var template = root.Schema;
            var copy = root.Rent();

            Assert.NotSame(template, copy);
            Assert.Same(template.schema, copy.schema);
            Assert.Equal(Names(template.getSubSchemaMap().keySet()), Names(copy.getSubSchemaMap().keySet()));
            Assert.Equal(Names(template.getFunctionNames()), Names(copy.getFunctionNames()));
            Assert.Contains("DUAL", Names(copy.getFunctionNames()));
            Assert.NotNull(copy.getSubSchema("metadata", true));

            var templateSub = template.getSubSchema("S", true)!;
            var copySub = copy.getSubSchema("S", true)!;
            Assert.NotSame(templateSub, copySub);
            Assert.Same(templateSub.schema, copySub.schema);
            Assert.Equal(Names(templateSub.getFunctionNames()), Names(copySub.getFunctionNames()));
            Assert.Contains("V", Names(copySub.getFunctionNames()));
            Assert.Equal(Names(templateSub.getTypeNames()), Names(copySub.getTypeNames()));
            Assert.Contains("T", Names(copySub.getTypeNames()));
            Assert.False(copySub.plus().isCacheEnabled());
            Assert.Same(copy, copySub.root());
        }

        [Fact]
        public void A_returned_copy_should_be_rented_again_while_nothing_altered_it()
        {
            var root = Build(TimeSpan.FromMinutes(5));
            var copy = root.Rent();

            root.Return(copy, altered: false);

            Assert.Same(copy, root.Rent());
        }

        [Fact]
        public void A_copy_DDL_altered_should_not_be_rented_again()
        {
            var root = Build(TimeSpan.FromMinutes(5));
            var copy = root.Rent();

            root.Return(copy, altered: true);

            Assert.NotSame(copy, root.Rent());
        }

        /// <summary>
        /// A recursive query's working table is registered at the root while it runs; one left behind marks
        /// the copy as altered even though no DDL ran.
        /// </summary>
        [Fact]
        public void A_copy_with_a_table_left_at_its_root_should_not_be_rented_again()
        {
            var root = Build(TimeSpan.FromMinutes(5));
            var copy = root.Rent();
            copy.add("left", new EmptyTable());

            root.Return(copy, altered: false);

            Assert.NotSame(copy, root.Rent());
            Assert.Null(root.Schema.getTable("left", true));
        }

        [Fact]
        public void A_copy_idle_for_the_idle_lifetime_should_not_be_rented_again()
        {
            var root = Build(TimeSpan.Zero);
            var copy = root.Rent();

            root.Return(copy, altered: false);

            Assert.NotSame(copy, root.Rent());
        }

        [Fact]
        public void A_discarded_copy_should_dispose_the_schemas_DDL_added_and_leave_the_template_alone()
        {
            var shared = new DisposableSchema();
            var root = CalciteDataSourceRoot.Build(new CalciteConnectionStringBuilder(Model), [r => r.add("D", shared)], sharing: CalciteRootSharing.Isolated, idleLifetime: TimeSpan.FromMinutes(5));
            var copy = root.Rent();
            var added = new DisposableSchema();
            copy.add("X", added);

            root.Return(copy, altered: true);

            Assert.True(added.Disposed);
            Assert.False(shared.Disposed);
        }

        [Fact]
        public void A_copy_returned_after_the_root_was_retired_should_not_be_kept()
        {
            var shared = new DisposableSchema();
            var root = CalciteDataSourceRoot.Build(new CalciteConnectionStringBuilder(Model), [r => r.add("D", shared)], sharing: CalciteRootSharing.Isolated, idleLifetime: TimeSpan.FromMinutes(5));
            root.Acquire();
            var copy = root.Rent();

            root.Retire();
            root.Return(copy, altered: false);
            Assert.False(shared.Disposed);

            root.Release();
            Assert.True(shared.Disposed);
        }

        [Fact]
        public void A_root_that_cannot_be_copied_should_be_refused_for_isolation()
        {
            Assert.Throws<ArgumentException>(() => CalciteDataSourceRoot.Build(new CalciteConnectionStringBuilder(), [], rootSchema: CalciteSchema.createRootSchema(true), sharing: CalciteRootSharing.Isolated));
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
