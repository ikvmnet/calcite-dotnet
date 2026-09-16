using System;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Covers the collection accessor, which ADO.NET has none of and JDBC spells <c>getArray</c>.
    /// </summary>
    /// <remarks>
    /// A collection is a core Calcite type, so reading one should not mean naming a .NET type through
    /// <c>GetFieldValue</c> and hoping it is the one the column produces. The rule these hold is that it is
    /// as strict as every other typed getter: a column that is not a collection is refused rather than
    /// wrapped in an array of one.
    /// </remarks>
    public class CalciteGetArrayTests
    {

        /// <summary>
        /// Opens a connection with <see cref="ArrayAnyTable"/> registered as <c>ANYT</c>.
        /// </summary>
        static CalciteConnection Open()
        {
            return new CalciteDataSourceBuilder(TestModels.InlineEmptyModelConnectionString)
                .ConfigureRootSchema(root => root.add("ANYT", new ArrayAnyTable()))
                .Build()
                .OpenConnection();
        }

        /// <summary>
        /// Runs a statement and advances to its single row.
        /// </summary>
        static CalciteDataReader Row(CalciteConnection c, string sql)
        {
            var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            var r = (CalciteDataReader)cmd.ExecuteReader();
            Assert.True(r.Read());
            return r;
        }

        [Fact]
        public void An_array_column_should_read_as_an_array()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[1, 2, 3]");

            Assert.Equal(new[] { 1, 2, 3 }, Assert.IsType<int[]>(r.GetArray(0)));
        }

        /// <summary>
        /// A <c>MULTISET</c> differs from an <c>ARRAY</c> in whether the order of the elements means
        /// anything, not in what holds them, so it answers here too.
        /// </summary>
        [Fact]
        public void A_multiset_column_should_read_as_an_array()
        {
            using var c = Open();
            using var r = Row(c, "SELECT MULTISET[1, 2, 3]");

            Assert.Equal(new[] { 1, 2, 3 }, Assert.IsType<int[]>(r.GetArray(0)));
        }

        [Fact]
        public void A_nested_array_column_should_read_at_every_level()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[ARRAY[1, 2], ARRAY[3]]");

            var outer = Assert.IsType<int[][]>(r.GetArray(0));
            Assert.Equal(new[] { 1, 2 }, outer[0]);
            Assert.Equal(new[] { 3 }, outer[1]);
        }

        /// <summary>
        /// An element that may be null makes the array nullable, an array having no other way to carry one.
        /// </summary>
        [Fact]
        public void An_array_holding_a_null_should_read_as_an_array_of_the_nullable_element()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[1, CAST(NULL AS INTEGER)]");

            Assert.Equal(new int?[] { 1, null }, Assert.IsType<int?[]>(r.GetArray(0)));
        }

        /// <summary>
        /// A <c>DATE</c> inside an array is a count of days and only the element type says so.
        /// </summary>
        [Fact]
        public void An_array_of_dates_should_read_as_dates()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[DATE '2020-01-02']");

            Assert.Equal(new[] { new DateTime(2020, 1, 2) }, Assert.IsType<DateTime[]>(r.GetArray(0)));
        }

        // ------------------------------------------------------------------------------------
        // Strict, like every other typed getter.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// A scalar is not a collection of one, a <c>MAP</c> is pairs rather than elements, and a <c>ROW</c>
        /// is fields. None of them is what this reads.
        /// </summary>
        [Theory]
        [InlineData("SELECT 1")]
        [InlineData("SELECT CAST('x' AS VARCHAR)")]
        [InlineData("SELECT MAP['a', 1]")]
        [InlineData("SELECT ROW(1, 2)")]
        public void A_column_that_is_not_a_collection_should_be_refused(string sql)
        {
            using var c = Open();
            using var r = Row(c, sql);

            Assert.Throws<InvalidCastException>(() => r.GetArray(0));
        }

        [Fact]
        public void A_null_collection_should_be_refused()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(NULL AS INTEGER ARRAY)");

            Assert.True(r.IsDBNull(0));
            Assert.Throws<InvalidCastException>(() => r.GetArray(0));
        }

        /// <summary>
        /// A column whose type says nothing leaves the value's own class to decide, here as everywhere else,
        /// so a list in an <c>ANY</c> column reads through this.
        /// </summary>
        [Fact]
        public void An_any_column_holding_a_list_should_read_as_an_array()
        {
            using var c = Open();
            using var r = Row(c, "SELECT \"L\" FROM \"ANYT\"");

            Assert.Equal(new[] { 1, 2, 3 }, Assert.IsType<int[]>(r.GetArray(0)));
        }

        [Fact]
        public void An_any_column_holding_a_scalar_should_be_refused()
        {
            using var c = Open();
            using var r = Row(c, "SELECT \"I\" FROM \"ANYT\"");

            Assert.Throws<InvalidCastException>(() => r.GetArray(0));
        }

        // ------------------------------------------------------------------------------------
        // Naming the element type.
        // ------------------------------------------------------------------------------------

        [Fact]
        public void Naming_the_element_type_should_answer_an_array_of_it()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[1, 2, 3]");

            Assert.Equal(new[] { 1, 2, 3 }, r.GetArray<int>(0));
        }

        [Fact]
        public void Naming_object_should_answer_an_array_of_object()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[1, 2]");

            Assert.Equal(new object[] { 1, 2 }, r.GetArray<object>(0));
        }

        /// <summary>
        /// <b>Naming the element type selects a mapping, and is not a cast of what the column reads back
        /// as.</b> A <c>DATE</c> is a <see cref="DateTime"/> by default and a <see cref="DateOnly"/> when
        /// asked, because the chain carries both; casting the default reading could never reach the second.
        /// </summary>
        [Fact]
        public void Naming_an_element_type_the_chain_carries_should_run_that_conversion()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[DATE '2020-01-02']");

            Assert.Equal(new[] { new DateTime(2020, 1, 2) }, Assert.IsType<DateTime[]>(r.GetArray(0)));
            Assert.Equal(new[] { new DateOnly(2020, 1, 2) }, r.GetArray<DateOnly>(0));
        }

        /// <summary>
        /// And a conversion the chain carries only when both types are named, which is nobody's default,
        /// is reached the same way.
        /// </summary>
        [Fact]
        public void Naming_an_element_type_that_is_nobody_s_default_should_still_be_reached()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[TIMESTAMP '2020-01-02 03:04:05']");

            Assert.Equal(new[] { new DateOnly(2020, 1, 2) }, r.GetArray<DateOnly>(0));
        }

        /// <summary>
        /// An element that may be null is refused where the named type cannot hold one, and answered where
        /// it can.
        /// </summary>
        [Fact]
        public void Naming_a_value_type_over_nullable_elements_should_be_refused()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[1, CAST(NULL AS INTEGER)]");

            Assert.Throws<InvalidCastException>(() => r.GetArray<int>(0));
            Assert.Equal(new int?[] { 1, null }, r.GetArray<int?>(0));
        }

        /// <summary>
        /// Naming an element type the chain does not carry to the column's is refused rather than converted
        /// into.
        /// </summary>
        [Fact]
        public void Naming_a_wider_element_type_should_be_refused()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[1, 2, 3]");

            Assert.Throws<InvalidCastException>(() => r.GetArray<long>(0));
        }

        [Fact]
        public void Naming_the_element_type_of_a_nested_array_should_answer_it()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[ARRAY[1, 2]]");

            Assert.Equal(new[] { 1, 2 }, r.GetArray<int[]>(0)[0]);
        }

        /// <summary>
        /// A table with an <c>ANY</c> column holding a scalar and one holding a list, so that the untyped
        /// path has something to read.
        /// </summary>
        sealed class ArrayAnyTable : AbstractTable, ScannableTable
        {

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory) =>
                new RelDataTypeFactory.Builder(typeFactory)
                    .add("I", SqlTypeName.ANY)
                    .add("L", SqlTypeName.ANY)
                    .build();

            /// <inheritdoc />
            public Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                list.add(java.lang.Integer.valueOf(1));
                list.add(java.lang.Integer.valueOf(2));
                list.add(java.lang.Integer.valueOf(3));

                return Linq4j.singletonEnumerable(new object[] { java.lang.Integer.valueOf(7), list });
            }

        }

    }

}
