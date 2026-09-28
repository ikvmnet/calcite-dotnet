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
    /// Covers <see cref="CalciteDataReader.GetArray(int)"/> and <see cref="CalciteDataReader.GetArray{T}(int)"/>,
    /// the collection accessor ADO.NET lacks and JDBC calls <c>getArray</c>.
    /// </summary>
    /// <remarks>
    /// The accessor is as strict as every other typed getter: a column that is not a collection is refused
    /// rather than wrapped in an array of one.
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
        /// A <c>MULTISET</c> differs from an <c>ARRAY</c> only in whether element order is significant, so it
        /// reads as an array too.
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
        /// A nullable element type gives an array of <see cref="Nullable{T}"/>.
        /// </summary>
        [Fact]
        public void An_array_holding_a_null_should_read_as_an_array_of_the_nullable_element()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[1, CAST(NULL AS INTEGER)]");

            Assert.Equal(new int?[] { 1, null }, Assert.IsType<int?[]>(r.GetArray(0)));
        }

        /// <summary>
        /// A <c>DATE</c> element is held as a count of days, and only the element type says it is a date.
        /// </summary>
        [Fact]
        public void An_array_of_dates_should_read_as_dates()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[DATE '2020-01-02']");

            Assert.Equal(new[] { new DateTime(2020, 1, 2) }, Assert.IsType<DateTime[]>(r.GetArray(0)));
        }

        // ------------------------------------------------------------------------------------
        // Columns that are not collections.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// A scalar is not a collection of one, a <c>MAP</c> is pairs rather than elements, and a <c>ROW</c>
        /// is fields, so each is refused.
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
        /// The refusal of a null says the value is null and points to <c>IsDBNull</c>, from both overloads.
        /// </summary>
        /// <remarks>
        /// The general refusal message quotes the value's class and value, which a null does not have.
        /// </remarks>
        [Fact]
        public void A_null_collection_should_be_refused_by_name()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(NULL AS INTEGER ARRAY)");

            foreach (var read in new Func<object>[] { () => r.GetArray(0), () => r.GetArray<int>(0) })
            {
                var e = Assert.Throws<InvalidCastException>(() => read());

                Assert.Contains("null value", e.Message);
                Assert.Contains("IsDBNull", e.Message);
                Assert.DoesNotContain("of type ''", e.Message);
            }
        }

        /// <summary>
        /// Under <c>ANY</c> the value's runtime class decides, so a list in an <c>ANY</c> column reads as an
        /// array.
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
        /// Naming the element type selects the mapping the elements are read with, rather than casting the
        /// default reading. A <c>DATE</c> reads as a <see cref="DateTime"/> by default and as a
        /// <see cref="DateOnly"/> when that is named.
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
        /// A conversion available only when both types are named, such as <c>TIMESTAMP</c> to
        /// <see cref="DateOnly"/>, is reached the same way.
        /// </summary>
        [Fact]
        public void Naming_an_element_type_that_is_nobody_s_default_should_still_be_reached()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[TIMESTAMP '2020-01-02 03:04:05']");

            Assert.Equal(new[] { new DateOnly(2020, 1, 2) }, r.GetArray<DateOnly>(0));
        }

        /// <summary>
        /// Nullable elements are refused where the named type cannot hold a null, and read where it can.
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
        /// An element type with no mapping to the column's element type is refused rather than converted to.
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
        /// A one-row table with two <c>ANY</c> columns, <c>I</c> holding an integer and <c>L</c> a list.
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
