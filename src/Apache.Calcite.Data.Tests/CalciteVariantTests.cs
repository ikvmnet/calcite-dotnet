using System;
using System.Collections.Generic;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Reading a <c>VARIANT</c>, which is the one Calcite type that carries its own type with the value.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Nothing reached one before these. A variant is <c>ANY</c> with the type written down rather than
    /// absent: Calcite holds it as a <c>VariantValue</c>, which is a Java object and so cannot be handed to
    /// a caller, and the payload's own type is what says what the payload is. That makes it the one place
    /// where what a column reads as is decided per row.
    /// </para>
    /// <para>
    /// A variant is built here with <c>CAST(x AS VARIANT)</c>. <c>PARSE_JSON</c> would be the other way in
    /// and is not registered in any of the function libraries this connection can select — measured against
    /// <c>standard</c>, <c>bigquery</c> and <c>spark</c>.
    /// </para>
    /// </remarks>
    public class CalciteVariantTests
    {

        static CalciteDataReader Row(CalciteConnection c, string sql)
        {
            var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            var r = (CalciteDataReader)cmd.ExecuteReader();
            Assert.True(r.Read());
            return r;
        }

        static CalciteConnection Open()
        {
            var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            return c;
        }

        /// <summary>
        /// Reads the one column of a one-row statement.
        /// </summary>
        static object Read(string sql)
        {
            using var c = Open();
            using var r = Row(c, sql);
            return r.GetValue(0);
        }

        // ------------------------------------------------------------------------------------
        // A scalar payload reads as the type it says it is.
        // ------------------------------------------------------------------------------------

        [Theory]
        [InlineData("TRUE", true)]
        [InlineData("CAST(1 AS TINYINT)", (sbyte)1)]
        [InlineData("CAST(1 AS SMALLINT)", (short)1)]
        [InlineData("1", 1)]
        [InlineData("CAST(1 AS BIGINT)", 1L)]
        [InlineData("CAST(1.5 AS REAL)", 1.5f)]
        [InlineData("CAST(1.5 AS DOUBLE)", 1.5d)]
        [InlineData("'ab'", "ab")]
        public void A_scalar_payload_should_read_as_its_own_type(string literal, object expected)
        {
            Assert.Equal(expected, Read($"SELECT CAST({literal} AS VARIANT)"));
        }

        [Fact]
        public void A_decimal_payload_should_read_as_a_decimal()
        {
            Assert.Equal(1.50m, Assert.IsType<decimal>(Read("SELECT CAST(CAST(1.5 AS DECIMAL(5,2)) AS VARIANT)")));
        }

        [Fact]
        public void A_binary_payload_should_read_as_bytes()
        {
            Assert.Equal(new byte[] { 1, 2 }, Assert.IsType<byte[]>(Read("SELECT CAST(x'0102' AS VARIANT)")));
        }

        /// <summary>
        /// The temporal types are what this exists for: Calcite stores a <c>DATE</c> as a count of days and
        /// a <c>TIMESTAMP</c> as a count of milliseconds, so an integer is one or the other only because the
        /// payload's type says so.
        /// </summary>
        [Fact]
        public void A_date_payload_should_read_as_a_date_and_not_a_count_of_days()
        {
            Assert.Equal(new DateTime(2020, 1, 2), Assert.IsType<DateTime>(Read("SELECT CAST(DATE '2020-01-02' AS VARIANT)")));
        }

        [Fact]
        public void A_time_payload_should_read_as_a_length_of_time()
        {
            Assert.Equal(new TimeSpan(3, 4, 5), Assert.IsType<TimeSpan>(Read("SELECT CAST(TIME '03:04:05' AS VARIANT)")));
        }

        [Fact]
        public void A_timestamp_payload_should_read_as_a_moment()
        {
            Assert.Equal(new DateTime(2020, 1, 2, 3, 4, 5), Assert.IsType<DateTime>(Read("SELECT CAST(TIMESTAMP '2020-01-02 03:04:05' AS VARIANT)")));
        }

        [Fact]
        public void A_uuid_payload_should_read_as_a_guid()
        {
            Assert.Equal(
                new Guid("123e4567-e89b-12d3-a456-426614174000"),
                Assert.IsType<Guid>(Read("SELECT CAST(CAST('123e4567-e89b-12d3-a456-426614174000' AS UUID) AS VARIANT)")));
        }

        /// <summary>
        /// An interval names itself by its scale — <c>INTERVAL_LONG</c> and <c>INTERVAL_SHORT</c> — rather
        /// than by a <c>SqlTypeName</c>, so it is the one payload family the name table cannot reach and it
        /// is cast on its own. Each reads as the declared type of its family does.
        /// </summary>
        [Fact]
        public void A_year_month_interval_payload_should_read_as_a_count_of_months()
        {
            Assert.Equal(14, Assert.IsType<int>(Read("SELECT CAST(INTERVAL '1-2' YEAR TO MONTH AS VARIANT)")));
        }

        [Fact]
        public void A_day_time_interval_payload_should_read_as_a_length_of_time()
        {
            Assert.Equal(new TimeSpan(1, 2, 3, 4), Assert.IsType<TimeSpan>(Read("SELECT CAST(INTERVAL '1 2:3:4' DAY TO SECOND AS VARIANT)")));
        }

        // ------------------------------------------------------------------------------------
        // The three nulls.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// A variant has more than one null and an ADO.NET caller has one. <c>VariantNull</c> is the variant
        /// type's own null, <c>VariantSqlNull</c> is the SQL null of a declared type, and a null variant
        /// column is a Java null like any other; all three are <see cref="DBNull"/>.
        /// </summary>
        [Theory]
        [InlineData("VARIANTNULL()")]
        [InlineData("CAST(CAST(NULL AS INTEGER) AS VARIANT)")]
        [InlineData("CAST(NULL AS VARIANT)")]
        public void Every_null_a_variant_can_be_should_read_as_one(string sql)
        {
            using var c = Open();
            using var r = Row(c, $"SELECT {sql}");

            Assert.True(r.IsDBNull(0));
            Assert.Equal(DBNull.Value, r.GetValue(0));
            Assert.Null(r.GetFieldValue<string>(0));
        }

        // ------------------------------------------------------------------------------------
        // A collection payload, which is walked rather than cast.
        // ------------------------------------------------------------------------------------

        [Fact]
        public void An_array_payload_should_read_as_an_array()
        {
            Assert.Equal(new[] { 1, 2, 3 }, Assert.IsType<int[]>(Read("SELECT CAST(ARRAY[1, 2, 3] AS VARIANT)")));
        }

        [Fact]
        public void An_array_payload_of_strings_should_read_as_strings()
        {
            Assert.Equal(new[] { "a", "b" }, Assert.IsType<string[]>(Read("SELECT CAST(ARRAY['a', 'b'] AS VARIANT)")));
        }

        /// <summary>
        /// The elements are read one at a time and each carries its own type, so a null among them makes the
        /// array nullable exactly as a declared <c>ARRAY</c> does.
        /// </summary>
        [Fact]
        public void An_array_payload_holding_a_null_should_read_as_an_array_of_the_nullable_element()
        {
            Assert.Equal(new int?[] { 1, null }, Assert.IsType<int?[]>(Read("SELECT CAST(ARRAY[1, NULL] AS VARIANT)")));
        }

        [Fact]
        public void A_nested_array_payload_should_read_at_every_level()
        {
            var outer = Assert.IsType<int[][]>(Read("SELECT CAST(ARRAY[ARRAY[1, 2]] AS VARIANT)"));

            Assert.Equal(new[] { 1, 2 }, Assert.Single(outer));
        }

        [Fact]
        public void A_map_payload_should_read_as_a_dictionary()
        {
            var map = Assert.IsType<Dictionary<string, int>>(Read("SELECT CAST(MAP['a', 1] AS VARIANT)"));

            Assert.Equal(1, map["a"]);
        }

        /// <summary>
        /// A variant column whose value is a variant reads the payload, so an array of variants is an array
        /// of whatever each one turned out to hold.
        /// </summary>
        [Fact]
        public void An_array_of_variants_should_read_each_payload()
        {
            Assert.Equal(new object[] { 1 }, Assert.IsType<object[]>(Read("SELECT ARRAY[CAST(1 AS VARIANT)]")));
        }

        // ------------------------------------------------------------------------------------
        // What Calcite gives no route to is refused and named, rather than guessed at.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// Measured against 1.43: a <c>MULTISET</c> and a <c>ROW</c> in a variant answer null to a cast to
        /// their own runtime type, to a cast to <c>ARRAY</c>, and to <c>item(1)</c>. There is nothing to
        /// read, so handing back the <c>VariantValue</c> or inventing a text form is what is refused.
        /// </summary>
        [Theory]
        [InlineData("MULTISET[1, 2]", "MULTISET")]
        [InlineData("ROW(1, 'a')", "ROW")]
        public void A_payload_with_no_route_to_its_contents_should_be_refused_by_name(string literal, string named)
        {
            using var c = Open();
            using var r = Row(c, $"SELECT CAST({literal} AS VARIANT)");

            var e = Assert.ThrowsAny<InvalidCastException>(() => r.GetValue(0));
            Assert.Contains(named, e.Message, StringComparison.Ordinal);
        }

        /// <summary>
        /// A map's keys are reachable only by casting the whole map to <c>MAP&lt;VARCHAR, VARCHAR&gt;</c>,
        /// which answers the keys and drops the values. Keys that are not character values come back null
        /// from that cast, and the entry is refused rather than lost.
        /// </summary>
        [Fact]
        public void A_map_payload_whose_keys_are_not_characters_should_be_refused()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(MAP[1, 2] AS VARIANT)");

            var e = Assert.ThrowsAny<InvalidCastException>(() => r.GetValue(0));
            Assert.Contains("MAP", e.Message, StringComparison.Ordinal);
        }

        // ------------------------------------------------------------------------------------
        // The column reports what a column whose type says nothing reports.
        // ------------------------------------------------------------------------------------

        [Fact]
        public void A_variant_column_should_report_itself_as_a_variant_holding_an_object()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(1 AS VARIANT)");

            Assert.Equal("VARIANT", r.GetDataTypeName(0));
            Assert.Equal(typeof(object), r.GetFieldType(0));
        }

        /// <summary>
        /// A variant does not make an accessor lenient. The payload is an <c>INTEGER</c>, so it reads
        /// through <see cref="CalciteDataReader.GetInt32"/> and <c>GetInt64</c> refuses it exactly as it
        /// refuses an <c>INTEGER</c> column.
        /// </summary>
        [Fact]
        public void A_typed_getter_over_a_variant_should_be_as_strict_as_over_the_payloads_own_type()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(1 AS VARIANT)");

            Assert.Equal(1, r.GetInt32(0));
            Assert.Throws<InvalidCastException>(() => r.GetInt64(0));
        }

        [Fact]
        public void GetCalciteValue_should_hand_back_the_variant_itself()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(1 AS VARIANT)");

            var v = Assert.IsAssignableFrom<org.apache.calcite.runtime.variant.VariantValue>(r.GetCalciteValue(0));
            Assert.Equal("INTEGER", v.getTypeString());
        }

    }

}
