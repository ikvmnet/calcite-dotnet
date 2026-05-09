using System;
using System.Data;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Coverage tests that exercise the standard ADO.NET data types end-to-end through the
    /// Calcite engine, both as query output (typed reader getters) and as command parameters.
    /// </summary>
    /// <remarks>
    /// Calcite's type system does not have a 1:1 mapping with every <see cref="DbType"/>; this
    /// suite documents the supported subset and the canonical CLR materializations.
    /// </remarks>
    public class CalciteTypeCoverageTests
    {

        // ------------------------------------------------------------------------------------
        // Output: typed reader getters over CAST(literal AS sql_type) expressions.
        // ------------------------------------------------------------------------------------

        [Fact]
        public void Output_Boolean_should_round_trip()
        {
            using var r = ExecuteSingleRow("VALUES (CAST(TRUE AS BOOLEAN))");
            Assert.Equal(typeof(bool), r.GetFieldType(0));
            Assert.True(r.GetBoolean(0));
        }

        [Fact]
        public void Output_TinyInt_should_round_trip_as_sbyte()
        {
            using var r = ExecuteSingleRow("VALUES (CAST(1 AS TINYINT))");
            // Calcite TINYINT maps to sbyte (signed) per ColumnAdapter.
            Assert.Equal(typeof(sbyte), r.GetFieldType(0));
            Assert.Equal((sbyte)1, (sbyte)r.GetValue(0));
        }

        [Fact]
        public void Output_SmallInt_should_round_trip_as_short()
        {
            using var r = ExecuteSingleRow("VALUES (CAST(1234 AS SMALLINT))");
            Assert.Equal(typeof(short), r.GetFieldType(0));
            Assert.Equal((short)1234, r.GetInt16(0));
        }

        [Fact]
        public void Output_Integer_should_round_trip_as_int()
        {
            using var r = ExecuteSingleRow("VALUES (CAST(123456 AS INTEGER))");
            Assert.Equal(typeof(int), r.GetFieldType(0));
            Assert.Equal(123456, r.GetInt32(0));
        }

        [Fact]
        public void Output_BigInt_should_round_trip_as_long()
        {
            using var r = ExecuteSingleRow("VALUES (CAST(9000000000 AS BIGINT))");
            Assert.Equal(typeof(long), r.GetFieldType(0));
            Assert.Equal(9000000000L, r.GetInt64(0));
        }

        [Fact]
        public void Output_Real_should_round_trip_as_float()
        {
            using var r = ExecuteSingleRow("VALUES (CAST(1.5 AS REAL))");
            Assert.Equal(typeof(float), r.GetFieldType(0));
            Assert.Equal(1.5f, r.GetFloat(0));
        }

        [Fact]
        public void Output_Double_should_round_trip()
        {
            using var r = ExecuteSingleRow("VALUES (CAST(3.14 AS DOUBLE))");
            Assert.Equal(typeof(double), r.GetFieldType(0));
            Assert.Equal(3.14d, r.GetDouble(0));
        }

        [Fact]
        public void Output_Decimal_should_round_trip()
        {
            using var r = ExecuteSingleRow("VALUES (CAST(123.45 AS DECIMAL(10,2)))");
            Assert.Equal(typeof(decimal), r.GetFieldType(0));
            Assert.Equal(123.45m, r.GetDecimal(0));
        }

        [Fact]
        public void Output_Varchar_should_round_trip_as_string()
        {
            using var r = ExecuteSingleRow("VALUES (CAST('hello' AS VARCHAR(16)))");
            Assert.Equal(typeof(string), r.GetFieldType(0));
            Assert.Equal("hello", r.GetString(0));
        }

        [Fact]
        public void Output_Char_should_round_trip_as_string()
        {
            using var r = ExecuteSingleRow("VALUES (CAST('a' AS CHAR(1)))");
            Assert.Equal(typeof(string), r.GetFieldType(0));
            Assert.Equal("a", r.GetString(0).TrimEnd());
        }

        [Fact(Skip = "Calcite emits DATE values as the raw int day-since-epoch; RowMaterializer needs to convert to DateTime based on column SQL type.")]
        public void Output_Date_should_round_trip_as_DateTime()
        {
            using var r = ExecuteSingleRow("VALUES (DATE '2024-01-15')");
            Assert.Equal(typeof(DateTime), r.GetFieldType(0));
            var dt = r.GetDateTime(0);
            Assert.Equal(2024, dt.Year);
            Assert.Equal(1, dt.Month);
            Assert.Equal(15, dt.Day);
        }

        [Fact(Skip = "Calcite emits TIMESTAMP values as the raw long ms-since-epoch; RowMaterializer needs to convert to DateTime based on column SQL type.")]
        public void Output_Timestamp_should_round_trip_as_DateTime()
        {
            using var r = ExecuteSingleRow("VALUES (TIMESTAMP '2024-01-15 12:34:56')");
            Assert.Equal(typeof(DateTime), r.GetFieldType(0));
            var dt = r.GetDateTime(0);
            Assert.Equal(new DateTime(2024, 1, 15, 12, 34, 56, DateTimeKind.Utc), dt.ToUniversalTime());
        }

        [Fact(Skip = "Calcite emits TIME values as the raw int ms-since-midnight; RowMaterializer needs to convert to TimeSpan based on column SQL type.")]
        public void Output_Time_should_round_trip_as_TimeSpan()
        {
            using var r = ExecuteSingleRow("VALUES (TIME '12:34:56')");
            Assert.Equal(typeof(TimeSpan), r.GetFieldType(0));
            var v = (TimeSpan)r.GetValue(0);
            Assert.Equal(12, v.Hours);
            Assert.Equal(34, v.Minutes);
            Assert.Equal(56, v.Seconds);
        }

        [Fact(Skip = "Calcite emits BINARY values as org.apache.calcite.avatica.util.ByteString; RowMaterializer needs to convert to byte[].")]
        public void Output_Binary_should_round_trip_as_byte_array()
        {
            using var r = ExecuteSingleRow("VALUES (X'01020304')");
            Assert.Equal(typeof(byte[]), r.GetFieldType(0));
            var bytes = (byte[])r.GetValue(0);
            Assert.Equal(new byte[] { 0x01, 0x02, 0x03, 0x04 }, bytes);
        }

        [Fact]
        public void Output_Null_should_be_DbNull()
        {
            using var r = ExecuteSingleRow("VALUES (CAST(NULL AS VARCHAR(8)))");
            Assert.True(r.IsDBNull(0));
            Assert.Equal(DBNull.Value, r.GetValue(0));
        }

        // ------------------------------------------------------------------------------------
        // Input: parameter binding for the standard DbType set, round-tripped via SELECT ?.
        // These tests are skipped pending wiring of CalciteParameterValue into
        // CalciteEngineClient.ExecuteAsync (dynamic parameter binding is not yet implemented).
        // ------------------------------------------------------------------------------------

        const string DynamicParametersNotImplemented =
            "Dynamic parameter binding is not yet wired through CalciteEngineClient.ExecuteAsync.";

        [Fact(Skip = DynamicParametersNotImplemented)]
        public void Parameter_Boolean_should_round_trip()
        {
            AssertParameterRoundTrip(DbType.Boolean, true);
        }

        [Fact(Skip = DynamicParametersNotImplemented)]
        public void Parameter_Byte_should_round_trip()
        {
            AssertParameterRoundTrip(DbType.Byte, (byte)5);
        }

        [Fact(Skip = DynamicParametersNotImplemented)]
        public void Parameter_Int16_should_round_trip()
        {
            AssertParameterRoundTrip(DbType.Int16, (short)1234);
        }

        [Fact(Skip = DynamicParametersNotImplemented)]
        public void Parameter_Int32_should_round_trip()
        {
            AssertParameterRoundTrip(DbType.Int32, 123456);
        }

        [Fact(Skip = DynamicParametersNotImplemented)]
        public void Parameter_Int64_should_round_trip()
        {
            AssertParameterRoundTrip(DbType.Int64, 9000000000L);
        }

        [Fact(Skip = DynamicParametersNotImplemented)]
        public void Parameter_Single_should_round_trip()
        {
            AssertParameterRoundTrip(DbType.Single, 1.5f);
        }

        [Fact(Skip = DynamicParametersNotImplemented)]
        public void Parameter_Double_should_round_trip()
        {
            AssertParameterRoundTrip(DbType.Double, 3.14d);
        }

        [Fact(Skip = DynamicParametersNotImplemented)]
        public void Parameter_Decimal_should_round_trip()
        {
            AssertParameterRoundTrip(DbType.Decimal, 123.45m);
        }

        [Fact(Skip = DynamicParametersNotImplemented)]
        public void Parameter_String_should_round_trip()
        {
            AssertParameterRoundTrip(DbType.String, "hello");
        }

        [Fact(Skip = DynamicParametersNotImplemented)]
        public void Parameter_DateTime_should_round_trip()
        {
            AssertParameterRoundTrip(DbType.DateTime, new DateTime(2024, 1, 15, 12, 34, 56, DateTimeKind.Utc));
        }

        [Fact(Skip = DynamicParametersNotImplemented)]
        public void Parameter_Date_should_round_trip()
        {
            AssertParameterRoundTrip(DbType.Date, new DateTime(2024, 1, 15));
        }

        [Fact(Skip = DynamicParametersNotImplemented)]
        public void Parameter_Time_should_round_trip()
        {
            AssertParameterRoundTrip(DbType.Time, new TimeSpan(12, 34, 56));
        }

        [Fact(Skip = DynamicParametersNotImplemented)]
        public void Parameter_Binary_should_round_trip()
        {
            AssertParameterRoundTrip(DbType.Binary, new byte[] { 1, 2, 3, 4 });
        }

        [Fact(Skip = DynamicParametersNotImplemented)]
        public void Parameter_Null_should_round_trip_as_DbNull()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (?)";
            var p = cmd.CreateParameter();
            p.ParameterName = "?";
            p.DbType = DbType.String;
            p.Value = DBNull.Value;
            cmd.Parameters.Add(p);

            var v = cmd.ExecuteScalar();
            Assert.True(v is null || v is DBNull);
        }

        // ------------------------------------------------------------------------------------
        // Helpers
        // ------------------------------------------------------------------------------------

        static System.Data.Common.DbDataReader ExecuteSingleRow(string sql)
        {
            var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            var r = cmd.ExecuteReader(CommandBehavior.CloseConnection);
            Assert.True(r.Read());
            return r;
        }

        static void AssertParameterRoundTrip(DbType dbType, object value)
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (?)";
            var p = cmd.CreateParameter();
            p.ParameterName = "?";
            p.DbType = dbType;
            p.Value = value;
            cmd.Parameters.Add(p);

            var actual = cmd.ExecuteScalar();
            Assert.NotNull(actual);

            if (value is byte[] bytes)
                Assert.Equal(bytes, (byte[])actual!);
            else if (value is DateTime dt && dbType == DbType.DateTime)
                Assert.Equal(dt, ((DateTime)actual!).ToUniversalTime());
            else
                Assert.Equal(value, Convert.ChangeType(actual, value.GetType()));
        }

    }

}
