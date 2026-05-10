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

        [Fact]
        public void Output_Date_should_round_trip_as_DateTime()
        {
            using var r = ExecuteSingleRow("VALUES (DATE '2024-01-15')");
            Assert.Equal(typeof(DateTime), r.GetFieldType(0));
            var dt = r.GetDateTime(0);
            Assert.Equal(2024, dt.Year);
            Assert.Equal(1, dt.Month);
            Assert.Equal(15, dt.Day);
        }

        [Fact]
        public void Output_Timestamp_should_round_trip_as_DateTime()
        {
            using var r = ExecuteSingleRow("VALUES (TIMESTAMP '2024-01-15 12:34:56')");
            Assert.Equal(typeof(DateTime), r.GetFieldType(0));
            var dt = r.GetDateTime(0);
            Assert.Equal(new DateTime(2024, 1, 15, 12, 34, 56, DateTimeKind.Utc), dt.ToUniversalTime());
        }

        [Fact]
        public void Output_Time_should_round_trip_as_TimeSpan()
        {
            using var r = ExecuteSingleRow("VALUES (TIME '12:34:56')");
            Assert.Equal(typeof(TimeSpan), r.GetFieldType(0));
            var v = (TimeSpan)r.GetValue(0);
            Assert.Equal(12, v.Hours);
            Assert.Equal(34, v.Minutes);
            Assert.Equal(56, v.Seconds);
        }

        [Fact]
        public void Output_Date_should_be_readable_as_DateOnly()
        {
            using var r = (CalciteDataReader)ExecuteSingleRow("VALUES (DATE '2024-01-15')");
            var v = r.GetFieldValue<DateOnly>(0);
            Assert.Equal(new DateOnly(2024, 1, 15), v);
        }

        [Fact]
        public void Output_Time_should_be_readable_as_TimeOnly()
        {
            using var r = (CalciteDataReader)ExecuteSingleRow("VALUES (TIME '12:34:56')");
            var v = r.GetFieldValue<TimeOnly>(0);
            Assert.Equal(new TimeOnly(12, 34, 56), v);
        }

        [Fact]
        public void Output_Timestamp_should_be_readable_as_DateTimeOffset()
        {
            using var r = (CalciteDataReader)ExecuteSingleRow("VALUES (TIMESTAMP '2024-01-15 12:34:56')");
            var v = r.GetFieldValue<DateTimeOffset>(0);
            Assert.Equal(new DateTimeOffset(2024, 1, 15, 12, 34, 56, TimeSpan.Zero), v.ToUniversalTime());
        }

        [Fact]
        public void Output_TimestampWithLocalTimeZone_should_be_DateTimeOffset()
        {
            // Calcite represents TIMESTAMP WITH LOCAL TIME ZONE as a UTC instant; surfaced as DateTimeOffset.
            using var r = (CalciteDataReader)ExecuteSingleRow("VALUES (CAST(TIMESTAMP '2024-01-15 12:34:56' AS TIMESTAMP WITH LOCAL TIME ZONE))");

            Assert.Equal(typeof(DateTimeOffset), r.GetFieldType(0));
            var v = r.GetFieldValue<DateTimeOffset>(0);
            Assert.Equal(TimeSpan.Zero, v.Offset);
            Assert.Equal(new DateTimeOffset(2024, 1, 15, 12, 34, 56, TimeSpan.Zero), v.ToUniversalTime());
        }

        [Fact]
        public void Output_TimestampWithTimeZone_should_be_DateTimeOffset()
        {
            // Calcite documents TIMESTAMP WITH TIME ZONE as a supported SQL type; surfaced as DateTimeOffset
            // (matches JDBC's TIMESTAMP_WITH_TIMEZONE convention used by IKVM.Jdbc).
            using var r = (CalciteDataReader)ExecuteSingleRow("VALUES (CAST(TIMESTAMP '2024-01-15 12:34:56' AS TIMESTAMP WITH TIME ZONE))");

            Assert.Equal(typeof(DateTimeOffset), r.GetFieldType(0));
            var v = r.GetFieldValue<DateTimeOffset>(0);
            Assert.Equal(new DateTimeOffset(2024, 1, 15, 12, 34, 56, TimeSpan.Zero), v.ToUniversalTime());
        }

        [Fact]
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

        [Fact]
        public void Output_DateTimeFunctions_should_be_retrievable_as_clr_types()
        {
            // Calcite datetime functions. Note: the Calcite docs describe CURRENT_TIME and
            // CURRENT_TIMESTAMP as TIMESTAMP WITH TIME ZONE, but at runtime CalciteSignature.columns
            // reports them as plain TIME / TIMESTAMP, so the ADO.NET surface mirrors that.
            //   LOCALTIME, LOCALTIME(p)            -> TIME      -> TimeSpan / TimeOnly
            //   LOCALTIMESTAMP, LOCALTIMESTAMP(p)  -> TIMESTAMP -> DateTime
            //   CURRENT_TIME                       -> TIME      -> TimeSpan
            //   CURRENT_DATE                       -> DATE      -> DateTime / DateOnly
            //   CURRENT_TIMESTAMP                  -> TIMESTAMP -> DateTime
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (LOCALTIME, LOCALTIME(3), LOCALTIMESTAMP, LOCALTIMESTAMP(3), CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP)";
            using var r = (CalciteDataReader)cmd.ExecuteReader();
            Assert.True(r.Read());

            Assert.Equal(typeof(TimeSpan), r.GetFieldType(0));
            var localTime = (TimeSpan)r.GetValue(0);
            Assert.InRange(localTime, TimeSpan.Zero, TimeSpan.FromDays(1));
            Assert.Equal(localTime, r.GetFieldValue<TimeOnly>(0).ToTimeSpan());

            Assert.Equal(typeof(TimeSpan), r.GetFieldType(1));
            var localTimeP = (TimeSpan)r.GetValue(1);
            Assert.InRange(localTimeP, TimeSpan.Zero, TimeSpan.FromDays(1));

            Assert.Equal(typeof(DateTime), r.GetFieldType(2));
            var localTimestamp = r.GetDateTime(2);
            Assert.NotEqual(default, localTimestamp);

            Assert.Equal(typeof(DateTime), r.GetFieldType(3));
            var localTimestampP = r.GetDateTime(3);
            Assert.NotEqual(default, localTimestampP);

            Assert.Equal(typeof(TimeSpan), r.GetFieldType(4));
            var currentTime = (TimeSpan)r.GetValue(4);
            Assert.InRange(currentTime, TimeSpan.Zero, TimeSpan.FromDays(1));

            Assert.Equal(typeof(DateTime), r.GetFieldType(5));
            var currentDate = r.GetDateTime(5);
            Assert.Equal(TimeSpan.Zero, currentDate.TimeOfDay);
            Assert.Equal(DateOnly.FromDateTime(currentDate), r.GetFieldValue<DateOnly>(5));

            Assert.Equal(typeof(DateTime), r.GetFieldType(6));
            var currentTimestamp = r.GetDateTime(6);
            Assert.NotEqual(default, currentTimestamp);
        }

        // ------------------------------------------------------------------------------------
        // Input: parameter binding for the standard DbType set, round-tripped via
        // VALUES (CAST(? AS <sql_type>)). Calcite is positional like ODBC: '?' placeholders
        // are bound by ordinal in the order Parameters were added; ParameterName is informational.
        // The CAST around '?' supplies the SQL type the planner needs at validation time.
        // ------------------------------------------------------------------------------------

        [Fact]
        public void Parameter_Boolean_should_round_trip()
        {
            AssertParameterRoundTrip("BOOLEAN", DbType.Boolean, true);
        }

        [Fact]
        public void Parameter_Byte_should_round_trip()
        {
            AssertParameterRoundTrip("SMALLINT", DbType.Byte, (byte)5);
        }

        [Fact]
        public void Parameter_Int16_should_round_trip()
        {
            AssertParameterRoundTrip("SMALLINT", DbType.Int16, (short)1234);
        }

        [Fact]
        public void Parameter_Int32_should_round_trip()
        {
            AssertParameterRoundTrip("INTEGER", DbType.Int32, 123456);
        }

        [Fact]
        public void Parameter_Int64_should_round_trip()
        {
            AssertParameterRoundTrip("BIGINT", DbType.Int64, 9000000000L);
        }

        [Fact]
        public void Parameter_Single_should_round_trip()
        {
            AssertParameterRoundTrip("REAL", DbType.Single, 1.5f);
        }

        [Fact]
        public void Parameter_Double_should_round_trip()
        {
            AssertParameterRoundTrip("DOUBLE", DbType.Double, 3.14d);
        }

        [Fact]
        public void Parameter_Decimal_should_round_trip()
        {
            AssertParameterRoundTrip("DECIMAL(10,2)", DbType.Decimal, 123.45m);
        }

        [Fact]
        public void Parameter_String_should_round_trip()
        {
            AssertParameterRoundTrip("VARCHAR(16)", DbType.String, "hello");
        }

        [Fact]
        public void Parameter_DateTime_should_round_trip()
        {
            AssertParameterRoundTrip("TIMESTAMP", DbType.DateTime, new DateTime(2024, 1, 15, 12, 34, 56, DateTimeKind.Utc));
        }

        [Fact]
        public void Parameter_Date_should_round_trip()
        {
            AssertParameterRoundTrip("DATE", DbType.Date, new DateTime(2024, 1, 15));
        }

        [Fact]
        public void Parameter_Time_should_round_trip()
        {
            AssertParameterRoundTrip("TIME", DbType.Time, new TimeSpan(12, 34, 56));
        }

        [Fact]
        public void Parameter_Binary_should_round_trip()
        {
            AssertParameterRoundTrip("VARBINARY", DbType.Binary, new byte[] { 1, 2, 3, 4 });
        }

        [Fact]
        public void Parameter_Null_should_round_trip_as_DbNull()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST(? AS VARCHAR(8)))";
            var p = cmd.CreateParameter();
            p.ParameterName = "?";
            p.DbType = DbType.String;
            p.Value = DBNull.Value;
            cmd.Parameters.Add(p);

            var v = cmd.ExecuteScalar();
            Assert.True(v is null || v is DBNull);
        }

        [Fact]
        public void Parameters_should_bind_positionally_in_collection_order()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST(? AS INTEGER) + CAST(? AS INTEGER))";
            cmd.Parameters.Add(new CalciteParameter("?", 10));
            cmd.Parameters.Add(new CalciteParameter("?", 32));

            Assert.Equal(42, Convert.ToInt32(cmd.ExecuteScalar()));
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

        static void AssertParameterRoundTrip(string sqlType, DbType dbType, object value)
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = $"VALUES (CAST(? AS {sqlType}))";
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
            else if (value is string s)
                Assert.Equal(s, ((string)actual!).TrimEnd());
            else
                Assert.Equal(value, Convert.ChangeType(actual, value.GetType()));
        }

    }

}
