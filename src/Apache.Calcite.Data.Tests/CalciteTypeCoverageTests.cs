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
            // Calcite TINYINT is a signed 8-bit integer; maps to CLR sbyte.
            Assert.Equal(typeof(sbyte), r.GetFieldType(0));
            Assert.Equal((sbyte)1, (sbyte)r.GetValue(0));
        }

        [Fact]
        public void Output_TinyIntUnsigned_should_round_trip_as_byte()
        {
            using var r = ExecuteSingleRow("VALUES (CAST(200 AS TINYINT UNSIGNED))");
            // Calcite TINYINT UNSIGNED is an unsigned 8-bit integer; maps to CLR byte.
            Assert.Equal(typeof(byte), r.GetFieldType(0));
            Assert.Equal((byte)200, r.GetByte(0));
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
        public void Output_Timestamp_with_milliseconds_should_round_trip()
        {
            using var r = ExecuteSingleRow("VALUES (TIMESTAMP '2023-06-15 08:30:45.123')");
            Assert.Equal(typeof(DateTime), r.GetFieldType(0));
            var dt = r.GetDateTime(0);
            Assert.Equal(new DateTime(2023, 6, 15, 8, 30, 45, 123, DateTimeKind.Utc), dt.ToUniversalTime());
        }

        [Fact]
        public void Output_Timestamp_near_unix_epoch_should_round_trip()
        {
            // 1970-01-01 00:00:00.001 — one millisecond after epoch
            using var r = ExecuteSingleRow("VALUES (TIMESTAMP '1970-01-01 00:00:00.001')");
            var dt = r.GetDateTime(0).ToUniversalTime();
            Assert.Equal(new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc), dt);
        }

        [Fact]
        public void Output_Timestamp_before_unix_epoch_should_round_trip()
        {
            // 1960-03-15 23:59:59.999 — negative Unix timestamp with fractional ms
            using var r = ExecuteSingleRow("VALUES (TIMESTAMP '1960-03-15 23:59:59.999')");
            var dt = r.GetDateTime(0).ToUniversalTime();
            Assert.Equal(new DateTime(1960, 3, 15, 23, 59, 59, 999, DateTimeKind.Utc), dt);
        }

        [Fact]
        public void Output_Timestamp_early_date_0001_should_round_trip()
        {
            // Earliest representable Calcite TIMESTAMP — proleptic Gregorian year 0001
            using var r = ExecuteSingleRow("VALUES (TIMESTAMP '0001-01-01 00:00:00')");
            var dt = r.GetDateTime(0).ToUniversalTime();
            Assert.Equal(1, dt.Year);
            Assert.Equal(1, dt.Month);
            Assert.Equal(1, dt.Day);
            Assert.Equal(0, dt.Hour);
            Assert.Equal(0, dt.Minute);
            Assert.Equal(0, dt.Second);
        }

        [Fact]
        public void Output_Timestamp_early_date_with_fractional_seconds_should_round_trip()
        {
            // 0001-01-01 00:00:00.456
            using var r = ExecuteSingleRow("VALUES (TIMESTAMP '0001-01-01 00:00:00.456')");
            var dt = r.GetDateTime(0).ToUniversalTime();
            Assert.Equal(1, dt.Year);
            Assert.Equal(1, dt.Month);
            Assert.Equal(1, dt.Day);
            Assert.Equal(456, dt.Millisecond);
        }

        [Fact]
        public void Parameter_Timestamp_sub_millisecond_ticks_are_truncated_to_ms()
        {
            // 0001-01-01T00:00:00.0102004 — ticks value 102004 (10.2004 ms).
            // Calcite TIMESTAMP resolution is milliseconds. The binding calls
            // DateTimeOffset.ToUnixTimeMilliseconds(), which truncates toward zero.
            // Because this date produces a large negative Unix timestamp, truncation
            // toward zero effectively rounds the fractional ms *up*: 10.2004 ms → 11 ms.
            var input = new DateTime(1, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(102004);
            Assert.Equal(10, input.Millisecond); // sanity: the CLR ms field is 10

            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST(? AS TIMESTAMP(3)))";
            var p = cmd.CreateParameter();
            p.ParameterName = "?";
            p.DbType = DbType.DateTime;
            p.Value = input;
            cmd.Parameters.Add(p);

            var actual = (DateTime)cmd.ExecuteScalar()!;
            // ToUnixTimeMilliseconds truncates toward zero → negative remainder rounds up to 11 ms.
            var expected = new DateTime(1, 1, 1, 0, 0, 0, 11, DateTimeKind.Utc);
            Assert.Equal(expected, actual.ToUniversalTime());
        }

        [Fact]
        public void Output_Timestamp_readable_as_DateTimeOffset_utc()
        {
            using var r = (CalciteDataReader)ExecuteSingleRow("VALUES (TIMESTAMP '2024-07-04 13:45:30.750')");
            // GetDateTime materialises as UTC DateTime; wrap it for a DateTimeOffset comparison.
            var dt = r.GetDateTime(0).ToUniversalTime();
            Assert.Equal(new DateTimeOffset(2024, 7, 4, 13, 45, 30, 750, TimeSpan.Zero), new DateTimeOffset(dt));
        }

        [Fact]
        public void Output_Date_near_min_value_should_round_trip()
        {
            // DATE 0001-01-01 — minimum date expressible in SQL
            using var r = ExecuteSingleRow("VALUES (DATE '0001-01-01')");
            Assert.Equal(typeof(DateTime), r.GetFieldType(0));
            var dt = r.GetDateTime(0);
            Assert.Equal(1, dt.Year);
            Assert.Equal(1, dt.Month);
            Assert.Equal(1, dt.Day);
        }

        [Fact]
        public void Output_Date_near_min_value_as_DateOnly()
        {
            using var r = (CalciteDataReader)ExecuteSingleRow("VALUES (DATE '0001-01-02')");
            var v = r.GetFieldValue<DateOnly>(0);
            Assert.Equal(new DateOnly(1, 1, 2), v);
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
        public void Output_Uuid_should_round_trip_as_Guid()
        {
            using var r = ExecuteSingleRow("VALUES (UUID 'cccccccc-0000-0000-0000-000000000001')");
            // Calcite's runtime representation of UUID is a java.util.UUID; it surfaces as a Guid.
            Assert.Equal(new Guid("cccccccc-0000-0000-0000-000000000001"), r.GetGuid(0));
            Assert.Equal(new Guid("cccccccc-0000-0000-0000-000000000001"), (Guid)r.GetValue(0));
        }

        [Fact]
        public void Output_Uuid_cast_from_a_string_should_round_trip_as_Guid()
        {
            using var r = ExecuteSingleRow("VALUES (CAST(CAST('cccccccc-0000-0000-0000-000000000001' AS VARCHAR) AS UUID))");
            Assert.Equal(new Guid("cccccccc-0000-0000-0000-000000000001"), r.GetGuid(0));
        }

        [Fact]
        public void Parameter_Boolean_should_round_trip()
        {
            AssertParameterRoundTrip("BOOLEAN", DbType.Boolean, true);
        }

        [Fact]
        public void Parameter_Byte_should_round_trip()
        {
            // CLR byte (unsigned 8-bit) maps to Calcite TINYINT UNSIGNED.
            AssertParameterRoundTrip("TINYINT UNSIGNED", DbType.Byte, (byte)5);
        }

        [Fact]
        public void Parameter_SByte_should_round_trip()
        {
            // CLR sbyte (signed 8-bit) maps to Calcite TINYINT.
            AssertParameterRoundTrip("TINYINT", DbType.SByte, (sbyte)-5);
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
        public void Parameter_Guid_should_round_trip()
        {
            AssertParameterRoundTrip("UUID", DbType.Guid, new Guid("cccccccc-0000-0000-0000-000000000001"));
        }

        [Fact]
        public void Parameter_UInt16_should_round_trip()
        {
            // UInt16 is widened to INTEGER (Java Integer) on the way in; the reader surfaces it
            // back as int, so we compare against the expected int value.
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST(? AS INTEGER))";
            var p = cmd.CreateParameter();
            p.ParameterName = "?";
            p.DbType = DbType.UInt16;
            p.Value = (ushort)65535;
            cmd.Parameters.Add(p);

            var actual = cmd.ExecuteScalar();
            Assert.Equal(65535, Convert.ToInt32(actual));
        }

        [Fact]
        public void Parameter_UInt32_should_round_trip()
        {
            // UInt32 is widened to BIGINT (Java Long) on the way in; the reader surfaces it as long.
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST(? AS BIGINT))";
            var p = cmd.CreateParameter();
            p.ParameterName = "?";
            p.DbType = DbType.UInt32;
            p.Value = (uint)4294967295;
            cmd.Parameters.Add(p);

            var actual = cmd.ExecuteScalar();
            Assert.Equal(4294967295L, Convert.ToInt64(actual));
        }

        [Fact]
        public void Parameter_UInt64_should_round_trip()
        {
            // UInt64 is sent as BigDecimal; the reader surfaces it as decimal.
            // Calcite's DECIMAL implementation is backed by Java long, so precision is capped at 19
            // digits (Long.MAX_VALUE). Use a value that fits within that constraint but still exceeds
            // the Int64 range to confirm the unsigned binding path is exercised.
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST(? AS DECIMAL(19,0)))";
            var p = cmd.CreateParameter();
            p.ParameterName = "?";
            p.DbType = DbType.UInt64;
            p.Value = 9_000_000_000_000_000_000UL;
            cmd.Parameters.Add(p);

            var actual = cmd.ExecuteScalar();
            Assert.Equal(9_000_000_000_000_000_000m, Convert.ToDecimal(actual));
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
        // Unsigned typed getters: GetByte/GetUInt16/32/64 on CalciteDataReader, reading the unsigned
        // Calcite SQL types. Each is the one representation Calcite's runtime produces for that type —
        // an org.joou.UByte for TINYINT UNSIGNED, a UShort for SMALLINT UNSIGNED, and so on — and
        // nothing else is that type, which the refusals below hold.
        // ------------------------------------------------------------------------------------

        [Fact]
        public void GetByte_should_read_tinyint_unsigned_as_byte()
        {
            using var r = ExecuteSingleRowTyped("VALUES (CAST(200 AS TINYINT UNSIGNED))");
            Assert.Equal(typeof(byte), r.GetFieldType(0));
            Assert.Equal((byte)200, r.GetByte(0));
            Assert.Equal((byte)200, r.GetFieldValue<byte>(0));
        }

        [Fact]
        public void GetUInt16_should_read_smallint_unsigned_as_ushort()
        {
            using var r = ExecuteSingleRowTyped("VALUES (CAST(65535 AS SMALLINT UNSIGNED))");
            Assert.Equal(typeof(ushort), r.GetFieldType(0));
            Assert.Equal((ushort)65535, r.GetUInt16(0));
            Assert.Equal((ushort)65535, r.GetFieldValue<ushort>(0));
        }

        [Fact]
        public void GetUInt32_should_read_integer_unsigned_as_uint()
        {
            using var r = ExecuteSingleRowTyped("VALUES (CAST(4294967295 AS INTEGER UNSIGNED))");
            Assert.Equal(typeof(uint), r.GetFieldType(0));
            Assert.Equal(4294967295u, r.GetUInt32(0));
            Assert.Equal(4294967295u, r.GetFieldValue<uint>(0));
        }

        [Fact]
        public void GetUInt64_should_read_bigint_unsigned_as_ulong()
        {
            using var r = ExecuteSingleRowTyped("VALUES (CAST(9000000000000000000 AS BIGINT UNSIGNED))");
            Assert.Equal(typeof(ulong), r.GetFieldType(0));
            Assert.Equal(9_000_000_000_000_000_000UL, r.GetUInt64(0));
            Assert.Equal(9_000_000_000_000_000_000UL, r.GetFieldValue<ulong>(0));
        }

        // ------------------------------------------------------------------------------------
        // Refusals. A typed getter answers for the type its column has and no other: ADO.NET's
        // contract is that GetXxx throws where the column is not an Xxx, not that it converts.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// Text in canonical GUID form is text. Parsing it here would have <c>GetGuid</c> answer for a
        /// type the column does not have; <c>CAST(x AS UUID)</c> is how a caller says it means one, and
        /// <see cref="Output_Uuid_cast_from_a_string_should_round_trip_as_Guid"/> is that.
        /// </summary>
        [Fact]
        public void GetGuid_should_refuse_a_character_column_holding_guid_text()
        {
            using var r = ExecuteSingleRow("VALUES (CAST('cccccccc-0000-0000-0000-000000000001' AS VARCHAR))");
            Assert.Throws<InvalidCastException>(() => r.GetGuid(0));
        }

        [Fact]
        public void GetByte_should_refuse_a_signed_tinyint()
        {
            using var r = ExecuteSingleRowTyped("VALUES (CAST(1 AS TINYINT))");
            Assert.Throws<InvalidCastException>(() => r.GetByte(0));
        }

        [Fact]
        public void GetSByte_should_refuse_a_smallint()
        {
            using var r = ExecuteSingleRowTyped("VALUES (CAST(1 AS SMALLINT))");
            Assert.Throws<InvalidCastException>(() => r.GetSByte(0));
        }

        [Fact]
        public void GetUInt16_should_refuse_a_signed_smallint()
        {
            using var r = ExecuteSingleRowTyped("VALUES (CAST(1 AS SMALLINT))");
            Assert.Throws<InvalidCastException>(() => r.GetUInt16(0));
        }

        [Fact]
        public void GetUInt32_should_refuse_a_signed_integer()
        {
            using var r = ExecuteSingleRowTyped("VALUES (CAST(1 AS INTEGER))");
            Assert.Throws<InvalidCastException>(() => r.GetUInt32(0));
        }

        [Fact]
        public void GetUInt64_should_refuse_a_signed_bigint()
        {
            using var r = ExecuteSingleRowTyped("VALUES (CAST(1 AS BIGINT))");
            Assert.Throws<InvalidCastException>(() => r.GetUInt64(0));
        }

        /// <summary>
        /// A <c>DECIMAL</c> wide enough to hold the same number is still a <c>DECIMAL</c>. Nineteen
        /// digits, not twenty: Calcite backs DECIMAL with a Java long and caps the precision there.
        /// </summary>
        [Fact]
        public void GetUInt64_should_refuse_a_decimal()
        {
            using var r = ExecuteSingleRowTyped("VALUES (CAST(1 AS DECIMAL(19, 0)))");
            Assert.Throws<InvalidCastException>(() => r.GetUInt64(0));
        }

        // ------------------------------------------------------------------------------------
        // Helpers
        // ------------------------------------------------------------------------------------

        static CalciteDataReader ExecuteSingleRowTyped(string sql)
        {
            var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            var r = (CalciteDataReader)cmd.ExecuteReader(CommandBehavior.CloseConnection);
            Assert.True(r.Read());
            return r;
        }

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
            else if (value is Guid g)
                Assert.Equal(g, (Guid)actual!);
            else
                Assert.Equal(value, Convert.ChangeType(actual, value.GetType()));
        }

    }

}
