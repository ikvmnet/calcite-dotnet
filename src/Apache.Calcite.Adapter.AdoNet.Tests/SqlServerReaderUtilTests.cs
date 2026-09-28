using System;
using System.Collections.Generic;
using System.Data.Common;

using Microsoft.Data.SqlClient;

using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Tests <see cref="AdoReaderUtil"/>'s mapping from a SQL Server value to the representation Calcite's
    /// runtime expects.
    /// </summary>
    /// <remarks>
    /// SQLite, which <see cref="AdoReaderUtilTests"/> reads from, returns nearly everything as
    /// <see cref="long"/>, <see cref="double"/> or <see cref="string"/>. SQL Server returns the declared type:
    /// a <c>uniqueidentifier</c> as a <see cref="Guid"/>, a <c>datetimeoffset</c> as a
    /// <see cref="DateTimeOffset"/>, a <c>tinyint</c> as a <see cref="byte"/>, which a typed accessor that
    /// casts would fail on.
    /// </remarks>
    public class SqlServerReaderUtilTests : IDisposable
    {

        SqlConnection _connection = null!;
        readonly List<DbCommand> _commands = [];

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public SqlServerReaderUtilTests()
        {
            if (SqlServerFixture.IsAvailable == false)
                Assert.Skip("No SQL Server LocalDB instance is reachable on this machine.");

            _connection = new SqlConnection(new SqlConnectionStringBuilder()
            {
                DataSource = @"(localdb)\MSSQLLocalDB",
                InitialCatalog = "master",
                IntegratedSecurity = true,
                TrustServerCertificate = true,
            }.ConnectionString);

            _connection.Open();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            foreach (var command in _commands)
                command.Dispose();

            _commands.Clear();
            _connection?.Dispose();
        }

        /// <summary>
        /// Returns a reader positioned on a single row holding the given expression. The command is kept until
        /// the test is disposed, because disposing it would close the reader.
        /// </summary>
        /// <param name="selectExpression">The text placed after <c>SELECT</c>, producing the single column to
        /// read.</param>
        /// <returns>An open reader already advanced onto the row.</returns>
        DbDataReader Row(string selectExpression)
        {
            var command = _connection.CreateCommand();
            command.CommandText = $"SELECT {selectExpression}";
            _commands.Add(command);

            var reader = command.ExecuteReader();
            Assert.True(reader.Read(), "expected one row");
            return reader;
        }

        /// <summary>
        /// A <c>tinyint</c> read as <c>SMALLINT</c> widens to a <see cref="java.lang.Short"/> without losing the
        /// upper half of its range, which a signed <c>TINYINT</c> could not hold.
        /// </summary>
        [Fact]
        public void ATinyIntWidensToAShort()
        {
            using var reader = Row("CAST(200 AS TINYINT)");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.SMALLINT);

            Assert.IsAssignableFrom<java.lang.Short>(value);
            Assert.Equal((short)200, ((java.lang.Short)value!).shortValue());
        }

        [Fact]
        public void ABitIsReadAsAJavaBoolean()
        {
            using var reader = Row("CAST(1 AS BIT)");
            Assert.True(((java.lang.Boolean)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.BOOLEAN)!).booleanValue());
        }

        [Fact]
        public void ARealIsReadAsAJavaFloat()
        {
            using var reader = Row("CAST(2.5 AS REAL)");
            Assert.Equal(2.5f, ((java.lang.Float)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.REAL)!).floatValue());
        }

        [Fact]
        public void AFloatIsReadAsAJavaDouble()
        {
            using var reader = Row("CAST(1.5 AS FLOAT)");
            Assert.Equal(1.5d, ((java.lang.Double)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DOUBLE)!).doubleValue());
        }

        [Fact]
        public void AMoneyKeepsItsScale()
        {
            using var reader = Row("CAST(12.34 AS MONEY)");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DECIMAL);

            Assert.IsAssignableFrom<java.math.BigDecimal>(value);
            Assert.Equal("12.3400", value!.ToString());
        }

        /// <summary>
        /// A <c>uniqueidentifier</c> is not read as a character column; formatting it as text would make it
        /// indistinguishable from a <c>CHAR(36)</c>.
        /// </summary>
        [Fact]
        public void AUniqueIdentifierIsNotReadAsAString()
        {
            using var reader = Row("CAST('3f2504e0-4f89-11d3-9a0c-0305e82c3301' AS UNIQUEIDENTIFIER)");

            Assert.ThrowsAny<InvalidCastException>(
                () => AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.CHAR));
        }

        /// <summary>
        /// A <c>uniqueidentifier</c> is read as a <c>UUID</c>, held in <c>org.apache.calcite.util.UuidValue</c>,
        /// the class Calcite's runtime uses for UUIDs.
        /// </summary>
        [Fact]
        public void AUniqueIdentifierIsReadAsAUuidValue()
        {
            using var reader = Row("CAST('3f2504e0-4f89-11d3-9a0c-0305e82c3301' AS UNIQUEIDENTIFIER)");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.UUID);

            Assert.IsAssignableFrom<org.apache.calcite.util.UuidValue>(value);
            Assert.Equal("3f2504e0-4f89-11d3-9a0c-0305e82c3301", value!.ToString());
        }

        /// <summary>
        /// A null <c>uniqueidentifier</c> reads as null.
        /// </summary>
        [Fact]
        public void ANullUniqueIdentifierIsNull()
        {
            using var reader = Row("CAST(NULL AS UNIQUEIDENTIFIER)");
            Assert.Null(AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.UUID));
        }

        [Fact]
        public void AVarbinaryIsReadAsAByteString()
        {
            using var reader = Row("CAST(0x01FF80 AS VARBINARY(8))");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.VARBINARY);

            Assert.IsAssignableFrom<org.apache.calcite.avatica.util.ByteString>(value);
            Assert.Equal(new byte[] { 0x01, 0xFF, 0x80 }, ((org.apache.calcite.avatica.util.ByteString)value!).getBytes());
        }

        #region Temporal

        /// <summary>
        /// 2020-01-15, as a count of whole days from the epoch.
        /// </summary>
        const int ExpectedDay = 18276;

        /// <summary>
        /// 2020-01-15T10:20:30, as the count of milliseconds from the epoch to that wall clock read as UTC.
        /// </summary>
        const long ExpectedMillis = 1579083630000L;

        [Fact]
        public void ADateIsReadAsDaysSinceTheEpoch()
        {
            using var reader = Row("CAST('2020-01-15' AS DATE)");
            Assert.Equal(ExpectedDay, ((java.lang.Integer)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DATE)!).intValue());
        }

        [Fact]
        public void ATimeIsReadAsMillisecondsSinceMidnight()
        {
            using var reader = Row("CAST('01:02:03.500' AS TIME(3))");
            Assert.Equal(3723500, ((java.lang.Integer)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TIME)!).intValue());
        }

        /// <summary>
        /// A timestamp carries no zone, so the count is to the wall clock read as UTC; reading it as local
        /// time would shift it by the machine's offset.
        /// </summary>
        [Fact]
        public void ATimestampDoesNotDependOnTheMachineTimeZone()
        {
            using var reader = Row("CAST('2020-01-15T10:20:30' AS DATETIME)");
            Assert.Equal(ExpectedMillis, ((java.lang.Long)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TIMESTAMP)!).longValue());
        }

        [Fact]
        public void ADateTime2IsTheSameTimestamp()
        {
            using var reader = Row("CAST('2020-01-15T10:20:30' AS DATETIME2(3))");
            Assert.Equal(ExpectedMillis, ((java.lang.Long)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TIMESTAMP)!).longValue());
        }

        /// <summary>
        /// A zoned timestamp is an instant. The provider returns it as a <see cref="DateTimeOffset"/>, and
        /// <see cref="DbDataReader.GetDateTime"/> rejects it.
        /// </summary>
        [Fact]
        public void AZonedTimestampIsReadAsAnInstant()
        {
            using var reader = Row("CAST('2020-01-15T10:20:30+00:00' AS DATETIMEOFFSET(3))");
            Assert.Equal(ExpectedMillis, ((java.lang.Long)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TIMESTAMP_TZ)!).longValue());
        }

        /// <summary>
        /// The offset is part of the value: the same wall clock at a different offset is a different instant.
        /// </summary>
        [Fact]
        public void AZonedTimestampHonoursItsOffset()
        {
            using var reader = Row("CAST('2020-01-15T10:20:30-05:00' AS DATETIMEOFFSET(3))");
            Assert.Equal(
                ExpectedMillis + 5 * 60 * 60 * 1000L,
                ((java.lang.Long)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TIMESTAMP_TZ)!).longValue());
        }

        #endregion

    }

}
