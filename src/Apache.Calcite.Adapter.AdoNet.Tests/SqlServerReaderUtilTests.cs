using System;
using System.Collections.Generic;
using System.Data.Common;

using Microsoft.Data.SqlClient;

using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers the mapping from a SQL Server value to the representation Calcite's runtime expects.
    /// </summary>
    /// <remarks>
    /// <see cref="AdoReaderUtilTests"/> reads from SQLite, which decodes nearly everything to
    /// <see cref="long"/>, <see cref="double"/> or <see cref="string"/> and so never presents the reader with
    /// a value of the type the column was declared in. SQL Server does: a <c>uniqueidentifier</c> arrives as
    /// a <see cref="Guid"/>, a <c>datetimeoffset</c> as a <see cref="DateTimeOffset"/>, a <c>tinyint</c> as a
    /// <see cref="byte"/>. Those are the cases a typed accessor casts and fails on.
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
        /// Returns a reader positioned on a single row holding the given expression.
        /// </summary>
        /// <param name="selectExpression"></param>
        /// <returns></returns>
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
        /// The whole of the server's tiny integer range reaches a <c>SMALLINT</c>, which is why that is what
        /// it is mapped to: the top half of it does not fit a signed <c>TINYINT</c>.
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
        /// A <c>uniqueidentifier</c> is not a character column and is not read as one. Formatting it would
        /// be the mapping answering for a type the column does not have, and the text it produced could
        /// not be told from a <c>CHAR(36)</c> that really is text.
        /// </summary>
        [Fact]
        public void AUniqueIdentifierIsNotReadAsAString()
        {
            using var reader = Row("CAST('3f2504e0-4f89-11d3-9a0c-0305e82c3301' AS UNIQUEIDENTIFIER)");

            Assert.ThrowsAny<InvalidCastException>(
                () => AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.CHAR));
        }

        /// <summary>
        /// It is a <c>UUID</c>, and the value is the sixteen bytes rather than the text:
        /// <c>org.apache.calcite.util.UuidValue</c> is the class Calcite's runtime holds them in, since
        /// CALCITE-7716 wrapped <c>java.util.UUID</c> to order a UUID unsigned as SQL does.
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
        /// A null <c>uniqueidentifier</c> is a null, the class being a reference.
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
        /// A timestamp carries no zone, so the count is to the wall clock read as UTC. Reading it as local
        /// time instead put every timestamp out by the machine's offset, and only a machine at UTC would
        /// have noticed.
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
        /// A zoned timestamp is an instant, and the provider hands one over as a
        /// <see cref="DateTimeOffset"/> — <see cref="DbDataReader.GetDateTime"/> refuses it outright.
        /// </summary>
        [Fact]
        public void AZonedTimestampIsReadAsAnInstant()
        {
            using var reader = Row("CAST('2020-01-15T10:20:30+00:00' AS DATETIMEOFFSET(3))");
            Assert.Equal(ExpectedMillis, ((java.lang.Long)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TIMESTAMP_TZ)!).longValue());
        }

        /// <summary>
        /// The offset is part of the value, not decoration: the same wall clock at a different offset is a
        /// different instant.
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
