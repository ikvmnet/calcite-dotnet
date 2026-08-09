using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.sql.type;

using System;
using System.Collections.Generic;
using System.Data.Common;

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
    [TestClass]
    public class SqlServerReaderUtilTests
    {

        SqlConnection _connection = null!;
        readonly List<DbCommand> _commands = [];

        [TestInitialize]
        public void Setup()
        {
            if (SqlServerFixture.IsAvailable == false)
                Assert.Inconclusive("No SQL Server LocalDB instance is reachable on this machine.");

            _connection = new SqlConnection(new SqlConnectionStringBuilder()
            {
                DataSource = @"(localdb)\MSSQLLocalDB",
                InitialCatalog = "master",
                IntegratedSecurity = true,
                TrustServerCertificate = true,
            }.ConnectionString);

            _connection.Open();
        }

        [TestCleanup]
        public void Cleanup()
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
            Assert.IsTrue(reader.Read(), "expected one row");
            return reader;
        }

        /// <summary>
        /// The whole of the server's tiny integer range reaches a <c>SMALLINT</c>, which is why that is what
        /// it is mapped to: the top half of it does not fit a signed <c>TINYINT</c>.
        /// </summary>
        [TestMethod]
        public void ATinyIntWidensToAShort()
        {
            using var reader = Row("CAST(200 AS TINYINT)");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.SMALLINT);

            Assert.IsInstanceOfType<java.lang.Short>(value);
            Assert.AreEqual((short)200, ((java.lang.Short)value!).shortValue());
        }

        [TestMethod]
        public void ABitIsReadAsAJavaBoolean()
        {
            using var reader = Row("CAST(1 AS BIT)");
            Assert.IsTrue(((java.lang.Boolean)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.BOOLEAN)!).booleanValue());
        }

        [TestMethod]
        public void ARealIsReadAsAJavaFloat()
        {
            using var reader = Row("CAST(2.5 AS REAL)");
            Assert.AreEqual(2.5f, ((java.lang.Float)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.REAL)!).floatValue());
        }

        [TestMethod]
        public void AFloatIsReadAsAJavaDouble()
        {
            using var reader = Row("CAST(1.5 AS FLOAT)");
            Assert.AreEqual(1.5d, ((java.lang.Double)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DOUBLE)!).doubleValue());
        }

        [TestMethod]
        public void AMoneyKeepsItsScale()
        {
            using var reader = Row("CAST(12.34 AS MONEY)");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DECIMAL);

            Assert.IsInstanceOfType<java.math.BigDecimal>(value);
            Assert.AreEqual("12.3400", value!.ToString());
        }

        /// <summary>
        /// A <c>uniqueidentifier</c> is a <see cref="Guid"/> to the provider, and Calcite is holding the
        /// column as a <c>CHAR(36)</c>: <see cref="DbDataReader.GetString"/> casts and refuses it.
        /// </summary>
        [TestMethod]
        public void AUniqueIdentifierIsReadAsItsText()
        {
            using var reader = Row("CAST('3f2504e0-4f89-11d3-9a0c-0305e82c3301' AS UNIQUEIDENTIFIER)");
            Assert.AreEqual("3f2504e0-4f89-11d3-9a0c-0305e82c3301", AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.CHAR));
        }

        [TestMethod]
        public void AVarbinaryIsReadAsAByteString()
        {
            using var reader = Row("CAST(0x01FF80 AS VARBINARY(8))");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.VARBINARY);

            Assert.IsInstanceOfType<org.apache.calcite.avatica.util.ByteString>(value);
            CollectionAssert.AreEqual(new byte[] { 0x01, 0xFF, 0x80 }, ((org.apache.calcite.avatica.util.ByteString)value!).getBytes());
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

        [TestMethod]
        public void ADateIsReadAsDaysSinceTheEpoch()
        {
            using var reader = Row("CAST('2020-01-15' AS DATE)");
            Assert.AreEqual(ExpectedDay, ((java.lang.Integer)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DATE)!).intValue());
        }

        [TestMethod]
        public void ATimeIsReadAsMillisecondsSinceMidnight()
        {
            using var reader = Row("CAST('01:02:03.500' AS TIME(3))");
            Assert.AreEqual(3723500, ((java.lang.Integer)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TIME)!).intValue());
        }

        /// <summary>
        /// A timestamp carries no zone, so the count is to the wall clock read as UTC. Reading it as local
        /// time instead put every timestamp out by the machine's offset, and only a machine at UTC would
        /// have noticed.
        /// </summary>
        [TestMethod]
        public void ATimestampDoesNotDependOnTheMachineTimeZone()
        {
            using var reader = Row("CAST('2020-01-15T10:20:30' AS DATETIME)");
            Assert.AreEqual(ExpectedMillis, ((java.lang.Long)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TIMESTAMP)!).longValue());
        }

        [TestMethod]
        public void ADateTime2IsTheSameTimestamp()
        {
            using var reader = Row("CAST('2020-01-15T10:20:30' AS DATETIME2(3))");
            Assert.AreEqual(ExpectedMillis, ((java.lang.Long)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TIMESTAMP)!).longValue());
        }

        /// <summary>
        /// A zoned timestamp is an instant, and the provider hands one over as a
        /// <see cref="DateTimeOffset"/> — <see cref="DbDataReader.GetDateTime"/> refuses it outright.
        /// </summary>
        [TestMethod]
        public void AZonedTimestampIsReadAsAnInstant()
        {
            using var reader = Row("CAST('2020-01-15T10:20:30+00:00' AS DATETIMEOFFSET(3))");
            Assert.AreEqual(ExpectedMillis, ((java.lang.Long)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TIMESTAMP_TZ)!).longValue());
        }

        /// <summary>
        /// The offset is part of the value, not decoration: the same wall clock at a different offset is a
        /// different instant.
        /// </summary>
        [TestMethod]
        public void AZonedTimestampHonoursItsOffset()
        {
            using var reader = Row("CAST('2020-01-15T10:20:30-05:00' AS DATETIMEOFFSET(3))");
            Assert.AreEqual(
                ExpectedMillis + 5 * 60 * 60 * 1000L,
                ((java.lang.Long)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TIMESTAMP_TZ)!).longValue());
        }

        #endregion

    }

}
