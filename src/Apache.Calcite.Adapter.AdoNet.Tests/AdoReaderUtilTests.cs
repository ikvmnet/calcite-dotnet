using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.sql.type;

using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers the mapping from a provider's value to the representation Calcite's runtime expects.
    /// </summary>
    /// <remarks>
    /// This mapping is reached from generated code in <c>AdoToEnumerableConverter</c> and from
    /// <see cref="Utils.ObjectArrayRowBuilder"/>, so what it returns is what every row in the enumerable
    /// convention is made of. Calcite's runtime reads those as boxed Java values, which is why the assertions
    /// are about <c>java.lang</c> types rather than .NET ones.
    /// </remarks>
    [TestClass]
    public class AdoReaderUtilTests
    {

        SqliteConnection _connection = null!;
        readonly List<DbCommand> _commands = [];

        [TestInitialize]
        public void Setup()
        {
            _connection = new SqliteConnection("Data Source=:memory:");
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
        /// <remarks>
        /// The command outlives the call: disposing it closes the reader it produced, and every assertion
        /// here happens after the reader is handed back.
        /// </remarks>
        DbDataReader Row(string selectExpression)
        {
            var command = _connection.CreateCommand();
            command.CommandText = $"SELECT {selectExpression}";
            _commands.Add(command);

            var reader = command.ExecuteReader();
            Assert.IsTrue(reader.Read(), "expected one row");
            return reader;
        }

        #region Integral types

        [TestMethod]
        public void BooleanIsReadAsAJavaBoolean()
        {
            using var reader = Row("1");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.BOOLEAN);

            Assert.IsInstanceOfType<java.lang.Boolean>(value);
            Assert.IsTrue(((java.lang.Boolean)value!).booleanValue());
        }

        [TestMethod]
        public void FalseIsReadAsAJavaBoolean()
        {
            using var reader = Row("0");
            Assert.IsFalse(((java.lang.Boolean)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.BOOLEAN)!).booleanValue());
        }

        [TestMethod]
        public void TinyIntIsReadAsAJavaByte()
        {
            using var reader = Row("7");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TINYINT);

            Assert.IsInstanceOfType<java.lang.Byte>(value);
            Assert.AreEqual((byte)7, ((java.lang.Byte)value!).byteValue());
        }

        [TestMethod]
        public void SmallIntIsReadAsAJavaShort()
        {
            using var reader = Row("-1234");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.SMALLINT);

            Assert.IsInstanceOfType<java.lang.Short>(value);
            Assert.AreEqual((short)-1234, ((java.lang.Short)value!).shortValue());
        }

        [TestMethod]
        public void IntegerIsReadAsAJavaInteger()
        {
            using var reader = Row("2147483647");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.INTEGER);

            Assert.IsInstanceOfType<java.lang.Integer>(value);
            Assert.AreEqual(int.MaxValue, ((java.lang.Integer)value!).intValue());
        }

        [TestMethod]
        public void BigIntIsReadAsAJavaLong()
        {
            using var reader = Row("9223372036854775807");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.BIGINT);

            Assert.IsInstanceOfType<java.lang.Long>(value);
            Assert.AreEqual(long.MaxValue, ((java.lang.Long)value!).longValue());
        }

        #endregion

        #region Approximate types

        [TestMethod]
        public void DoubleIsReadAsAJavaDouble()
        {
            using var reader = Row("3.5");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DOUBLE);

            Assert.IsInstanceOfType<java.lang.Double>(value);
            Assert.AreEqual(3.5d, ((java.lang.Double)value!).doubleValue());
        }

        /// <summary>
        /// FLOAT is eight bytes in Calcite and shares DOUBLE's representation — <c>getJavaClass</c> returns
        /// <c>Double</c> for both, and marks the pairing "sic". Reading one as a four byte float silently
        /// loses precision.
        /// </summary>
        [TestMethod]
        public void FloatSharesDoublesRepresentation()
        {
            using var reader = Row("3.5");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.FLOAT);

            Assert.IsInstanceOfType<java.lang.Double>(value);
            Assert.IsNotInstanceOfType<java.lang.Float>(value);
            Assert.AreEqual(3.5d, ((java.lang.Double)value!).doubleValue());
        }

        /// <summary>
        /// REAL is the four byte one.
        /// </summary>
        [TestMethod]
        public void RealIsReadAsAJavaFloat()
        {
            using var reader = Row("3.5");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.REAL);

            Assert.IsInstanceOfType<java.lang.Float>(value);
            Assert.AreEqual(3.5f, ((java.lang.Float)value!).floatValue());
        }

        /// <summary>
        /// A decimal is exact, so it travels as a <see cref="java.math.BigDecimal"/> rather than through a
        /// double that could not represent it.
        /// </summary>
        [TestMethod]
        public void DecimalIsReadAsABigDecimal()
        {
            using var reader = Row("'123.456'");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DECIMAL);

            Assert.IsInstanceOfType<java.math.BigDecimal>(value);
            Assert.AreEqual("123.456", value!.ToString());
        }

        #endregion

        #region Binary

        [TestMethod]
        public void VarbinaryIsReadAsAByteString()
        {
            using var reader = Row("x'01FF80'");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.VARBINARY);

            Assert.IsInstanceOfType<org.apache.calcite.avatica.util.ByteString>(value);
            CollectionAssert.AreEqual(new byte[] { 0x01, 0xFF, 0x80 }, ((org.apache.calcite.avatica.util.ByteString)value!).getBytes());
        }

        [TestMethod]
        public void BinaryIsReadAsAByteString()
        {
            using var reader = Row("x'AB'");
            Assert.IsInstanceOfType<org.apache.calcite.avatica.util.ByteString>(
                AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.BINARY));
        }

        #endregion

        #region Character types

        [TestMethod]
        public void VarcharIsReadAsAString()
        {
            using var reader = Row("'hello'");
            Assert.AreEqual("hello", AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.VARCHAR));
        }

        [TestMethod]
        public void CharIsReadAsAString()
        {
            using var reader = Row("'abc'");
            Assert.AreEqual("abc", AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.CHAR));
        }

        [TestMethod]
        public void AnEmptyStringIsNotNull()
        {
            using var reader = Row("''");
            Assert.AreEqual("", AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.VARCHAR));
        }

        #endregion

        #region Temporal types

        /// <summary>
        /// A date is a count of whole days since 1 January 1970 in an <see cref="java.lang.Integer"/>, which
        /// is what <c>SqlFunctions.internalToDate</c> decodes with <c>LocalDate.ofEpochDay</c>.
        /// </summary>
        [TestMethod]
        public void DateIsReadAsDaysSinceTheEpoch()
        {
            using var reader = Row("'2024-03-15'");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DATE);

            Assert.IsInstanceOfType<java.lang.Integer>(value);
            Assert.AreEqual(
                new DateOnly(2024, 3, 15).DayNumber - new DateOnly(1970, 1, 1).DayNumber,
                ((java.lang.Integer)value!).intValue());
        }

        [TestMethod]
        public void TheEpochItselfIsDayZero()
        {
            using var reader = Row("'1970-01-01'");
            Assert.AreEqual(0, ((java.lang.Integer)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DATE)!).intValue());
        }

        [TestMethod]
        public void ADateBeforeTheEpochIsNegative()
        {
            using var reader = Row("'1969-12-31'");
            Assert.AreEqual(-1, ((java.lang.Integer)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DATE)!).intValue());
        }

        /// <summary>
        /// A date is not a timestamp. They were once the same line, which meant a date arrived 86,400,000
        /// times too large and boxed as the wrong type.
        /// </summary>
        [TestMethod]
        public void ADateIsNotAMillisecondCount()
        {
            using var reader = Row("'2024-03-15'");
            var date = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DATE);

            Assert.IsInstanceOfType<java.lang.Integer>(date);
            Assert.IsNotInstanceOfType<java.lang.Long>(date);
            Assert.IsTrue(((java.lang.Integer)date!).intValue() < 100_000, "a day count, not milliseconds");
        }

        /// <summary>
        /// The reason the date component is taken directly: a value whose <see cref="DateTime.Kind"/> is
        /// unspecified would pick up the machine's offset on the way through
        /// <see cref="DateTimeOffset"/>, and midnight west of UTC would fall to the day before.
        /// </summary>
        [TestMethod]
        public void MidnightDoesNotDependOnTheMachineTimeZone()
        {
            using var reader = Row("'2024-03-15 00:00:00'");
            var value = (java.lang.Integer)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DATE)!;

            Assert.AreEqual(
                new DateOnly(2024, 3, 15).DayNumber - new DateOnly(1970, 1, 1).DayNumber,
                value.intValue());
        }

        [TestMethod]
        public void TimestampIsReadAsAJavaLong()
        {
            using var reader = Row("'2024-03-15 12:30:45'");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TIMESTAMP);

            Assert.IsInstanceOfType<java.lang.Long>(value);
        }

        /// <summary>
        /// The contract that matters: what the adapter produces is what the reader edge decodes. A date used
        /// to arrive as a <see cref="java.lang.Long"/>, which <c>CalciteResultValue</c> has no case for, so
        /// every date column threw on its first row.
        /// </summary>
        [TestMethod]
        public void ADateSurvivesTheRoundTripToADotNetDate()
        {
            using var reader = Row("'2024-03-15'");
            var value = (java.lang.Integer)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DATE)!;

            // the decode CalciteResultValue performs for SqlTypeName.DATE
            var decoded = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(value.intValue());

            Assert.AreEqual(new DateTime(2024, 3, 15, 0, 0, 0, DateTimeKind.Utc), decoded);
        }

        #endregion

        #region Other and null

        /// <summary>
        /// OTHER is the escape hatch, and hands back whatever the provider gave.
        /// </summary>
        [TestMethod]
        public void OtherIsReadAsTheProviderValue()
        {
            using var reader = Row("'passthrough'");
            Assert.AreEqual("passthrough", AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.OTHER));
        }

        [TestMethod]
        public void NullTypeIsAlwaysNull()
        {
            using var reader = Row("1");
            Assert.IsNull(AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.NULL));
        }

        [TestMethod]
        public void EveryTypeReadsADatabaseNullAsNull()
        {
            SqlTypeName[] types = [
                SqlTypeName.BOOLEAN, SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.INTEGER,
                SqlTypeName.BIGINT, SqlTypeName.FLOAT, SqlTypeName.DOUBLE, SqlTypeName.CHAR,
                SqlTypeName.VARCHAR, SqlTypeName.OTHER, SqlTypeName.DATE, SqlTypeName.TIMESTAMP,
            ];

            foreach (var type in types)
            {
                using var reader = Row("NULL");
                Assert.IsNull(AdoReaderUtil.GetDbReaderValue(reader, 0, type), $"{type.name()} should read NULL as null");
            }
        }

        #endregion

        #region Unsupported

        /// <summary>
        /// A type with no mapping is refused by name rather than silently mis-read.
        /// </summary>
        [TestMethod]
        public void AnUnmappedTypeIsRefusedByName()
        {
            using var reader = Row("1");

            var e = Assert.ThrowsExactly<AdoCalciteException>(
                () => AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.GEOMETRY));

            StringAssert.Contains(e.Message, nameof(SqlTypeName.GEOMETRY));
        }

        #endregion

        #region Overload agreement

        /// <summary>
        /// The <c>RelDataType</c> overload is the one a row builder reaches, and has to agree with the one
        /// generated code reaches.
        /// </summary>
        [TestMethod]
        public void BothOverloadsAgree()
        {
            var factory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
            var type = factory.createSqlType(SqlTypeName.INTEGER);

            using var reader = Row("42");
            var byType = AdoReaderUtil.GetDbReaderValue(reader, 0, type);
            var byName = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.INTEGER);

            Assert.AreEqual(byName, byType);
            Assert.AreEqual(42, ((java.lang.Integer)byType!).intValue());
        }

        #endregion

        #region Accessors

        [TestMethod]
        public void AccessorsReadNullIndependently()
        {
            using var reader = Row("NULL");

            Assert.IsNull(AdoReaderUtil.GetBoolean(reader, 0));
            Assert.IsNull(AdoReaderUtil.GetByte(reader, 0));
            Assert.IsNull(AdoReaderUtil.GetShort(reader, 0));
            Assert.IsNull(AdoReaderUtil.GetInt(reader, 0));
            Assert.IsNull(AdoReaderUtil.GetLong(reader, 0));
            Assert.IsNull(AdoReaderUtil.GetString(reader, 0));
            Assert.IsNull(AdoReaderUtil.GetValue(reader, 0));
        }

        #endregion

    }

}
