using System;
using System.Collections.Generic;
using System.Data.Common;

using FluentAssertions;

using Microsoft.Data.Sqlite;

using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Tests <see cref="AdoReaderUtil"/>'s mapping from a provider's value to the representation Calcite's
    /// runtime expects.
    /// </summary>
    /// <remarks>
    /// The converters and <see cref="Utils.ObjectArrayRowBuilder"/> build rows from these values, and
    /// Calcite's runtime reads them as boxed Java values, so the assertions are about <c>java.lang</c> types.
    /// </remarks>
    public class AdoReaderUtilTests : IDisposable
    {

        SqliteConnection _connection = null!;
        readonly List<DbCommand> _commands = [];

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public AdoReaderUtilTests()
        {
            _connection = new SqliteConnection("Data Source=:memory:");
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
        /// <remarks>
        /// The command is kept until the test is disposed, because disposing it would close the reader.
        /// </remarks>
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

        #region Integral types

        [Fact]
        public void BooleanIsReadAsAJavaBoolean()
        {
            using var reader = Row("1");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.BOOLEAN);

            Assert.IsAssignableFrom<java.lang.Boolean>(value);
            Assert.True(((java.lang.Boolean)value!).booleanValue());
        }

        [Fact]
        public void FalseIsReadAsAJavaBoolean()
        {
            using var reader = Row("0");
            Assert.False(((java.lang.Boolean)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.BOOLEAN)!).booleanValue());
        }

        [Fact]
        public void TinyIntIsReadAsAJavaByte()
        {
            using var reader = Row("7");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TINYINT);

            Assert.IsAssignableFrom<java.lang.Byte>(value);
            Assert.Equal((byte)7, ((java.lang.Byte)value!).byteValue());
        }

        /// <summary>
        /// Calcite's <c>TINYINT</c> is signed, but IKVM maps Java's <c>byte</c> to the unsigned CLR
        /// <see cref="byte"/>, so the sign travels in the bits; <c>java.lang.Byte.toString</c> reads them signed.
        /// </summary>
        [Fact]
        public void ANegativeTinyIntKeepsItsSign()
        {
            using var reader = Row("-1");
            var value = (java.lang.Byte)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TINYINT)!;

            Assert.Equal("-1", value.ToString());
        }

        [Fact]
        public void SmallIntIsReadAsAJavaShort()
        {
            using var reader = Row("-1234");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.SMALLINT);

            Assert.IsAssignableFrom<java.lang.Short>(value);
            Assert.Equal((short)-1234, ((java.lang.Short)value!).shortValue());
        }

        [Fact]
        public void IntegerIsReadAsAJavaInteger()
        {
            using var reader = Row("2147483647");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.INTEGER);

            Assert.IsAssignableFrom<java.lang.Integer>(value);
            Assert.Equal(int.MaxValue, ((java.lang.Integer)value!).intValue());
        }

        [Fact]
        public void BigIntIsReadAsAJavaLong()
        {
            using var reader = Row("9223372036854775807");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.BIGINT);

            Assert.IsAssignableFrom<java.lang.Long>(value);
            Assert.Equal(long.MaxValue, ((java.lang.Long)value!).longValue());
        }

        #endregion

        #region Unsigned types

        /// <summary>
        /// Each unsigned type reads across its whole range. Calcite's <c>getJavaClass</c> maps each unsigned
        /// type to a joou class rather than to the signed type's class.
        /// </summary>
        /// <param name="expression">The SQLite expression that produces the value to read.</param>
        /// <param name="typeName">The name of the unsigned <see cref="SqlTypeName"/> the value is read as.</param>
        /// <param name="expected">The value's expected string form once read.</param>
        [Theory]
        [InlineData("0", nameof(SqlTypeName.UTINYINT), "0")]
        [InlineData("200", nameof(SqlTypeName.UTINYINT), "200")]
        [InlineData("255", nameof(SqlTypeName.UTINYINT), "255")]
        [InlineData("65535", nameof(SqlTypeName.USMALLINT), "65535")]
        [InlineData("4294967295", nameof(SqlTypeName.UINTEGER), "4294967295")]
        [InlineData("'18446744073709551615'", nameof(SqlTypeName.UBIGINT), "18446744073709551615")]
        public void AnUnsignedValueKeepsTheWholeOfItsRange(string expression, string typeName, string expected)
        {
            using var reader = Row(expression);
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.valueOf(typeName));

            Assert.Equal(expected, value!.ToString());
        }

        /// <summary>
        /// The class matters as well as the value: <c>Apache.Calcite.Data</c> recognises a <c>UTINYINT</c> by
        /// its joou <c>UByte</c> class, and a <see cref="java.lang.Short"/> holding the same number is not one.
        /// </summary>
        [Fact]
        public void AnUnsignedValueIsAJoouValue()
        {
            using var reader = Row("200");

            Assert.IsAssignableFrom<org.joou.UByte>(AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.UTINYINT));
        }

        /// <summary>
        /// The byte that is 200 unsigned is -56 signed, so an unsigned tiny integer must be read as
        /// <c>UTINYINT</c> and not <c>TINYINT</c>.
        /// </summary>
        [Fact]
        public void TheSameByteSignedAndUnsignedAreDifferentNumbers()
        {
            using var signed = Row("-56");
            using var unsigned = Row("200");

            Assert.Equal("-56", AdoReaderUtil.GetDbReaderValue(signed, 0, SqlTypeName.TINYINT)!.ToString());
            Assert.Equal("200", AdoReaderUtil.GetDbReaderValue(unsigned, 0, SqlTypeName.UTINYINT)!.ToString());
        }

        /// <summary>
        /// A database null in an unsigned column reads as null.
        /// </summary>
        [Fact]
        public void AnUnsignedNullIsNull()
        {
            using var reader = Row("CAST(NULL AS INTEGER)");

            Assert.Null(AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.UTINYINT));
        }

        #endregion

        #region Approximate types

        [Fact]
        public void DoubleIsReadAsAJavaDouble()
        {
            using var reader = Row("3.5");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DOUBLE);

            Assert.IsAssignableFrom<java.lang.Double>(value);
            Assert.Equal(3.5d, ((java.lang.Double)value!).doubleValue());
        }

        /// <summary>
        /// <c>FLOAT</c> is eight bytes in Calcite and shares <c>DOUBLE</c>'s representation:
        /// <c>getJavaClass</c> returns <c>Double</c> for both. Reading it as a four-byte float loses precision.
        /// </summary>
        [Fact]
        public void FloatSharesDoublesRepresentation()
        {
            using var reader = Row("3.5");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.FLOAT);

            Assert.IsAssignableFrom<java.lang.Double>(value);
            Assert.IsNotAssignableFrom<java.lang.Float>(value);
            Assert.Equal(3.5d, ((java.lang.Double)value!).doubleValue());
        }

        /// <summary>
        /// <c>REAL</c> is the four-byte type.
        /// </summary>
        [Fact]
        public void RealIsReadAsAJavaFloat()
        {
            using var reader = Row("3.5");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.REAL);

            Assert.IsAssignableFrom<java.lang.Float>(value);
            Assert.Equal(3.5f, ((java.lang.Float)value!).floatValue());
        }

        /// <summary>
        /// A decimal is exact, so it is read as a <see cref="java.math.BigDecimal"/> rather than a double.
        /// </summary>
        [Fact]
        public void DecimalIsReadAsABigDecimal()
        {
            using var reader = Row("'123.456'");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DECIMAL);

            Assert.IsAssignableFrom<java.math.BigDecimal>(value);
            Assert.Equal("123.456", value!.ToString());
        }

        #endregion

        #region Binary

        [Fact]
        public void VarbinaryIsReadAsAByteString()
        {
            using var reader = Row("x'01FF80'");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.VARBINARY);

            Assert.IsAssignableFrom<org.apache.calcite.avatica.util.ByteString>(value);
            Assert.Equal(new byte[] { 0x01, 0xFF, 0x80 }, ((org.apache.calcite.avatica.util.ByteString)value!).getBytes());
        }

        [Fact]
        public void BinaryIsReadAsAByteString()
        {
            using var reader = Row("x'AB'");
            Assert.IsAssignableFrom<org.apache.calcite.avatica.util.ByteString>(
                AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.BINARY));
        }

        #endregion

        #region Character types

        [Fact]
        public void VarcharIsReadAsAString()
        {
            using var reader = Row("'hello'");
            Assert.Equal("hello", AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.VARCHAR));
        }

        [Fact]
        public void CharIsReadAsAString()
        {
            using var reader = Row("'abc'");
            Assert.Equal("abc", AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.CHAR));
        }

        [Fact]
        public void AnEmptyStringIsNotNull()
        {
            using var reader = Row("''");
            Assert.Equal("", AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.VARCHAR));
        }

        #endregion

        #region Temporal types

        /// <summary>
        /// A date is a count of whole days since 1 January 1970 in a <see cref="java.lang.Integer"/>, which
        /// <c>SqlFunctions.internalToDate</c> decodes with <c>LocalDate.ofEpochDay</c>.
        /// </summary>
        [Fact]
        public void DateIsReadAsDaysSinceTheEpoch()
        {
            using var reader = Row("'2024-03-15'");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DATE);

            Assert.IsAssignableFrom<java.lang.Integer>(value);
            Assert.Equal(
                new DateOnly(2024, 3, 15).DayNumber - new DateOnly(1970, 1, 1).DayNumber,
                ((java.lang.Integer)value!).intValue());
        }

        [Fact]
        public void TheEpochItselfIsDayZero()
        {
            using var reader = Row("'1970-01-01'");
            Assert.Equal(0, ((java.lang.Integer)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DATE)!).intValue());
        }

        [Fact]
        public void ADateBeforeTheEpochIsNegative()
        {
            using var reader = Row("'1969-12-31'");
            Assert.Equal(-1, ((java.lang.Integer)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DATE)!).intValue());
        }

        /// <summary>
        /// A date is a day count in an <see cref="java.lang.Integer"/>, not a millisecond count in a
        /// <see cref="java.lang.Long"/> as a timestamp is.
        /// </summary>
        [Fact]
        public void ADateIsNotAMillisecondCount()
        {
            using var reader = Row("'2024-03-15'");
            var date = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DATE);

            Assert.IsAssignableFrom<java.lang.Integer>(date);
            Assert.IsNotAssignableFrom<java.lang.Long>(date);
            Assert.True(((java.lang.Integer)date!).intValue() < 100_000, "a day count, not milliseconds");
        }

        /// <summary>
        /// The date component is taken directly from the provider's <see cref="DateTime"/>. Converting an
        /// unspecified <see cref="DateTime.Kind"/> through <see cref="DateTimeOffset"/> would apply the
        /// machine's offset, and midnight west of UTC would fall on the day before.
        /// </summary>
        [Fact]
        public void MidnightDoesNotDependOnTheMachineTimeZone()
        {
            using var reader = Row("'2024-03-15 00:00:00'");
            var value = (java.lang.Integer)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DATE)!;

            Assert.Equal(
                new DateOnly(2024, 3, 15).DayNumber - new DateOnly(1970, 1, 1).DayNumber,
                value.intValue());
        }

        [Fact]
        public void TimestampIsReadAsAJavaLong()
        {
            using var reader = Row("'2024-03-15 12:30:45'");
            var value = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.TIMESTAMP);

            Assert.IsAssignableFrom<java.lang.Long>(value);
        }

        /// <summary>
        /// The day count the adapter produces decodes back to the stored date.
        /// </summary>
        [Fact]
        public void ADateSurvivesTheRoundTripToADotNetDate()
        {
            using var reader = Row("'2024-03-15'");
            var value = (java.lang.Integer)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DATE)!;

            // a DATE day count decoded to a DateTime
            var decoded = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(value.intValue());

            Assert.Equal(new DateTime(2024, 3, 15, 0, 0, 0, DateTimeKind.Utc), decoded);
        }

        #endregion

        #region Other and null

        /// <summary>
        /// <c>OTHER</c> returns the provider's value unchanged.
        /// </summary>
        [Fact]
        public void OtherIsReadAsTheProviderValue()
        {
            using var reader = Row("'passthrough'");
            Assert.Equal("passthrough", AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.OTHER));
        }

        [Fact]
        public void NullTypeIsAlwaysNull()
        {
            using var reader = Row("1");
            Assert.Null(AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.NULL));
        }

        [Fact]
        public void EveryTypeReadsADatabaseNullAsNull()
        {
            SqlTypeName[] types = [
                SqlTypeName.BOOLEAN, SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.INTEGER,
                SqlTypeName.BIGINT, SqlTypeName.FLOAT, SqlTypeName.DOUBLE, SqlTypeName.CHAR,
                SqlTypeName.VARCHAR, SqlTypeName.OTHER, SqlTypeName.DATE, SqlTypeName.TIMESTAMP,
                SqlTypeName.UUID,
            ];

            foreach (var type in types)
            {
                using var reader = Row("NULL");
                AdoReaderUtil.GetDbReaderValue(reader, 0, type).Should().BeNull($"{type.name()} should read NULL as null");
            }
        }

        #endregion

        #region Unsupported

        /// <summary>
        /// A type with no mapping throws an exception naming the type.
        /// </summary>
        [Fact]
        public void AnUnmappedTypeIsRefusedByName()
        {
            using var reader = Row("1");

            var e = Assert.Throws<AdoCalciteException>(
                () => AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.GEOMETRY));

            Assert.Contains(nameof(SqlTypeName.GEOMETRY), e.Message);
        }

        #endregion

        #region Overload agreement

        /// <summary>
        /// The <c>RelDataType</c> overload agrees with the <see cref="SqlTypeName"/> overload.
        /// </summary>
        [Fact]
        public void BothOverloadsAgree()
        {
            var factory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
            var type = factory.createSqlType(SqlTypeName.INTEGER);

            using var reader = Row("42");
            var byType = AdoReaderUtil.GetDbReaderValue(reader, 0, type);
            var byName = AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.INTEGER);

            Assert.Equal(byName, byType);
            Assert.Equal(42, ((java.lang.Integer)byType!).intValue());
        }

        #endregion

        #region Accessors

        [Fact]
        public void AccessorsReadNullIndependently()
        {
            using var reader = Row("NULL");

            Assert.Null(AdoReaderUtil.GetBoolean(reader, 0));
            Assert.Null(AdoReaderUtil.GetByte(reader, 0));
            Assert.Null(AdoReaderUtil.GetShort(reader, 0));
            Assert.Null(AdoReaderUtil.GetInt(reader, 0));
            Assert.Null(AdoReaderUtil.GetLong(reader, 0));
            Assert.Null(AdoReaderUtil.GetString(reader, 0));
            Assert.Null(AdoReaderUtil.GetValue(reader, 0));
        }

        #endregion

    }

}
