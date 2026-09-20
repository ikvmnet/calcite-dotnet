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
    /// Covers the mapping from a provider's value to the representation Calcite's runtime expects.
    /// </summary>
    /// <remarks>
    /// This mapping is reached from generated code in <c>AdoToEnumerableConverter</c> and from
    /// <see cref="Utils.ObjectArrayRowBuilder"/>, so what it returns is what every row in the enumerable
    /// convention is made of. Calcite's runtime reads those as boxed Java values, which is why the assertions
    /// are about <c>java.lang</c> types rather than .NET ones.
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
        /// Calcite's TINYINT is signed, and Java's <c>byte</c> is IKVM's unsigned one, so the sign has to
        /// travel in the bits rather than in the type.
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
        /// Calcite's unsigned types are not a variation on the signed ones: <c>getJavaClass</c> answers a
        /// joou value for each, and <c>CalciteResultValue</c> is written to decode exactly those. Every one
        /// of them threw <c>Unsupported SQL type mapping</c> until there was a case for it, so
        /// <c>AdoTable</c>'s mapping of <c>UInt16</c>, <c>UInt32</c> and <c>UInt64</c> could type a column
        /// and never read one.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="typeName"></param>
        /// <param name="expected"></param>
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
        /// The class matters as much as the value: <c>CalciteResultValue</c> decodes a <c>UTINYINT</c> by
        /// asking whether the value is a joou <c>UByte</c>, and a <see cref="java.lang.Short"/> holding the
        /// same number is not one.
        /// </summary>
        [Fact]
        public void AnUnsignedValueIsAJoouValue()
        {
            using var reader = Row("200");

            Assert.IsAssignableFrom<org.joou.UByte>(AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.UTINYINT));
        }

        /// <summary>
        /// The distinction the whole mapping turns on. 200 in a signed <c>TINYINT</c> is -56, which is why
        /// an unsigned tiny integer is a <c>UTINYINT</c> and not a <c>TINYINT</c>.
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
        /// An unsigned value is null-safe like every other, the joou types being references.
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
        /// FLOAT is eight bytes in Calcite and shares DOUBLE's representation — <c>getJavaClass</c> returns
        /// <c>Double</c> for both, and marks the pairing "sic". Reading one as a four byte float silently
        /// loses precision.
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
        /// REAL is the four byte one.
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
        /// A decimal is exact, so it travels as a <see cref="java.math.BigDecimal"/> rather than through a
        /// double that could not represent it.
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
        /// A date is a count of whole days since 1 January 1970 in an <see cref="java.lang.Integer"/>, which
        /// is what <c>SqlFunctions.internalToDate</c> decodes with <c>LocalDate.ofEpochDay</c>.
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
        /// A date is not a timestamp. They were once the same line, which meant a date arrived 86,400,000
        /// times too large and boxed as the wrong type.
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
        /// The reason the date component is taken directly: a value whose <see cref="DateTime.Kind"/> is
        /// unspecified would pick up the machine's offset on the way through
        /// <see cref="DateTimeOffset"/>, and midnight west of UTC would fall to the day before.
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
        /// The contract that matters: what the adapter produces is what the reader edge decodes. A date used
        /// to arrive as a <see cref="java.lang.Long"/>, which <c>CalciteResultValue</c> has no case for, so
        /// every date column threw on its first row.
        /// </summary>
        [Fact]
        public void ADateSurvivesTheRoundTripToADotNetDate()
        {
            using var reader = Row("'2024-03-15'");
            var value = (java.lang.Integer)AdoReaderUtil.GetDbReaderValue(reader, 0, SqlTypeName.DATE)!;

            // the decode CalciteResultValue performs for SqlTypeName.DATE
            var decoded = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(value.intValue());

            Assert.Equal(new DateTime(2024, 3, 15, 0, 0, 0, DateTimeKind.Utc), decoded);
        }

        #endregion

        #region Other and null

        /// <summary>
        /// OTHER is the escape hatch, and hands back whatever the provider gave.
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
        /// A type with no mapping is refused by name rather than silently mis-read.
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
        /// The <c>RelDataType</c> overload is the one a row builder reaches, and has to agree with the one
        /// generated code reaches.
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
