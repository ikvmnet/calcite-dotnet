using System;

using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Data.Common.Tests
{

    public class ClrTypeRegistryTests
    {

        readonly JavaTypeFactoryImpl _types = new();
        readonly ClrTypeRegistry _registry;

        public ClrTypeRegistryTests()
        {
            _registry = new ClrTypeMapper().Bind(_types);
        }

        RelDataType Sql(SqlTypeName name) => _types.createTypeWithNullability(_types.createSqlType(name), true);

        RelDataType Sql(SqlTypeName name, int precision) => _types.createTypeWithNullability(_types.createSqlType(name, precision), true);

        // ------------------------------------------------------------------------------------
        // Which CLR type a Calcite type is read back as.
        // ------------------------------------------------------------------------------------

        [Theory]
        [InlineData(nameof(SqlTypeName.BOOLEAN), typeof(bool))]
        [InlineData(nameof(SqlTypeName.TINYINT), typeof(sbyte))]
        [InlineData(nameof(SqlTypeName.SMALLINT), typeof(short))]
        [InlineData(nameof(SqlTypeName.INTEGER), typeof(int))]
        [InlineData(nameof(SqlTypeName.BIGINT), typeof(long))]
        [InlineData(nameof(SqlTypeName.UTINYINT), typeof(byte))]
        [InlineData(nameof(SqlTypeName.USMALLINT), typeof(ushort))]
        [InlineData(nameof(SqlTypeName.UINTEGER), typeof(uint))]
        [InlineData(nameof(SqlTypeName.UBIGINT), typeof(ulong))]
        [InlineData(nameof(SqlTypeName.REAL), typeof(float))]
        [InlineData(nameof(SqlTypeName.DOUBLE), typeof(double))]
        [InlineData(nameof(SqlTypeName.FLOAT), typeof(double))]
        [InlineData(nameof(SqlTypeName.DECIMAL), typeof(decimal))]
        [InlineData(nameof(SqlTypeName.VARCHAR), typeof(string))]
        [InlineData(nameof(SqlTypeName.CHAR), typeof(string))]
        [InlineData(nameof(SqlTypeName.VARBINARY), typeof(byte[]))]
        [InlineData(nameof(SqlTypeName.BINARY), typeof(byte[]))]
        [InlineData(nameof(SqlTypeName.DATE), typeof(DateTime))]
        [InlineData(nameof(SqlTypeName.TIME), typeof(TimeSpan))]
        [InlineData(nameof(SqlTypeName.TIMESTAMP), typeof(DateTime))]
        [InlineData(nameof(SqlTypeName.TIMESTAMP_TZ), typeof(DateTimeOffset))]
        [InlineData(nameof(SqlTypeName.TIME_TZ), typeof(DateTimeOffset))]
        public void Should_read_back_as(string sqlTypeName, Type expected)
        {
            Assert.Equal(expected, _registry.GetClrType(Sql(SqlTypeName.valueOf(sqlTypeName))));
        }

        [Fact]
        public void Should_read_an_unclaimed_type_as_object()
        {
            Assert.Equal(typeof(object), _registry.GetClrType(Sql(SqlTypeName.OTHER)));
        }

        // ------------------------------------------------------------------------------------
        // Which Calcite type a CLR type is written as.
        // ------------------------------------------------------------------------------------

        [Theory]
        [InlineData(typeof(bool), nameof(SqlTypeName.BOOLEAN))]
        [InlineData(typeof(sbyte), nameof(SqlTypeName.TINYINT))]
        [InlineData(typeof(byte), nameof(SqlTypeName.UTINYINT))]
        [InlineData(typeof(int), nameof(SqlTypeName.INTEGER))]
        [InlineData(typeof(ulong), nameof(SqlTypeName.UBIGINT))]
        [InlineData(typeof(float), nameof(SqlTypeName.REAL))]
        [InlineData(typeof(double), nameof(SqlTypeName.DOUBLE))]
        [InlineData(typeof(decimal), nameof(SqlTypeName.DECIMAL))]
        [InlineData(typeof(string), nameof(SqlTypeName.VARCHAR))]
        [InlineData(typeof(byte[]), nameof(SqlTypeName.VARBINARY))]
        [InlineData(typeof(DateTime), nameof(SqlTypeName.TIMESTAMP))]
        [InlineData(typeof(DateTimeOffset), nameof(SqlTypeName.TIMESTAMP_TZ))]
        [InlineData(typeof(TimeSpan), nameof(SqlTypeName.TIME))]
        [InlineData(typeof(DateOnly), nameof(SqlTypeName.DATE))]
        [InlineData(typeof(TimeOnly), nameof(SqlTypeName.TIME))]
        [InlineData(typeof(Guid), nameof(SqlTypeName.CHAR))]
        public void Should_be_written_as(Type clrType, string expected)
        {
            var mapping = _registry.RequireMapping(clrType, null);
            Assert.Equal(expected, mapping.RelType.getSqlTypeName().name());
        }

        [Fact]
        public void A_Guid_should_be_written_as_a_CHAR_36()
        {
            Assert.Equal(36, _registry.RequireMapping(typeof(Guid), null).RelType.getPrecision());
        }

        [Fact]
        public void A_CHAR_should_not_be_read_back_as_a_Guid()
        {
            // nothing about a CHAR(36) says it holds one, and the entry that writes a Guid as one is not a
            // claim over every column of that width
            Assert.Equal(typeof(string), _registry.GetClrType(Sql(SqlTypeName.CHAR, 36)));
        }

        [Fact]
        public void A_DATE_should_not_be_what_a_bare_DateTime_is_written_as()
        {
            Assert.Equal(nameof(SqlTypeName.TIMESTAMP), _registry.RequireMapping(typeof(DateTime), null).RelType.getSqlTypeName().name());
        }

        // ------------------------------------------------------------------------------------
        // The type factory is the authority on what holds a value.
        // ------------------------------------------------------------------------------------

        [Theory]
        [InlineData(nameof(SqlTypeName.BOOLEAN), typeof(java.lang.Boolean))]
        [InlineData(nameof(SqlTypeName.TINYINT), typeof(java.lang.Byte))]
        [InlineData(nameof(SqlTypeName.SMALLINT), typeof(java.lang.Short))]
        [InlineData(nameof(SqlTypeName.INTEGER), typeof(java.lang.Integer))]
        [InlineData(nameof(SqlTypeName.BIGINT), typeof(java.lang.Long))]
        [InlineData(nameof(SqlTypeName.UTINYINT), typeof(org.joou.UByte))]
        [InlineData(nameof(SqlTypeName.UBIGINT), typeof(org.joou.ULong))]
        [InlineData(nameof(SqlTypeName.REAL), typeof(java.lang.Float))]
        [InlineData(nameof(SqlTypeName.DOUBLE), typeof(java.lang.Double))]
        [InlineData(nameof(SqlTypeName.FLOAT), typeof(java.lang.Double))]
        [InlineData(nameof(SqlTypeName.DECIMAL), typeof(java.math.BigDecimal))]
        [InlineData(nameof(SqlTypeName.VARCHAR), typeof(string))]
        [InlineData(nameof(SqlTypeName.DATE), typeof(java.lang.Integer))]
        [InlineData(nameof(SqlTypeName.TIME), typeof(java.lang.Integer))]
        [InlineData(nameof(SqlTypeName.TIMESTAMP), typeof(java.lang.Long))]
        [InlineData(nameof(SqlTypeName.VARBINARY), typeof(org.apache.calcite.avatica.util.ByteString))]
        public void Should_be_held_in(string sqlTypeName, Type expected)
        {
            Assert.Equal(expected, _registry.RequireMapping(null, Sql(SqlTypeName.valueOf(sqlTypeName))).RepresentationType);
        }

        // ------------------------------------------------------------------------------------
        // Round trips.
        // ------------------------------------------------------------------------------------

        [Theory]
        [InlineData(nameof(SqlTypeName.BOOLEAN), true)]
        [InlineData(nameof(SqlTypeName.INTEGER), 123456)]
        [InlineData(nameof(SqlTypeName.BIGINT), 9000000000L)]
        [InlineData(nameof(SqlTypeName.SMALLINT), (short)1234)]
        [InlineData(nameof(SqlTypeName.REAL), 1.5f)]
        [InlineData(nameof(SqlTypeName.DOUBLE), 3.25d)]
        [InlineData(nameof(SqlTypeName.VARCHAR), "hello")]
        public void Should_round_trip(string sqlTypeName, object value)
        {
            var relType = Sql(SqlTypeName.valueOf(sqlTypeName));
            var stored = _registry.ToCalcite(null, relType, value);
            Assert.Equal(value, _registry.FromCalcite(null, relType, stored));
        }

        [Fact]
        public void Should_round_trip_the_whole_of_a_UBIGINT()
        {
            // the decimal string is the route because valueOf(long) refuses anything above long.MaxValue,
            // which is half of what the type holds
            var relType = Sql(SqlTypeName.UBIGINT);
            var stored = _registry.ToCalcite(null, relType, ulong.MaxValue);
            Assert.Equal(ulong.MaxValue, _registry.FromCalcite(null, relType, stored));
        }

        [Fact]
        public void Should_round_trip_a_date()
        {
            var relType = Sql(SqlTypeName.DATE);
            var stored = _registry.ToCalcite(null, relType, new DateTime(2024, 1, 15, 0, 0, 0, DateTimeKind.Utc));
            Assert.Equal(java.lang.Integer.valueOf(19737), stored);
            Assert.Equal(new DateTime(2024, 1, 15, 0, 0, 0, DateTimeKind.Utc), _registry.FromCalcite(null, relType, stored));
        }

        [Fact]
        public void Should_round_trip_a_decimal()
        {
            var relType = _types.createTypeWithNullability(_types.createSqlType(SqlTypeName.DECIMAL, 10, 2), true);
            var stored = _registry.ToCalcite(null, relType, 123.45m);
            Assert.IsType<java.math.BigDecimal>(stored);
            Assert.Equal(123.45m, _registry.FromCalcite(null, relType, stored));
        }

        [Fact]
        public void Should_convert_rather_than_cast_a_width_the_source_chose()
        {
            // a provider that decoded a SMALLINT column as a byte, or a caller that wrote a long into an
            // INTEGER parameter: both are lossless and both are what the mapping meant
            Assert.Equal(java.lang.Short.valueOf(7), _registry.ToCalcite(null, Sql(SqlTypeName.SMALLINT), (byte)7));
            Assert.Equal(java.lang.Integer.valueOf(7), _registry.ToCalcite(null, Sql(SqlTypeName.INTEGER), 7L));
        }

        // ------------------------------------------------------------------------------------
        // Naming both types reaches a conversion that is nobody's default.
        // ------------------------------------------------------------------------------------

        [Fact]
        public void Should_read_a_DATE_as_a_DateOnly_when_asked()
        {
            var relType = Sql(SqlTypeName.DATE);
            var stored = _registry.ToCalcite(null, relType, new DateOnly(2024, 1, 15));
            Assert.Equal(new DateOnly(2024, 1, 15), _registry.FromCalcite(typeof(DateOnly), relType, stored));
        }

        [Fact]
        public void Should_not_reach_a_conversion_that_was_never_written()
        {
            // a BIGINT is not read as an int just because both are integers; the caller who wants that
            // narrowing states it
            Assert.Null(_registry.GetMapping(typeof(int), Sql(SqlTypeName.BIGINT)));
        }

        // ------------------------------------------------------------------------------------
        // Extension.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// Reads a VARCHAR as a Uri, which nothing built in does.
        /// </summary>
        sealed class UriResolver : IClrTypeResolver
        {

            readonly ClrTypeMappingCollection _mappings = new();

            public UriResolver()
            {
                _mappings.Add(typeof(Uri), SqlTypeName.VARCHAR, v => ((Uri)v).ToString(), v => new Uri((string)v));
            }

            public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context) => _mappings.GetMapping(clrType, relType, context);

        }

        [Fact]
        public void A_prepended_resolver_should_win()
        {
            var registry = new ClrTypeMapper().Prepend(new UriResolver()).Bind(_types);
            var relType = Sql(SqlTypeName.VARCHAR);

            Assert.Equal(typeof(Uri), registry.GetClrType(relType));
            Assert.Equal("https://calcite.apache.org/", registry.ToCalcite(null, relType, new Uri("https://calcite.apache.org/")));
            Assert.Equal(new Uri("https://calcite.apache.org/"), registry.FromCalcite(null, relType, "https://calcite.apache.org/"));
        }

        [Fact]
        public void A_prepended_resolver_should_pass_on_what_it_does_not_claim()
        {
            var registry = new ClrTypeMapper().Prepend(new UriResolver()).Bind(_types);

            Assert.Equal(typeof(int), registry.GetClrType(Sql(SqlTypeName.INTEGER)));
        }

        [Fact]
        public void Registering_the_same_resolver_twice_should_register_it_once()
        {
            var mapper = new ClrTypeMapper().Prepend(new UriResolver()).Prepend(new UriResolver());

            Assert.Equal(2, mapper.Resolvers.Count);
        }

        /// <summary>
        /// Answers an INTEGER with something that is not one.
        /// </summary>
        sealed class WrongResolver : IClrTypeResolver
        {

            readonly ClrTypeMappingCollection _mappings = new();

            public WrongResolver()
            {
                _mappings.Add(typeof(int), SqlTypeName.INTEGER, v => v.ToString(), v => v);
            }

            public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context) => _mappings.GetMapping(clrType, relType, context);

        }

        [Fact]
        public void A_mapping_that_answers_with_the_wrong_class_should_be_caught_at_the_boundary()
        {
            var registry = new ClrTypeMapper().Prepend(new WrongResolver()).Bind(_types);

            var e = Assert.Throws<ClrTypeMappingException>(() => registry.ToCalcite(null, Sql(SqlTypeName.INTEGER), 1));
            Assert.Contains("java.lang.Integer", e.Message);
        }

        // ------------------------------------------------------------------------------------
        // A type the schema carried through itself.
        // ------------------------------------------------------------------------------------

        public sealed class Carried
        {

            public override bool Equals(object? obj) => obj is Carried;

            public override int GetHashCode() => 1;

        }

        [Fact]
        public void A_column_typed_with_createJavaType_should_be_held_in_that_class()
        {
            // getJavaClass answers the class the JavaType carries, ahead of every SqlTypeName it knows, and
            // under IKVM that class can be a CLR one
            var relType = _types.createJavaType((java.lang.Class)typeof(Carried));

            Assert.Equal(typeof(Carried), ClrTypeMapping.RepresentationTypeOf(_types, relType));
            Assert.Equal(nameof(SqlTypeName.OTHER), relType.getSqlTypeName().name());
        }

        [Fact]
        public void A_column_typed_with_createJavaType_should_pass_its_value_through_untouched()
        {
            var relType = _types.createJavaType((java.lang.Class)typeof(Carried));
            var value = new Carried();

            Assert.Same(value, _registry.ToCalcite(null, relType, value));
            Assert.Same(value, _registry.FromCalcite(null, relType, value));
        }

    }

}
