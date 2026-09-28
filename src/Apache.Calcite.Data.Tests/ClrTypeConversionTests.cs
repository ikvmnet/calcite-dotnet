using System;
using System.Collections.Generic;

using Apache.Calcite.Data.Common;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Covers the conversions the default type mapper performs in both directions, and the collections that
    /// recurse through it.
    /// </summary>
    /// <remarks>
    /// Most tests assert a round trip, which fails if either direction is wrong. Where Calcite's storage form
    /// matters in its own right, such as a <c>DATE</c> held as an <c>Integer</c> count of days, the held value
    /// is checked separately.
    /// </remarks>
    public class ClrTypeConversionTests
    {

        static readonly org.apache.calcite.adapter.java.JavaTypeFactory Factory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

        static readonly ClrTypeRegistry Registry = new ClrTypeMapper().Bind(Factory);

        static RelDataType Type(SqlTypeName name, bool nullable = true)
        {
            return Factory.createTypeWithNullability(Factory.createSqlType(name), nullable);
        }

        static RelDataType ArrayOf(RelDataType element)
        {
            return Factory.createArrayType(element, -1);
        }

        /// <summary>
        /// Writes a value and reads it back, returning what was read.
        /// </summary>
        static object? RoundTrip(RelDataType type, object value, Type? clrType = null)
        {
            var held = Registry.ToCalcite(clrType, type, value);
            Assert.NotNull(held);

            return Registry.FromCalcite(clrType, type, held);
        }

        // ------------------------------------------------------------------------------------
        // The scalars, each the default in both directions.
        // ------------------------------------------------------------------------------------

        public static TheoryData<string, object> Scalars => new()
        {
            { nameof(SqlTypeName.BOOLEAN), true },
            { nameof(SqlTypeName.TINYINT), (sbyte)-8 },
            { nameof(SqlTypeName.SMALLINT), (short)-16 },
            { nameof(SqlTypeName.INTEGER), -32 },
            { nameof(SqlTypeName.BIGINT), -64L },
            { nameof(SqlTypeName.UTINYINT), (byte)8 },
            { nameof(SqlTypeName.USMALLINT), (ushort)16 },
            { nameof(SqlTypeName.UINTEGER), 32u },
            { nameof(SqlTypeName.UBIGINT), 64ul },
            { nameof(SqlTypeName.REAL), 1.5f },
            { nameof(SqlTypeName.DOUBLE), 2.5d },
            { nameof(SqlTypeName.DECIMAL), 12.34m },
            { nameof(SqlTypeName.VARCHAR), "hello" },
            { nameof(SqlTypeName.VARBINARY), new byte[] { 1, 2, 3 } },
            { nameof(SqlTypeName.TIMESTAMP), new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc) },
            { nameof(SqlTypeName.TIME), TimeSpan.FromMilliseconds(3661000) },
        };

        [Theory]
        [MemberData(nameof(Scalars))]
        public void Every_default_scalar_should_round_trip(string sqlTypeName, object value)
        {
            Assert.Equal(value, RoundTrip(Type(SqlTypeName.valueOf(sqlTypeName)), value));
        }

        [Theory]
        [MemberData(nameof(Scalars))]
        public void Every_default_scalar_should_name_the_clr_type_it_reads_back_as(string sqlTypeName, object value)
        {
            Assert.Equal(value.GetType(), Registry.GetClrType(Type(SqlTypeName.valueOf(sqlTypeName))));
        }

        /// <summary>
        /// <c>CHAR</c>, <c>FLOAT</c>, <c>BINARY</c> and <c>DATE</c> have no CLR type of their own and read back
        /// as the type of <c>VARCHAR</c>, <c>DOUBLE</c>, <c>VARBINARY</c> and <c>TIMESTAMP</c> respectively.
        /// </summary>
        [Fact]
        public void A_char_should_read_back_as_a_string()
        {
            Assert.Equal(typeof(string), Registry.GetClrType(Type(SqlTypeName.CHAR)));
            Assert.Equal("x", RoundTrip(Type(SqlTypeName.CHAR), "x"));
        }

        [Fact]
        public void A_float_should_read_back_as_a_double()
        {
            Assert.Equal(typeof(double), Registry.GetClrType(Type(SqlTypeName.FLOAT)));
            Assert.Equal(2.5d, RoundTrip(Type(SqlTypeName.FLOAT), 2.5d));
        }

        [Fact]
        public void A_binary_should_read_back_as_bytes()
        {
            Assert.Equal(typeof(byte[]), Registry.GetClrType(Type(SqlTypeName.BINARY)));
            Assert.Equal(new byte[] { 9 }, RoundTrip(Type(SqlTypeName.BINARY), new byte[] { 9 }));
        }

        [Fact]
        public void A_date_should_read_back_as_a_date_time()
        {
            Assert.Equal(typeof(DateTime), Registry.GetClrType(Type(SqlTypeName.DATE)));
            Assert.Equal(new DateTime(2020, 1, 2), RoundTrip(Type(SqlTypeName.DATE), new DateTime(2020, 1, 2)));
        }

        /// <summary>
        /// Calcite holds a <c>DATE</c> as an <c>Integer</c> count of days since the epoch and a
        /// <c>TIMESTAMP</c> as a <c>Long</c> count of milliseconds.
        /// </summary>
        [Fact]
        public void A_date_should_be_held_as_a_count_of_days()
        {
            var held = Registry.ToCalcite(null, Type(SqlTypeName.DATE), new DateTime(1970, 1, 11));

            Assert.Equal(java.lang.Integer.valueOf(10), held);
        }

        [Fact]
        public void A_timestamp_should_be_held_as_a_count_of_milliseconds()
        {
            var held = Registry.ToCalcite(null, Type(SqlTypeName.TIMESTAMP), new DateTime(1970, 1, 1, 0, 0, 1, DateTimeKind.Utc));

            Assert.Equal(java.lang.Long.valueOf(1000), held);
        }

        /// <summary>
        /// Both zoned timestamp types read back as a <see cref="DateTimeOffset"/>.
        /// </summary>
        [Theory]
        [InlineData(nameof(SqlTypeName.TIMESTAMP_TZ))]
        [InlineData(nameof(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE))]
        public void A_zoned_timestamp_should_round_trip_as_an_offset(string sqlTypeName)
        {
            var value = new DateTimeOffset(2020, 1, 2, 3, 4, 5, TimeSpan.Zero);
            var type = Type(SqlTypeName.valueOf(sqlTypeName));

            Assert.Equal(typeof(DateTimeOffset), Registry.GetClrType(type));
            Assert.Equal(value, RoundTrip(type, value));
        }

        /// <summary>
        /// A column of type <c>NULL</c> reads as null whatever value it is handed.
        /// </summary>
        [Fact]
        public void A_null_type_should_read_as_null()
        {
            Assert.Null(Registry.FromCalcite(null, Type(SqlTypeName.NULL), java.lang.Integer.valueOf(1)));
        }

        // ------------------------------------------------------------------------------------
        // What a bare CLR value is written as.
        // ------------------------------------------------------------------------------------

        [Theory]
        [InlineData(nameof(SqlTypeName.BOOLEAN), true)]
        [InlineData(nameof(SqlTypeName.INTEGER), 1)]
        [InlineData(nameof(SqlTypeName.BIGINT), 1L)]
        [InlineData(nameof(SqlTypeName.DOUBLE), 1.0d)]
        [InlineData(nameof(SqlTypeName.REAL), 1.0f)]
        [InlineData(nameof(SqlTypeName.VARCHAR), "x")]
        public void A_bare_value_should_be_written_as_the_type_it_pairs_with(string sqlTypeName, object value)
        {
            var mapping = Registry.RequireMapping(value.GetType(), null);

            Assert.Equal(SqlTypeName.valueOf(sqlTypeName), mapping.RelType.getSqlTypeName());
        }

        /// <summary>
        /// A bare <see cref="DateTime"/> is a <c>TIMESTAMP</c> and never a <c>DATE</c>, and a
        /// <see cref="DateOnly"/> is a <c>DATE</c>.
        /// </summary>
        [Fact]
        public void A_bare_date_time_should_be_written_as_a_timestamp()
        {
            Assert.Equal(SqlTypeName.TIMESTAMP, Registry.RequireMapping(typeof(DateTime), null).RelType.getSqlTypeName());
        }

        [Fact]
        public void A_bare_date_only_should_be_written_as_a_date()
        {
            Assert.Equal(SqlTypeName.DATE, Registry.RequireMapping(typeof(DateOnly), null).RelType.getSqlTypeName());
        }

        /// <summary>
        /// A <c>DATE</c> column reads back as a <see cref="DateOnly"/> only when that type is asked for.
        /// </summary>
        [Fact]
        public void A_date_should_read_back_as_a_date_only_only_when_asked()
        {
            Assert.NotEqual(typeof(DateOnly), Registry.GetClrType(Type(SqlTypeName.DATE)));
            Assert.Equal(new DateOnly(2020, 1, 2), RoundTrip(Type(SqlTypeName.DATE), new DateOnly(2020, 1, 2), typeof(DateOnly)));
        }

        /// <summary>
        /// A conversion that is not a default is available when both types are named: a <c>TIMESTAMP</c> can
        /// be read as a <see cref="DateOnly"/>.
        /// </summary>
        [Fact]
        public void A_named_conversion_should_be_legal_without_being_a_default()
        {
            Assert.NotNull(Registry.GetMapping(typeof(DateOnly), Type(SqlTypeName.TIMESTAMP)));
            Assert.NotEqual(typeof(DateOnly), Registry.GetClrType(Type(SqlTypeName.TIMESTAMP)));
        }

        /// <summary>
        /// Text is never read as a <see cref="Guid"/>, and a <see cref="Guid"/> is never written as text.
        /// Parsing text is a conversion and a typed getter is a cast, so a <c>CHAR</c> or <c>VARCHAR</c>
        /// column holding a GUID's text is a string.
        /// </summary>
        [Theory]
        [InlineData(nameof(SqlTypeName.CHAR))]
        [InlineData(nameof(SqlTypeName.VARCHAR))]
        public void A_character_column_should_never_be_read_as_a_guid(string sqlTypeName)
        {
            var type = Type(SqlTypeName.valueOf(sqlTypeName));

            Assert.Null(Registry.GetMapping(typeof(Guid), type));
            Assert.Throws<ClrTypeMappingException>(() => Registry.RequireMapping(typeof(Guid), type));
        }

        [Theory]
        [InlineData(nameof(SqlTypeName.CHAR))]
        [InlineData(nameof(SqlTypeName.VARCHAR))]
        public void A_guid_should_never_be_written_as_text(string sqlTypeName)
        {
            Assert.Null(Registry.GetMapping(typeof(Guid), Type(SqlTypeName.valueOf(sqlTypeName))));
        }

        /// <summary>
        /// A <c>UUID</c> is the one thing a <see cref="Guid"/> pairs with, in both directions.
        /// </summary>
        [Fact]
        public void A_guid_should_pair_with_uuid_and_nothing_else()
        {
            var value = Guid.NewGuid();

            Assert.Equal(SqlTypeName.UUID, Registry.RequireMapping(typeof(Guid), null).RelType.getSqlTypeName());
            Assert.Equal(typeof(Guid), Registry.GetClrType(Type(SqlTypeName.UUID)));
            Assert.Equal(value, RoundTrip(Type(SqlTypeName.UUID), value));
        }

        /// <summary>
        /// A character column holding a GUID's text reads as that text.
        /// </summary>
        [Fact]
        public void A_character_column_holding_guid_text_should_read_as_a_string()
        {
            var text = Guid.NewGuid().ToString();

            Assert.Equal(text, RoundTrip(Type(SqlTypeName.VARCHAR), text));
        }

        /// <summary>
        /// A pair no mapping covers is refused rather than converted: a <c>BIGINT</c> cannot be read as an
        /// <see cref="int"/>.
        /// </summary>
        [Fact]
        public void A_pair_the_table_does_not_carry_should_be_refused()
        {
            Assert.Null(Registry.GetMapping(typeof(int), Type(SqlTypeName.BIGINT)));
            Assert.Throws<ClrTypeMappingException>(() => Registry.RequireMapping(typeof(int), Type(SqlTypeName.BIGINT)));
        }

        // ------------------------------------------------------------------------------------
        // ANY, and types no mapping claims.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// <c>ANY</c> says nothing about what it holds, so its mapping reads each value by its runtime class.
        /// That applies to <c>ANY</c> only, not to every type without a mapping.
        /// </summary>
        [Fact]
        public void An_any_column_should_be_read_by_the_value_s_own_class()
        {
            var type = Type(SqlTypeName.ANY);

            Assert.NotNull(Registry.GetMapping(null, type));
            Assert.Equal(typeof(object), Registry.GetClrType(type));
            Assert.Equal(5, Registry.FromCalcite(null, type, java.lang.Integer.valueOf(5)));
            Assert.Equal("x", Registry.FromCalcite(null, type, "x"));
        }

        /// <summary>
        /// A collection in an <c>ANY</c> column is read the same way, element by element.
        /// </summary>
        [Fact]
        public void An_any_column_holding_a_collection_should_still_be_read()
        {
            var list = new java.util.ArrayList();
            list.add(java.lang.Integer.valueOf(1));
            list.add(java.lang.Integer.valueOf(2));

            Assert.Equal(new[] { 1, 2 }, Registry.FromCalcite(null, Type(SqlTypeName.ANY), list));
        }

        /// <summary>
        /// A type no mapping claims has no mapping, and reading or writing it throws. <c>CURSOR</c> stands in
        /// for the case; in practice it is usually a <c>RelDataType</c> from a schema that names no
        /// <c>SqlTypeName</c>.
        /// </summary>
        [Fact]
        public void An_unclaimed_type_should_have_no_mapping()
        {
            var type = Type(SqlTypeName.CURSOR);

            Assert.Null(Registry.GetMapping(null, type));
            Assert.Throws<ClrTypeMappingException>(() => Registry.RequireMapping(null, type));
            Assert.Throws<ClrTypeMappingException>(() => Registry.FromCalcite(null, type, java.lang.Integer.valueOf(1)));
        }

        /// <summary>
        /// A caller supplies a mapping for an unclaimed type by prepending a resolver.
        /// </summary>
        [Fact]
        public void A_caller_should_be_able_to_claim_an_unclaimed_type()
        {
            var registry = new ClrTypeMapper().Prepend(new OtherResolver()).Bind(Factory);
            var type = Type(SqlTypeName.CURSOR);

            Assert.Equal(typeof(string), registry.GetClrType(type));
            Assert.Equal("1", registry.FromCalcite(null, type, java.lang.Integer.valueOf(1)));
        }

        sealed class OtherResolver : IClrTypeResolver
        {

            public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context)
            {
                if (relType is not null && relType.getSqlTypeName() == SqlTypeName.CURSOR && (clrType is null || clrType == typeof(string)))
                    return new DelegateClrTypeMapping(context, relType, typeof(string), v => v, v => v.ToString()!);

                return null;
            }

        }

        // ------------------------------------------------------------------------------------
        // Calcite types with no DbType counterpart, and CLR types with no Calcite counterpart.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// A year-month interval is a count of months whichever of the three it is, so an
        /// <c>INTERVAL YEAR</c> of two years is twenty-four. .NET has no interval that counts months, a
        /// <see cref="TimeSpan"/> being a fixed number of ticks, so the count is the value.
        /// </summary>
        [Theory]
        [InlineData(nameof(SqlTypeName.INTERVAL_YEAR))]
        [InlineData(nameof(SqlTypeName.INTERVAL_YEAR_MONTH))]
        [InlineData(nameof(SqlTypeName.INTERVAL_MONTH))]
        public void A_year_month_interval_should_read_back_as_a_count_of_months(string sqlTypeName)
        {
            var type = Type(SqlTypeName.valueOf(sqlTypeName));

            Assert.Equal(typeof(int), Registry.GetClrType(type));
            Assert.Equal(24, RoundTrip(type, 24));
            Assert.Equal(java.lang.Integer.valueOf(24), Registry.ToCalcite(null, type, 24));
        }

        /// <summary>
        /// A day-time interval is a fixed length of time and so is a <see cref="TimeSpan"/>, so the two
        /// correspond exactly.
        /// </summary>
        [Theory]
        [InlineData(nameof(SqlTypeName.INTERVAL_DAY))]
        [InlineData(nameof(SqlTypeName.INTERVAL_DAY_SECOND))]
        [InlineData(nameof(SqlTypeName.INTERVAL_HOUR))]
        [InlineData(nameof(SqlTypeName.INTERVAL_MINUTE))]
        [InlineData(nameof(SqlTypeName.INTERVAL_SECOND))]
        public void A_day_time_interval_should_read_back_as_a_time_span(string sqlTypeName)
        {
            var type = Type(SqlTypeName.valueOf(sqlTypeName));
            var value = TimeSpan.FromSeconds(90);

            Assert.Equal(typeof(TimeSpan), Registry.GetClrType(type));
            Assert.Equal(value, RoundTrip(type, value));
            Assert.Equal(java.lang.Long.valueOf(90000), Registry.ToCalcite(null, type, value));
        }

        /// <summary>
        /// A <c>GEOMETRY</c> is well-known text here, which is how Calcite's own JDBC presents one: there is
        /// no .NET geometry this package can hand out and the JTS one is a Java object.
        /// </summary>
        [Fact]
        public void A_geometry_should_read_back_as_well_known_text()
        {
            var type = Type(SqlTypeName.GEOMETRY);

            Assert.Equal(typeof(string), Registry.GetClrType(type));
            Assert.Equal("POINT (1.5 2.5)", RoundTrip(type, "POINT (1.5 2.5)"));
        }

        [Fact]
        public void A_geometry_should_be_held_as_a_jts_geometry()
        {
            var held = Registry.ToCalcite(null, Type(SqlTypeName.GEOMETRY), "POINT (1.5 2.5)");

            Assert.IsAssignableFrom<org.locationtech.jts.geom.Geometry>(held);
        }

        /// <summary>
        /// A <c>CHAR</c> is a string in Calcite's runtime, so a bare <see cref="char"/> is written as a string
        /// of one character.
        /// </summary>
        [Fact]
        public void A_bare_char_should_be_written_as_a_one_character_string()
        {
            var mapping = Registry.RequireMapping(typeof(char), null);

            Assert.Equal(SqlTypeName.CHAR, mapping.RelType.getSqlTypeName());
            Assert.Equal("x", Registry.ToCalcite(typeof(char), null, 'x'));
        }

        /// <summary>
        /// Calcite has no unbounded integer type, so a <see cref="System.Numerics.BigInteger"/> is written as a
        /// <c>DECIMAL</c>.
        /// </summary>
        [Fact]
        public void A_bare_big_integer_should_be_written_as_a_decimal()
        {
            var value = System.Numerics.BigInteger.Parse("123456789012345678901234567890");
            var mapping = Registry.RequireMapping(typeof(System.Numerics.BigInteger), null);

            Assert.Equal(SqlTypeName.DECIMAL, mapping.RelType.getSqlTypeName());
            Assert.Equal(value, RoundTrip(mapping.RelType, value, typeof(System.Numerics.BigInteger)));
        }

        /// <summary>
        /// A bare collection's Calcite type is built from its element type's mapping, recursively.
        /// </summary>
        [Fact]
        public void A_bare_array_should_be_written_as_an_array_of_its_element()
        {
            var mapping = Registry.RequireMapping(typeof(int[]), null);

            Assert.Equal(SqlTypeName.ARRAY, mapping.RelType.getSqlTypeName());
            Assert.Equal(SqlTypeName.INTEGER, mapping.RelType.getComponentType().getSqlTypeName());
        }

        [Fact]
        public void A_bare_nested_array_should_be_written_at_every_level()
        {
            var mapping = Registry.RequireMapping(typeof(int[][]), null);
            var type = mapping.RelType;

            Assert.Equal(SqlTypeName.ARRAY, type.getSqlTypeName());
            Assert.Equal(SqlTypeName.ARRAY, type.getComponentType().getSqlTypeName());
            Assert.Equal(SqlTypeName.INTEGER, type.getComponentType().getComponentType().getSqlTypeName());
        }

        [Fact]
        public void A_bare_dictionary_should_be_written_as_a_map()
        {
            var mapping = Registry.RequireMapping(typeof(System.Collections.Generic.Dictionary<string, int>), null);

            Assert.Equal(SqlTypeName.MAP, mapping.RelType.getSqlTypeName());
            Assert.Equal(SqlTypeName.VARCHAR, mapping.RelType.getKeyType().getSqlTypeName());
            Assert.Equal(SqlTypeName.INTEGER, mapping.RelType.getValueType().getSqlTypeName());
        }

        /// <summary>
        /// A <see cref="T:byte[]"/> is an array in .NET and a <c>VARBINARY</c> in SQL, so it is deliberately
        /// not treated as a collection of bytes.
        /// </summary>
        [Fact]
        public void A_bare_byte_array_should_stay_a_binary()
        {
            Assert.Equal(SqlTypeName.VARBINARY, Registry.RequireMapping(typeof(byte[]), null).RelType.getSqlTypeName());
        }

        // ------------------------------------------------------------------------------------
        // Collections, which recurse through the same registry.
        // ------------------------------------------------------------------------------------

        [Fact]
        public void An_array_should_read_back_as_an_array_of_its_element()
        {
            var type = ArrayOf(Type(SqlTypeName.INTEGER, nullable: false));

            Assert.Equal(typeof(int[]), Registry.GetClrType(type));
            Assert.Equal(new[] { 1, 2, 3 }, RoundTrip(type, new[] { 1, 2, 3 }));
        }

        [Fact]
        public void A_multiset_should_read_back_the_same_way_an_array_does()
        {
            var type = Factory.createMultisetType(Type(SqlTypeName.INTEGER, nullable: false), -1);

            Assert.Equal(typeof(int[]), Registry.GetClrType(type));
            Assert.Equal(new[] { 1, 2 }, RoundTrip(type, new[] { 1, 2 }));
        }

        /// <summary>
        /// A nullable element type gives an array of <see cref="Nullable{T}"/>, since an array of a value type
        /// cannot hold a null otherwise.
        /// </summary>
        [Fact]
        public void An_array_of_a_nullable_element_should_be_an_array_of_nullable()
        {
            var type = ArrayOf(Type(SqlTypeName.INTEGER, nullable: true));

            Assert.Equal(typeof(int?[]), Registry.GetClrType(type));
            Assert.Equal(new int?[] { 1, null, 3 }, RoundTrip(type, new int?[] { 1, null, 3 }));
        }

        /// <summary>
        /// An empty array, and one holding only nulls, keep their element type, because the element type comes
        /// from the declared type rather than from the values.
        /// </summary>
        [Fact]
        public void An_empty_array_should_keep_its_element_type()
        {
            var type = ArrayOf(Type(SqlTypeName.INTEGER, nullable: false));

            Assert.Equal(Array.Empty<int>(), RoundTrip(type, Array.Empty<int>()));
        }

        [Fact]
        public void An_array_of_nothing_but_nulls_should_keep_its_element_type()
        {
            var type = ArrayOf(Type(SqlTypeName.INTEGER, nullable: true));

            Assert.Equal(new int?[] { null, null }, RoundTrip(type, new int?[] { null, null }));
        }

        /// <summary>
        /// An array of arrays is the element's mapping wrapped twice; no mapping names <c>int[][]</c>
        /// directly.
        /// </summary>
        [Fact]
        public void A_nested_array_should_read_back_as_a_nested_array()
        {
            var type = ArrayOf(ArrayOf(Type(SqlTypeName.INTEGER, nullable: false)));

            Assert.Equal(typeof(int[][]), Registry.GetClrType(type));

            var value = new[] { new[] { 1, 2 }, new[] { 3 } };
            var back = Assert.IsType<int[][]>(RoundTrip(type, value));

            Assert.Equal(new[] { 1, 2 }, back[0]);
            Assert.Equal(new[] { 3 }, back[1]);
        }

        [Fact]
        public void A_thrice_nested_array_should_read_back_at_three_levels()
        {
            var type = ArrayOf(ArrayOf(ArrayOf(Type(SqlTypeName.INTEGER, nullable: false))));

            Assert.Equal(typeof(int[][][]), Registry.GetClrType(type));

            var value = new[] { new[] { new[] { 7 } } };
            var back = Assert.IsType<int[][][]>(RoundTrip(type, value));

            Assert.Equal(7, back[0][0][0]);
        }

        [Fact]
        public void An_array_of_strings_should_read_back_as_strings()
        {
            var type = ArrayOf(Type(SqlTypeName.VARCHAR, nullable: false));

            Assert.Equal(typeof(string[]), Registry.GetClrType(type));
            Assert.Equal(new[] { "a", "b" }, RoundTrip(type, new[] { "a", "b" }));
        }

        /// <summary>
        /// A <c>DATE</c> inside an array is still a count of days, and only the element type says so.
        /// </summary>
        [Fact]
        public void An_array_of_dates_should_read_back_as_dates()
        {
            var type = ArrayOf(Type(SqlTypeName.DATE, nullable: false));

            Assert.Equal(typeof(DateTime[]), Registry.GetClrType(type));
            Assert.Equal(new[] { new DateTime(2020, 1, 2) }, RoundTrip(type, new[] { new DateTime(2020, 1, 2) }));
        }

        // ------------------------------------------------------------------------------------
        // Maps and rows.
        // ------------------------------------------------------------------------------------

        [Fact]
        public void A_map_should_read_back_as_a_dictionary()
        {
            var type = Factory.createMapType(Type(SqlTypeName.VARCHAR, nullable: false), Type(SqlTypeName.INTEGER, nullable: false));

            Assert.Equal(typeof(Dictionary<string, int>), Registry.GetClrType(type));

            var back = Assert.IsType<Dictionary<string, int>>(RoundTrip(type, new Dictionary<string, int> { ["a"] = 1 }));
            Assert.Equal(1, back["a"]);
        }

        /// <summary>
        /// A <see cref="Dictionary{TKey, TValue}"/> cannot hold a null key, so a map whose declared key type is
        /// nullable reads as an array of pairs, for every row whether or not its keys are null.
        /// </summary>
        [Fact]
        public void A_map_with_a_nullable_key_should_read_back_as_pairs()
        {
            var type = Factory.createMapType(Type(SqlTypeName.VARCHAR, nullable: true), Type(SqlTypeName.INTEGER, nullable: false));

            Assert.Equal(typeof(KeyValuePair<string, int>[]), Registry.GetClrType(type));
        }

        [Fact]
        public void A_map_of_arrays_should_recurse_through_its_value()
        {
            var type = Factory.createMapType(Type(SqlTypeName.VARCHAR, nullable: false), ArrayOf(Type(SqlTypeName.INTEGER, nullable: false)));

            Assert.Equal(typeof(Dictionary<string, int[]>), Registry.GetClrType(type));

            var back = Assert.IsType<Dictionary<string, int[]>>(RoundTrip(type, new Dictionary<string, int[]> { ["a"] = [1, 2] }));
            Assert.Equal(new[] { 1, 2 }, back["a"]);
        }

        [Fact]
        public void A_row_should_read_back_as_an_object_array()
        {
            var type = Factory.builder()
                .add("A", Type(SqlTypeName.INTEGER, nullable: false))
                .add("B", Type(SqlTypeName.VARCHAR, nullable: false))
                .build();

            Assert.Equal(typeof(object[]), Registry.GetClrType(type));
            Assert.Equal(new object[] { 1, "x" }, RoundTrip(type, new object[] { 1, "x" }));
        }

        [Fact]
        public void An_array_of_rows_should_recurse_through_its_element()
        {
            var row = Factory.builder().add("A", Type(SqlTypeName.INTEGER, nullable: false)).build();
            var type = ArrayOf(row);

            Assert.Equal(typeof(object[][]), Registry.GetClrType(type));

            var back = Assert.IsType<object[][]>(RoundTrip(type, new[] { new object[] { 5 } }));
            Assert.Equal(5, back[0][0]);
        }

        /// <summary>
        /// <c>RepresentationType</c> is the class Calcite's runtime holds the value in, which for most types
        /// is a Java class; <c>ClrType</c> is the .NET type a caller is handed. They coincide only where
        /// Calcite's runtime holds a .NET type.
        /// </summary>
        [Fact]
        public void The_representation_should_not_be_the_clr_type()
        {
            var mapping = Registry.RequireMapping(null, Type(SqlTypeName.INTEGER));

            Assert.Equal(typeof(int), mapping.ClrType);
            Assert.Equal(typeof(java.lang.Integer), mapping.RepresentationType);
            Assert.NotEqual(mapping.ClrType, mapping.RepresentationType);
        }

        /// <summary>
        /// A written value is an instance of the mapping's <c>RepresentationType</c>, which a mapping's first
        /// conversion checks.
        /// </summary>
        [Fact]
        public void A_written_value_should_be_of_the_representation_type()
        {
            var mapping = Registry.RequireMapping(null, Type(SqlTypeName.INTEGER));
            var held = Registry.ToCalcite(null, Type(SqlTypeName.INTEGER), 5);

            Assert.NotNull(held);
            Assert.IsType<java.lang.Integer>(held);
            Assert.True(mapping.RepresentationType.IsInstanceOfType(held));
        }

        /// <summary>
        /// A mapping that returns the wrong class is refused when it converts, rather than failing later
        /// inside a plan.
        /// </summary>
        [Fact]
        public void A_mapping_that_answers_with_the_wrong_class_should_be_refused()
        {
            var registry = new ClrTypeMapper().Prepend(new WrongRepresentationResolver()).Bind(Factory);

            Assert.Throws<ClrTypeMappingException>(() => registry.ToCalcite(null, Type(SqlTypeName.INTEGER), 5));
        }

        sealed class WrongRepresentationResolver : IClrTypeResolver
        {

            public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context)
            {
                if (relType is not null && relType.getSqlTypeName() == SqlTypeName.INTEGER && (clrType is null || clrType == typeof(int)))
                    // a CLR int where an INTEGER is held in a java.lang.Integer
                    return new DelegateClrTypeMapping(context, relType, typeof(int), v => v, v => v);

                return null;
            }

        }

        // ------------------------------------------------------------------------------------
        // The DbType and CalciteDbType a mapping infers.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// A mapping infers its <see cref="System.Data.DbType"/> from its .NET type and its
        /// <see cref="CalciteDbType"/> from its Calcite type.
        /// </summary>
        [Theory]
        [InlineData(nameof(SqlTypeName.INTEGER), System.Data.DbType.Int32, CalciteDbType.Integer)]
        [InlineData(nameof(SqlTypeName.BIGINT), System.Data.DbType.Int64, CalciteDbType.BigInt)]
        [InlineData(nameof(SqlTypeName.UINTEGER), System.Data.DbType.UInt32, CalciteDbType.UInteger)]
        [InlineData(nameof(SqlTypeName.VARCHAR), System.Data.DbType.String, CalciteDbType.VarChar)]
        [InlineData(nameof(SqlTypeName.UUID), System.Data.DbType.Guid, CalciteDbType.Uuid)]
        public void A_mapping_should_infer_both_names(string sqlTypeName, System.Data.DbType dbType, CalciteDbType calciteDbType)
        {
            var mapping = Registry.RequireMapping(null, Type(SqlTypeName.valueOf(sqlTypeName)));

            Assert.Equal(dbType, mapping.DbType);
            Assert.Equal(calciteDbType, mapping.CalciteDbType);
        }

        /// <summary>
        /// A <c>DATE</c> and a <c>TIMESTAMP</c> share a <see cref="System.Data.DbType"/> because both read
        /// back as a <see cref="DateTime"/>, and have different <see cref="CalciteDbType"/> values because
        /// they are different Calcite types.
        /// </summary>
        [Fact]
        public void Two_calcite_types_read_as_one_clr_type_should_share_a_db_type_and_not_a_calcite_one()
        {
            var date = Registry.RequireMapping(null, Type(SqlTypeName.DATE));
            var timestamp = Registry.RequireMapping(null, Type(SqlTypeName.TIMESTAMP));

            Assert.Equal(date.ClrType, timestamp.ClrType);
            Assert.Equal(date.DbType, timestamp.DbType);
            Assert.NotEqual(date.CalciteDbType, timestamp.CalciteDbType);
        }

        /// <summary>
        /// A collection is <see cref="System.Data.DbType.Object"/>, since <see cref="System.Data.DbType"/> has
        /// no collection member, and its <see cref="CalciteDbType"/> combines the collection and element flags.
        /// </summary>
        [Fact]
        public void A_collection_mapping_should_infer_both_names()
        {
            var mapping = Registry.RequireMapping(null, ArrayOf(Type(SqlTypeName.INTEGER, nullable: false)));

            Assert.Equal(System.Data.DbType.Object, mapping.DbType);
            Assert.Equal(CalciteDbType.Array | CalciteDbType.Integer, mapping.CalciteDbType);
        }

        /// <summary>
        /// A mapping from a caller's resolver infers both without declaring either.
        /// </summary>
        [Fact]
        public void A_caller_mapping_should_infer_both_names()
        {
            var registry = new ClrTypeMapper().Prepend(new UpperCaseResolver()).Bind(Factory);
            var mapping = registry.RequireMapping(null, Type(SqlTypeName.VARCHAR));

            Assert.Equal(System.Data.DbType.String, mapping.DbType);
            Assert.Equal(CalciteDbType.VarChar, mapping.CalciteDbType);
        }

        /// <summary>
        /// A collection's mapping exposes its element's mapping, so a caller can walk the tree of mappings.
        /// </summary>
        [Fact]
        public void A_collection_mapping_should_expose_the_mapping_it_wraps()
        {
            var type = ArrayOf(ArrayOf(Type(SqlTypeName.INTEGER, nullable: false)));
            var outer = Assert.IsType<CollectionClrTypeMapping>(Registry.RequireMapping(null, type));
            var inner = Assert.IsType<CollectionClrTypeMapping>(outer.ElementMapping);

            Assert.Equal(typeof(int), inner.ElementMapping.ClrType);
            Assert.Equal(SqlTypeName.INTEGER, inner.ElementMapping.RelType.getSqlTypeName());
        }

        /// <summary>
        /// A caller's resolver applies to a collection's elements, because the element mapping is resolved
        /// through the registry.
        /// </summary>
        [Fact]
        public void A_caller_mapping_should_be_reached_through_a_collection()
        {
            var registry = new ClrTypeMapper().Prepend(new UpperCaseResolver()).Bind(Factory);
            var type = ArrayOf(Type(SqlTypeName.VARCHAR, nullable: false));

            var held = registry.ToCalcite(null, type, new[] { "a", "b" });

            Assert.Equal(new[] { "A", "B" }, registry.FromCalcite(null, type, held));
        }

        sealed class UpperCaseResolver : IClrTypeResolver
        {

            public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context)
            {
                if (relType is not null && relType.getSqlTypeName() == SqlTypeName.VARCHAR && (clrType is null || clrType == typeof(string)))
                    return new DelegateClrTypeMapping(context, relType, typeof(string), v => (string)v, v => ((string)v).ToUpperInvariant());

                return null;
            }

        }

    }

}
