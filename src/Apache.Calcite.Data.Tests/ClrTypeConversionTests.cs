using System;
using System.Collections.Generic;

using Apache.Calcite.Data.Common;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Covers every conversion the built-in chain performs without anyone registering anything, in both
    /// directions, and the collections that recurse through it.
    /// </summary>
    /// <remarks>
    /// A round trip is the assertion worth making. It fails if either half is wrong and, unlike checking a
    /// converted value against a literal, it cannot be satisfied by two mistakes that cancel. The
    /// representation is checked separately where the storage form is the interesting fact — a <c>DATE</c>
    /// is a count of days in an <c>Integer</c>, and nothing about a <see cref="DateTime"/> says so.
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
        /// Writes a value and reads it back, answering what came back.
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
        /// The types whose .NET value is not what the pairing above would suggest, because the CLR type they
        /// pair with is already spoken for.
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
        /// The storage form, which is the fact the .NET value cannot carry: a <c>DATE</c> is a count of days
        /// and a <c>TIMESTAMP</c> a count of milliseconds, both integers.
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
        /// The zoned temporal types, which all read back as an offset.
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
        /// A <c>NULL</c> column is null whatever a provider handed over, which is the whole of that type.
        /// </summary>
        [Fact]
        public void A_null_type_should_read_as_null()
        {
            Assert.Null(Registry.FromCalcite(null, Type(SqlTypeName.NULL), java.lang.Integer.valueOf(1)));
        }

        // ------------------------------------------------------------------------------------
        // What a bare CLR value is written as, which is the other direction of the same table.
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
        /// <see cref="DateOnly"/> is the reverse. The two directions are separate facts, which is what the
        /// match flags are for.
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
        /// And a <c>DATE</c> column is never read back as a <see cref="DateOnly"/> unless asked, which is
        /// the same fact from the other side.
        /// </summary>
        [Fact]
        public void A_date_should_read_back_as_a_date_only_only_when_asked()
        {
            Assert.NotEqual(typeof(DateOnly), Registry.GetClrType(Type(SqlTypeName.DATE)));
            Assert.Equal(new DateOnly(2020, 1, 2), RoundTrip(Type(SqlTypeName.DATE), new DateOnly(2020, 1, 2), typeof(DateOnly)));
        }

        /// <summary>
        /// A conversion nobody defaults to is still legal when both types are named. This is the
        /// "can a caller read a <c>TIMESTAMP</c> as a <see cref="DateOnly"/>" question, answered yes.
        /// </summary>
        [Fact]
        public void A_named_conversion_should_be_legal_without_being_a_default()
        {
            Assert.NotNull(Registry.GetMapping(typeof(DateOnly), Type(SqlTypeName.TIMESTAMP)));
            Assert.NotEqual(typeof(DateOnly), Registry.GetClrType(Type(SqlTypeName.TIMESTAMP)));
        }

        /// <summary>
        /// <b>Text is never read as a <see cref="Guid"/>, and a <see cref="Guid"/> is never written as
        /// text.</b> Parsing text into a <see cref="Guid"/> is a conversion and a typed getter is a cast,
        /// so a <c>CHAR</c> or <c>VARCHAR</c> column holding something that looks like one is still a
        /// string. The pairing existed only because Calcite had no <c>UUID</c> type before 1.43; the ADO
        /// adapter typed a provider <c>uniqueidentifier</c> as <c>CHAR(36)</c> for the same reason and
        /// stopped.
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
        /// And a character column still reads as a string, so refusing the Guid costs nothing else.
        /// </summary>
        [Fact]
        public void A_character_column_holding_guid_text_should_read_as_a_string()
        {
            var text = Guid.NewGuid().ToString();

            Assert.Equal(text, RoundTrip(Type(SqlTypeName.VARCHAR), text));
        }

        /// <summary>
        /// And a pair nothing in the table carries is refused rather than guessed at. This is the
        /// "can a caller read a <c>BIGINT</c> as an <see cref="int"/>" question, answered no.
        /// </summary>
        [Fact]
        public void A_pair_the_table_does_not_carry_should_be_refused()
        {
            Assert.Null(Registry.GetMapping(typeof(int), Type(SqlTypeName.BIGINT)));
            Assert.Throws<ClrTypeMappingException>(() => Registry.RequireMapping(typeof(int), Type(SqlTypeName.BIGINT)));
        }

        // ------------------------------------------------------------------------------------
        // What the table does not claim, which is refused rather than guessed at.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// <c>ANY</c> says nothing about what it holds, so its mapping is a reading of the value's own
        /// class. That is an entry for <c>ANY</c> and not a rule about every type nobody claimed.
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
        /// And it descends, because what an <c>ANY</c> holds has no declared type either.
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
        /// <b>A type the table does not claim has no mapping.</b> The <c>ANY</c> entry used to claim every
        /// type nobody else had, so anything unmapped was read by guessing at the value's runtime class. It
        /// says so instead. <c>CURSOR</c> stands in for the case here because it is reachable and named;
        /// the one that matters in practice is a <c>RelDataType</c> a schema supplied that names no
        /// <c>SqlTypeName</c> at all.
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
        /// And a caller that wants one read says so, which is what the chain is for.
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
        // The types ADO.NET's list has no name for, which are most of what Calcite has beyond the shared
        // ones and were all falling to the catch-all.
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
        /// A <c>CHAR</c> is a string in Calcite's runtime, so a bare <see cref="char"/> is a string of one.
        /// Without this a CLR <see cref="char"/> reached a plan unconverted.
        /// </summary>
        [Fact]
        public void A_bare_char_should_be_written_as_a_one_character_string()
        {
            var mapping = Registry.RequireMapping(typeof(char), null);

            Assert.Equal(SqlTypeName.CHAR, mapping.RelType.getSqlTypeName());
            Assert.Equal("x", Registry.ToCalcite(typeof(char), null, 'x'));
        }

        /// <summary>
        /// Calcite has no unbounded integer type, and a <c>DECIMAL</c> is what an integer of any width is.
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
        /// A bare collection names no Calcite type and the one it wants is built from its element's, which
        /// is why this recurses the same way reading does.
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
        /// The element's nullability is the array's element type, an array having no other way to hold a
        /// null.
        /// </summary>
        [Fact]
        public void An_array_of_a_nullable_element_should_be_an_array_of_nullable()
        {
            var type = ArrayOf(Type(SqlTypeName.INTEGER, nullable: true));

            Assert.Equal(typeof(int?[]), Registry.GetClrType(type));
            Assert.Equal(new int?[] { 1, null, 3 }, RoundTrip(type, new int?[] { 1, null, 3 }));
        }

        /// <summary>
        /// An empty collection and one holding nothing but nulls still know what they are, because the type
        /// says so and nothing is measured from the values.
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
        /// The recursion, which is the whole point: an array of arrays is the element's mapping wrapped
        /// twice, and no entry anywhere names <c>int[][]</c>.
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
        /// A map whose key type admits a null cannot be a dictionary at all, so the declared type decides
        /// the shape and it decides it the same way for every row.
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
        /// <c>RepresentationType</c> is not <c>ClrType</c>. One is the class the value is actually held in
        /// between the plan and here, which for nearly every Calcite type is a Java class reached through
        /// IKVM; the other is the .NET type a caller is handed. They coincide only where Calcite's runtime
        /// already holds a .NET type, which is why both exist and why the registry checks one against the
        /// other.
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
        /// And the representation is what the value actually is on the way in, which is the thing the
        /// check on a mapping's first conversion enforces.
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
        /// A mapping that answers with the wrong class fails at the boundary rather than inside a plan
        /// several frames away, which is what that check is for.
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
        // What a mapping says about itself, which is where both fixed lists are inferred from.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// Every mapping infers both names, and from different halves of itself: the ADO.NET one from the
        /// .NET type it presents, the Calcite one from the Calcite type it is for. Neither is stated by the
        /// table, so a mapping a caller registers gets both without doing anything.
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
        /// The two names are inferred from different halves, which shows where one is exact and the other
        /// approximates. A <c>DATE</c> and a <c>TIMESTAMP</c> are one <see cref="System.Data.DbType"/>
        /// because both are read back as a <see cref="DateTime"/>, and two Calcite names because they are
        /// two types.
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
        /// A collection infers both too, and the ADO.NET list has nothing for one.
        /// </summary>
        [Fact]
        public void A_collection_mapping_should_infer_both_names()
        {
            var mapping = Registry.RequireMapping(null, ArrayOf(Type(SqlTypeName.INTEGER, nullable: false)));

            Assert.Equal(System.Data.DbType.Object, mapping.DbType);
            Assert.Equal(CalciteDbType.Array | CalciteDbType.Integer, mapping.CalciteDbType);
        }

        /// <summary>
        /// A mapping a caller registered for a type of its own infers both without stating either, which is
        /// the point of deriving rather than declaring them.
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
        /// A collection's mapping holds its element's, which is what makes the tree walkable by a caller
        /// doing introspection rather than conversion.
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
        /// A caller's own resolver reaches inside a collection without the collection knowing, because the
        /// element is resolved through the registry rather than by a table here.
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
