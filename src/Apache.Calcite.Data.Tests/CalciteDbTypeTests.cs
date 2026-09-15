using System;
using System.Data;

using Apache.Calcite.Data.Common;

using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Covers the three ways a type can be named and what is kept and lost carrying a name between them.
    /// </summary>
    /// <remarks>
    /// Every one of these is a claim about a lossy mapping, which is the kind of table that rots silently:
    /// a member added to <see cref="SqlTypeName"/> upstream, or a case quietly dropped here, changes an
    /// answer with nothing to notice it. The round trips are the part worth having — a name that survives
    /// the trip out and back is exactly stated, and one that does not is named here as approximate.
    /// </remarks>
    public class CalciteDbTypeTests
    {

        // ------------------------------------------------------------------------------------
        // SqlTypeName to CalciteDbType and back. Every scalar Calcite has a name for.
        // ------------------------------------------------------------------------------------

        public static TheoryData<string, CalciteDbType> Scalars => new()
        {
            { nameof(SqlTypeName.BOOLEAN), CalciteDbType.Boolean },
            { nameof(SqlTypeName.TINYINT), CalciteDbType.TinyInt },
            { nameof(SqlTypeName.SMALLINT), CalciteDbType.SmallInt },
            { nameof(SqlTypeName.INTEGER), CalciteDbType.Integer },
            { nameof(SqlTypeName.BIGINT), CalciteDbType.BigInt },
            { nameof(SqlTypeName.UTINYINT), CalciteDbType.UTinyInt },
            { nameof(SqlTypeName.USMALLINT), CalciteDbType.USmallInt },
            { nameof(SqlTypeName.UINTEGER), CalciteDbType.UInteger },
            { nameof(SqlTypeName.UBIGINT), CalciteDbType.UBigInt },
            { nameof(SqlTypeName.DECIMAL), CalciteDbType.Decimal },
            { nameof(SqlTypeName.REAL), CalciteDbType.Real },
            { nameof(SqlTypeName.DOUBLE), CalciteDbType.Double },
            { nameof(SqlTypeName.FLOAT), CalciteDbType.Float },
            { nameof(SqlTypeName.CHAR), CalciteDbType.Char },
            { nameof(SqlTypeName.VARCHAR), CalciteDbType.VarChar },
            { nameof(SqlTypeName.BINARY), CalciteDbType.Binary },
            { nameof(SqlTypeName.VARBINARY), CalciteDbType.VarBinary },
            { nameof(SqlTypeName.DATE), CalciteDbType.Date },
            { nameof(SqlTypeName.TIME), CalciteDbType.Time },
            { nameof(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE), CalciteDbType.TimeWithLocalTimeZone },
            { nameof(SqlTypeName.TIME_TZ), CalciteDbType.TimeTz },
            { nameof(SqlTypeName.TIMESTAMP), CalciteDbType.Timestamp },
            { nameof(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE), CalciteDbType.TimestampWithLocalTimeZone },
            { nameof(SqlTypeName.TIMESTAMP_TZ), CalciteDbType.TimestampTz },
            { nameof(SqlTypeName.INTERVAL_YEAR), CalciteDbType.IntervalYear },
            { nameof(SqlTypeName.INTERVAL_YEAR_MONTH), CalciteDbType.IntervalYearMonth },
            { nameof(SqlTypeName.INTERVAL_MONTH), CalciteDbType.IntervalMonth },
            { nameof(SqlTypeName.INTERVAL_DAY), CalciteDbType.IntervalDay },
            { nameof(SqlTypeName.INTERVAL_DAY_HOUR), CalciteDbType.IntervalDayHour },
            { nameof(SqlTypeName.INTERVAL_DAY_MINUTE), CalciteDbType.IntervalDayMinute },
            { nameof(SqlTypeName.INTERVAL_DAY_SECOND), CalciteDbType.IntervalDaySecond },
            { nameof(SqlTypeName.INTERVAL_HOUR), CalciteDbType.IntervalHour },
            { nameof(SqlTypeName.INTERVAL_HOUR_MINUTE), CalciteDbType.IntervalHourMinute },
            { nameof(SqlTypeName.INTERVAL_HOUR_SECOND), CalciteDbType.IntervalHourSecond },
            { nameof(SqlTypeName.INTERVAL_MINUTE), CalciteDbType.IntervalMinute },
            { nameof(SqlTypeName.INTERVAL_MINUTE_SECOND), CalciteDbType.IntervalMinuteSecond },
            { nameof(SqlTypeName.INTERVAL_SECOND), CalciteDbType.IntervalSecond },
            { nameof(SqlTypeName.UUID), CalciteDbType.Uuid },
            { nameof(SqlTypeName.GEOMETRY), CalciteDbType.Geometry },
            { nameof(SqlTypeName.VARIANT), CalciteDbType.Variant },
            { nameof(SqlTypeName.MEASURE), CalciteDbType.Measure },
            { nameof(SqlTypeName.ROW), CalciteDbType.Row },
            { nameof(SqlTypeName.ANY), CalciteDbType.Any },
            { nameof(SqlTypeName.NULL), CalciteDbType.Null },
            { nameof(SqlTypeName.OTHER), CalciteDbType.Other },
        };

        [Theory]
        [MemberData(nameof(Scalars))]
        public void Every_named_scalar_should_round_trip(string sqlTypeName, CalciteDbType expected)
        {
            var name = SqlTypeName.valueOf(sqlTypeName);

            Assert.Equal(expected, CalciteDbTypes.Of(name));
            Assert.Equal(name, CalciteDbTypes.ToSqlTypeName(expected));
            Assert.Equal(name, CalciteDbTypes.BaseTypeName(expected));
        }

        /// <summary>
        /// The members Calcite has that this list deliberately does not, which must answer
        /// <see cref="CalciteDbType.Unknown"/> rather than something near.
        /// </summary>
        [Theory]
        [InlineData(nameof(SqlTypeName.DISTINCT))]
        [InlineData(nameof(SqlTypeName.STRUCTURED))]
        [InlineData(nameof(SqlTypeName.COLUMN_LIST))]
        [InlineData(nameof(SqlTypeName.DYNAMIC_STAR))]
        [InlineData(nameof(SqlTypeName.FUNCTION))]
        [InlineData(nameof(SqlTypeName.SARG))]
        [InlineData(nameof(SqlTypeName.UNKNOWN))]
        // an operator flag inside a RexLiteral, never a column type
        [InlineData(nameof(SqlTypeName.SYMBOL))]
        // an argument type of a table function, never a column type
        [InlineData(nameof(SqlTypeName.CURSOR))]
        public void An_unnamed_sql_type_should_be_unknown(string sqlTypeName)
        {
            Assert.Equal(CalciteDbType.Unknown, CalciteDbTypes.Of(SqlTypeName.valueOf(sqlTypeName)));
        }

        [Fact]
        public void A_null_sql_type_should_be_unknown()
        {
            Assert.Equal(CalciteDbType.Unknown, CalciteDbTypes.Of((SqlTypeName?)null));
            Assert.Equal(CalciteDbType.Unknown, CalciteDbTypes.Of((org.apache.calcite.rel.type.RelDataType?)null));
        }

        [Fact]
        public void An_unknown_name_should_have_no_sql_type()
        {
            Assert.Null(CalciteDbTypes.ToSqlTypeName(CalciteDbType.Unknown));
            Assert.Null(CalciteDbTypes.BaseTypeName(CalciteDbType.Unknown));
        }

        // ------------------------------------------------------------------------------------
        // DbType, both directions. The narrow list, where the losses are.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// The names ADO.NET's list states exactly, which survive the trip out and back.
        /// </summary>
        [Theory]
        [InlineData(CalciteDbType.Boolean, DbType.Boolean)]
        [InlineData(CalciteDbType.TinyInt, DbType.SByte)]
        [InlineData(CalciteDbType.SmallInt, DbType.Int16)]
        [InlineData(CalciteDbType.Integer, DbType.Int32)]
        [InlineData(CalciteDbType.BigInt, DbType.Int64)]
        [InlineData(CalciteDbType.UTinyInt, DbType.Byte)]
        [InlineData(CalciteDbType.USmallInt, DbType.UInt16)]
        [InlineData(CalciteDbType.UInteger, DbType.UInt32)]
        [InlineData(CalciteDbType.UBigInt, DbType.UInt64)]
        [InlineData(CalciteDbType.Decimal, DbType.Decimal)]
        [InlineData(CalciteDbType.Real, DbType.Single)]
        [InlineData(CalciteDbType.Double, DbType.Double)]
        [InlineData(CalciteDbType.Char, DbType.StringFixedLength)]
        [InlineData(CalciteDbType.VarChar, DbType.String)]
        [InlineData(CalciteDbType.VarBinary, DbType.Binary)]
        [InlineData(CalciteDbType.Uuid, DbType.Guid)]
        [InlineData(CalciteDbType.Date, DbType.Date)]
        [InlineData(CalciteDbType.Time, DbType.Time)]
        [InlineData(CalciteDbType.Timestamp, DbType.DateTime)]
        [InlineData(CalciteDbType.TimestampTz, DbType.DateTimeOffset)]
        public void A_name_both_lists_carry_should_round_trip_through_db_type(CalciteDbType type, DbType dbType)
        {
            Assert.Equal(dbType, CalciteDbTypes.ToDbType(type));
            Assert.Equal(type, CalciteDbTypes.FromDbType(dbType));
        }

        /// <summary>
        /// The names that reach a <see cref="DbType"/> and do not come back as themselves, because several
        /// Calcite types share one <see cref="DbType"/> and the trip back has to pick one.
        /// </summary>
        [Theory]
        [InlineData(CalciteDbType.Float, DbType.Double, CalciteDbType.Double)]
        [InlineData(CalciteDbType.Binary, DbType.Binary, CalciteDbType.VarBinary)]
        [InlineData(CalciteDbType.TimeTz, DbType.DateTimeOffset, CalciteDbType.TimestampTz)]
        [InlineData(CalciteDbType.TimeWithLocalTimeZone, DbType.DateTimeOffset, CalciteDbType.TimestampTz)]
        [InlineData(CalciteDbType.TimestampWithLocalTimeZone, DbType.DateTimeOffset, CalciteDbType.TimestampTz)]
        public void A_name_that_shares_a_db_type_should_come_back_as_the_shared_one(CalciteDbType type, DbType dbType, CalciteDbType back)
        {
            Assert.Equal(dbType, CalciteDbTypes.ToDbType(type));
            Assert.Equal(back, CalciteDbTypes.FromDbType(dbType));
        }

        /// <summary>
        /// The names ADO.NET's list has nothing for, which is most of what Calcite has beyond the shared
        /// ones. <see cref="DbType.Object"/> is what it has for "not one of these".
        /// </summary>
        [Theory]
        [InlineData(CalciteDbType.IntervalDay)]
        [InlineData(CalciteDbType.IntervalYearMonth)]
        [InlineData(CalciteDbType.IntervalSecond)]
        [InlineData(CalciteDbType.Geometry)]
        [InlineData(CalciteDbType.Variant)]
        [InlineData(CalciteDbType.Measure)]
        [InlineData(CalciteDbType.Row)]
        [InlineData(CalciteDbType.Any)]
        [InlineData(CalciteDbType.Other)]
        [InlineData(CalciteDbType.Unknown)]
        public void A_name_ado_has_nothing_for_should_be_object(CalciteDbType type)
        {
            Assert.Equal(DbType.Object, CalciteDbTypes.ToDbType(type));
        }

        /// <summary>
        /// A collection is not a scalar in anyone's list, so it is <see cref="DbType.Object"/> whatever its
        /// element is.
        /// </summary>
        [Theory]
        [InlineData(CalciteDbType.Array | CalciteDbType.Integer)]
        [InlineData(CalciteDbType.Multiset | CalciteDbType.VarChar)]
        [InlineData(CalciteDbType.Map)]
        [InlineData(CalciteDbType.Array)]
        public void A_collection_should_be_object_in_the_ado_list(CalciteDbType type)
        {
            Assert.Equal(DbType.Object, CalciteDbTypes.ToDbType(type));
        }

        /// <summary>
        /// The ANSI members name the same Calcite types as their Unicode counterparts, Calcite having one
        /// character type family.
        /// </summary>
        [Theory]
        [InlineData(DbType.AnsiString, CalciteDbType.VarChar)]
        [InlineData(DbType.AnsiStringFixedLength, CalciteDbType.Char)]
        [InlineData(DbType.Xml, CalciteDbType.VarChar)]
        [InlineData(DbType.Currency, CalciteDbType.Decimal)]
        [InlineData(DbType.VarNumeric, CalciteDbType.Decimal)]
        [InlineData(DbType.DateTime2, CalciteDbType.Timestamp)]
        public void A_db_type_alias_should_name_the_same_calcite_type(DbType dbType, CalciteDbType expected)
        {
            Assert.Equal(expected, CalciteDbTypes.FromDbType(dbType));
        }

        [Fact]
        public void Db_type_object_should_name_nothing()
        {
            Assert.Equal(CalciteDbType.Unknown, CalciteDbTypes.FromDbType(DbType.Object));
        }

        // ------------------------------------------------------------------------------------
        // Collections: one level fits in the flag, further levels do not.
        // ------------------------------------------------------------------------------------

        static org.apache.calcite.adapter.java.JavaTypeFactory Factory()
        {
            return new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
        }

        [Fact]
        public void An_array_should_carry_its_element_in_the_flag()
        {
            var f = Factory();
            var type = f.createArrayType(f.createSqlType(SqlTypeName.INTEGER), -1);

            Assert.Equal(CalciteDbType.Array | CalciteDbType.Integer, CalciteDbTypes.Of(type));
            Assert.Equal(SqlTypeName.ARRAY, CalciteDbTypes.ToSqlTypeName(CalciteDbTypes.Of(type)));
            Assert.Equal(SqlTypeName.INTEGER, CalciteDbTypes.BaseTypeName(CalciteDbTypes.Of(type)));
        }

        [Fact]
        public void A_multiset_should_be_a_different_flag_from_an_array()
        {
            var f = Factory();
            var array = f.createArrayType(f.createSqlType(SqlTypeName.INTEGER), -1);
            var multiset = f.createMultisetType(f.createSqlType(SqlTypeName.INTEGER), -1);

            Assert.NotEqual(CalciteDbTypes.Of(array), CalciteDbTypes.Of(multiset));
            Assert.Equal(CalciteDbType.Multiset | CalciteDbType.Integer, CalciteDbTypes.Of(multiset));
        }

        /// <summary>
        /// A second level has nowhere to go in one flag. The flag says it is an array and stops, which is
        /// the documented limit and not a defect — <c>GetRelDataType</c> is what does not approximate.
        /// </summary>
        [Fact]
        public void A_nested_array_should_keep_the_flag_and_lose_the_element()
        {
            var f = Factory();
            var type = f.createArrayType(f.createArrayType(f.createSqlType(SqlTypeName.INTEGER), -1), -1);

            Assert.Equal(CalciteDbType.Array, CalciteDbTypes.Of(type));
            Assert.Equal(CalciteDbType.Unknown, CalciteDbTypes.BaseType(CalciteDbTypes.Of(type)));
            Assert.Equal(SqlTypeName.ARRAY, CalciteDbTypes.ToSqlTypeName(CalciteDbTypes.Of(type)));
        }

        [Fact]
        public void A_thrice_nested_array_should_be_the_same_flag_as_a_twice_nested_one()
        {
            var f = Factory();
            var twice = f.createArrayType(f.createArrayType(f.createSqlType(SqlTypeName.INTEGER), -1), -1);
            var thrice = f.createArrayType(twice, -1);

            Assert.Equal(CalciteDbTypes.Of(twice), CalciteDbTypes.Of(thrice));
        }

        /// <summary>
        /// An array of multisets nests too, so the element contributes nothing even though both levels have
        /// names of their own.
        /// </summary>
        [Fact]
        public void An_array_of_multisets_should_lose_the_element()
        {
            var f = Factory();
            var type = f.createArrayType(f.createMultisetType(f.createSqlType(SqlTypeName.INTEGER), -1), -1);

            Assert.Equal(CalciteDbType.Array, CalciteDbTypes.Of(type));
        }

        /// <summary>
        /// A map's key and value are two types and the flag has one field, so it says only that it is a map.
        /// </summary>
        [Fact]
        public void A_map_should_name_only_itself()
        {
            var f = Factory();
            var type = f.createMapType(f.createSqlType(SqlTypeName.VARCHAR), f.createSqlType(SqlTypeName.INTEGER));

            Assert.Equal(CalciteDbType.Map, CalciteDbTypes.Of(type));
            Assert.Equal(SqlTypeName.MAP, CalciteDbTypes.ToSqlTypeName(CalciteDbTypes.Of(type)));
        }

        [Fact]
        public void An_array_of_maps_should_lose_the_element()
        {
            var f = Factory();
            var map = f.createMapType(f.createSqlType(SqlTypeName.VARCHAR), f.createSqlType(SqlTypeName.INTEGER));

            Assert.Equal(CalciteDbType.Array, CalciteDbTypes.Of(f.createArrayType(map, -1)));
        }

        /// <summary>
        /// A row element is a scalar as far as the flag is concerned: it occupies one column and has a name.
        /// </summary>
        [Fact]
        public void An_array_of_rows_should_keep_the_element()
        {
            var f = Factory();
            var row = f.builder().add("A", f.createSqlType(SqlTypeName.INTEGER)).build();

            Assert.Equal(CalciteDbType.Array | CalciteDbType.Row, CalciteDbTypes.Of(f.createArrayType(row, -1)));
        }

        /// <summary>
        /// The flags do not collide with the base, which is what makes reading the two halves apart work at
        /// all.
        /// </summary>
        [Fact]
        public void The_collection_flags_should_not_overlap_the_base()
        {
            foreach (var value in Enum.GetValues<CalciteDbType>())
            {
                if (value is CalciteDbType.Array or CalciteDbType.Multiset or CalciteDbType.Map)
                {
                    Assert.True(CalciteDbTypes.IsCollection(value));
                    continue;
                }

                Assert.False(CalciteDbTypes.IsCollection(value));
                Assert.Equal(value, CalciteDbTypes.BaseType(value));
            }
        }

        /// <summary>
        /// Every member of the list is a type. A mask is not one, and lives where the bit arithmetic does.
        /// </summary>
        [Fact]
        public void Every_member_of_the_list_should_be_a_type()
        {
            foreach (var value in Enum.GetValues<CalciteDbType>())
                Assert.True(
                    value is CalciteDbType.Unknown || CalciteDbTypes.ToSqlTypeName(value) is not null,
                    $"{value} names no Calcite type.");
        }

    }

}
