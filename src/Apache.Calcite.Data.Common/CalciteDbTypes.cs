using System;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// Converts between Calcite types, <see cref="CalciteDbType"/>, <see cref="SqlTypeName"/> and
    /// <see cref="System.Data.DbType"/>.
    /// </summary>
    /// <remarks>
    /// The conversions are lossy. A type with no <see cref="CalciteDbType"/> member is
    /// <see cref="CalciteDbType.Unknown"/>, and a <see cref="CalciteDbType"/> names only a type name, not a
    /// precision, scale, length or element type, so <see cref="ToSqlTypeName"/> returns a
    /// <see cref="SqlTypeName"/> from which the caller builds the full type.
    /// </remarks>
    public static class CalciteDbTypes
    {

        /// <summary>
        /// Selects the base type bits of a <see cref="CalciteDbType"/>, below the collection flags.
        /// </summary>
        /// <remarks>
        /// Kept here rather than in <see cref="CalciteDbType"/> because it names no type; callers use
        /// <see cref="BaseType"/>.
        /// </remarks>
        const CalciteDbType BaseMask = (CalciteDbType)0x0FFFFFFF;

        /// <summary>
        /// Returns a type with its collection flags removed.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>The element type of a collection, or <paramref name="type"/> itself where it is not a
        /// collection. A <c>MAP</c>, or a collection whose element is a collection, gives
        /// <see cref="CalciteDbType.Unknown"/>.</returns>
        public static CalciteDbType BaseType(CalciteDbType type)
        {
            return type & BaseMask;
        }

        /// <summary>
        /// Returns whether a type is a collection.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns><see langword="true"/> where <see cref="CalciteDbType.Array"/>,
        /// <see cref="CalciteDbType.Multiset"/> or <see cref="CalciteDbType.Map"/> is set.</returns>
        public static bool IsCollection(CalciteDbType type)
        {
            return (type & ~BaseMask) != 0;
        }

        /// <summary>
        /// Returns the <see cref="CalciteDbType"/> naming a Calcite type.
        /// </summary>
        /// <param name="relType">The type, or <see langword="null"/>.</param>
        /// <returns>The <see cref="CalciteDbType"/>, or <see cref="CalciteDbType.Unknown"/> where there is no
        /// member for the type or <paramref name="relType"/> is <see langword="null"/>.</returns>
        /// <remarks>
        /// An <c>ARRAY</c> or <c>MULTISET</c> is its flag combined with its element type, so
        /// <c>INTEGER ARRAY</c> is <c>Array | Integer</c>. Where the element is itself a collection the result
        /// is the flag alone. A <c>MAP</c> is <see cref="CalciteDbType.Map"/> alone.
        /// </remarks>
        public static CalciteDbType Of(RelDataType? relType)
        {
            if (relType is null)
                return CalciteDbType.Unknown;

            var name = relType.getSqlTypeName();
            if (name is null)
                return CalciteDbType.Unknown;

            switch (name.name())
            {
                case nameof(SqlTypeName.ARRAY):
                    return CalciteDbType.Array | Base(relType.getComponentType());

                case nameof(SqlTypeName.MULTISET):
                    return CalciteDbType.Multiset | Base(relType.getComponentType());

                case nameof(SqlTypeName.MAP):
                    return CalciteDbType.Map;

                default:
                    return Of(name);
            }
        }

        /// <summary>
        /// Returns the base type an element contributes to its collection's <see cref="CalciteDbType"/>, which
        /// is <see cref="CalciteDbType.Unknown"/> where the element is itself a collection.
        /// </summary>
        static CalciteDbType Base(RelDataType? element)
        {
            var of = Of(element);

            return IsCollection(of) ? CalciteDbType.Unknown : of;
        }

        /// <summary>
        /// Returns the <see cref="CalciteDbType"/> naming a <see cref="SqlTypeName"/>.
        /// </summary>
        /// <param name="name">The type name, or <see langword="null"/>.</param>
        /// <returns>The <see cref="CalciteDbType"/>, or <see cref="CalciteDbType.Unknown"/> where there is no
        /// member for the name. <c>ARRAY</c>, <c>MULTISET</c> and <c>MAP</c> give
        /// <see cref="CalciteDbType.Unknown"/>; use the <see cref="RelDataType"/> overload for those.</returns>
        /// <remarks>
        /// Matches on the Java enum constant's name, since ordinals are not stable across Calcite versions.
        /// </remarks>
        public static CalciteDbType Of(SqlTypeName? name)
        {
            if (name is null)
                return CalciteDbType.Unknown;

            return name.name() switch
            {
                nameof(SqlTypeName.BOOLEAN) => CalciteDbType.Boolean,
                nameof(SqlTypeName.TINYINT) => CalciteDbType.TinyInt,
                nameof(SqlTypeName.SMALLINT) => CalciteDbType.SmallInt,
                nameof(SqlTypeName.INTEGER) => CalciteDbType.Integer,
                nameof(SqlTypeName.BIGINT) => CalciteDbType.BigInt,
                nameof(SqlTypeName.UTINYINT) => CalciteDbType.UTinyInt,
                nameof(SqlTypeName.USMALLINT) => CalciteDbType.USmallInt,
                nameof(SqlTypeName.UINTEGER) => CalciteDbType.UInteger,
                nameof(SqlTypeName.UBIGINT) => CalciteDbType.UBigInt,
                nameof(SqlTypeName.DECIMAL) => CalciteDbType.Decimal,
                nameof(SqlTypeName.REAL) => CalciteDbType.Real,
                nameof(SqlTypeName.DOUBLE) => CalciteDbType.Double,
                nameof(SqlTypeName.FLOAT) => CalciteDbType.Float,
                nameof(SqlTypeName.CHAR) => CalciteDbType.Char,
                nameof(SqlTypeName.VARCHAR) => CalciteDbType.VarChar,
                nameof(SqlTypeName.BINARY) => CalciteDbType.Binary,
                nameof(SqlTypeName.VARBINARY) => CalciteDbType.VarBinary,
                nameof(SqlTypeName.DATE) => CalciteDbType.Date,
                nameof(SqlTypeName.TIME) => CalciteDbType.Time,
                nameof(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE) => CalciteDbType.TimeWithLocalTimeZone,
                nameof(SqlTypeName.TIME_TZ) => CalciteDbType.TimeTz,
                nameof(SqlTypeName.TIMESTAMP) => CalciteDbType.Timestamp,
                nameof(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) => CalciteDbType.TimestampWithLocalTimeZone,
                nameof(SqlTypeName.TIMESTAMP_TZ) => CalciteDbType.TimestampTz,
                nameof(SqlTypeName.INTERVAL_YEAR) => CalciteDbType.IntervalYear,
                nameof(SqlTypeName.INTERVAL_YEAR_MONTH) => CalciteDbType.IntervalYearMonth,
                nameof(SqlTypeName.INTERVAL_MONTH) => CalciteDbType.IntervalMonth,
                nameof(SqlTypeName.INTERVAL_DAY) => CalciteDbType.IntervalDay,
                nameof(SqlTypeName.INTERVAL_DAY_HOUR) => CalciteDbType.IntervalDayHour,
                nameof(SqlTypeName.INTERVAL_DAY_MINUTE) => CalciteDbType.IntervalDayMinute,
                nameof(SqlTypeName.INTERVAL_DAY_SECOND) => CalciteDbType.IntervalDaySecond,
                nameof(SqlTypeName.INTERVAL_HOUR) => CalciteDbType.IntervalHour,
                nameof(SqlTypeName.INTERVAL_HOUR_MINUTE) => CalciteDbType.IntervalHourMinute,
                nameof(SqlTypeName.INTERVAL_HOUR_SECOND) => CalciteDbType.IntervalHourSecond,
                nameof(SqlTypeName.INTERVAL_MINUTE) => CalciteDbType.IntervalMinute,
                nameof(SqlTypeName.INTERVAL_MINUTE_SECOND) => CalciteDbType.IntervalMinuteSecond,
                nameof(SqlTypeName.INTERVAL_SECOND) => CalciteDbType.IntervalSecond,
                nameof(SqlTypeName.UUID) => CalciteDbType.Uuid,
                nameof(SqlTypeName.GEOMETRY) => CalciteDbType.Geometry,
                nameof(SqlTypeName.VARIANT) => CalciteDbType.Variant,
                nameof(SqlTypeName.MEASURE) => CalciteDbType.Measure,
                nameof(SqlTypeName.ROW) => CalciteDbType.Row,
                nameof(SqlTypeName.ANY) => CalciteDbType.Any,
                nameof(SqlTypeName.NULL) => CalciteDbType.Null,
                nameof(SqlTypeName.OTHER) => CalciteDbType.Other,
                _ => CalciteDbType.Unknown,
            };
        }

        /// <summary>
        /// Returns the <see cref="System.Data.DbType"/> nearest a <see cref="CalciteDbType"/>.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>The <see cref="System.Data.DbType"/>, or <see cref="System.Data.DbType.Object"/> where
        /// there is no near member.</returns>
        /// <remarks>
        /// The intervals, <c>GEOMETRY</c>, <c>VARIANT</c>, <c>MEASURE</c>, <c>ROW</c>, <c>ANY</c>,
        /// <c>NULL</c>, <c>OTHER</c> and every collection give <see cref="System.Data.DbType.Object"/>.
        /// <c>FLOAT</c> gives <see cref="System.Data.DbType.Double"/>, and the zoned temporal types give
        /// <see cref="System.Data.DbType.DateTimeOffset"/>.
        /// </remarks>
        public static System.Data.DbType ToDbType(CalciteDbType type)
        {
            if (IsCollection(type))
                return System.Data.DbType.Object;

            return BaseType(type) switch
            {
                CalciteDbType.Boolean => System.Data.DbType.Boolean,
                CalciteDbType.TinyInt => System.Data.DbType.SByte,
                CalciteDbType.SmallInt => System.Data.DbType.Int16,
                CalciteDbType.Integer => System.Data.DbType.Int32,
                CalciteDbType.BigInt => System.Data.DbType.Int64,
                CalciteDbType.UTinyInt => System.Data.DbType.Byte,
                CalciteDbType.USmallInt => System.Data.DbType.UInt16,
                CalciteDbType.UInteger => System.Data.DbType.UInt32,
                CalciteDbType.UBigInt => System.Data.DbType.UInt64,
                CalciteDbType.Decimal => System.Data.DbType.Decimal,
                CalciteDbType.Real => System.Data.DbType.Single,
                CalciteDbType.Double => System.Data.DbType.Double,
                CalciteDbType.Float => System.Data.DbType.Double,
                CalciteDbType.Char => System.Data.DbType.StringFixedLength,
                CalciteDbType.VarChar => System.Data.DbType.String,
                CalciteDbType.Binary => System.Data.DbType.Binary,
                CalciteDbType.VarBinary => System.Data.DbType.Binary,
                CalciteDbType.Date => System.Data.DbType.Date,
                CalciteDbType.Time => System.Data.DbType.Time,
                CalciteDbType.TimeWithLocalTimeZone => System.Data.DbType.DateTimeOffset,
                CalciteDbType.TimeTz => System.Data.DbType.DateTimeOffset,
                CalciteDbType.Timestamp => System.Data.DbType.DateTime,
                CalciteDbType.TimestampWithLocalTimeZone => System.Data.DbType.DateTimeOffset,
                CalciteDbType.TimestampTz => System.Data.DbType.DateTimeOffset,
                CalciteDbType.Uuid => System.Data.DbType.Guid,
                _ => System.Data.DbType.Object,
            };
        }

        /// <summary>
        /// Returns the <see cref="CalciteDbType"/> nearest a <see cref="System.Data.DbType"/>.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>The <see cref="CalciteDbType"/>, or <see cref="CalciteDbType.Unknown"/> where there is no
        /// near member, as for <see cref="System.Data.DbType.Object"/>.</returns>
        /// <remarks>
        /// Where several Calcite types correspond, the result is the usual SQL one:
        /// <see cref="System.Data.DbType.DateTime"/> and <see cref="System.Data.DbType.DateTime2"/> give
        /// <c>TIMESTAMP</c>, <see cref="System.Data.DbType.DateTimeOffset"/> gives <c>TIMESTAMP WITH TIME
        /// ZONE</c>, and <see cref="System.Data.DbType.Binary"/> gives <c>VARBINARY</c>. The ANSI string
        /// members give the same types as their Unicode counterparts, and
        /// <see cref="System.Data.DbType.Xml"/> gives <c>VARCHAR</c>.
        /// </remarks>
        public static CalciteDbType FromDbType(System.Data.DbType type)
        {
            return type switch
            {
                System.Data.DbType.Boolean => CalciteDbType.Boolean,
                System.Data.DbType.SByte => CalciteDbType.TinyInt,
                System.Data.DbType.Int16 => CalciteDbType.SmallInt,
                System.Data.DbType.Int32 => CalciteDbType.Integer,
                System.Data.DbType.Int64 => CalciteDbType.BigInt,
                System.Data.DbType.Byte => CalciteDbType.UTinyInt,
                System.Data.DbType.UInt16 => CalciteDbType.USmallInt,
                System.Data.DbType.UInt32 => CalciteDbType.UInteger,
                System.Data.DbType.UInt64 => CalciteDbType.UBigInt,
                System.Data.DbType.Single => CalciteDbType.Real,
                System.Data.DbType.Double => CalciteDbType.Double,
                System.Data.DbType.Decimal or System.Data.DbType.Currency or System.Data.DbType.VarNumeric => CalciteDbType.Decimal,
                System.Data.DbType.String or System.Data.DbType.AnsiString or System.Data.DbType.Xml => CalciteDbType.VarChar,
                System.Data.DbType.StringFixedLength or System.Data.DbType.AnsiStringFixedLength => CalciteDbType.Char,
                System.Data.DbType.Binary => CalciteDbType.VarBinary,
                System.Data.DbType.Guid => CalciteDbType.Uuid,
                System.Data.DbType.Date => CalciteDbType.Date,
                System.Data.DbType.Time => CalciteDbType.Time,
                System.Data.DbType.DateTime or System.Data.DbType.DateTime2 => CalciteDbType.Timestamp,
                System.Data.DbType.DateTimeOffset => CalciteDbType.TimestampTz,
                _ => CalciteDbType.Unknown,
            };
        }

        /// <summary>
        /// Returns the <see cref="SqlTypeName"/> a <see cref="CalciteDbType"/> names.
        /// </summary>
        /// <param name="type">The type. A collection flag takes precedence, so <c>Array | Integer</c> gives
        /// <c>ARRAY</c>; <see cref="BaseTypeName"/> gives the element's <c>INTEGER</c>.</param>
        /// <returns>The type name, or <see langword="null"/> for <see cref="CalciteDbType.Unknown"/>.</returns>
        public static SqlTypeName? ToSqlTypeName(CalciteDbType type)
        {
            if ((type & CalciteDbType.Array) != 0)
                return SqlTypeName.ARRAY;
            if ((type & CalciteDbType.Multiset) != 0)
                return SqlTypeName.MULTISET;
            if ((type & CalciteDbType.Map) != 0)
                return SqlTypeName.MAP;

            return BaseTypeName(type);
        }

        /// <summary>
        /// Returns the <see cref="SqlTypeName"/> the base of a <see cref="CalciteDbType"/> names, ignoring
        /// any collection flag.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>The type name, or <see langword="null"/> where the base type is
        /// <see cref="CalciteDbType.Unknown"/>.</returns>
        public static SqlTypeName? BaseTypeName(CalciteDbType type)
        {
            return BaseType(type) switch
            {
                CalciteDbType.Boolean => SqlTypeName.BOOLEAN,
                CalciteDbType.TinyInt => SqlTypeName.TINYINT,
                CalciteDbType.SmallInt => SqlTypeName.SMALLINT,
                CalciteDbType.Integer => SqlTypeName.INTEGER,
                CalciteDbType.BigInt => SqlTypeName.BIGINT,
                CalciteDbType.UTinyInt => SqlTypeName.UTINYINT,
                CalciteDbType.USmallInt => SqlTypeName.USMALLINT,
                CalciteDbType.UInteger => SqlTypeName.UINTEGER,
                CalciteDbType.UBigInt => SqlTypeName.UBIGINT,
                CalciteDbType.Decimal => SqlTypeName.DECIMAL,
                CalciteDbType.Real => SqlTypeName.REAL,
                CalciteDbType.Double => SqlTypeName.DOUBLE,
                CalciteDbType.Float => SqlTypeName.FLOAT,
                CalciteDbType.Char => SqlTypeName.CHAR,
                CalciteDbType.VarChar => SqlTypeName.VARCHAR,
                CalciteDbType.Binary => SqlTypeName.BINARY,
                CalciteDbType.VarBinary => SqlTypeName.VARBINARY,
                CalciteDbType.Date => SqlTypeName.DATE,
                CalciteDbType.Time => SqlTypeName.TIME,
                CalciteDbType.TimeWithLocalTimeZone => SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE,
                CalciteDbType.TimeTz => SqlTypeName.TIME_TZ,
                CalciteDbType.Timestamp => SqlTypeName.TIMESTAMP,
                CalciteDbType.TimestampWithLocalTimeZone => SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                CalciteDbType.TimestampTz => SqlTypeName.TIMESTAMP_TZ,
                CalciteDbType.IntervalYear => SqlTypeName.INTERVAL_YEAR,
                CalciteDbType.IntervalYearMonth => SqlTypeName.INTERVAL_YEAR_MONTH,
                CalciteDbType.IntervalMonth => SqlTypeName.INTERVAL_MONTH,
                CalciteDbType.IntervalDay => SqlTypeName.INTERVAL_DAY,
                CalciteDbType.IntervalDayHour => SqlTypeName.INTERVAL_DAY_HOUR,
                CalciteDbType.IntervalDayMinute => SqlTypeName.INTERVAL_DAY_MINUTE,
                CalciteDbType.IntervalDaySecond => SqlTypeName.INTERVAL_DAY_SECOND,
                CalciteDbType.IntervalHour => SqlTypeName.INTERVAL_HOUR,
                CalciteDbType.IntervalHourMinute => SqlTypeName.INTERVAL_HOUR_MINUTE,
                CalciteDbType.IntervalHourSecond => SqlTypeName.INTERVAL_HOUR_SECOND,
                CalciteDbType.IntervalMinute => SqlTypeName.INTERVAL_MINUTE,
                CalciteDbType.IntervalMinuteSecond => SqlTypeName.INTERVAL_MINUTE_SECOND,
                CalciteDbType.IntervalSecond => SqlTypeName.INTERVAL_SECOND,
                CalciteDbType.Uuid => SqlTypeName.UUID,
                CalciteDbType.Geometry => SqlTypeName.GEOMETRY,
                CalciteDbType.Variant => SqlTypeName.VARIANT,
                CalciteDbType.Measure => SqlTypeName.MEASURE,
                CalciteDbType.Row => SqlTypeName.ROW,
                CalciteDbType.Any => SqlTypeName.ANY,
                CalciteDbType.Null => SqlTypeName.NULL,
                CalciteDbType.Other => SqlTypeName.OTHER,
                _ => null,
            };
        }

    }

}
