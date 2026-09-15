using System;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// Carries a Calcite type to and from the <see cref="CalciteDbType"/> naming it.
    /// </summary>
    /// <remarks>
    /// Best effort in both directions, and by design. Going in, Calcite's type model is open and a type may
    /// name no <c>SqlTypeName</c> at all, so anything the enum has no member for is
    /// <see cref="CalciteDbType.Unknown"/>. Coming out, a name is not a type — a <c>DECIMAL</c> has a
    /// precision and a scale, a <c>VARCHAR</c> a length, a collection an element — so
    /// <see cref="ToSqlTypeName"/> answers the name and leaves building the type to a caller that has a type
    /// factory and the rest of the facts.
    /// </remarks>
    public static class CalciteDbTypes
    {

        /// <summary>
        /// Separates the base type from the collection flags above it.
        /// </summary>
        /// <remarks>
        /// Private, and not a member of <see cref="CalciteDbType"/>: every member of that list names a type,
        /// and a mask does not. Putting one there would offer a caller something to bitwise-and with in
        /// place of asking <see cref="BaseType"/>, and would turn up in its values and in its
        /// <see cref="object.ToString"/>.
        /// </remarks>
        const CalciteDbType BaseMask = (CalciteDbType)0x0FFFFFFF;

        /// <summary>
        /// Returns the element half of a name, which is the whole of it where it is not a collection.
        /// </summary>
        /// <param name="type">The name.</param>
        /// <returns>The base type, or <see cref="CalciteDbType.Unknown"/> where there is none — which is
        /// what a collection that nests answers, one bit having nowhere to put a second level.</returns>
        public static CalciteDbType BaseType(CalciteDbType type)
        {
            return type & BaseMask;
        }

        /// <summary>
        /// Returns whether a name is a collection of something.
        /// </summary>
        /// <param name="type">The name.</param>
        /// <returns><see langword="true"/> for an <c>ARRAY</c>, a <c>MULTISET</c> or a <c>MAP</c>.</returns>
        public static bool IsCollection(CalciteDbType type)
        {
            return (type & ~BaseMask) != 0;
        }

        /// <summary>
        /// Returns the <see cref="CalciteDbType"/> naming a Calcite type.
        /// </summary>
        /// <param name="relType">The type, or <see langword="null"/>.</param>
        /// <returns>The name, or <see cref="CalciteDbType.Unknown"/> where this list has none.</returns>
        /// <remarks>
        /// A collection contributes its flag and its element contributes the base, so an
        /// <c>INTEGER ARRAY</c> is <c>Array | Integer</c>. An element that is itself a collection has no
        /// base to contribute — one bit cannot spell <c>INTEGER ARRAY ARRAY</c> — so such a type is the
        /// collection flag alone and the element is read from the mapping instead.
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
                    // the key and the value are two types and this is one field; the mapping carries them
                    return CalciteDbType.Map;

                default:
                    return Of(name);
            }
        }

        /// <summary>
        /// Returns the base an element contributes, which is nothing where the element is itself a
        /// collection.
        /// </summary>
        static CalciteDbType Base(RelDataType? element)
        {
            var of = Of(element);

            return IsCollection(of) ? CalciteDbType.Unknown : of;
        }

        /// <summary>
        /// Returns the <see cref="CalciteDbType"/> naming a <see cref="SqlTypeName"/>.
        /// </summary>
        /// <param name="name">The name, or <see langword="null"/>.</param>
        /// <returns>The name, or <see cref="CalciteDbType.Unknown"/> where this list has none.</returns>
        /// <remarks>
        /// Switched on the enum constant's name rather than its ordinal, which is the rule everywhere a
        /// Java enum is read here: ordinals are not stable across Calcite versions and names are.
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
        /// <param name="type"></param>
        /// <returns>The name, or <see cref="System.Data.DbType.Object"/> where that shared list has none.</returns>
        /// <remarks>
        /// Lossy by construction and in one direction only. <see cref="System.Data.DbType"/> is the list
        /// every provider shares, so the unsigned integers keep their own names but the intervals,
        /// <c>VARIANT</c>, <c>MEASURE</c>, <c>GEOMETRY</c>, <c>ROW</c> and every collection are
        /// <see cref="System.Data.DbType.Object"/>, which is what ADO.NET has for "not one of these". Going
        /// back the other way loses more: see <see cref="FromDbType"/>.
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
        /// <param name="type"></param>
        /// <returns>The name, or <see cref="CalciteDbType.Unknown"/> where none is near enough.</returns>
        /// <remarks>
        /// The lossier direction, and lossy in a way worth knowing about rather than hiding. Several
        /// <see cref="System.Data.DbType"/> members name a .NET type that Calcite spells more than one way,
        /// so a choice has to be made and it is made toward the type SQL means by default:
        /// <see cref="System.Data.DbType.DateTime"/> is a <c>TIMESTAMP</c> and not a <c>DATE</c>,
        /// <see cref="System.Data.DbType.DateTimeOffset"/> a <c>TIMESTAMP WITH TIME ZONE</c> and not a
        /// zoned <c>TIME</c>, <see cref="System.Data.DbType.Binary"/> a <c>VARBINARY</c> and not a
        /// <c>BINARY</c>. The ANSI members map to the same types as their Unicode counterparts, Calcite
        /// having one character type family. <see cref="System.Data.DbType.Object"/> means "not one of
        /// these" and carries nothing, so it is <see cref="CalciteDbType.Unknown"/>.
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
        /// <param name="type">The name. Collection flags are read, so <c>Array | Integer</c> answers
        /// <c>ARRAY</c> and <see cref="BaseTypeName"/> answers <c>INTEGER</c>.</param>
        /// <returns>The type name, or <see langword="null"/> where there is none.</returns>
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
        /// <param name="type"></param>
        /// <returns>The type name, or <see langword="null"/> where there is none.</returns>
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
