using System;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// Names a Calcite SQL type, including those <see cref="System.Data.DbType"/> has no member for.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="System.Data.DbType"/> has no members for the unsigned integers, the zoned temporal types,
    /// the intervals, <c>UUID</c>, <c>GEOMETRY</c>, <c>MEASURE</c> or <c>VARIANT</c>, and does not tell
    /// <c>ARRAY</c>, <c>MULTISET</c> and <c>MAP</c> apart. This enumeration does.
    /// </para>
    /// <para>
    /// A collection is a flag combined with its element type: <c>CalciteDbType.Array | CalciteDbType.Integer</c>
    /// is <c>INTEGER ARRAY</c>. A collection whose element is itself a collection, such as
    /// <c>INTEGER ARRAY ARRAY</c>, is the flag over <see cref="Unknown"/>; the element type is available from
    /// the column's mapping. A <see cref="Map"/> never carries its key or value types.
    /// </para>
    /// <para>
    /// Only types that can appear in a result column or a parameter have members. Any other type, including
    /// one a schema defines that has no <c>SqlTypeName</c>, is <see cref="Unknown"/>; its values are still
    /// converted by the mapping.
    /// </para>
    /// </remarks>
    [Flags]
    public enum CalciteDbType
    {

        /// <summary>
        /// A type with no member of its own.
        /// </summary>
        Unknown = 0,

        /// <summary>
        /// <c>BOOLEAN</c>.
        /// </summary>
        Boolean = 1,

        /// <summary>
        /// <c>TINYINT</c>, a signed 8-bit integer.
        /// </summary>
        TinyInt = 2,

        /// <summary>
        /// <c>SMALLINT</c>.
        /// </summary>
        SmallInt = 3,

        /// <summary>
        /// <c>INTEGER</c>.
        /// </summary>
        Integer = 4,

        /// <summary>
        /// <c>BIGINT</c>.
        /// </summary>
        BigInt = 5,

        /// <summary>
        /// <c>UTINYINT</c>, an unsigned 8-bit integer.
        /// </summary>
        UTinyInt = 6,

        /// <summary>
        /// <c>USMALLINT</c>, an unsigned 16-bit integer.
        /// </summary>
        USmallInt = 7,

        /// <summary>
        /// <c>UINTEGER</c>, an unsigned 32-bit integer.
        /// </summary>
        UInteger = 8,

        /// <summary>
        /// <c>UBIGINT</c>, an unsigned 64-bit integer.
        /// </summary>
        UBigInt = 9,

        /// <summary>
        /// <c>DECIMAL</c>.
        /// </summary>
        Decimal = 10,

        /// <summary>
        /// <c>REAL</c>, a 4-byte floating-point number.
        /// </summary>
        Real = 11,

        /// <summary>
        /// <c>DOUBLE</c>, an 8-byte floating-point number.
        /// </summary>
        Double = 12,

        /// <summary>
        /// <c>FLOAT</c>, which Calcite holds as an 8-byte floating-point number, like <see cref="Double"/>.
        /// </summary>
        Float = 13,

        /// <summary>
        /// <c>CHAR</c>, fixed length.
        /// </summary>
        Char = 14,

        /// <summary>
        /// <c>VARCHAR</c>.
        /// </summary>
        VarChar = 15,

        /// <summary>
        /// <c>BINARY</c>, fixed length.
        /// </summary>
        Binary = 16,

        /// <summary>
        /// <c>VARBINARY</c>.
        /// </summary>
        VarBinary = 17,

        /// <summary>
        /// <c>DATE</c>, held as a count of days since the epoch.
        /// </summary>
        Date = 18,

        /// <summary>
        /// <c>TIME</c>, held as a count of milliseconds since midnight.
        /// </summary>
        Time = 19,

        /// <summary>
        /// <c>TIME WITH LOCAL TIME ZONE</c>.
        /// </summary>
        TimeWithLocalTimeZone = 20,

        /// <summary>
        /// <c>TIME WITH TIME ZONE</c>.
        /// </summary>
        TimeTz = 21,

        /// <summary>
        /// <c>TIMESTAMP</c>, held as a count of milliseconds since the epoch.
        /// </summary>
        Timestamp = 22,

        /// <summary>
        /// <c>TIMESTAMP WITH LOCAL TIME ZONE</c>.
        /// </summary>
        TimestampWithLocalTimeZone = 23,

        /// <summary>
        /// <c>TIMESTAMP WITH TIME ZONE</c>.
        /// </summary>
        TimestampTz = 24,

        /// <summary>
        /// <c>INTERVAL YEAR</c>.
        /// </summary>
        IntervalYear = 25,

        /// <summary>
        /// <c>INTERVAL YEAR TO MONTH</c>.
        /// </summary>
        IntervalYearMonth = 26,

        /// <summary>
        /// <c>INTERVAL MONTH</c>.
        /// </summary>
        IntervalMonth = 27,

        /// <summary>
        /// <c>INTERVAL DAY</c>.
        /// </summary>
        IntervalDay = 28,

        /// <summary>
        /// <c>INTERVAL DAY TO HOUR</c>.
        /// </summary>
        IntervalDayHour = 29,

        /// <summary>
        /// <c>INTERVAL DAY TO MINUTE</c>.
        /// </summary>
        IntervalDayMinute = 30,

        /// <summary>
        /// <c>INTERVAL DAY TO SECOND</c>.
        /// </summary>
        IntervalDaySecond = 31,

        /// <summary>
        /// <c>INTERVAL HOUR</c>.
        /// </summary>
        IntervalHour = 32,

        /// <summary>
        /// <c>INTERVAL HOUR TO MINUTE</c>.
        /// </summary>
        IntervalHourMinute = 33,

        /// <summary>
        /// <c>INTERVAL HOUR TO SECOND</c>.
        /// </summary>
        IntervalHourSecond = 34,

        /// <summary>
        /// <c>INTERVAL MINUTE</c>.
        /// </summary>
        IntervalMinute = 35,

        /// <summary>
        /// <c>INTERVAL MINUTE TO SECOND</c>.
        /// </summary>
        IntervalMinuteSecond = 36,

        /// <summary>
        /// <c>INTERVAL SECOND</c>.
        /// </summary>
        IntervalSecond = 37,

        /// <summary>
        /// <c>UUID</c>.
        /// </summary>
        Uuid = 38,

        /// <summary>
        /// <c>GEOMETRY</c>.
        /// </summary>
        Geometry = 39,

        /// <summary>
        /// <c>VARIANT</c>, whose values each carry their own type.
        /// </summary>
        Variant = 40,

        /// <summary>
        /// <c>MEASURE</c>.
        /// </summary>
        Measure = 41,

        /// <summary>
        /// <c>ROW</c>, a fixed sequence of named fields. The field types are available from the column's
        /// mapping.
        /// </summary>
        Row = 43,

        /// <summary>
        /// <c>ANY</c>, which does not say what it holds.
        /// </summary>
        Any = 44,

        /// <summary>
        /// <c>NULL</c>, the type whose only value is null.
        /// </summary>
        Null = 45,

        /// <summary>
        /// <c>OTHER</c>, Calcite's type name for a Java class that has no SQL type name of its own.
        /// </summary>
        Other = 47,

        /// <summary>
        /// An <c>ARRAY</c> of the base type it is combined with, as
        /// <c>CalciteDbType.Array | CalciteDbType.Integer</c>.
        /// </summary>
        Array = 0x10000000,

        /// <summary>
        /// A <c>MULTISET</c> of the base type it is combined with.
        /// </summary>
        Multiset = 0x20000000,

        /// <summary>
        /// A <c>MAP</c>. The key and value types are available from the column's mapping.
        /// </summary>
        Map = 0x40000000,

    }

}
