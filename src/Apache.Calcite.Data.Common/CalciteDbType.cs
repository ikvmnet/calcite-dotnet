using System;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// Names a Calcite SQL type, including the ones <see cref="System.Data.DbType"/> has no name for.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="System.Data.DbType"/> is the list every provider shares and it is missing most of what
    /// Calcite has: the unsigned integers, the three zoned temporal types, the thirteen interval types,
    /// <c>UUID</c>, <c>GEOMETRY</c>, <c>MEASURE</c>, <c>VARIANT</c>, and the difference between an
    /// <c>ARRAY</c>, a <c>MULTISET</c> and a <c>MAP</c>. Everything it cannot name collapses to
    /// <see cref="System.Data.DbType.Object"/>. This is the provider-specific list that keeps those apart,
    /// and it is what a caller names when it means a Calcite type rather than a .NET one.
    /// </para>
    /// <para>
    /// <b>A collection is a flag over its element.</b> <c>CalciteDbType.Array | CalciteDbType.Integer</c> is
    /// an <c>INTEGER ARRAY</c>, which is convenient for the case that is almost all of them and is how a
    /// provider-specific type list usually spells one. It is deliberately not the whole story: Calcite nests
    /// without limit, and one bit cannot spell <c>INTEGER ARRAY ARRAY</c>. A type that nests answers
    /// <see cref="Array"/> or <see cref="Multiset"/> over <see cref="Unknown"/>, and the element's own type
    /// is read from the mapping, which recurses where this cannot.
    /// </para>
    /// <para>
    /// <b>Only what a caller can meet.</b> This names the types that reach a result column or a parameter,
    /// which is not every <c>SqlTypeName</c> Calcite declares. <c>SYMBOL</c> is an operator flag inside a
    /// <c>RexLiteral</c> and <c>CURSOR</c> is an argument type of a table function; neither survives into a
    /// row type, and offering either would invite a caller to set it on a parameter where it means nothing.
    /// <c>SARG</c>, <c>COLUMN_LIST</c>, <c>DYNAMIC_STAR</c>, <c>DISTINCT</c>, <c>STRUCTURED</c> and
    /// <c>FUNCTION</c> are left out for the same reason. All of them read as <see cref="Unknown"/>.
    /// </para>
    /// <para>
    /// <b>Best effort, in both directions.</b> Calcite's type model is open — a schema may supply a type
    /// that names no <c>SqlTypeName</c> at all — and this enum is a fixed list, so a type it cannot name
    /// is <see cref="Unknown"/> rather than an error or a new member. <see cref="Unknown"/> is a real
    /// answer and not a failure: the mapping still converts the value, and only this shorthand for it is
    /// unavailable.
    /// </para>
    /// </remarks>
    [Flags]
    public enum CalciteDbType
    {

        /// <summary>
        /// A type this list has no name for, which includes every type a schema supplied itself.
        /// </summary>
        Unknown = 0,

        /// <summary>
        /// <c>BOOLEAN</c>.
        /// </summary>
        Boolean = 1,

        /// <summary>
        /// <c>TINYINT</c>, which is signed, Java's <c>byte</c> being signed.
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
        /// <c>REAL</c>, the four-byte floating point type.
        /// </summary>
        Real = 11,

        /// <summary>
        /// <c>DOUBLE</c>.
        /// </summary>
        Double = 12,

        /// <summary>
        /// <c>FLOAT</c>, which in SQL and in Calcite is eight bytes and shares <see cref="Double"/>'s
        /// representation. <c>JavaTypeFactoryImpl</c> marks the case "sic".
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
        /// <c>VARIANT</c>, which carries its payload's type per value rather than per column.
        /// </summary>
        Variant = 40,

        /// <summary>
        /// <c>MEASURE</c>.
        /// </summary>
        Measure = 41,

        /// <summary>
        /// <c>ROW</c>, a fixed sequence of named fields. Which fields is read from the mapping; there is no
        /// room for them here.
        /// </summary>
        Row = 43,

        /// <summary>
        /// <c>ANY</c>, which says nothing about what it holds and leaves the value's own class to decide.
        /// </summary>
        Any = 44,

        /// <summary>
        /// <c>NULL</c>, the type whose only value is null.
        /// </summary>
        Null = 45,

        /// <summary>
        /// <c>OTHER</c>, which is what Calcite answers for a class it has no SQL name for.
        /// </summary>
        Other = 47,

        /// <summary>
        /// The mask separating the base type from the collection flags above it.
        /// </summary>
        BaseTypeMask = 0x0FFFFFFF,

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
        /// A <c>MAP</c>. The key and value types do not fit here and are read from the mapping.
        /// </summary>
        Map = 0x40000000,

    }

}
