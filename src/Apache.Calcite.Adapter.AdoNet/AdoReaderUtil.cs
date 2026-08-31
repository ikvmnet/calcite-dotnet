using System;
using System.Data.Common;

// only ever named inside a method body -- a using is not a member signature, and it is member
// signatures Janino has to be able to load. See the class remarks.
using Apache.Calcite.Data.Common;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Various utilities for working with an ADO data reader.
    /// </summary>
    /// <remarks>
    /// Every conversion here is the <see cref="AdoReaderMapping"/>'s. What a provider hands back is a CLR
    /// value and what a plan reads is the representation Calcite holds that type in, which is the same
    /// crossing a command parameter makes in the other direction — and the two were separate tables that
    /// disagreed about whether to convert or to cast.
    ///
    /// <para><b>No member of this class may name a type outside what Janino can load.</b> A plan of
    /// <c>EnumerableConvention</c> is Java source naming
    /// <c>cli.Apache.Calcite.Adapter.AdoNet.AdoReaderUtil.GetDbReaderValue</c>, and resolving that call
    /// makes Janino reflect over the whole class and load the type of every member it declares. Anything a
    /// caller's own mapping is passed through lives on <see cref="AdoReaderMapping"/> for that reason.</para>
    /// </remarks>
    public static class AdoReaderUtil
    {

        /// <summary>
        /// Gets a value from the reader in the representation Calcite holds the supplied type in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static object? GetDbReaderValue(DbDataReader reader, int index, RelDataType type)
        {
            return AdoReaderMapping.GetDbReaderValue(reader, index, type, AdoReaderMapping.Default);
        }

        /// <summary>
        /// Gets a value from the reader in the representation Calcite holds the named type in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <param name="typeName"></param>
        /// <returns></returns>
        /// <remarks>
        /// The overload generated code reaches, a <see cref="SqlTypeName"/> being what a block can carry.
        /// </remarks>
        public static object? GetDbReaderValue(DbDataReader reader, int index, SqlTypeName typeName)
        {
            ArgumentNullException.ThrowIfNull(typeName);

            return GetDbReaderValue(reader, index, AdoReaderMapping.TypeOf(typeName));
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Boolean"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetBoolean(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToBoolean);

        /// <summary>
        /// Gets a <see cref="java.lang.Byte"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// Calcite's <c>TINYINT</c> is signed, so this is an <see cref="sbyte"/> and not the <see cref="byte"/>
        /// the <see cref="DbDataReader.GetByte"/> accessor answers with. A provider whose own tiny integer is
        /// unsigned — SQL Server's is — maps to <c>UTINYINT</c> and comes through <see cref="GetUByte"/>
        /// instead.
        /// </remarks>
        public static object? GetByte(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToTinyInt);

        /// <summary>
        /// Gets a <see cref="java.lang.Short"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetShort(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToSmallInt);

        /// <summary>
        /// Gets a <see cref="java.lang.Integer"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetInt(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToInteger);

        /// <summary>
        /// Gets a <see cref="java.lang.Long"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetLong(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToBigInt);

        /// <summary>
        /// Gets an <see cref="org.joou.UByte"/>, which is what Calcite holds a <c>UTINYINT</c> in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// The unsigned types are not a variation on the signed ones: <c>getJavaClass</c> answers a joou
        /// wrapper rather than a <c>java.lang</c> one, and a <see cref="java.lang.Short"/> in its place would
        /// be a value of the wrong class for the type the row declares.
        /// </remarks>
        public static object? GetUByte(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToUTinyInt);

        /// <summary>
        /// Gets an <see cref="org.joou.UShort"/>, which is what Calcite holds a <c>USMALLINT</c> in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetUShort(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToUSmallInt);

        /// <summary>
        /// Gets an <see cref="org.joou.UInteger"/>, which is what Calcite holds a <c>UINTEGER</c> in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetUInt(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToUInteger);

        /// <summary>
        /// Gets an <see cref="org.joou.ULong"/>, which is what Calcite holds a <c>UBIGINT</c> in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetULong(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToUBigInt);

        /// <summary>
        /// Gets a <see cref="java.lang.Double"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetDouble(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToDouble);

        /// <summary>
        /// Gets a <see cref="java.lang.Float"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetFloat(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToReal);

        /// <summary>
        /// Gets a <see cref="SqlTypeName.DATE"/> in Calcite's internal representation.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// A date is a count of whole days since 1 January 1970, held in a <see cref="java.lang.Integer"/>.
        /// It is not a millisecond count, which is what a <see cref="SqlTypeName.TIMESTAMP"/> is.
        /// </remarks>
        public static object? GetDate(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToDate);

        /// <summary>
        /// Gets a <see cref="SqlTypeName.TIMESTAMP"/> in Calcite's internal representation.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetTimestamp(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToTimestamp);

        /// <summary>
        /// Gets a <see cref="SqlTypeName.TIMESTAMP_TZ"/> in Calcite's internal representation.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// A provider that has a type for a zoned timestamp hands back a <see cref="DateTimeOffset"/> and
        /// refuses <see cref="DbDataReader.GetDateTime"/> outright — SQL Server's <c>datetimeoffset</c> does;
        /// one that does not is read as UTC, the offset being the thing it had no way to tell us.
        /// </remarks>
        public static object? GetTimestampTz(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToTimestampTz);

        /// <summary>
        /// Gets a <see cref="SqlTypeName.DECIMAL"/> as the <see cref="java.math.BigDecimal"/> Calcite holds
        /// one in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetDecimal(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToDecimal);

        /// <summary>
        /// Gets a <see cref="SqlTypeName.VARBINARY"/> as the <c>ByteString</c> Calcite holds one in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetBinary(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToBinary);

        /// <summary>
        /// Gets a <see cref="SqlTypeName.TIME"/> in Calcite's internal representation.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// A time is a count of milliseconds since midnight held in a <see cref="java.lang.Integer"/>, the
        /// same shape a <see cref="SqlTypeName.DATE"/> uses for days. Providers surface one either as a span
        /// or as a whole timestamp whose date part is to be ignored.
        /// </remarks>
        public static object? GetTime(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToTime);

        /// <summary>
        /// Gets a <see cref="string"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// A column Calcite holds as <see cref="SqlTypeName.CHAR"/> or <see cref="SqlTypeName.VARCHAR"/> need
        /// not be a string to the provider: SQL Server hands back a <see cref="Guid"/> for a
        /// <c>uniqueidentifier</c>, which <c>AdoTable</c> types as <c>CHAR(36)</c>, and
        /// <see cref="DbDataReader.GetString"/> casts rather than converts and refuses it. Formatting the
        /// value is what the type says it is.
        /// </remarks>
        public static object? GetString(DbDataReader reader, int index) => AdoReaderMapping.Read(reader, index, CalciteValues.ToChar);

        /// <summary>
        /// Gets the native provider value.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetValue(DbDataReader reader, int index)
        {
            ArgumentNullException.ThrowIfNull(reader);

            if (reader.IsDBNull(index))
                return null;

            var value = reader.GetValue(index);
            return value == DBNull.Value ? null : value;
        }

    }

}
