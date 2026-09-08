using System;
using System.Data.Common;

using Apache.Calcite.Data.Types;

using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Reads a cell of an ADO.NET reader as the representation Calcite holds its type in.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Every conversion here is a <see cref="ClrTypeRegistry"/>'s. What a provider hands back is a CLR
    /// value and what a plan reads is the representation Calcite holds that type in, which is the same
    /// crossing a command parameter makes in the other direction — and the two were separate tables that
    /// disagreed about whether to convert or to cast.
    /// </para>
    /// <para>
    /// <b>The mapping is the schema's, and both generated routes fetch it off the schema.</b> A plan of
    /// <c>EnumerableConvention</c> is Java source naming
    /// <c>cli.Apache.Calcite.Adapter.AdoNet.AdoReaderUtil.GetDbReaderValue</c>, and it reaches the
    /// registry through <c>Schemas.unwrap</c> — the same call it has always used to reach the
    /// <see cref="AdoDataSource"/>, which is a CLR class named the same way.
    /// </para>
    /// <para>
    /// This class was split in two while a member naming a <see cref="ClrTypeRegistry"/> was believed to
    /// break Janino's resolution of every call to it. That was measured, but at IKVM 8.15.0, inside the
    /// window where <c>CustomAssemblyClassLoaderAttribute</c> was internal. It does not reproduce at
    /// 8.16.0: measured again, with the registry in this class's signatures and in the generated block,
    /// against Calcite's own connection and both of this provider's modes. What caused it at 8.15.0 is
    /// not isolated, and no member here is written around it any more.
    /// </para>
    /// </remarks>
    public static class AdoReaderUtil
    {

        /// <summary>
        /// The mapping used where a caller supplies none.
        /// </summary>
        /// <remarks>
        /// Bound to a type factory of its own, because a schema is shared by connections that each have
        /// one and what a mapping is checked against must not depend on which of them asked. The built-in
        /// mappings do not depend on it either way: a <c>TIMESTAMP</c> is a count of milliseconds in a
        /// <c>Long</c> whoever built the type.
        /// </remarks>
        public static ClrTypeRegistry Default { get; } = new ClrTypeMapper().Bind(new JavaTypeFactoryImpl());

        /// <summary>
        /// Gets a value from the reader in the representation the supplied mapping holds the type in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <param name="type"></param>
        /// <param name="registry"></param>
        /// <returns></returns>
        /// <remarks>
        /// The overload both generated routes call. The registry arrives as an expression fetching it off
        /// the schema, and the type as the whole <see cref="RelDataType"/> — a constant in an expression
        /// tree, and <c>EnumerableRelImplementor.stash</c> in a block of Java source, which hands the
        /// object to the generated class rather than trying to write it out.
        /// </remarks>
        public static object? GetDbReaderValue(DbDataReader reader, int index, RelDataType type, ClrTypeRegistry registry)
        {
            ArgumentNullException.ThrowIfNull(reader);
            ArgumentNullException.ThrowIfNull(type);
            ArgumentNullException.ThrowIfNull(registry);

            if (reader.IsDBNull(index))
                return null;

            // OTHER is the adapter's escape hatch and the one type read without conversion: AdoSchema
            // types a provider column it cannot name as one so that the rest of the table stays readable,
            // and whatever the provider handed over is the only representation of it there is. Converting
            // would be a guess -- a DateTime would become a count of milliseconds and read back as a
            // number -- and it is not what the mapping means by OTHER either, since a bare Object is that
            // type too and a value bound to one does have to cross.
            if (type.getSqlTypeName().name() == nameof(SqlTypeName.OTHER))
                return reader.GetValue(index) is var other && other == DBNull.Value ? null : other;

            try
            {
                return registry.ToCalcite(null, type, reader.GetValue(index));
            }
            catch (ClrTypeMappingException e)
            {
                // the adapter answers in its own exception. The chain always has an answer for a type -- a
                // type nothing claims falls to the catch-all, which reads the value's own class -- so what
                // reaches here is a mapping that refused the value or answered with the wrong class, not an
                // unnamed type
                throw new AdoCalciteException($"Cannot read column {index} as {type.getFullTypeString()}.", e);
            }
        }

        /// <summary>
        /// Builds the Calcite type a name alone stands for, for the route that carries only a name.
        /// </summary>
        /// <param name="typeName"></param>
        /// <returns></returns>
        /// <remarks>
        /// The facets are not needed to pick a conversion — a <c>VARCHAR(16)</c> and a <c>VARCHAR(255)</c>
        /// are both read as a string.
        /// </remarks>
        internal static RelDataType TypeOf(SqlTypeName typeName)
        {
            return Default.TypeFactory.createTypeWithNullability(Default.TypeFactory.createSqlType(typeName), true);
        }

        /// <summary>
        /// Reads a value and converts it, or answers <see langword="null"/> where the column is null.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <param name="convert"></param>
        /// <returns></returns>
        internal static object? Read(DbDataReader reader, int index, Func<object, object> convert)
        {
            ArgumentNullException.ThrowIfNull(reader);

            return reader.IsDBNull(index) ? null : convert(reader.GetValue(index));
        }

        /// <summary>
        /// Gets a value from the reader in the representation Calcite holds the supplied type in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static object? GetDbReaderValue(DbDataReader reader, int index, RelDataType type)
        {
            return GetDbReaderValue(reader, index, type, Default);
        }

        /// <summary>
        /// Gets a value from the reader in the representation Calcite holds the named type in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <param name="typeName"></param>
        /// <returns></returns>
        /// <remarks>
        /// A name and nothing else, so the facets and a collection's component type are lost. Neither
        /// generated route needs it — both carry the whole <see cref="RelDataType"/> — and it is here for
        /// a caller that has only a name.
        /// </remarks>
        public static object? GetDbReaderValue(DbDataReader reader, int index, SqlTypeName typeName)
        {
            ArgumentNullException.ThrowIfNull(typeName);

            return GetDbReaderValue(reader, index, TypeOf(typeName));
        }

        /// <summary>
        /// Gets a <see cref="java.lang.Boolean"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetBoolean(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToBoolean);

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
        public static object? GetByte(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToTinyInt);

        /// <summary>
        /// Gets a <see cref="java.lang.Short"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetShort(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToSmallInt);

        /// <summary>
        /// Gets a <see cref="java.lang.Integer"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetInt(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToInteger);

        /// <summary>
        /// Gets a <see cref="java.lang.Long"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetLong(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToBigInt);

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
        public static object? GetUByte(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToUTinyInt);

        /// <summary>
        /// Gets an <see cref="org.joou.UShort"/>, which is what Calcite holds a <c>USMALLINT</c> in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetUShort(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToUSmallInt);

        /// <summary>
        /// Gets an <see cref="org.joou.UInteger"/>, which is what Calcite holds a <c>UINTEGER</c> in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetUInt(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToUInteger);

        /// <summary>
        /// Gets an <see cref="org.joou.ULong"/>, which is what Calcite holds a <c>UBIGINT</c> in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetULong(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToUBigInt);

        /// <summary>
        /// Gets a <see cref="java.lang.Double"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetDouble(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToDouble);

        /// <summary>
        /// Gets a <see cref="java.lang.Float"/>.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetFloat(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToReal);

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
        public static object? GetDate(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToDate);

        /// <summary>
        /// Gets a <see cref="SqlTypeName.TIMESTAMP"/> in Calcite's internal representation.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetTimestamp(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToTimestamp);

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
        public static object? GetTimestampTz(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToTimestampTz);

        /// <summary>
        /// Gets a <see cref="SqlTypeName.DECIMAL"/> as the <see cref="java.math.BigDecimal"/> Calcite holds
        /// one in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetDecimal(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToDecimal);

        /// <summary>
        /// Gets a <see cref="SqlTypeName.VARBINARY"/> as the <c>ByteString</c> Calcite holds one in.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static object? GetBinary(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToBinary);

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
        public static object? GetTime(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToTime);

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
        public static object? GetString(DbDataReader reader, int index) => Read(reader, index, CalciteValues.ToChar);

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
