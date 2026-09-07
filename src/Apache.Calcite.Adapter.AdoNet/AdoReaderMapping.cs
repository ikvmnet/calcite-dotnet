using System;
using System.Data.Common;

using Apache.Calcite.Data.Types;

using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Reads a cell of an ADO.NET reader as the representation Calcite holds its type in.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Every member that names a <see cref="ClrTypeRegistry"/> is here rather than on
    /// <see cref="AdoReaderUtil"/>, and that is a hard constraint rather than tidiness. A plan of
    /// <c>EnumerableConvention</c> is Java source naming
    /// <c>cli.Apache.Calcite.Adapter.AdoNet.AdoReaderUtil.GetDbReaderValue</c>, and Janino resolves that
    /// call by reflecting over the class and loading the type of every member it declares — so one
    /// signature its classloader cannot name breaks every generated reader, including calls to methods that
    /// have nothing to do with it. Measured: an overload taking a <see cref="ClrTypeRegistry"/> made all of
    /// them fail with <c>Cannot load class "cli.Apache.Calcite.Data.Types.ClrTypeRegistry" through the
    /// given ClassLoader</c>.
    /// </para>
    /// <para>
    /// The consequence is that a per-connection mapping cannot reach the generated route at all, only this
    /// one. A caller that wants its own on a scan calls in here.
    /// </para>
    /// </remarks>
    public static class AdoReaderMapping
    {

        /// <summary>
        /// The mapping used where a caller supplies none.
        /// </summary>
        /// <remarks>
        /// Bound to a type factory of its own, because the route generated code takes carries a
        /// <c>SqlTypeName</c> constant and has nowhere to put a session's. The built-in mappings do not
        /// depend on which factory asked: a <c>TIMESTAMP</c> is a count of milliseconds in a <c>Long</c>
        /// whoever built the type.
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
            if (type.getSqlTypeName().name() == nameof(org.apache.calcite.sql.type.SqlTypeName.OTHER))
                return reader.GetValue(index) is var value && value == DBNull.Value ? null : value;

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
        internal static RelDataType TypeOf(org.apache.calcite.sql.type.SqlTypeName typeName)
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

    }

}
