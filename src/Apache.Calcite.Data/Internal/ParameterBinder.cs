using System;
using System.Collections.Generic;
using System.Data;

using Apache.Calcite.Data.Common;
using Apache.Calcite.Extensions.Prepare;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Converts the parameter values an ADO.NET caller bound into the representations Calcite's runtime
    /// holds them in.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Calcite's dynamic parameters are positional and reach a plan through the <c>DataContext</c> as
    /// <c>?0</c>, <c>?1</c>, … Every value goes through the session's type mappings, as result values do in
    /// the other direction, because a .NET object in a plan whose row types are Java classes fails the first
    /// comparison it meets.
    /// </para>
    /// <para>
    /// A value is converted to the type the validator inferred for its placeholder, not the type the caller
    /// named: the plan reads it as the inferred type regardless. The caller's <see cref="DbType"/> selects
    /// which .NET type the value is converted from, among the mappings the registry has for the inferred
    /// type; <see cref="DbType.Object"/> means none was named and the inferred type's default mapping is
    /// used.
    /// </para>
    /// </remarks>
    internal static class ParameterBinder
    {

        /// <summary>
        /// Converts the bound values into the representations passed to the <c>DataContext</c>.
        /// </summary>
        /// <param name="parameters">The values as the caller bound them. May be empty.</param>
        /// <param name="registry">The session's type mappings.</param>
        /// <param name="signature">The prepared statement, which carries the inferred placeholder types.</param>
        /// <returns>The converted values in positional order.</returns>
        /// <exception cref="ClrTypeMappingException">No mapping converts a value to its placeholder's type.</exception>
        /// <remarks>
        /// <see langword="null"/> and <see cref="DBNull"/> bind as SQL null. A value beyond the statement's
        /// placeholders is converted by its <see cref="DbType"/>, or its own type, and left for Calcite to reject.
        /// </remarks>
        public static IReadOnlyList<object?> Bind(IReadOnlyList<CalciteParameterValue> parameters, ClrTypeRegistry registry, IClrPrepare.Signature signature)
        {
            ArgumentNullException.ThrowIfNull(registry);
            ArgumentNullException.ThrowIfNull(signature);

            if (parameters is null || parameters.Count == 0)
                return Array.Empty<object?>();

            var inferred = signature.ParameterRowType?.getFieldList();

            var result = new object?[parameters.Count];
            for (var i = 0; i < parameters.Count; i++)
            {
                var parameter = parameters[i];
                var value = parameter.Value;

                // both spellings of a SQL null, and neither reaches a mapping
                if (value is null || value is DBNull)
                {
                    result[i] = null;
                    continue;
                }

                // the placeholder's type where the statement has one at this position; an extra value is
                // Calcite's to reject, not a conversion failure here
                var relType = inferred is not null && i < inferred.size()
                    ? ((RelDataTypeField)inferred.get(i)).getType()
                    : null;

                result[i] = registry.ToCalcite(Selected(registry, parameter.DbType, relType), relType, value);
            }

            return result;
        }

        /// <summary>
        /// Returns the .NET type the caller's <see cref="DbType"/> selects, or <see langword="null"/> to use
        /// the placeholder type's default mapping.
        /// </summary>
        /// <param name="registry">The session's type mappings.</param>
        /// <param name="dbType">The type the caller named.</param>
        /// <param name="relType">The type the validator inferred for the placeholder, or <see langword="null"/>.</param>
        /// <returns>The CLR type, or <see langword="null"/>.</returns>
        /// <remarks>
        /// <see cref="DbType.Object"/>, which is also what a value of an unlisted type infers, selects
        /// nothing. Where the registry has no mapping between the named type and the placeholder's type, the
        /// named type is ignored rather than the value refused: a <see cref="ushort"/> bound to an
        /// <c>INTEGER</c> placeholder has no <c>(ushort, INTEGER)</c> mapping, because reading an
        /// <c>INTEGER</c> as a <see cref="ushort"/> would narrow, yet the value itself fits.
        /// </remarks>
        static Type? Selected(ClrTypeRegistry registry, DbType dbType, RelDataType? relType)
        {
            if (dbType == DbType.Object)
                return null;

            var clrType = CalciteTypeMap.ToClrType(dbType);
            if (relType is null)
                return clrType;

            return registry.GetMapping(clrType, relType) is null ? null : clrType;
        }

    }

}
