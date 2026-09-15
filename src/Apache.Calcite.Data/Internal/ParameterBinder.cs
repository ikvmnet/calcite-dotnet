using System;
using System.Collections.Generic;
using System.Data;

using Apache.Calcite.Data.Common;
using Apache.Calcite.Extensions.Prepare;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Converts the values an ADO.NET caller bound into the representations Calcite's runtime holds them
    /// in.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Calcite's dynamic parameters are positional and reach a plan through the <c>DataContext</c> as
    /// <c>?0</c>, <c>?1</c>, … A value handed over as it stood would be a .NET object loose in a plan whose
    /// row types are Java classes, and the first thing to compare it against something would fail. So every
    /// value crosses through the session's type mappings, which is the same boundary a result value crosses
    /// coming back.
    /// </para>
    /// <para>
    /// <b>A parameter arrives as the type the validator inferred for its placeholder, not the one the
    /// caller named.</b> Calcite refuses a placeholder it cannot infer a type for, so by the time there is
    /// a plan there is a type, and the plan reads the value as that type whatever was said about it. Naming
    /// <see cref="DbType.Date"/> against a placeholder Calcite made a <c>TIMESTAMP</c> used to hand the plan
    /// a count of days where it read a count of milliseconds, and threw partway through the scan.
    /// </para>
    /// <para>
    /// <b>What the caller's naming does is choose the .NET reading.</b> It selects among the mappings
    /// written for the inferred type, which is how a caller reaches a conversion carried only when both
    /// types are named. <see cref="CalciteParameter"/> keeps its three ways of naming a type consistent, so
    /// the <see cref="DbType"/> carries that choice faithfully however it was stated;
    /// <see cref="DbType.Object"/> means nothing was said and the inferred type's own default is used.
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
        /// <exception cref="ClrTypeMappingException">Where nothing maps a value to its placeholder's type.</exception>
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

                // the placeholder's own type where the statement has one for this position. A statement with
                // no placeholders carries none, and a caller binding more values than the statement takes is
                // Calcite's complaint to make rather than a conversion failure here.
                var relType = inferred is not null && i < inferred.size()
                    ? ((RelDataTypeField)inferred.get(i)).getType()
                    : null;

                result[i] = registry.ToCalcite(Selected(registry, parameter.DbType, relType), relType, value);
            }

            return result;
        }

        /// <summary>
        /// Returns the .NET type a caller's naming picks, or <see langword="null"/> to take the
        /// placeholder's own default.
        /// </summary>
        /// <param name="registry">The session's type mappings.</param>
        /// <param name="dbType">The type the caller named.</param>
        /// <param name="relType">The type the validator inferred for the placeholder.</param>
        /// <returns>The CLR type, or <see langword="null"/>.</returns>
        /// <remarks>
        /// <para>
        /// <see cref="DbType.Object"/> is what a parameter reads when nothing was said about it and its
        /// value's type has no name on that list, which covers a collection, a dictionary and a type of a
        /// caller's own. It names nothing, so the placeholder's own default is used, which is what naming
        /// nothing should mean.
        /// </para>
        /// <para>
        /// <b>The naming selects, and does not constrain.</b> Where nothing carries the named type to the
        /// placeholder's, the naming is dropped rather than the value refused: a <see cref="ushort"/> bound
        /// to a placeholder Calcite inferred as <c>INTEGER</c> is a widening that fits, and there is no
        /// <c>(ushort, INTEGER)</c> entry because reading an <c>INTEGER</c> back as a <see cref="ushort"/>
        /// is a narrowing that does not. The two directions are not the same question, and a parameter is
        /// the one where the placeholder's type is the constraint and the caller's word is a preference.
        /// </para>
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
