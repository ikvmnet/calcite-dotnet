using System;
using System.Collections.Generic;
using System.Data;

using Apache.Calcite.Data.Common;
using Apache.Calcite.Extensions.Prepare;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Converts <see cref="CalciteParameterValue"/> entries supplied by the ADO.NET layer into the
    /// representations Calcite holds them in.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Calcite's dynamic parameters are positional (<c>?</c>) and exposed at execution time through the
    /// <c>DataContext</c> as <c>?0</c>, <c>?1</c>, and so on. What each value becomes is the connection's
    /// <see cref="ClrTypeRegistry"/>, the same table the reader answers with; this decides only which
    /// question to ask it.
    /// </para>
    /// <para>
    /// <b>The type the plan will read the slot as is the validator's, not the caller's.</b> Calcite refuses
    /// a placeholder whose type it cannot infer from the SQL around it — <c>VALUES (?)</c> is an illegal
    /// use of a dynamic parameter — so by the time there is a plan there is a type for every parameter, and
    /// the signature carries it. A caller's <see cref="DbType"/> is a second opinion about the same slot,
    /// and where the two disagree it is the validator's that the generated code was built against: binding
    /// <see cref="DbType.Date"/> to a placeholder Calcite inferred as <c>TIMESTAMP</c> used to hand a count
    /// of days in an <c>Integer</c> to a plan reading a count of milliseconds from a <c>Long</c>, which
    /// threw an <see cref="InvalidCastException"/> partway through the scan. The <see cref="DbType"/> is
    /// still consulted — it is how a caller reaches a conversion written for that pair of types — but it
    /// chooses among the mappings for the type Calcite decided rather than replacing it.
    /// </para>
    /// </remarks>
    internal static class ParameterBinder
    {

        /// <summary>
        /// Converts a list of <see cref="CalciteParameterValue"/> instances into the representations passed
        /// to the <c>DataContext</c> at execution time.
        /// </summary>
        /// <param name="registry">The connection's type mapping.</param>
        /// <param name="signature">The prepared statement, for the types the validator inferred.</param>
        /// <param name="parameters">The parameter values to bind. May be <see langword="null"/> or empty.</param>
        /// <returns>
        /// An array of converted values in positional order, or an empty array where there are none.
        /// </returns>
        public static IReadOnlyList<object?> Bind(ClrTypeRegistry registry, IClrPrepare.Signature signature, IReadOnlyList<CalciteParameterValue> parameters)
        {
            ArgumentNullException.ThrowIfNull(registry);
            ArgumentNullException.ThrowIfNull(signature);

            if (parameters is null || parameters.Count == 0)
                return Array.Empty<object?>();

            var fields = signature.ParameterRowType?.getFieldList();

            var result = new object?[parameters.Count];
            for (var i = 0; i < parameters.Count; i++)
            {
                var inferred = fields is not null && i < fields.size() ? ((RelDataTypeField)fields.get(i)).getType() : null;
                result[i] = Convert(registry, parameters[i], inferred);
            }

            return result;
        }

        /// <summary>
        /// Converts a single <see cref="CalciteParameterValue"/>.
        /// </summary>
        /// <param name="registry"></param>
        /// <param name="p"></param>
        /// <param name="inferred">The type the validator gave the placeholder, where there is one.</param>
        /// <returns></returns>
        static object? Convert(ClrTypeRegistry registry, CalciteParameterValue p, RelDataType? inferred)
        {
            var value = p.Value;
            if (value is null || value is DBNull)
                return null;

            // DbType.Object is ADO.NET's way of saying nothing was stated, so the value's own type is what
            // the caller meant
            var clrType = p.DbType == DbType.Object ? value.GetType() : DbTypeMap.ToClrType(p.DbType);

            if (inferred is null)
                return registry.ToCalcite(clrType, null, value);

            // the conversion written for this pair if there is one, and otherwise whatever the inferred type
            // is written as -- which converts rather than casts, so a caller who wrote an int into a slot
            // Calcite made a BIGINT is carried across rather than refused
            var mapping = registry.GetMapping(clrType, inferred) ?? registry.RequireMapping(null, inferred);
            return mapping.ConvertToCalcite(value);
        }

    }

}
