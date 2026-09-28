using System;
using System.Collections.Generic;

using org.apache.calcite.runtime.rtti;
using org.apache.calcite.runtime.variant;
using org.apache.calcite.sql.type;

using Name = org.apache.calcite.runtime.rtti.RuntimeTypeInformation.RuntimeSqlTypeName;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Reads a Calcite <c>VARIANT</c> as the .NET value it holds.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Calcite's runtime holds a <c>VARIANT</c> as a <c>VariantValue</c>: <c>VariantNonNull</c> for a value,
    /// <c>VariantSqlNull</c> for the SQL null of a declared type, <c>VariantNull</c> for the variant's own
    /// null. As with <see cref="SqlTypeName.ANY"/>, the payload's own type decides how it is read.
    /// <c>VariantClrTypeMapping</c> is the reading result columns use; this class is reached only when
    /// <see cref="CalciteValues.TryConvertTo"/> meets a variant.
    /// </para>
    /// <para>
    /// A scalar is read with <c>getTypeString()</c>, which names the payload's type, and <c>cast()</c> to a
    /// <c>BasicSqlTypeRtti</c> of that same type, which returns the payload in Calcite's storage form for
    /// <see cref="CalciteValues.FromScalar"/> to decode. <c>cast</c> is Calcite's SQL cast and converts (a
    /// <c>DOUBLE</c> 1.5 cast to <c>BIGINT</c> is 1), so it is only called with the variant's own type.
    /// </para>
    /// <para>
    /// An array is read with <c>item(1)</c>, <c>item(2)</c>, … until <c>item</c> answers null, because a
    /// variant records only <c>ARRAY</c> and not its element type. A map's keys are read by casting to
    /// <c>MAP&lt;VARCHAR, VARCHAR&gt;</c>, which returns the keys without their values, and each value by
    /// <c>item(key)</c>; keys that are not character values do not survive that cast and are refused.
    /// </para>
    /// <para>
    /// A <c>MULTISET</c> answers null to every <c>item</c> and a <c>ROW</c> only to field names the variant
    /// does not carry, and Calcite offers no other public route to either, so <see cref="ToClr"/> refuses
    /// both, naming the type.
    /// </para>
    /// </remarks>
    internal static class CalciteVariants
    {

        /// <summary>
        /// Returns whether a value is one of the two nulls a variant can be.
        /// </summary>
        /// <param name="value">The value to test.</param>
        /// <returns><see langword="true"/> for a <c>VariantSqlNull</c> (a SQL null of a declared type) or a
        /// <c>VariantNull</c> (the variant's own null, which a JSON <c>null</c> parses to).</returns>
        public static bool IsNull(object? value)
        {
            return value is VariantNull || value is VariantSqlNull;
        }

        /// <summary>
        /// Returns the .NET value a variant holds.
        /// </summary>
        /// <param name="value">The variant.</param>
        /// <returns>The .NET value, or <see langword="null"/> where the variant is null.</returns>
        /// <exception cref="InvalidCastException">The payload is a type whose contents Calcite gives no public
        /// way to read, or a map whose keys are not character values.</exception>
        public static object? ToClr(VariantValue value)
        {
            ArgumentNullException.ThrowIfNull(value);

            if (IsNull(value))
                return null;

            var name = value.getTypeString();

            switch (name)
            {
                case nameof(Name.ARRAY):
                    return Elements(value);

                case nameof(Name.MAP):
                    return Entries(value);
            }

            if (Scalar(name) is not Name scalar)
                throw new InvalidCastException($"A variant holding a {name} cannot be read: Calcite exposes no way to reach its contents.");

            return CalciteValues.FromScalar(name, value.cast(new BasicSqlTypeRtti(scalar)));
        }

        /// <summary>
        /// Returns the runtime type a name stands for, where it is a scalar whose payload can be read.
        /// </summary>
        /// <param name="name">The name <c>getTypeString()</c> returned.</param>
        /// <returns>The type, or <see langword="null"/> where the name is not a readable scalar.</returns>
        /// <remarks>
        /// The set is listed explicitly, so a name outside it is refused rather than cast. Matching on names
        /// rather than ordinals keeps it independent of the enum's declaration order. For every type
        /// <see cref="CalciteValues.FromScalar"/> decodes, these names are the same as
        /// <see cref="SqlTypeName"/>'s.
        /// </remarks>
        static Name? Scalar(string name)
        {
            return name switch
            {
                nameof(Name.BOOLEAN) => Name.BOOLEAN,
                nameof(Name.TINYINT) => Name.TINYINT,
                nameof(Name.SMALLINT) => Name.SMALLINT,
                nameof(Name.INTEGER) => Name.INTEGER,
                nameof(Name.BIGINT) => Name.BIGINT,
                nameof(Name.UTINYINT) => Name.UTINYINT,
                nameof(Name.USMALLINT) => Name.USMALLINT,
                nameof(Name.UINTEGER) => Name.UINTEGER,
                nameof(Name.UBIGINT) => Name.UBIGINT,
                nameof(Name.DECIMAL) => Name.DECIMAL,
                nameof(Name.REAL) => Name.REAL,
                nameof(Name.DOUBLE) => Name.DOUBLE,
                nameof(Name.DATE) => Name.DATE,
                nameof(Name.TIME) => Name.TIME,
                nameof(Name.TIME_WITH_LOCAL_TIME_ZONE) => Name.TIME_WITH_LOCAL_TIME_ZONE,
                nameof(Name.TIME_TZ) => Name.TIME_TZ,
                nameof(Name.TIMESTAMP) => Name.TIMESTAMP,
                nameof(Name.TIMESTAMP_WITH_LOCAL_TIME_ZONE) => Name.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                nameof(Name.TIMESTAMP_TZ) => Name.TIMESTAMP_TZ,
                nameof(Name.VARCHAR) => Name.VARCHAR,
                nameof(Name.VARBINARY) => Name.VARBINARY,
                nameof(Name.UUID) => Name.UUID,
                _ => null,
            };
        }

        /// <summary>
        /// Returns the elements of an array variant, converted.
        /// </summary>
        static Array Elements(VariantValue value)
        {
            var items = new List<object?>();

            // one-based, and null past the end; a variant offers no length
            for (var i = 1; value.item(java.lang.Integer.valueOf(i)) is VariantValue element; i++)
                items.Add(ToClr(element));

            return CalciteValues.Pack(items.ToArray());
        }

        /// <summary>
        /// Returns the entries of a map variant, converted.
        /// </summary>
        static object Entries(VariantValue value)
        {
            // the cast answers the keys and drops the values; item() then reads each value by its key
            var arguments = new RuntimeTypeInformation[] { new BasicSqlTypeRtti(Name.VARCHAR), new BasicSqlTypeRtti(Name.VARCHAR) };
            if (value.cast(new GenericSqlTypeRtti(Name.MAP, arguments)) is not java.util.Map map)
                throw new InvalidCastException("A variant holding a MAP cannot be read: its keys did not answer as character values.");

            var keys = new List<object?>();
            var values = new List<object?>();

            for (var i = map.keySet().iterator(); i.hasNext();)
            {
                var key = i.next();
                if (key is null)
                    throw new InvalidCastException("A variant holding a MAP whose keys are not character values cannot be read: Calcite exposes no way to enumerate them.");

                keys.Add(CalciteValues.ToClr(key, null));
                values.Add(value.item(key) is VariantValue item ? ToClr(item) : null);
            }

            return CalciteValues.PackMap(keys.ToArray(), values.ToArray());
        }

    }

}
