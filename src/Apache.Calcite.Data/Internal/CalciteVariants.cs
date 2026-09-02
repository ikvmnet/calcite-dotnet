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
    /// A <c>VARIANT</c> is a value that carries its own type. Calcite's runtime holds one as a
    /// <c>VariantValue</c> — <c>VariantNonNull</c> for a value, <c>VariantSqlNull</c> for the SQL null of
    /// a declared type, <c>VariantNull</c> for the variant's own null — and none of those is a type a
    /// .NET consumer can be handed, so none of them leaves this class. It is <see cref="SqlTypeName.ANY"/>
    /// again with the type written down rather than absent, and it reads the same way: the payload's own
    /// type is what says what the payload is.
    ///
    /// <para><b>Reading the payload takes two public calls and nothing else.</b> <c>getTypeString()</c>
    /// names the payload's runtime type, and <c>cast()</c> against a <c>BasicSqlTypeRtti</c> of that same
    /// name hands the payload back. Naming its own type is the point: <c>cast</c> is Calcite's SQL cast
    /// and it converts — measured, a variant holding a <c>DOUBLE</c> of 1.5 casts to <c>BIGINT</c> as 1 —
    /// so it is only ever called here with the type the variant says it already is, which makes it a read
    /// and not a conversion. The payload comes back in Calcite's storage form, a <c>DATE</c> as a count of
    /// days like anywhere else, and <see cref="CalciteValues.FromScalar"/> decodes it by the same name.</para>
    ///
    /// <para><b>An array is walked, not cast.</b> <c>item(1)</c>, <c>item(2)</c>, … each answer a
    /// <c>VariantValue</c> of their own and <c>null</c> past the end, so the elements convert recursively
    /// and carry their own types. Casting an array would need the element type, which a variant does not
    /// record — it keeps a <c>RuntimeSqlTypeName</c>, not a full <c>RuntimeTypeInformation</c>, so
    /// <c>getTypeString()</c> on one answers <c>ARRAY</c> and nothing more.</para>
    ///
    /// <para><b>A map is met halfway.</b> There is no key enumeration, and the one thing that answers the
    /// keys is a cast to <c>MAP&lt;VARCHAR, VARCHAR&gt;</c>, which returns the keys and drops every value.
    /// The values come back one at a time through <c>item(key)</c>. That works where the keys are
    /// character values, which is what a variant map is for; a key of any other type comes back null from
    /// the cast, and rather than lose the entry this refuses.</para>
    ///
    /// <para><b>What cannot be read at all is refused, and named.</b> A <c>MULTISET</c> answers null to
    /// every <c>item</c>, and a <c>ROW</c> answers only to its field names, which the variant does not
    /// carry either. Neither has a public route to its contents in Calcite 1.42, so neither is guessed
    /// at: <see cref="ToClr"/> throws and says which one it was. Handing back the <c>VariantValue</c>
    /// would put a Java object in a caller's hands, and inventing a text form for it would be worse.</para>
    /// </remarks>
    internal static class CalciteVariants
    {

        /// <summary>
        /// Returns whether a value is one of the two nulls a variant can be.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>VariantSqlNull</c> is a SQL null that remembers the type it was null of;
        /// <c>VariantNull</c> is the variant type's own null, the one a JSON <c>null</c> parses to. An
        /// ADO.NET caller has one null and both are it.
        /// </remarks>
        public static bool IsNull(object? value)
        {
            return value is VariantNull || value is VariantSqlNull;
        }

        /// <summary>
        /// Returns the .NET value a variant holds.
        /// </summary>
        /// <param name="value">The variant.</param>
        /// <returns>The .NET value, or <see langword="null"/> where the variant is null.</returns>
        /// <exception cref="InvalidCastException">Where the payload has no public route to its contents.</exception>
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
        /// Returns the runtime type a name stands for, where it is one whose payload can be read.
        /// </summary>
        /// <param name="name"></param>
        /// <returns>The type, or <see langword="null"/> where the name is not a readable scalar.</returns>
        /// <remarks>
        /// Written out rather than resolved through the enum's <c>valueOf</c>, which is what this project
        /// does with a Java enum in any case, and which here also states the set: a name that is not on
        /// it is refused rather than cast blindly. The names are <see cref="SqlTypeName"/>'s own for every
        /// type <see cref="CalciteValues.FromScalar"/> decodes, which is why passing this name to it
        /// works.
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

            // one-based, and null past the end, which is the only length a variant offers
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
