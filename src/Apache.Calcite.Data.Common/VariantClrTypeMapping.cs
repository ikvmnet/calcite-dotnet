using System;
using System.Collections.Generic;

using org.apache.calcite.rel.type;
using org.apache.calcite.runtime.rtti;
using org.apache.calcite.runtime.variant;
using org.apache.calcite.sql.type;

using Name = org.apache.calcite.runtime.rtti.RuntimeTypeInformation.RuntimeSqlTypeName;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// A <c>VARIANT</c>, read by asking the value what type it is and mapping it as that.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A <c>VARIANT</c> carries its own type, which is <c>ANY</c> written the other way round: <c>ANY</c>
    /// says nothing and leaves the runtime class to decide, a variant says it per value. Calcite holds one
    /// as a <c>VariantValue</c> — <c>VariantNonNull</c> for a value, <c>VariantSqlNull</c> for the SQL null
    /// of a declared type, <c>VariantNull</c> for the variant's own null — and none of those may be handed
    /// to a caller.
    /// </para>
    /// <para>
    /// <b>Reading the payload takes two public calls.</b> <c>getTypeString()</c> names the payload's type
    /// and <c>cast()</c> against a <c>BasicSqlTypeRtti</c> of that same name hands the payload back. Naming
    /// its own type is the point: <c>cast</c> is Calcite's SQL cast and it converts, a <c>DOUBLE</c> of 1.5
    /// casting to <c>BIGINT</c> as 1, so it is only ever called here with the type the variant says it
    /// already is, which makes it a read and not a conversion. What comes back is Calcite's storage form, a
    /// <c>DATE</c> as a count of days like anywhere else, and the registry decodes it by the same name.
    /// </para>
    /// <para>
    /// <b>The registry decodes it, which is what makes this different from reading a variant by hand.</b>
    /// A payload named <c>INTEGER</c> is carried across by whatever mapping the chain answers for
    /// <c>INTEGER</c>, so a resolver a caller put in front applies inside a variant exactly as it does to a
    /// column of that type.
    /// </para>
    /// <para>
    /// <b>An array is walked, not cast.</b> <c>item(1)</c>, <c>item(2)</c>, … each answer a
    /// <c>VariantValue</c> of their own and null past the end, so elements convert recursively and carry
    /// their own types. Casting one would need the element type, and a variant keeps a
    /// <c>RuntimeSqlTypeName</c> rather than a full <c>RuntimeTypeInformation</c>: <c>getTypeString()</c>
    /// answers <c>ARRAY</c> and nothing more.
    /// </para>
    /// <para>
    /// <b>A map is met halfway.</b> Nothing enumerates the keys except a cast to
    /// <c>MAP&lt;VARCHAR, VARCHAR&gt;</c>, which answers the keys and drops every value; the values come
    /// back one at a time through <c>item(key)</c>. That works where the keys are character values, which
    /// is what a variant map is for, and a key of any other type comes back null from that cast.
    /// </para>
    /// <para>
    /// <b>What has no public route to its contents is refused and named.</b> A <c>MULTISET</c> answers null
    /// to every <c>item</c> and a <c>ROW</c> only to field names the variant does not carry, so neither is
    /// guessed at. Handing back the <c>VariantValue</c> would put a Java object in a caller's hands and
    /// inventing a text form for it would be worse.
    /// </para>
    /// </remarks>
    public sealed class VariantClrTypeMapping : ClrTypeMapping
    {

        readonly ClrTypeContext _context;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="relType">The <c>VARIANT</c> type.</param>
        /// <remarks>
        /// <see cref="ClrTypeMapping.ClrType"/> is <see cref="object"/>, because a variant's type is a
        /// property of each value and not of the column: what any one value reads back as is whatever its
        /// own payload type reads back as, and the column cannot promise which that will be.
        /// </remarks>
        public VariantClrTypeMapping(ClrTypeContext context, RelDataType relType) :
            base(context, relType, typeof(object))
        {
            _context = context;
        }

        /// <inheritdoc />
        /// <remarks>
        /// <c>VariantSqlNull</c> is a SQL null that remembers the type it was null of and
        /// <c>VariantNull</c> is the variant type's own null, the one a JSON <c>null</c> parses to. An
        /// ADO.NET caller has one null and both are it.
        /// </remarks>
        public override bool IsNull(object value)
        {
            return value is VariantNull or VariantSqlNull;
        }

        /// <inheritdoc />
        /// <remarks>
        /// A variant carries its payload's type with the payload, so the column says nothing and the value
        /// says everything.
        /// </remarks>
        public override bool DescribesValue => false;

        /// <inheritdoc />
        /// <remarks>
        /// Not supported. Calcite builds a variant with its <c>VARIANT</c> constructor inside a plan, and
        /// there is no public route to one from outside; a caller writes the payload's own type and casts
        /// in SQL.
        /// </remarks>
        public override object? ToCalcite(object value)
        {
            throw new ClrTypeMappingException("A VARIANT cannot be written from a .NET value: cast the payload's own type to VARIANT in SQL instead.");
        }

        /// <inheritdoc />
        public override object? FromCalcite(object value)
        {
            if (value is not VariantValue variant)
                throw new ClrTypeMappingException($"A VARIANT is held in a VariantValue, and a {value.GetType()} is not one.");

            return Read(variant);
        }

        /// <summary>
        /// Reads a variant as the .NET value its payload's own type calls for.
        /// </summary>
        /// <param name="value">The variant.</param>
        /// <returns>The .NET value, or <see langword="null"/> where the variant is null.</returns>
        /// <exception cref="ClrTypeMappingException">Where the payload has no public route to its contents.</exception>
        object? Read(VariantValue value)
        {
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
                throw new ClrTypeMappingException($"A variant holding a {name} cannot be read: Calcite exposes no way to reach its contents.");

            var payload = value.cast(new BasicSqlTypeRtti(scalar));
            if (payload is null)
                return null;

            // the payload's own type, carried across by whatever the chain answers for it, so a resolver a
            // caller registered reaches inside a variant too
            var relType = _context.TypeFactory.createTypeWithNullability(
                _context.TypeFactory.createSqlType(SqlTypeName.valueOf(name)), true);

            return _context.Registry.RequireMapping(null, relType).FromCalcite(payload);
        }

        /// <summary>
        /// Returns the runtime type a name stands for, where it is one whose payload can be read.
        /// </summary>
        /// <param name="name"></param>
        /// <returns>The type, or <see langword="null"/> where the name is not a readable scalar.</returns>
        /// <remarks>
        /// Written out rather than resolved through the enum's <c>valueOf</c>, which is what this project
        /// does with a Java enum in any case, and which here also states the set: a name that is not on it
        /// is refused rather than cast blindly. The names are <see cref="SqlTypeName"/>'s own for every
        /// type on the list, which is why passing the same name to the type factory works.
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
        /// <param name="value">The variant.</param>
        /// <returns>An array of the type the converted elements share.</returns>
        Array Elements(VariantValue value)
        {
            var items = new List<object?>();

            // one-based, and null past the end, which is the only length a variant offers
            for (var i = 1; value.item(java.lang.Integer.valueOf(i)) is VariantValue element; i++)
                items.Add(Read(element));

            return Pack(items);
        }

        /// <summary>
        /// Returns the entries of a map variant, converted.
        /// </summary>
        /// <param name="value">The variant.</param>
        /// <returns>A dictionary of the types the converted entries share.</returns>
        /// <exception cref="ClrTypeMappingException">Where the keys are not character values.</exception>
        object Entries(VariantValue value)
        {
            // the cast answers the keys and drops the values; item() then reads each value by its key
            var arguments = new RuntimeTypeInformation[] { new BasicSqlTypeRtti(Name.VARCHAR), new BasicSqlTypeRtti(Name.VARCHAR) };
            if (value.cast(new GenericSqlTypeRtti(Name.MAP, arguments)) is not java.util.Map map)
                throw new ClrTypeMappingException("A variant holding a MAP cannot be read: its keys did not answer as character values.");

            var keys = new List<object?>();
            var values = new List<object?>();

            for (var i = map.keySet().iterator(); i.hasNext();)
            {
                var key = i.next();
                if (key is null)
                    throw new ClrTypeMappingException("A variant holding a MAP whose keys are not character values cannot be read: Calcite exposes no way to enumerate them.");

                keys.Add(key as string ?? key.ToString());
                values.Add(value.item(key) is VariantValue item ? Read(item) : null);
            }

            var dictionary = (System.Collections.IDictionary)Activator.CreateInstance(
                typeof(Dictionary<,>).MakeGenericType(typeof(string), Unify(values)), keys.Count)!;

            for (var i = 0; i < keys.Count; i++)
                dictionary[keys[i]!] = values[i];

            return dictionary;
        }

        /// <summary>
        /// Returns converted elements as an array of the type they share.
        /// </summary>
        /// <param name="items"></param>
        /// <returns>The array.</returns>
        /// <remarks>
        /// Measured from the values rather than declared, because a variant records no element type: it
        /// keeps a <c>RuntimeSqlTypeName</c>, so an array variant says <c>ARRAY</c> and nothing about what
        /// is in it. This is the one place where measuring is the only thing available.
        /// </remarks>
        static Array Pack(List<object?> items)
        {
            var element = Unify(items);
            var array = Array.CreateInstance(element, items.Count);
            for (var i = 0; i < items.Count; i++)
                array.SetValue(items[i], i);

            return array;
        }

        /// <summary>
        /// Returns the type every value has, or <see cref="object"/> where they do not agree on one.
        /// </summary>
        /// <param name="items"></param>
        /// <returns>The shared type.</returns>
        /// <remarks>
        /// A null among values of a value type makes the type nullable rather than <see cref="object"/>, so
        /// an array holding a null still names what it holds. An empty sequence has no type to read and is
        /// <see cref="object"/>.
        /// </remarks>
        static Type Unify(List<object?> items)
        {
            Type? common = null;
            var nulls = false;

            foreach (var item in items)
            {
                if (item is null)
                {
                    nulls = true;
                    continue;
                }

                var type = item.GetType();
                if (common is null)
                    common = type;
                else if (common != type)
                    return typeof(object);
            }

            if (common is null)
                return typeof(object);

            return nulls && common.IsValueType ? typeof(Nullable<>).MakeGenericType(common) : common;
        }

    }

}
