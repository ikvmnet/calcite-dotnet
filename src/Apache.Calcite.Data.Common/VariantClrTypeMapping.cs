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
    /// The mapping for a <c>VARIANT</c>, which reads each value according to the type its payload carries.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Calcite holds a variant in a <c>VariantValue</c>. A scalar payload is read by casting the variant to
    /// the type it reports through <c>getTypeString()</c>, which returns the payload without converting it,
    /// and then converting that with the chain's mapping for the type, so a caller's resolvers apply inside a
    /// variant as they do to a column. A year-month interval payload reads as an <see cref="int"/> count of
    /// months and a day-time interval as a <see cref="TimeSpan"/>.
    /// </para>
    /// <para>
    /// An <c>ARRAY</c> payload reads as an array of its converted elements, whose element type is the
    /// runtime type they all share (or <see cref="object"/>), since the variant records no element type. A
    /// <c>MAP</c> payload reads as a <see cref="Dictionary{TKey, TValue}"/> with <see cref="string"/> keys,
    /// and only where its keys are character values: Calcite offers no other way to enumerate them. Any other
    /// payload type, including <c>MULTISET</c> and <c>ROW</c>, throws <see cref="ClrTypeMappingException"/>.
    /// </para>
    /// <para>
    /// A variant cannot be written from a .NET value; cast a value of the payload's type to <c>VARIANT</c> in
    /// SQL instead.
    /// </para>
    /// </remarks>
    public sealed class VariantClrTypeMapping : ClrTypeMapping
    {

        readonly ClrTypeContext _context;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="context">The context the mapping is resolved in.</param>
        /// <param name="relType">The <c>VARIANT</c> type.</param>
        /// <remarks>
        /// <see cref="ClrTypeMapping.ClrType"/> is <see cref="object"/>, since the CLR type of each value
        /// depends on that value's payload type.
        /// </remarks>
        public VariantClrTypeMapping(ClrTypeContext context, RelDataType relType) :
            base(context, relType, typeof(object))
        {
            _context = context;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Both <c>VariantSqlNull</c>, a SQL null of some declared type, and <c>VariantNull</c>, the
        /// variant's own null (for example a JSON <c>null</c>), are null.
        /// </remarks>
        public override bool IsNull(object value)
        {
            return value is VariantNull or VariantSqlNull;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Always <see langword="false"/>: each value carries its own type.
        /// </remarks>
        public override bool DescribesValue => false;

        /// <inheritdoc />
        /// <remarks>
        /// Not supported: always throws <see cref="ClrTypeMappingException"/>. Write the payload's own type and
        /// cast it to <c>VARIANT</c> in SQL.
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
        /// Converts a variant to the .NET value for its payload's type.
        /// </summary>
        /// <param name="value">The variant.</param>
        /// <returns>The .NET value, or <see langword="null"/> where the variant is null.</returns>
        /// <exception cref="ClrTypeMappingException">The payload type cannot be read.</exception>
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

                // an interval reports INTERVAL_LONG (year-month, a count of months) or INTERVAL_SHORT
                // (day-time, a count of milliseconds), neither of which is a SqlTypeName to look a mapping up
                // by, so each is decoded as the declared interval types of its family are
                case nameof(Name.INTERVAL_LONG):
                    return Interval(value, Name.INTERVAL_LONG, CalciteValues.FromIntervalMonths);

                case nameof(Name.INTERVAL_SHORT):
                    return Interval(value, Name.INTERVAL_SHORT, CalciteValues.FromIntervalTime);
            }

            if (Scalar(name) is not Name scalar)
                throw new ClrTypeMappingException($"A variant holding a {name} cannot be read: Calcite exposes no way to reach its contents.");

            var payload = value.cast(new BasicSqlTypeRtti(scalar));
            if (payload is null)
                return null;

            // converted through the chain, so that a caller's resolvers apply to the payload too; the
            // payload is in Calcite's storage form, such as a DATE's count of days
            var relType = _context.TypeFactory.createTypeWithNullability(
                _context.TypeFactory.createSqlType(SqlTypeName.valueOf(name)), true);

            return _context.Registry.RequireMapping(null, relType).FromCalcite(payload);
        }

        /// <summary>
        /// Returns an interval payload, cast to the interval type the variant reports and decoded.
        /// </summary>
        /// <param name="value">The variant.</param>
        /// <param name="scale">The interval type the variant reports.</param>
        /// <param name="decode">Converts the count to the .NET value.</param>
        /// <returns>The .NET value, or <see langword="null"/> where the cast returns null.</returns>
        static object? Interval(VariantValue value, Name scale, Func<object, object> decode)
        {
            return value.cast(new BasicSqlTypeRtti(scale)) is { } payload ? decode(payload) : null;
        }

        /// <summary>
        /// Returns the runtime type a type name stands for, where it is a scalar type whose payload can be
        /// read.
        /// </summary>
        /// <param name="name">The name <c>getTypeString()</c> returned.</param>
        /// <returns>The type, or <see langword="null"/> where the name is not a readable scalar.</returns>
        /// <remarks>
        /// The set is listed explicitly, so that any other name is refused rather than cast. Every name on it
        /// is also a <see cref="SqlTypeName"/>, which <see cref="Read"/> relies on to build the payload's
        /// Calcite type.
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

            // item() is one-based and returns null past the end; a variant offers no length
            for (var i = 1; value.item(java.lang.Integer.valueOf(i)) is VariantValue element; i++)
                items.Add(Read(element));

            return Pack(items);
        }

        /// <summary>
        /// Returns the entries of a map variant, converted.
        /// </summary>
        /// <param name="value">The variant.</param>
        /// <returns>A dictionary with <see cref="string"/> keys and the value type the converted values
        /// share.</returns>
        /// <exception cref="ClrTypeMappingException">The keys are not character values.</exception>
        object Entries(VariantValue value)
        {
            // casting to MAP<VARCHAR, VARCHAR> is the only way to enumerate the keys; its values are not
            // usable, so each value is read by key with item()
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
        /// Returns converted elements in an array of the type they share.
        /// </summary>
        /// <remarks>
        /// The element type is taken from the values because an array variant reports only <c>ARRAY</c>, not
        /// its element type.
        /// </remarks>
        /// <param name="items">The converted values, in order; any may be <see langword="null"/>.</param>
        /// <returns>A new array whose element type is what <see cref="Unify"/> answers for the values.</returns>
        static Array Pack(List<object?> items)
        {
            var element = Unify(items);
            var array = Array.CreateInstance(element, items.Count);
            for (var i = 0; i < items.Count; i++)
                array.SetValue(items[i], i);

            return array;
        }

        /// <summary>
        /// Returns the runtime type every non-null value has, or <see cref="object"/> where they differ or
        /// there are none.
        /// </summary>
        /// <remarks>
        /// Where the shared type is a value type and a value is null, the result is the nullable form of that
        /// type.
        /// </remarks>
        /// <param name="items">The converted values; any may be <see langword="null"/>.</param>
        /// <returns>The shared runtime type, its nullable form, or <see cref="object"/>.</returns>
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
