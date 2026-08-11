using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.Clr
{

    /// <summary>
    /// Reads one .NET object as a row of the values Calcite holds.
    /// </summary>
    /// <remarks>
    /// The other half of <see cref="ClrTypeMapper.ColumnRowType"/>: that says a member is a
    /// <c>DECIMAL(19, 6)</c>, and this is what makes the value one. Compiled once per type, so a scan is a
    /// delegate call and a property read per column rather than reflection per row.
    ///
    /// <para><b>Every element is boxed the Java way.</b> A row of an <see cref="ClrObjectTable"/> never
    /// reaches this, its values already being Calcite's; a row of a <see cref="ClrProjectingTable"/> is an
    /// <c>object?[]</c>, and an <see cref="int"/> put in one by the CLR is a <c>System.Int32</c> that
    /// <c>SqlFunctions.toInt</c> refuses. <c>ClrEnumUtils.Convert</c> to <see cref="object"/> is what boxes
    /// it as a <c>java.lang.Integer</c>, which is the same call <c>JavaRowFormatExtensions.ObjectArray</c>
    /// makes for every array row this convention builds.</para>
    /// </remarks>
    public static class ClrRowConverter
    {

        static readonly ConcurrentDictionary<Type, Func<object, object?[]>> Cache = new();

        /// <summary>
        /// Returns the reader of one row of <paramref name="type"/>.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static Func<object, object?[]> Of(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return Cache.GetOrAdd(type, static t => Compile(t));
        }

        /// <summary>
        /// Builds and compiles the reader.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        static Func<object, object?[]> Compile(Type type)
        {
            var members = ClrReflect.Members(type);
            var parameter = Expression.Parameter(typeof(object), "row");
            var row = Expression.Convert(parameter, type);

            var elements = new Expression[members.Count];
            for (int i = 0; i < members.Count; i++)
                elements[i] = Value(Expression.MakeMemberAccess(row, members[i].Member), members[i].Type);

            return Expression.Lambda<Func<object, object?[]>>(
                Expression.NewArrayInit(typeof(object), elements), parameter).Compile();
        }

        /// <summary>
        /// Returns the expression yielding one member as the value its column holds.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        static Expression Value(Expression value, Type type)
        {
            // a nullable value type is a null or the value it wraps, and asking HasValue first is the whole
            // of the difference between it and a Java box
            if (Nullable.GetUnderlyingType(type) is Type underlying)
                return Expression.Condition(
                    Expression.Property(value, "HasValue"),
                    Value(Expression.Property(value, "Value"), underlying),
                    Expression.Constant(null, typeof(object)),
                    typeof(object));

            if (ClrTypeMapper.IsNativelyRepresented(type))
                return ClrEnumUtils.Convert(value, typeof(object));

            if (type.IsEnum)
                return ClrEnumUtils.Convert(Expression.Convert(value, Enum.GetUnderlyingType(type)), typeof(object));

            var converter = ClrValues.Converter(type)
                ?? throw new NotSupportedException($"There is no conversion from '{type}' to a value Calcite holds.");

            // the converted value is a long, an int or a string; the first two are still the CLR's and are
            // boxed the Java way on the way into the array, exactly as a native member is
            return ClrEnumUtils.Convert(Expression.Call(null, converter, value), typeof(object));
        }

    }

}
