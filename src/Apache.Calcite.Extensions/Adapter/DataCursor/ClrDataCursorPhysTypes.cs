using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.adapter.enumerable;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Converts a cursor of one row format into a cursor of another.
    /// </summary>
    /// <remarks>
    /// <c>ClrPhysType.ConvertTo</c> for a plan of this convention. That member is <c>PhysType.convertTo</c>,
    /// the one member of a physical type that takes a <em>sequence</em> rather than a row, and so the one
    /// member a second convention cannot share: it writes the per-row selector over the sequence
    /// convention's <c>Select</c>, and a cursor needs it over <see cref="ClrDataCursorDefaults.Select"/>.
    /// The selector itself is a row's business and is built here as <c>ClrPhysTypeImpl</c> builds it, from
    /// the row-shaped members every physical type has; only the operator it is carried by differs.
    /// </remarks>
    static class ClrDataCursorPhysTypes
    {

        /// <summary>
        /// Converts an opened cursor of the given physical type to one whose rows use the given row format.
        /// </summary>
        /// <param name="physType">The physical type of the rows the cursor carries.</param>
        /// <param name="expression">The open.</param>
        /// <param name="targetFormat"></param>
        /// <returns></returns>
        public static Expression ConvertTo(ClrPhysType physType, Expression expression, JavaRowFormat targetFormat)
        {
            ArgumentNullException.ThrowIfNull(physType);
            ArgumentNullException.ThrowIfNull(expression);
            ArgumentNullException.ThrowIfNull(targetFormat);

            if (physType.Format == targetFormat)
                return expression;

            var (selector, targetRowType) = Reformatter(physType, targetFormat);

            return Expression.Call(null,
                ClrDataCursorBuiltInMethod.Select.MakeGenericMethod(physType.RowType, targetRowType),
                expression,
                selector);
        }

        /// <summary>
        /// Converts an open that awaits a cursor of the given physical type to one whose rows use the given
        /// row format.
        /// </summary>
        /// <param name="implementor">The implementor, whose token the awaiting open is passed.</param>
        /// <param name="physType">The physical type of the rows the cursor carries.</param>
        /// <param name="expression">The open.</param>
        /// <param name="targetFormat"></param>
        /// <returns></returns>
        public static Expression ConvertToAsync(ClrDataCursorRelImplementor implementor, ClrPhysType physType, Expression expression, JavaRowFormat targetFormat)
        {
            ArgumentNullException.ThrowIfNull(implementor);
            ArgumentNullException.ThrowIfNull(physType);
            ArgumentNullException.ThrowIfNull(expression);
            ArgumentNullException.ThrowIfNull(targetFormat);

            if (physType.Format == targetFormat)
                return expression;

            var (selector, targetRowType) = Reformatter(physType, targetFormat);

            return ClrDataCursorBuiltInMethod.CallAsync(implementor,
                ClrDataCursorBuiltInMethod.SelectAsync.MakeGenericMethod(physType.RowType, targetRowType),
                expression,
                selector);
        }

        /// <summary>
        /// Returns the per-row selector that rewrites a row of one format into another, and the type it
        /// yields.
        /// </summary>
        /// <remarks>
        /// What <c>ClrPhysTypeImpl</c> builds for its two <c>ConvertTo</c> members: the target physical type
        /// is strict, as Calcite's is, and what the cursor carries is the target's row type — a one column
        /// row of a value is the value, and a cursor of it still carries the box.
        /// </remarks>
        static (LambdaExpression Selector, Type TargetRowType) Reformatter(ClrPhysType physType, JavaRowFormat targetFormat)
        {
            var o_ = Expression.Parameter(physType.RowType, "o");
            var fieldCount = physType.RelRowType.getFieldCount();

            var targetPhysType = ClrPhysTypeImpl.Of(physType.TypeFactory, physType.RelRowType, targetFormat, false);
            var targetRowType = targetPhysType.RowType;

            var references = new List<Expression>(fieldCount);
            for (int i = 0; i < fieldCount; i++)
                references.Add(physType.FieldReference(o_, i));

            var body = ClrEnumUtils.Convert(targetPhysType.Record(references), targetRowType);

            return (Expression.Lambda(typeof(Func<,>).MakeGenericType(physType.RowType, targetRowType), body, o_), targetRowType);
        }

    }

}
