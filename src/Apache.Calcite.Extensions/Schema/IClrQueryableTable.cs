using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Schema
{

    /// <summary>
    /// A table that produces its rows as a .NET sequence of its own element type, reached through an
    /// expression.
    /// </summary>
    /// <remarks>
    /// The counterpart of <see cref="QueryableTable"/>. Where an <see cref="IClrScannableTable"/> is called
    /// and returns arrays, this declares its own element type and returns a
    /// <see cref="System.Linq.Expressions.Expression"/> that the scan compiles into the plan, so the table's
    /// reading code is inlined rather than called through an interface.
    ///
    /// <para><see cref="GetExpression"/> is required and <see cref="GetAsyncExpression"/> defaults to
    /// reading the synchronous sequence asynchronously, without suspending. A table whose rows can only be
    /// produced asynchronously implements <see cref="GetAsyncExpression"/> and writes
    /// <see cref="GetExpression"/> as a call that drains it.</para>
    ///
    /// <para><c>QueryableTable.asQueryable</c> has no counterpart: nothing in this package translates a
    /// linq4j <c>Queryable</c>.</para>
    /// </remarks>
    public interface IClrQueryableTable : Table
    {

        /// <summary>
        /// Gets the type of one row.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>QueryableTable.getElementType</c>. It determines the row format the scan
        /// uses, as the element type does for a <see cref="QueryableTable"/>.
        /// </remarks>
        Type ElementType { get; }

        /// <summary>
        /// Returns an expression that produces this table's rows synchronously.
        /// </summary>
        /// <param name="schema">The schema the table was resolved in, or <see langword="null"/> if it was not
        /// resolved through a schema.</param>
        /// <param name="tableName">The name the table was resolved by.</param>
        /// <returns>An expression of type <c>IEnumerable&lt;<see cref="ElementType"/>&gt;</c>. It must not be
        /// <see langword="null"/>.</returns>
        /// <remarks>
        /// The counterpart of <c>QueryableTable.getExpression(SchemaPlus, String, Class)</c>, without the
        /// class parameter. The expression is compiled into the plan and evaluated when the plan is opened.
        /// The values in each row must be the Java values Calcite's type factory uses for the columns.
        /// </remarks>
        Expression GetExpression(SchemaPlus? schema, string tableName);

        /// <summary>
        /// Returns an expression that produces this table's rows asynchronously.
        /// </summary>
        /// <param name="schema">The schema the table was resolved in, or <see langword="null"/> if it was not
        /// resolved through a schema.</param>
        /// <param name="tableName">The name the table was resolved by.</param>
        /// <returns>An expression of type <c>IAsyncEnumerable&lt;<see cref="ElementType"/>&gt;</c>. It must
        /// not be <see langword="null"/>.</returns>
        /// <remarks>
        /// By default wraps <see cref="GetExpression"/> in a call that reads the synchronous sequence as an
        /// asynchronous one without suspending. The plan passes the open's cancellation token to
        /// <c>GetAsyncEnumerator</c>.
        /// </remarks>
        Expression GetAsyncExpression(SchemaPlus? schema, string tableName)
        {
            return Expression.Call(
                null,
                ToAsyncEnumerable.MakeGenericMethod(ElementType),
                GetExpression(schema, tableName),
                Expression.Default(typeof(CancellationToken)));
        }

        /// <summary>
        /// <see cref="ClrSequences.ToAsyncEnumerable{TSource}"/>, called by the default
        /// <see cref="GetAsyncExpression"/>.
        /// </summary>
        private static readonly MethodInfo ToAsyncEnumerable = typeof(ClrSequences).GetMethod(nameof(ClrSequences.ToAsyncEnumerable))
            ?? throw new InvalidOperationException($"'{nameof(ClrSequences.ToAsyncEnumerable)}' is missing.");

    }

}
