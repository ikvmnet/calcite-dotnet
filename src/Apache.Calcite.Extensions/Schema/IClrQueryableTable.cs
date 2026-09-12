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
    /// and yields arrays, this states an element type of its own and hands back an expression the scan
    /// composes into the plan — so a table can put its own reading inline, a provider call or a channel
    /// read, rather than behind an interface call per scan.
    ///
    /// <para><b><see cref="GetExpression"/> is required and <see cref="GetAsyncExpression"/> is optional</b>,
    /// exactly as for <see cref="IClrScannableTable"/> and for the nodes of this convention. A table writes
    /// the halves it has; the default builds the awaiting expression by reading the pulled one across, which
    /// costs a state machine and no thread. A table whose reading can only be awaited writes
    /// <see cref="GetAsyncExpression"/> and builds <see cref="GetExpression"/> as a call to a drain of its
    /// own.</para>
    ///
    /// <para><c>QueryableTable.asQueryable</c> has no counterpart and is not an omission: a linq4j
    /// <c>Queryable</c> is Java's LINQ, translated by <c>LixToRelTranslator</c>, which is package-private
    /// and takes a <c>Prepare</c>. Nothing here can reach it and nothing would call it.</para>
    /// </remarks>
    public interface IClrQueryableTable : Table
    {

        /// <summary>
        /// Returns the type of one row.
        /// </summary>
        /// <remarks>
        /// <c>QueryableTable.getElementType</c>, as a CLR <see cref="Type"/>. It decides the row format the
        /// scan uses, through <c>ClrEnumerableTableScan.DeduceElementType</c>.
        /// </remarks>
        Type ElementType { get; }

        /// <summary>
        /// Returns the expression by which the plan reaches this table's rows.
        /// </summary>
        /// <param name="schema">The schema the table was resolved in, which may be null for a table that
        /// was not resolved through a catalog reader.</param>
        /// <param name="tableName">The name it was resolved by.</param>
        /// <returns>An expression whose type is <c>IEnumerable&lt;<see cref="ElementType"/>&gt;</c>.</returns>
        /// <remarks>
        /// <c>QueryableTable.getExpression(SchemaPlus, String, Class)</c>, less the class: that parameter
        /// exists so a caller can ask for a <c>Queryable</c> or an <c>Enumerable</c>, and there is one
        /// answer here.
        ///
        /// <para>The values in a row are Java's, as they are for every table Calcite reads.</para>
        /// </remarks>
        Expression GetExpression(SchemaPlus? schema, string tableName);

        /// <summary>
        /// Returns the expression by which a plan that awaits its rows reaches this table's.
        /// </summary>
        /// <param name="schema">The schema the table was resolved in, which may be null for a table that
        /// was not resolved through a catalog reader.</param>
        /// <param name="tableName">The name it was resolved by.</param>
        /// <returns>An expression whose type is
        /// <c>IAsyncEnumerable&lt;<see cref="ElementType"/>&gt;</c>.</returns>
        /// <remarks>
        /// A <see cref="Expression"/> rather than a linq4j one, because it is composed into a plan of this
        /// convention and everything in one of those is a CLR tree. It is the one place a table author
        /// writes an expression rather than a method.
        ///
        /// <para>By default <see cref="GetExpression"/> with a read across built over it. That default does
        /// put a small piece of plan building inside the SPI, which is the scan's job everywhere else, and
        /// it is here so that the two halves of this interface behave the way the two halves of
        /// <see cref="IClrScannableTable"/> do. A table that overrides it never runs this.</para>
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
        /// <see cref="ClrSequences.ToAsyncEnumerable{TSource}"/>, which the default
        /// <see cref="GetAsyncExpression"/> builds its call to.
        /// </summary>
        private static readonly MethodInfo ToAsyncEnumerable = typeof(ClrSequences).GetMethod(nameof(ClrSequences.ToAsyncEnumerable))
            ?? throw new InvalidOperationException($"'{nameof(ClrSequences.ToAsyncEnumerable)}' is missing.");

    }

}
