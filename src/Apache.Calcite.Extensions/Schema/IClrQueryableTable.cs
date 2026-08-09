using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Schema
{

    /// <summary>
    /// A table that produces its rows as an <see cref="IEnumerable{T}"/> of its own element type, reached
    /// through an expression.
    /// </summary>
    /// <remarks>
    /// The counterpart of <see cref="QueryableTable"/>, and the synchronous half of the pair whose other
    /// half is <see cref="IClrAsyncQueryableTable"/>. Where an <see cref="IClrScannableTable"/> is called
    /// and yields arrays, this states an element type of its own and hands back an expression the scan
    /// composes into the plan — so a table can put its own reading inline rather than behind an interface
    /// call.
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

    }

}
