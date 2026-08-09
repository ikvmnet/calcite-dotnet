using System.Linq.Expressions;

using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Schema
{

    /// <summary>
    /// A table that produces its rows as an <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/> of
    /// its own element type, reached through an expression.
    /// </summary>
    /// <remarks>
    /// The counterpart of <see cref="QueryableTable"/>, which is the other half of Calcite's table SPI:
    /// where a <see cref="ScannableTable"/> yields <c>Object[]</c> and is called, a
    /// <see cref="QueryableTable"/> states an element type of its own and hands back an <em>expression</em>
    /// that reaches the data, which the scan composes into the plan rather than calling.
    ///
    /// <para><see cref="IClrAsyncScannableTable"/> is the first of those and this is the second. A table
    /// implements whichever suits it: the scannable one is simpler and its rows are arrays; this one lets a
    /// table's rows be a type of its own — a record, an entity — and lets the reading be inlined into the
    /// compiled plan rather than reached through an interface call per scan.</para>
    ///
    /// <para><c>QueryableTable.asQueryable</c> has no counterpart here and is not an omission. A linq4j
    /// <c>Queryable</c> is Java's LINQ, translated by <c>LixToRelTranslator</c>, which is package-private and
    /// takes a <c>Prepare</c>; nothing in this convention can reach it and nothing would call it. The other
    /// two members are what a scan actually uses.</para>
    /// </remarks>
    public interface IClrAsyncQueryableTable : Table
    {

        /// <summary>
        /// Returns the type of one row.
        /// </summary>
        /// <remarks>
        /// <c>QueryableTable.getElementType</c>, as a CLR <see cref="System.Type"/> rather than a
        /// <c>java.lang.reflect.Type</c>: the sequence this table hands back is a CLR one and its element
        /// type is a CLR type. It decides the row format the scan uses, exactly as Calcite's does.
        /// </remarks>
        System.Type ElementType { get; }

        /// <summary>
        /// Returns the expression by which the plan reaches this table's rows.
        /// </summary>
        /// <param name="schema">The schema the table was resolved in, which may be null for a table that
        /// was not resolved through a catalog reader.</param>
        /// <param name="tableName">The name it was resolved by.</param>
        /// <returns>An expression whose type is
        /// <c>IAsyncEnumerable&lt;<see cref="ElementType"/>&gt;</c>.</returns>
        /// <remarks>
        /// <c>QueryableTable.getExpression(SchemaPlus, String, Class)</c>, less the class: that parameter
        /// exists so a caller can ask for a <c>Queryable</c> or an <c>Enumerable</c>, and there is one
        /// answer here.
        ///
        /// <para>A <see cref="System.Linq.Expressions.Expression"/> rather than a linq4j one, because it is
        /// composed into a plan of this convention and everything in one of those is a CLR tree. It is the
        /// one place a table author writes an expression rather than a method, and it is what lets a table
        /// put its own reading inline — a provider call, a channel read — into the compiled plan.</para>
        ///
        /// <para>The values in a row are Java's, as they are for a <see cref="IClrAsyncScannableTable"/> and
        /// for every table Calcite reads: <c>java.lang.Integer</c>, <c>java.lang.String</c>. Everything
        /// downstream is Calcite's, and every boundary where a value crosses between the two runtimes is an
        /// adapter.</para>
        /// </remarks>
        Expression GetAsyncExpression(SchemaPlus? schema, string tableName);

    }

}
