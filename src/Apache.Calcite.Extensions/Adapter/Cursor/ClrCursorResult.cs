using System;
using System.Linq.Expressions;


using org.apache.calcite.adapter.enumerable;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Result of <see cref="ClrCursorRel.Implement"/>: an expression that opens a cursor synchronously, and
    /// the physical type of its rows.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableRel.Result</c>, but carries an expression of type <c>IClrCursor&lt;TRow&gt;</c>
    /// rather than a linq4j block, because a parent composes it into its own expression. Create one with
    /// <see cref="ClrCursorRelImplementor.Result"/>.
    /// </remarks>
    public class ClrCursorResult
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="expression">The expression whose value is the opened cursor.</param>
        /// <param name="physType">The physical type of the rows.</param>
        /// <param name="format">The row format.</param>
        /// <remarks>
        /// Internal so that nodes create results through <see cref="ClrCursorRelImplementor.Result"/>, which
        /// checks the cursor's element type against the physical type.
        /// </remarks>
        internal ClrCursorResult(Expression expression, ClrPhysType physType, JavaRowFormat format)
        {
            Expression = expression ?? throw new ArgumentNullException(nameof(expression));
            PhysType = physType ?? throw new ArgumentNullException(nameof(physType));
            Format = format ?? throw new ArgumentNullException(nameof(format));
        }

        /// <summary>
        /// Gets the expression whose value is the opened cursor, a <c>IClrCursor&lt;TRow&gt;</c>.
        /// </summary>
        public Expression Expression { get; }

        /// <summary>
        /// Gets the physical type of the rows: their CLR type and how it maps onto the logical row type.
        /// </summary>
        public ClrPhysType PhysType { get; }

        /// <summary>
        /// Gets the row format.
        /// </summary>
        public JavaRowFormat Format { get; }

    }

}
