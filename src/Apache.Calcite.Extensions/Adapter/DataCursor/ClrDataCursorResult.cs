using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.adapter.enumerable;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Result of implementing a relational expression of the <see cref="ClrDataCursorConvention"/> calling
    /// convention, as an open that acquires synchronously.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableRel.Result</c>. Calcite carries a linq4j block whose value is the
    /// enumerable it returns; this carries the expression whose value is the opened cursor, a
    /// <c>ClrDataCursor&lt;TRow&gt;</c> of the physical row type, because a parent composes it into its own
    /// open rather than appending to a method body.
    /// </remarks>
    public class ClrDataCursorResult
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="expression">Expression whose value is the opened cursor.</param>
        /// <param name="physType">The Java type returned by this relational expression, and how it maps onto the fields of the logical row type.</param>
        /// <param name="format">How a row is represented.</param>
        /// <remarks>
        /// Internal, so that <see cref="ClrDataCursorRelImplementor.Result"/> is the only way a node has of
        /// making one. That method is where the cursor is required to carry the rows its physical type says
        /// it carries, and a node that built its own result would not be asked.
        /// </remarks>
        internal ClrDataCursorResult(Expression expression, ClrPhysType physType, JavaRowFormat format)
        {
            Expression = expression ?? throw new ArgumentNullException(nameof(expression));
            PhysType = physType ?? throw new ArgumentNullException(nameof(physType));
            Format = format ?? throw new ArgumentNullException(nameof(format));
        }

        /// <summary>
        /// Gets the expression whose value is the opened cursor, a <c>ClrDataCursor&lt;TRow&gt;</c>.
        /// </summary>
        public Expression Expression { get; }

        /// <summary>
        /// Gets the Java type returned by this relational expression, and how it maps onto the fields of the
        /// logical row type.
        /// </summary>
        public ClrPhysType PhysType { get; }

        /// <summary>
        /// Gets how a row is represented.
        /// </summary>
        public JavaRowFormat Format { get; }

    }

}
