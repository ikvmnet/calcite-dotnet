using System;
using System.Linq.Expressions;
using System.Threading.Tasks;


using org.apache.calcite.adapter.enumerable;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Result of implementing a relational expression of the <see cref="ClrCursorConvention"/> calling
    /// convention as an awaiting open.
    /// </summary>
    /// <remarks>
    /// Returned by <see cref="ClrCursorRel.ImplementAsync"/> and <see cref="ClrCursorRelImplementor.VisitChildAsync"/>;
    /// the synchronous members return a <see cref="ClrCursorResult"/>. The expression's value is a
    /// <see cref="ValueTask{TResult}"/> of an <c>IClrCursor&lt;TRow&gt;</c>, the same cursor type the synchronous
    /// open produces. Instances are created with <see cref="ClrCursorRelImplementor.ResultAsync"/>.
    /// </remarks>
    public class ClrCursorAsyncResult
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="expression">Expression whose value is the awaited open.</param>
        /// <param name="physType">The physical type of the rows, and how it maps onto the fields of the
        /// logical row type.</param>
        /// <param name="format">How a row is represented.</param>
        internal ClrCursorAsyncResult(Expression expression, ClrPhysType physType, JavaRowFormat format)
        {
            Expression = expression ?? throw new ArgumentNullException(nameof(expression));
            PhysType = physType ?? throw new ArgumentNullException(nameof(physType));
            Format = format ?? throw new ArgumentNullException(nameof(format));
        }

        /// <summary>
        /// Gets the expression whose value is the awaited open, a
        /// <c>ValueTask&lt;IClrCursor&lt;TRow&gt;&gt;</c>.
        /// </summary>
        public Expression Expression { get; }

        /// <summary>
        /// Gets the physical type of the rows, and how it maps onto the fields of the logical row type.
        /// </summary>
        public ClrPhysType PhysType { get; }

        /// <summary>
        /// Gets how a row is represented.
        /// </summary>
        public JavaRowFormat Format { get; }

    }

}
