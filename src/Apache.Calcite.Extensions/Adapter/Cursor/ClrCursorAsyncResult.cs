using System;
using System.Linq.Expressions;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.adapter.enumerable;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Result of implementing a relational expression of the <see cref="ClrCursorConvention"/> calling
    /// convention, as an open that awaits its acquisition.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrCursorResult"/>'s companion, and the return type of the awaiting fork:
    /// <see cref="ClrCursorRel.ImplementAsync"/> and
    /// <see cref="ClrCursorRelImplementor.VisitChildAsync"/> answer this, and the synchronous members
    /// answer the other. The expression's value is a <see cref="ValueTask{TResult}"/> of a
    /// <c>ClrCursor&lt;TRow&gt;</c>: the cursor is the same one the other fork opens, and what is awaited
    /// is the way to it. Two types rather than one is what makes the two hierarchies checkable — a body
    /// cannot silently hand up the wrong kind, because the wrong kind does not compile.
    /// </remarks>
    public class ClrCursorAsyncResult
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="expression">Expression whose value is the awaited open.</param>
        /// <param name="physType">The Java type returned by this relational expression, and how it maps onto
        /// the fields of the logical row type.</param>
        /// <param name="format">How a row is represented.</param>
        /// <remarks>
        /// Internal, so that <see cref="ClrCursorRelImplementor.ResultAsync"/> is the only way a node
        /// has of making one, exactly as for the synchronous result.
        /// </remarks>
        internal ClrCursorAsyncResult(Expression expression, ClrPhysType physType, JavaRowFormat format)
        {
            Expression = expression ?? throw new ArgumentNullException(nameof(expression));
            PhysType = physType ?? throw new ArgumentNullException(nameof(physType));
            Format = format ?? throw new ArgumentNullException(nameof(format));
        }

        /// <summary>
        /// Gets the expression whose value is the awaited open, a
        /// <c>ValueTask&lt;ClrCursor&lt;TRow&gt;&gt;</c>.
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
