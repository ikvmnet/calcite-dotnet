using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using org.apache.calcite.adapter.enumerable;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Result of implementing a relational expression of the <see cref="ClrEnumerableConvention"/> calling
    /// convention into an <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrEnumerableResult"/>'s companion, and the return type of the awaiting fork:
    /// <see cref="ClrEnumerableRel.ImplementAsync"/> and
    /// <see cref="ClrEnumerableRelImplementor.VisitChildAsync"/> answer this, and the pulled members answer
    /// the other. Two types rather than one is what makes the two hierarchies checkable: a body cannot
    /// silently hand up the wrong kind, because the wrong kind does not compile.
    ///
    /// <para>Crossing between them is <see cref="ClrEnumerableRelImplementor.Awaited"/> and
    /// <see cref="ClrEnumerableRelImplementor.Pulled"/>, and each is written at the site that wants it
    /// rather than inferred from what came back. That is the whole difference from the arrangement before:
    /// there was one result type, so a node's kind could only be recovered by testing the type of the
    /// expression it carried, and every visit did exactly that on the way back up.</para>
    ///
    /// <para>It carries no cancellation token and no other state of its own. A token enters an
    /// <see cref="IAsyncEnumerable{T}"/> at <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator"/>, so a plan
    /// carries none and the operators take <c>default</c>.</para>
    /// </remarks>
    public class ClrEnumerableAsyncResult
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="expression">Expression yielding the sequence of rows.</param>
        /// <param name="physType">The Java type returned by this relational expression, and how it maps onto
        /// the fields of the logical row type.</param>
        /// <param name="format">How a row is represented.</param>
        /// <remarks>
        /// Internal, so that <see cref="ClrEnumerableRelImplementor.ResultAsync"/> is the only way a node has
        /// of making one, exactly as for the pulled result. That method is where a sequence is required to be
        /// an <see cref="IAsyncEnumerable{T}"/> of the rows its physical type says it carries.
        /// </remarks>
        internal ClrEnumerableAsyncResult(Expression expression, ClrPhysType physType, JavaRowFormat format)
        {
            Expression = expression ?? throw new ArgumentNullException(nameof(expression));
            PhysType = physType ?? throw new ArgumentNullException(nameof(physType));
            Format = format ?? throw new ArgumentNullException(nameof(format));
        }

        /// <summary>
        /// Gets the expression yielding the sequence of rows, which is an
        /// <see cref="IAsyncEnumerable{T}"/>.
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
