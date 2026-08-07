using System;
using System.Linq.Expressions;

using org.apache.calcite.adapter.enumerable;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Result of implementing a relational expression of the <see cref="ClrEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableRel.Result</c>. Calcite carries a linq4j block, whose value is the
    /// enumerable it returns; this carries the expression yielding the enumerable directly, because a parent
    /// composes it into its own rather than appending to a method body.
    /// </remarks>
    public class ClrEnumerableResult
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="expression">Expression yielding the sequence of rows.</param>
        /// <param name="physType">The Java type returned by this relational expression, and how it maps onto the fields of the logical row type.</param>
        /// <param name="format">How a row is represented.</param>
        /// <remarks>
        /// Internal, so that <see cref="ClrEnumerableRelImplementor.Result"/> is the only way a node has of
        /// making one. That method is where a sequence is required to carry the rows its physical type says
        /// it carries, and a node that built its own result would not be asked. One did, and was the last
        /// node in the plan still handing up a CLR primitive where the row type was a Java box — correct
        /// only because it had been fixed by hand, and silently wrong again the moment anything about it
        /// changed.
        /// </remarks>
        internal ClrEnumerableResult(Expression expression, PhysType physType, JavaRowFormat format)
        {
            Expression = expression ?? throw new ArgumentNullException(nameof(expression));
            PhysType = physType ?? throw new ArgumentNullException(nameof(physType));
            Format = format ?? throw new ArgumentNullException(nameof(format));
        }

        /// <summary>
        /// Gets the expression yielding the sequence of rows.
        /// </summary>
        public Expression Expression { get; }

        /// <summary>
        /// Gets the Java type returned by this relational expression, and how it maps onto the fields of the
        /// logical row type.
        /// </summary>
        public PhysType PhysType { get; }

        /// <summary>
        /// Gets how a row is represented.
        /// </summary>
        public JavaRowFormat Format { get; }

    }

}
