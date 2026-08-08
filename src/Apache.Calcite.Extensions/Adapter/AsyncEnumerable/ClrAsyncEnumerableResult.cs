using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.adapter.enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Result of implementing a relational expression of the <see cref="ClrAsyncEnumerableConvention"/>
    /// calling convention.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrEnumerableResult"/>, member for member, for a plan whose sequences are
    /// <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/>.
    ///
    /// <para>It is a separate type rather than the same one despite being structurally identical, and that
    /// is the point of it: a shared result would let a synchronous expression pass through an asynchronous
    /// implementor and compile. The type is the check, and the check is
    /// <see cref="ClrAsyncEnumerableRelImplementor.Result"/> — which is why the constructor is internal
    /// here for the same reason it is there.</para>
    /// </remarks>
    public class ClrAsyncEnumerableResult
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="expression">Expression yielding the sequence of rows.</param>
        /// <param name="physType">The Java type returned by this relational expression, and how it maps onto the fields of the logical row type.</param>
        /// <param name="format">How a row is represented.</param>
        internal ClrAsyncEnumerableResult(Expression expression, ClrPhysType physType, JavaRowFormat format)
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
        public ClrPhysType PhysType { get; }

        /// <summary>
        /// Gets how a row is represented.
        /// </summary>
        public JavaRowFormat Format { get; }

    }

}
