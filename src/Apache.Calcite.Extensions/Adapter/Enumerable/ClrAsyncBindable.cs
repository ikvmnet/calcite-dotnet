using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// A compiled plan of the <see cref="ClrEnumerableConvention"/> calling convention that yields its rows
    /// asynchronously.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrBindable"/> over an <see cref="IAsyncEnumerable{T}"/>. The plan behind it came from the
    /// same nodes; what differs is which implementor built it.
    /// </remarks>
    sealed class ClrAsyncBindable(Func<DataContext, IAsyncEnumerable<object>> plan, Type elementType) : IClrAsyncBindable
    {

        readonly Func<DataContext, IAsyncEnumerable<object>> plan = plan ?? throw new ArgumentNullException(nameof(plan));
        readonly Type elementType = elementType ?? throw new ArgumentNullException(nameof(elementType));

        /// <inheritdoc />
        public IAsyncEnumerable<object> Bind(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            return plan(root);
        }

        /// <inheritdoc />
        public Type ElementType => elementType;

    }

}
