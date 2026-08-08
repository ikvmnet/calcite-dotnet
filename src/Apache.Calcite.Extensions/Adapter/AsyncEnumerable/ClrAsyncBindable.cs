using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// A compiled plan, held as the delegate it compiled to.
    /// </summary>
    /// <param name="plan">The compiled plan.</param>
    /// <param name="elementType">The Java type of one row.</param>
    /// <remarks>
    /// <c>ClrBindable</c> over an <see cref="IAsyncEnumerable{T}"/>.
    /// </remarks>
    sealed class ClrAsyncBindable(Func<DataContext, IAsyncEnumerable<object>> plan, java.lang.reflect.Type elementType) : IClrAsyncBindable
    {

        readonly Func<DataContext, IAsyncEnumerable<object>> plan = plan ?? throw new ArgumentNullException(nameof(plan));
        readonly java.lang.reflect.Type elementType = elementType ?? throw new ArgumentNullException(nameof(elementType));

        /// <inheritdoc />
        public IAsyncEnumerable<object> Bind(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            return plan(root);
        }

        /// <inheritdoc />
        public java.lang.reflect.Type ElementType => elementType;

    }

}
