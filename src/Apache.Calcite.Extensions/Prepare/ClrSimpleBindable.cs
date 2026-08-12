using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// The one row a statement prepared by <c>ClrPrepareImpl.SimplePrepare</c> produces.
    /// </summary>
    /// <remarks>
    /// <c>CalcitePrepareImpl.simplePrepare</c> writes this as the lambda
    /// <c>dataContext -&gt; Linq4j.asEnumerable(list)</c>, which it can because a <c>Bindable</c> is one
    /// method. It is a class here because a statement of either convention is prepared through the same
    /// path, and the two bind to different sequences.
    /// </remarks>
    /// <param name="row">The row, which is the value itself — the result has one column.</param>
    sealed class ClrSimpleBindable(object row) : IClrBindable, IClrAsyncBindable
    {

        readonly object row = row ?? throw new ArgumentNullException(nameof(row));

        /// <inheritdoc />
        IEnumerable<object> IClrBindable.Bind(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            return [row];
        }

        /// <inheritdoc />
        IAsyncEnumerable<object> IClrAsyncBindable.Bind(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            return ClrAsyncEnumerableDefaults.Singleton(row);
        }

        /// <inheritdoc />
        public Type ElementType => row.GetType();

    }

}
