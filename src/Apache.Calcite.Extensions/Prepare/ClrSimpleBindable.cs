using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// A plan that returns the single row of a statement prepared by <c>ClrPrepareImpl.SimplePrepare</c>.
    /// </summary>
    /// <remarks>
    /// The counterpart of the <c>dataContext -&gt; Linq4j.asEnumerable(list)</c> lambda in
    /// <c>CalcitePrepareImpl.simplePrepare</c>.
    /// </remarks>
    /// <param name="row">The row; the result has one column, so this is the column's value.</param>
    sealed class ClrSimpleBindable(object row) : IClrCursorFactory
    {

        readonly object row = row ?? throw new ArgumentNullException(nameof(row));


        /// <inheritdoc />
        IClrCursor IClrCursorFactory.Open(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            return Adapter.Cursor.ClrCursorDefaults.AsCursor([row]);
        }

        /// <inheritdoc />
        ValueTask<IClrCursor> IClrCursorFactory.OpenAsync(DataContext root, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(root);

            return new ValueTask<IClrCursor>(Adapter.Cursor.ClrCursorDefaults.AsCursor([row]));
        }

        /// <inheritdoc />
        public Type ElementType => row.GetType();

    }

}
