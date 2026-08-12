using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;
using org.apache.calcite.avatica;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// The one row an <c>EXPLAIN</c> produces.
    /// </summary>
    /// <param name="explanation">The plan, rendered.</param>
    /// <param name="cursorFactory">How the row is read back, which decides whether it is an array or the
    /// text itself.</param>
    sealed class ClrExplainBindable(string explanation, Meta.CursorFactory cursorFactory) : IClrBindable, IClrAsyncBindable
    {

        readonly string explanation = explanation ?? throw new ArgumentNullException(nameof(explanation));
        readonly Meta.CursorFactory cursorFactory = cursorFactory ?? throw new ArgumentNullException(nameof(cursorFactory));

        /// <inheritdoc />
        IEnumerable<object> IClrBindable.Bind(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            return [Row];
        }

        /// <inheritdoc />
        IAsyncEnumerable<object> IClrAsyncBindable.Bind(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            return ClrAsyncEnumerableDefaults.Singleton(Row);
        }

        /// <inheritdoc />
        public Type ElementType => IsArray ? typeof(string[]) : typeof(string);

        /// <summary>
        /// Returns the row, which is the text or an array holding it.
        /// </summary>
        object Row => IsArray ? new[] { explanation } : explanation;

        /// <summary>
        /// Returns whether the row is an array holding the text rather than the text itself.
        /// </summary>
        bool IsArray => cursorFactory.style.name() == nameof(Meta.Style.ARRAY);

    }

}
