using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;
using org.apache.calcite.avatica;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// A plan that returns the single row of an <c>EXPLAIN</c>.
    /// </summary>
    /// <param name="explanation">The rendered plan.</param>
    /// <param name="cursorFactory">How the row is read back: with style <c>ARRAY</c> the row is a
    /// one-element array holding the text, and otherwise the text itself.</param>
    sealed class ClrExplainBindable(string explanation, Meta.CursorFactory cursorFactory) : IClrCursorFactory
    {

        readonly string explanation = explanation ?? throw new ArgumentNullException(nameof(explanation));
        readonly Meta.CursorFactory cursorFactory = cursorFactory ?? throw new ArgumentNullException(nameof(cursorFactory));


        /// <inheritdoc />
        IClrCursor IClrCursorFactory.Open(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            return Adapter.Cursor.ClrCursorDefaults.AsCursor([Row]);
        }

        /// <inheritdoc />
        ValueTask<IClrCursor> IClrCursorFactory.OpenAsync(DataContext root, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(root);

            return new ValueTask<IClrCursor>(Adapter.Cursor.ClrCursorDefaults.AsCursor([Row]));
        }

        /// <inheritdoc />
        public Type ElementType => IsArray ? typeof(string[]) : typeof(string);

        /// <summary>
        /// Gets the row: the text, or an array holding it.
        /// </summary>
        object Row => IsArray ? new[] { explanation } : explanation;

        /// <summary>
        /// Gets whether the row is an array holding the text rather than the text itself.
        /// </summary>
        bool IsArray => cursorFactory.style.name() == nameof(Meta.Style.ARRAY);

    }

}
