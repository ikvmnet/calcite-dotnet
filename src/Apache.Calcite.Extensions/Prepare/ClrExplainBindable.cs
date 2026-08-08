using System;
using System.Collections.Generic;

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
    /// <remarks>
    /// <c>Prepare.prepareSql</c> answers an <c>EXPLAIN</c> with a <c>PreparedExplain</c> rather than going
    /// through <c>implement</c>, so there is no plan of any convention to compile — the text is already in
    /// hand. This is <c>CalcitePreparedExplain.getBindable</c>, which wraps that text in a singleton
    /// enumerable, written against <see cref="IClrBindable"/> instead.
    /// </remarks>
    sealed class ClrExplainBindable(string explanation, Meta.CursorFactory cursorFactory) : IClrBindable
    {

        readonly string explanation = explanation ?? throw new ArgumentNullException(nameof(explanation));
        readonly Meta.CursorFactory cursorFactory = cursorFactory ?? throw new ArgumentNullException(nameof(cursorFactory));

        /// <inheritdoc />
        public IEnumerable<object> Bind(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            yield return IsArray ? new[] { explanation } : explanation;
        }

        /// <inheritdoc />
        /// <remarks>
        /// A CLR type, as the interface says, and the rows really are these: <see cref="Bind"/> yields a
        /// <see cref="string"/> or a <c>string[]</c> and nothing here goes near the type factory.
        /// </remarks>
        public Type ElementType => IsArray ? typeof(string[]) : typeof(string);

        /// <summary>
        /// Returns whether the row is an array holding the text rather than the text itself.
        /// </summary>
        bool IsArray => cursorFactory.style.name() == nameof(Meta.Style.ARRAY);

    }

}
