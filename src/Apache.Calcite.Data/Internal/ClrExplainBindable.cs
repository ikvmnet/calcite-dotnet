using System;
using System.Collections.Generic;

using Apache.Calcite.Linq;

using org.apache.calcite;
using org.apache.calcite.avatica;

namespace Apache.Calcite.Data.Internal
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
        public java.lang.reflect.Type ElementType =>
            IsArray ? (java.lang.Class)typeof(string[]) : (java.lang.Class)typeof(java.lang.String);

        /// <summary>
        /// Returns whether the row is an array holding the text rather than the text itself.
        /// </summary>
        bool IsArray => cursorFactory.style.name() == nameof(Meta.Style.ARRAY);

    }

}
