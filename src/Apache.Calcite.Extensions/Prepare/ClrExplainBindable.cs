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
    /// <remarks>
    /// <c>Prepare.prepareSql</c> answers an <c>EXPLAIN</c> with a <c>PreparedExplain</c> rather than going
    /// through <c>implement</c>, so there is no plan of any convention to compile — the text is already in
    /// hand. This is <c>CalcitePreparedExplain.getBindable</c>, which wraps that text in a singleton
    /// enumerable, written against <see cref="IClrBindableBase"/> instead.
    ///
    /// <para><b>It answers both interfaces, and it is the only thing that does.</b> Every other prepared
    /// result is one convention's and refuses the other's reader, because a sequence that has to be pulled
    /// is not one that can be awaited. This one holds a string. Which convention the plan behind the text
    /// was optimized into was settled at prepare time — <c>ExecuteReader</c> or <c>ExecuteReaderAsync</c> is
    /// the only way that demand is stated, so an <c>EXPLAIN</c> asked for asynchronously renders the
    /// asynchronous plan — and by the time the row is bound there is nothing left of that plan to be
    /// faithful to. Refusing the asynchronous reader here would mean an <c>EXPLAIN</c> could not be read at
    /// all by a caller who only has that method.</para>
    /// </remarks>
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
        /// <remarks>
        /// A CLR type, as the interface says, and the rows really are these: <see cref="Row"/> is a
        /// <see cref="string"/> or a <c>string[]</c> and nothing here goes near the type factory.
        /// </remarks>
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
