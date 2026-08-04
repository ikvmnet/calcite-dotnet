using System;

namespace Apache.Calcite.Linq.Runtime
{

    /// <summary>
    /// An <see cref="org.apache.calcite.runtime.Enumerables.Emitter"/> backed by a delegate.
    /// </summary>
    /// <param name="emitter"></param>
    /// <remarks>
    /// What <c>EnumerableMatch</c> declares as an anonymous class, and the only thing of a MATCH_RECOGNIZE
    /// plan that is a loop: it walks the rows of one match and hands each to the consumer. Every parameter
    /// type is fixed by the interface, so unlike the other adapters there is nothing to close over.
    /// </remarks>
    public sealed class DelegateEmitter(Action<java.util.List, java.util.List, java.util.List, int, java.util.function.Consumer> emitter) :
        org.apache.calcite.runtime.Enumerables.Emitter
    {

        readonly Action<java.util.List, java.util.List, java.util.List, int, java.util.function.Consumer> emitter = emitter ?? throw new ArgumentNullException(nameof(emitter));

        /// <inheritdoc />
        public void emit(java.util.List rows, java.util.List rowStates, java.util.List symbols, int match, java.util.function.Consumer consumer)
        {
            emitter(rows, rowStates, symbols, match, consumer);
        }

    }

}
