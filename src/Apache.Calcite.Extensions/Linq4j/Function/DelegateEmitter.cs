using System;

using Apache.Calcite.Extensions.Linq4j.Tree;

namespace Apache.Calcite.Extensions.Linq4j.Function
{

    /// <summary>
    /// An <see cref="org.apache.calcite.runtime.Enumerables.Emitter"/> backed by a delegate.
    /// </summary>
    /// <param name="emitter">Implements <c>emit</c>.</param>
    /// <remarks>
    /// What the anonymous <c>Emitter</c> class <c>EnumerableMatch</c> generates for MATCH_RECOGNIZE
    /// becomes when translated. It walks the rows of one match and passes each to the consumer.
    /// </remarks>
    sealed class DelegateEmitter(Action<java.util.List, java.util.List, java.util.List, int, java.util.function.Consumer> emitter) :
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
