using System;

using org.apache.calcite.linq4j;

using Apache.Calcite.Extensions.Linq4j.Tree;

namespace Apache.Calcite.Extensions.Linq4j
{

    /// <summary>
    /// A linq4j <see cref="Enumerator"/> backed by four delegates.
    /// </summary>
    /// <remarks>
    /// What an anonymous <c>Enumerator</c> class in a Calcite-generated block, such as a calc's, becomes when
    /// translated. The class's fields become variables of the block that creates the delegates, which all
    /// four close over, so they live as long as one instance of the class would.
    /// </remarks>
    sealed class DelegateEnumerator : Enumerator
    {

        readonly Func<object> onCurrent;
        readonly Func<bool> onMoveNext;
        readonly Action onReset;
        readonly Action onClose;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="current">Implements <c>current()</c>.</param>
        /// <param name="moveNext">Implements <c>moveNext()</c>.</param>
        /// <param name="reset">Implements <c>reset()</c>.</param>
        /// <param name="close">Implements <c>close()</c> and <see cref="Dispose"/>.</param>
        public DelegateEnumerator(Func<object> current, Func<bool> moveNext, Action reset, Action close)
        {
            onCurrent = current ?? throw new ArgumentNullException(nameof(current));
            onMoveNext = moveNext ?? throw new ArgumentNullException(nameof(moveNext));
            onReset = reset ?? throw new ArgumentNullException(nameof(reset));
            onClose = close ?? throw new ArgumentNullException(nameof(close));
        }

        /// <inheritdoc />
        public object current() => onCurrent();

        /// <inheritdoc />
        public bool moveNext() => onMoveNext();

        /// <inheritdoc />
        public void reset() => onReset();

        /// <inheritdoc />
        public void close() => onClose();

        /// <inheritdoc />
        /// <remarks>
        /// IKVM maps <c>java.lang.AutoCloseable</c>, which <c>Enumerator</c> extends, onto
        /// <see cref="IDisposable"/>.
        /// </remarks>
        public void Dispose() => onClose();

    }

}
