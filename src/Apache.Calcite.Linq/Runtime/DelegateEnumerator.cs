using System;

using org.apache.calcite.linq4j;

namespace Apache.Calcite.Linq.Runtime
{

    /// <summary>
    /// A linq4j <see cref="Enumerator"/> backed by four delegates.
    /// </summary>
    /// <remarks>
    /// An anonymous class of one method is a lambda, and every other adapter here is that. This one is not:
    /// an enumerator is four methods over state they share, which is a thing rather than a function. Calcite
    /// generates one wherever a sub-plan of <c>EnumerableConvention</c> reads its input — a calc, a table
    /// function — so it is what the converter meets most often.
    ///
    /// <para>The shared state is not held here. It stays where the class declared it: the fields become
    /// variables of the block that builds these delegates, and all four close over them, which is the same
    /// lifetime one instance of the anonymous class would have had.</para>
    /// </remarks>
    public sealed class DelegateEnumerator : Enumerator
    {

        readonly Func<object> onCurrent;
        readonly Func<bool> onMoveNext;
        readonly Action onReset;
        readonly Action onClose;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="current"></param>
        /// <param name="moveNext"></param>
        /// <param name="reset"></param>
        /// <param name="close"></param>
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
        /// IKVM maps <c>java.lang.AutoCloseable</c>, which an enumerator extends, onto
        /// <see cref="IDisposable"/>, so closing one from Java arrives here.
        /// </remarks>
        public void Dispose() => onClose();

    }

}
