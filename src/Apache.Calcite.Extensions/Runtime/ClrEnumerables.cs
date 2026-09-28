using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// A sequence whose <see cref="IEnumerable{T}.GetEnumerator"/> runs a factory.
    /// </summary>
    /// <remarks>
    /// The counterpart of linq4j's <c>AbstractEnumerable</c>. A cursor plan read as a sequence is opened when
    /// its enumerator is obtained, as a linq4j plan is. A C# iterator method would instead defer the open
    /// to the first <c>MoveNext</c>.
    /// </remarks>
    sealed class ClrEnumerable<T> : IEnumerable<T>
    {

        readonly Func<IEnumerator<T>> _factory;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="factory">Called once per <see cref="GetEnumerator"/> call.</param>
        public ClrEnumerable(Func<IEnumerator<T>> factory)
        {
            ArgumentNullException.ThrowIfNull(factory);

            _factory = factory;
        }

        /// <inheritdoc />
        public IEnumerator<T> GetEnumerator() => _factory();

        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    }

    /// <summary>
    /// An asynchronous sequence whose <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator"/> runs a
    /// factory.
    /// </summary>
    /// <remarks>
    /// The asynchronous counterpart of <see cref="ClrEnumerable{T}"/>. <c>GetAsyncEnumerator</c> cannot
    /// await, so a factory whose open has to await does it in the first <c>MoveNextAsync</c> instead.
    /// </remarks>
    sealed class ClrAsyncEnumerable<T> : IAsyncEnumerable<T>
    {

        readonly Func<CancellationToken, IAsyncEnumerator<T>> _factory;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="factory">Called once per <see cref="GetAsyncEnumerator"/> call, with its token.</param>
        public ClrAsyncEnumerable(Func<CancellationToken, IAsyncEnumerator<T>> factory)
        {
            ArgumentNullException.ThrowIfNull(factory);

            _factory = factory;
        }

        /// <inheritdoc />
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) => _factory(cancellationToken);

    }

    /// <summary>
    /// An asynchronous enumerator over a row loop that also owns the sources the loop reads from.
    /// </summary>
    /// <remarks>
    /// Disposing an async iterator that never advanced runs none of its <c>finally</c> blocks, so the
    /// sources are disposed here unconditionally. That matches linq4j's <c>close()</c>, which closes a
    /// source whether or not a row was read.
    /// </remarks>
    sealed class AcquiredAsyncEnumerator<T> : IAsyncEnumerator<T>
    {

        readonly IAsyncEnumerator<T> _rows;
        readonly IAsyncDisposable?[] _acquired;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="rows">The row loop.</param>
        /// <param name="acquired">The sources the loop reads, disposed in order after the loop.</param>
        public AcquiredAsyncEnumerator(IAsyncEnumerator<T> rows, params IAsyncDisposable?[] acquired)
        {
            ArgumentNullException.ThrowIfNull(rows);
            ArgumentNullException.ThrowIfNull(acquired);

            _rows = rows;
            _acquired = acquired;
        }

        /// <inheritdoc />
        public T Current => _rows.Current;

        /// <inheritdoc />
        public ValueTask<bool> MoveNextAsync() => _rows.MoveNextAsync();

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            try
            {
                await _rows.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                foreach (var acquired in _acquired)
                    if (acquired is not null)
                        await acquired.DisposeAsync().ConfigureAwait(false);
            }
        }

    }

}
