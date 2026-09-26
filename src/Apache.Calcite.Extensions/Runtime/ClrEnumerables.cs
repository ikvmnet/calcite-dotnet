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
    /// The counterpart of linq4j's <c>AbstractEnumerable</c>: linq4j runs a plan by obtaining its
    /// enumerator, and a C# iterator method cannot say that, deferring everything, acquisition included, to
    /// the first <c>MoveNext</c>. So a cursor plan read as a sequence is one of these, and opening the plan is
    /// the factory, run where a sequence acquires.
    /// </remarks>
    sealed class ClrEnumerable<T> : IEnumerable<T>
    {

        readonly Func<IEnumerator<T>> _factory;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="factory">Runs once per <see cref="GetEnumerator"/>, exactly as linq4j's
        /// <c>enumerator()</c> runs once per call.</param>
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
    /// <see cref="ClrEnumerable{T}"/> for an awaiting read, with the one difference the CLR imposes:
    /// <c>GetAsyncEnumerator</c> cannot await, so a factory that has to await its acquisition does it in the
    /// first <c>MoveNextAsync</c>, and says so at the site.
    /// </remarks>
    sealed class ClrAsyncEnumerable<T> : IAsyncEnumerable<T>
    {

        readonly Func<CancellationToken, IAsyncEnumerator<T>> _factory;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="factory">Runs once per <see cref="GetAsyncEnumerator"/>.</param>
        public ClrAsyncEnumerable(Func<CancellationToken, IAsyncEnumerator<T>> factory)
        {
            ArgumentNullException.ThrowIfNull(factory);

            _factory = factory;
        }

        /// <inheritdoc />
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) => _factory(cancellationToken);

    }

    /// <summary>
    /// An asynchronous enumerator over a row loop, owning the enumerators the factory acquired for it.
    /// </summary>
    /// <remarks>
    /// Disposing an async iterator that never moved runs none of its <c>finally</c> blocks, so what the
    /// factory acquired is disposed here, unconditionally, which is linq4j's <c>close()</c> contract: a
    /// wrapping <c>Enumerator</c> closes its source whether or not a row was ever read.
    /// </remarks>
    sealed class AcquiredAsyncEnumerator<T> : IAsyncEnumerator<T>
    {

        readonly IAsyncEnumerator<T> _rows;
        readonly IAsyncDisposable?[] _acquired;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="rows">The row loop, reading from the acquired enumerators.</param>
        /// <param name="acquired">What the factory acquired, disposed after the loop, in order.</param>
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
