using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// The shorthand for the operators' dominant shape: acquire the source's enumerator at
    /// <c>GetEnumerator</c>, hand it to a row loop, own its disposal.
    /// </summary>
    static class ClrEnumerables
    {

        /// <summary>
        /// Returns a sequence whose <c>GetEnumerator</c> acquires <paramref name="source"/>'s enumerator,
        /// as linq4j's operators acquire theirs inside <c>enumerator()</c>, and hands it to
        /// <paramref name="rows"/>. The acquired enumerator is disposed whether or not the loop ever ran.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="rows">Builds the row loop over the acquired enumerator.</param>
        /// <returns></returns>
        internal static IEnumerable<TResult> Acquiring<TSource, TResult>(this IEnumerable<TSource> source, Func<IEnumerator<TSource>, IEnumerator<TResult>> rows)
        {
            return new ClrEnumerable<TResult>(() =>
            {
                var e = source.GetEnumerator();
                return new AcquiredEnumerator<TResult>(rows(e), e);
            });
        }

        /// <summary>
        /// <see cref="Acquiring{TSource, TResult}(IEnumerable{TSource}, Func{IEnumerator{TSource}, IEnumerator{TResult}})"/>
        /// for the asynchronous convention: the token enters at <c>GetAsyncEnumerator</c>, where .NET puts
        /// it, and is handed on to the loop.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="rows">Builds the row loop over the acquired enumerator and the caller's token.</param>
        /// <returns></returns>
        internal static IAsyncEnumerable<TResult> Acquiring<TSource, TResult>(this IAsyncEnumerable<TSource> source, Func<IAsyncEnumerator<TSource>, CancellationToken, IAsyncEnumerator<TResult>> rows)
        {
            return new ClrAsyncEnumerable<TResult>(cancellationToken =>
            {
                var e = source.GetAsyncEnumerator(cancellationToken);
                return new AcquiredAsyncEnumerator<TResult>(rows(e, cancellationToken), e);
            });
        }

    }

    /// <summary>
    /// A sequence whose <see cref="IEnumerable{T}.GetEnumerator"/> runs a factory.
    /// </summary>
    /// <remarks>
    /// The counterpart of linq4j's <c>AbstractEnumerable</c>, and the seam the whole convention's
    /// execution model hangs on: linq4j runs a plan by obtaining its enumerator — <c>where</c> acquires
    /// its source's enumerator inside <c>enumerator()</c>, <c>orderBy</c> drains its whole input there,
    /// and the JDBC leaf executes its statement there — and <c>moveNext</c> only reads rows. A C#
    /// iterator method cannot say that: it defers everything, acquisition included, to the first
    /// <c>MoveNext</c>. So an operator builds one of these, and puts in the factory exactly what linq4j
    /// puts in <c>enumerator()</c>.
    ///
    /// <para>Where linq4j defers deliberately — the CALCITE-2909 memoized join lookups — the factory
    /// defers the same way, and says so at the site. Deferral is the marked exception, not the
    /// default.</para>
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
    /// An enumerator over a row loop, owning the enumerators the factory acquired for it.
    /// </summary>
    /// <remarks>
    /// The row loop is a C# iterator, and disposing one that never moved runs none of its
    /// <c>finally</c> blocks — so an enumerator the factory acquired eagerly would leak if the loop
    /// were trusted to dispose it. This owns them instead: <see cref="Dispose"/> disposes the loop and
    /// then each acquired enumerator, unconditionally, which is linq4j's <c>close()</c> contract — a
    /// wrapping <c>Enumerator</c> closes its source whether or not a row was ever read.
    /// </remarks>
    sealed class AcquiredEnumerator<T> : IEnumerator<T>
    {

        readonly IEnumerator<T> _rows;
        readonly IDisposable?[] _acquired;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="rows">The row loop, reading from the acquired enumerators.</param>
        /// <param name="acquired">What the factory acquired, disposed after the loop, in order.</param>
        public AcquiredEnumerator(IEnumerator<T> rows, params IDisposable?[] acquired)
        {
            ArgumentNullException.ThrowIfNull(rows);
            ArgumentNullException.ThrowIfNull(acquired);

            _rows = rows;
            _acquired = acquired;
        }

        /// <inheritdoc />
        public T Current => _rows.Current;

        /// <inheritdoc />
        object? IEnumerator.Current => Current;

        /// <inheritdoc />
        public bool MoveNext() => _rows.MoveNext();

        /// <inheritdoc />
        public void Reset() => throw new NotSupportedException();

        /// <inheritdoc />
        public void Dispose()
        {
            try
            {
                _rows.Dispose();
            }
            finally
            {
                foreach (var acquired in _acquired)
                    acquired?.Dispose();
            }
        }

    }

    /// <summary>
    /// An asynchronous sequence whose <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator"/> runs a
    /// factory.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrEnumerable{T}"/> for the asynchronous convention, with the one difference the CLR
    /// imposes: <c>GetAsyncEnumerator</c> cannot await, so a factory acquires its source enumerators —
    /// acquisition is synchronous all the way down — but work that must await, a sort's drain above
    /// all, stays in the first <c>MoveNextAsync</c> and is stated at the site. linq4j has no
    /// asynchronous counterpart to transcribe; the contract mirrored is the synchronous one's, less
    /// what an <c>IAsyncEnumerable</c> cannot say.
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
    /// <see cref="AcquiredEnumerator{T}"/> for the asynchronous convention: disposing an async
    /// iterator that never moved runs none of its <c>finally</c> blocks, so what the factory acquired
    /// is disposed here, unconditionally.
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
