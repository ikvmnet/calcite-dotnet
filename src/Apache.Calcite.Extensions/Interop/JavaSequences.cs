using System;
using System.Collections;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.linq4j;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Carries a sequence of rows between a linq4j <see cref="Enumerable"/> and an
    /// <see cref="IEnumerable{T}"/>.
    /// </summary>
    /// <remarks>
    /// This is the whole of what a converter between <c>EnumerableConvention</c> and
    /// <see cref="ClrEnumerableConvention"/> does. The rows are not touched: both conventions ask the same
    /// <c>JavaTypeFactory</c> what a field is, so a row that crossed the boundary is the row that arrived.
    /// </remarks>
    static class JavaSequences
    {

        /// <summary>
        /// Reads a linq4j sequence as a .NET one.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// This is an adapter boundary, so it converts rather than casts. linq4j erases its element type, so a
        /// sequence whose rows Calcite's type factory calls <c>int</c> yields <c>java.lang.Integer</c> at
        /// runtime; Java unboxes that at the use site and the CLR will not. A row that is an
        /// <c>Object[]</c> — which is nearly all of them — passes the type test in
        /// <see cref="JavaValues.As{T}"/> and costs nothing.
        /// </remarks>
        public static IEnumerable<TSource> FromJava<TSource>(org.apache.calcite.linq4j.Enumerable source)
        {
            ArgumentNullException.ThrowIfNull(source);

            // a linq4j sub-plan executes at enumerator() -- ResultSetEnumerable runs its JDBC statement
            // there, AdoReaderEnumerable its DbCommand -- so the crossing calls it where the rest of the
            // plan's acquisition happens: at GetEnumerator, closed by the owner whether or not a row was
            // ever read
            return new Runtime.ClrEnumerable<TSource>(() =>
            {
                var enumerator = source.enumerator();
                return new Runtime.AcquiredEnumerator<TSource>(FromJavaRows<TSource>(enumerator), new JavaCloseable(enumerator));
            });
        }

        /// <summary>
        /// The row loop of <see cref="FromJava"/>, over an enumerator the factory acquired.
        /// </summary>
        static IEnumerator<TSource> FromJavaRows<TSource>(org.apache.calcite.linq4j.Enumerator enumerator)
        {
            while (enumerator.moveNext())
                yield return JavaValues.As<TSource>(enumerator.current());
        }

        /// <summary>
        /// Closes a linq4j enumerator from a disposal.
        /// </summary>
        sealed class JavaCloseable(org.apache.calcite.linq4j.Enumerator enumerator) : IDisposable
        {

            /// <inheritdoc />
            public void Dispose() => enumerator.close();

        }

        /// <summary>
        /// Reads a linq4j sequence as an asynchronous .NET one.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="FromJava{TSource}"/> yielding into an <see cref="IAsyncEnumerable{T}"/>. It converts
        /// rather than casts for the reason that method gives.
        ///
        /// <para>Nothing here suspends, and that is the honest shape of it: the source is a linq4j
        /// <c>Enumerator</c>, which is pulled. Producing an asynchronous sequence that always completes
        /// synchronously costs a state machine and no thread -- it is not the sync-over-async this
        /// convention refuses, which is a caller blocked waiting. A plan reading a Calcite sub-plan this way
        /// is simply not asynchronous over that part of itself, and cannot be.</para>
        /// </remarks>
        public static IAsyncEnumerable<TSource> FromJavaAsync<TSource>(org.apache.calcite.linq4j.Enumerable source, System.Threading.CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);

            // enumerator() runs at GetAsyncEnumerator for the reason FromJava gives; the token that
            // matters is GetAsyncEnumerator's, which is the one the parameter used to combine with and
            // every generated call site passes default for
            return new Runtime.ClrAsyncEnumerable<TSource>(token =>
            {
                var enumerator = source.enumerator();
                return new Runtime.AcquiredAsyncEnumerator<TSource>(FromJavaAsyncRows<TSource>(enumerator, token), new AsyncJavaCloseable(enumerator));
            });
        }

        /// <summary>
        /// The row loop of <see cref="FromJavaAsync"/>, over an enumerator the factory acquired. Nothing
        /// here suspends, and that is the honest shape of it: the source is pulled, and an asynchronous
        /// sequence that always completes synchronously costs a state machine and no thread.
        /// </summary>
        static async IAsyncEnumerator<TSource> FromJavaAsyncRows<TSource>(org.apache.calcite.linq4j.Enumerator enumerator, System.Threading.CancellationToken cancellationToken)
        {
            while (enumerator.moveNext())
            {
                cancellationToken.ThrowIfCancellationRequested();

                yield return JavaValues.As<TSource>(enumerator.current());
            }

            await System.Threading.Tasks.Task.CompletedTask;
        }

        /// <summary>
        /// Closes a linq4j enumerator from an asynchronous disposal, which completes synchronously.
        /// </summary>
        sealed class AsyncJavaCloseable(org.apache.calcite.linq4j.Enumerator enumerator) : IAsyncDisposable
        {

            /// <inheritdoc />
            public System.Threading.Tasks.ValueTask DisposeAsync()
            {
                enumerator.close();
                return default;
            }

        }






        /// <summary>
        /// Reads a .NET sequence as a linq4j one.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        public static org.apache.calcite.linq4j.Enumerable ToJava<TSource>(IEnumerable<TSource> source)
        {
            ArgumentNullException.ThrowIfNull(source);

            return new JavaEnumerable<TSource>(source);
        }

        /// <summary>
        /// A linq4j <see cref="Enumerable"/> reading a .NET sequence.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        sealed class JavaEnumerable<TSource>(IEnumerable<TSource> source) : AbstractEnumerable
        {

            /// <inheritdoc />
            public override Enumerator enumerator()
            {
                return new JavaEnumerator<TSource>(source);
            }

        }

        /// <summary>
        /// A linq4j <see cref="Enumerator"/> reading a .NET one.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <remarks>
        /// linq4j positions before the first row and advances on <c>moveNext</c>, which is what
        /// <see cref="IEnumerator"/> does, so the two agree except over <c>reset</c>.
        ///
        /// <para><c>reset</c> means "be positioned before the first row again", and it is live: linq4j's
        /// <c>CartesianProductEnumerator.moveNext</c> calls it to rewind the inner side once per row of the
        /// outer, so a sequence of ours reaching a cartesian product of Calcite's is asked for it. A .NET
        /// iterator refuses <see cref="IEnumerator.Reset"/> -- the compiler generates a throw -- so the
        /// sequence is enumerated afresh instead. That is what the linq4j enumerable it stands for would do
        /// when asked for a second enumerator, and it is why this holds the sequence rather than one
        /// enumerator of it.</para>
        /// </remarks>
        sealed class JavaEnumerator<TSource>(IEnumerable<TSource> source) : Enumerator
        {

            IEnumerator<TSource> enumerator = source.GetEnumerator();

            /// <inheritdoc />
            public object? current() => enumerator.Current;

            /// <inheritdoc />
            public bool moveNext() => enumerator.MoveNext();

            /// <inheritdoc />
            public void reset()
            {
                enumerator.Dispose();
                enumerator = source.GetEnumerator();
            }

            /// <inheritdoc />
            public void close() => enumerator.Dispose();

            /// <inheritdoc />
            /// <remarks>
            /// IKVM maps <c>java.lang.AutoCloseable</c>, which a linq4j Enumerator extends, onto
            /// <see cref="IDisposable"/>, so closing one from Java arrives here.
            /// </remarks>
            public void Dispose() => enumerator.Dispose();

        }

    }

}
