using System;
using System.Collections;
using System.Collections.Generic;

using org.apache.calcite.linq4j;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Reads an <see cref="IEnumerable{T}"/> as a linq4j <see cref="Enumerable"/>.
    /// </summary>
    /// <remarks>
    /// What a generator of Calcite's is handed where it reads a sequence — the table function scan's window
    /// reads its input this way. The rows are not touched: both sides ask the same <c>JavaTypeFactory</c>
    /// what a field is, so a row that crossed the boundary is the row that arrived. Reading the other way,
    /// a linq4j sequence as this project's rows, is <see cref="JavaCursors"/>'.
    /// </remarks>
    static class JavaSequences
    {

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
