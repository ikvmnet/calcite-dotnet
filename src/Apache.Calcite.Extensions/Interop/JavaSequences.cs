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
    /// Used where Calcite-generated code reads a sequence, such as the input of a windowing table function.
    /// Rows pass through unchanged; both sides already use the type factory's Java values.
    /// <see cref="JavaCursors"/> converts in the other direction.
    /// </remarks>
    static class JavaSequences
    {

        /// <summary>
        /// Reads a .NET sequence as a linq4j one.
        /// </summary>
        /// <typeparam name="TSource">The type of the sequence's elements.</typeparam>
        /// <param name="source">The .NET sequence; each linq4j enumerator enumerates it afresh.</param>
        /// <returns>A linq4j <c>Enumerable</c> over <paramref name="source"/>, whose elements are passed through unconverted.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public static org.apache.calcite.linq4j.Enumerable ToJava<TSource>(IEnumerable<TSource> source)
        {
            ArgumentNullException.ThrowIfNull(source);

            return new JavaEnumerable<TSource>(source);
        }

        /// <summary>
        /// A linq4j <see cref="Enumerable"/> reading a .NET sequence.
        /// </summary>
        /// <typeparam name="TSource">The type of the sequence's elements.</typeparam>
        /// <param name="source">The .NET sequence each enumerator reads.</param>
        sealed class JavaEnumerable<TSource>(IEnumerable<TSource> source) : AbstractEnumerable
        {

            /// <inheritdoc />
            public override Enumerator enumerator()
            {
                return new JavaEnumerator<TSource>(source);
            }

        }

        /// <summary>
        /// A linq4j <see cref="Enumerator"/> reading a .NET sequence.
        /// </summary>
        /// <remarks>
        /// <c>reset</c> is called in practice: linq4j's <c>CartesianProductEnumerator</c> rewinds its inner
        /// side once per outer row. An iterator method's <see cref="IEnumerator.Reset"/> throws, so
        /// <c>reset</c> enumerates the sequence afresh, which is why this holds the sequence rather than one
        /// enumerator.
        /// </remarks>
        /// <typeparam name="TSource">The type of the sequence's elements.</typeparam>
        /// <param name="source">The .NET sequence, enumerated once at construction and again at each <c>reset</c>.</param>
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
            /// IKVM maps <c>java.lang.AutoCloseable</c>, which a linq4j <c>Enumerator</c> extends, onto
            /// <see cref="IDisposable"/>.
            /// </remarks>
            public void Dispose() => enumerator.Dispose();

        }

    }

}
