using System;
using System.Collections;
using System.Collections.Generic;

using org.apache.calcite.linq4j;

namespace Apache.Calcite.Linq.Runtime
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
        public static IEnumerable<TSource> FromJava<TSource>(Enumerable source)
        {
            ArgumentNullException.ThrowIfNull(source);

            var enumerator = source.enumerator();

            try
            {
                while (enumerator.moveNext())
                    yield return JavaValues.As<TSource>(enumerator.current());
            }
            finally
            {
                enumerator.close();
            }
        }

        /// <summary>
        /// Runs a compiled plan and reads its rows as a linq4j sequence.
        /// </summary>
        /// <param name="plan"></param>
        /// <param name="root"></param>
        /// <returns></returns>
        /// <remarks>
        /// What a converter out of this convention calls. Calcite compiles its side from generated source,
        /// which cannot mention an object, so the delegate travels on the DataContext and is called from here.
        /// </remarks>
        public static Enumerable Bind(System.Func<org.apache.calcite.DataContext, IEnumerable> plan, org.apache.calcite.DataContext root)
        {
            ArgumentNullException.ThrowIfNull(plan);

            return ToJava(System.Linq.Enumerable.Cast<object>(plan(root)));
        }

        /// <summary>
        /// Reads a .NET sequence as a linq4j one.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        public static Enumerable ToJava<TSource>(IEnumerable<TSource> source)
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
                return new JavaEnumerator<TSource>(source.GetEnumerator());
            }

        }

        /// <summary>
        /// A linq4j <see cref="Enumerator"/> reading a .NET one.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <remarks>
        /// linq4j positions before the first row and advances on <c>moveNext</c>, which is what
        /// <see cref="IEnumerator"/> does, so the two agree except over <c>reset</c>, which .NET is allowed to
        /// refuse.
        /// </remarks>
        sealed class JavaEnumerator<TSource>(IEnumerator<TSource> source) : Enumerator
        {

            /// <inheritdoc />
            public object current() => source.Current!;

            /// <inheritdoc />
            public bool moveNext() => source.MoveNext();

            /// <inheritdoc />
            public void reset() => source.Reset();

            /// <inheritdoc />
            public void close() => source.Dispose();

            /// <inheritdoc />
            /// <remarks>
            /// IKVM maps <c>java.lang.AutoCloseable</c>, which a linq4j Enumerator extends, onto
            /// <see cref="IDisposable"/>, so closing one from Java arrives here.
            /// </remarks>
            public void Dispose() => source.Dispose();

        }

    }

}
