using System;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;
using org.apache.calcite.linq4j;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Carries rows between a linq4j <see cref="Enumerable"/> and a <see cref="ClrCursor"/>.
    /// </summary>
    /// <remarks>
    /// Used by the converters between <c>EnumerableConvention</c> and <c>ClrCursorConvention</c>. Both
    /// conventions take their field types from the same <c>JavaTypeFactory</c>, so the values in a row need
    /// no conversion.
    /// </remarks>
    static class JavaCursors
    {

        /// <summary>
        /// Opens a cursor over a linq4j sequence.
        /// </summary>
        /// <remarks>
        /// Calls <c>enumerator()</c> immediately, since that is where a linq4j sub-plan does its work (a
        /// <c>ResultSetEnumerable</c> executes its statement there). The cursor's
        /// <see cref="ClrCursor.ReadAsync"/> always completes synchronously.
        /// </remarks>
        /// <typeparam name="TSource">The CLR type of the sequence's elements.</typeparam>
        /// <param name="source">The linq4j sequence.</param>
        /// <returns>An open cursor over the sequence's enumerator; disposing it closes the enumerator.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public static IClrCursor<TSource> FromJava<TSource>(Enumerable source)
        {
            ArgumentNullException.ThrowIfNull(source);

            return new JavaEnumeratorCursor<TSource>(source.enumerator());
        }

        /// <summary>
        /// <see cref="FromJava{TSource}"/> as an awaiting open, which completes synchronously.
        /// </summary>
        /// <typeparam name="TSource">The CLR type of the sequence's elements.</typeparam>
        /// <param name="source">The linq4j sequence.</param>
        /// <param name="cancellationToken">Not observed; the open completes before it could be.</param>
        /// <returns>A completed task of the open cursor.</returns>
        public static ValueTask<IClrCursor<TSource>> FromJavaAsync<TSource>(Enumerable source, CancellationToken cancellationToken)
        {
            return new ValueTask<IClrCursor<TSource>>(FromJava<TSource>(source));
        }

        /// <summary>
        /// A cursor over a linq4j <see cref="Enumerator"/>.
        /// </summary>
        sealed class JavaEnumeratorCursor<TSource>(Enumerator enumerator) : ClrCursor<TSource>
        {

            TSource current = default!;

            /// <inheritdoc />
            public override TSource Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                if (enumerator.moveNext() == false)
                    return false;

                current = JavaValues.As<TSource>(enumerator.current());
                return true;
            }

            /// <inheritdoc />
            /// <remarks>
            /// The token is checked before the advance but cannot interrupt <c>moveNext()</c>. Calcite code
            /// observes cancellation through <c>DataContext.Variable.CANCEL_FLAG</c>, which the data context
            /// is responsible for setting.
            /// </remarks>
            public override ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();

                return new ValueTask<bool>(Read());
            }

            /// <inheritdoc />
            public override void Dispose() => enumerator.close();

        }

        /// <summary>
        /// Reads a plan of the cursor convention as a linq4j sequence.
        /// </summary>
        /// <param name="plan">The sub-plan.</param>
        /// <param name="root">The context the query is being run against.</param>
        /// <returns>A sequence whose <c>enumerator()</c> opens the plan synchronously.</returns>
        public static Enumerable ToJava(ClrPlan<IClrCursor> plan, DataContext root)
        {
            ArgumentNullException.ThrowIfNull(plan);
            ArgumentNullException.ThrowIfNull(root);

            return new CursorEnumerable(plan, root);
        }

        /// <summary>
        /// A linq4j <see cref="Enumerable"/> that opens the plan for each enumerator.
        /// </summary>
        sealed class CursorEnumerable(ClrPlan<IClrCursor> plan, DataContext root) : AbstractEnumerable
        {

            /// <inheritdoc />
            public override Enumerator enumerator()
            {
                return new CursorEnumerator(plan, root);
            }

        }

        /// <summary>
        /// A linq4j <see cref="Enumerator"/> reading a cursor.
        /// </summary>
        /// <remarks>
        /// A cursor is forward-only, so <c>reset</c>, which linq4j's <c>CartesianProductEnumerator</c> calls
        /// once per outer row, disposes the cursor and opens the plan again.
        /// </remarks>
        sealed class CursorEnumerator(ClrPlan<IClrCursor> plan, DataContext root) : Enumerator
        {

            IClrCursor cursor = plan.Invoke(root);

            /// <inheritdoc />
            public object? current() => cursor.Current;

            /// <inheritdoc />
            public bool moveNext() => cursor.Read();

            /// <inheritdoc />
            public void reset()
            {
                cursor.Dispose();
                cursor = plan.Invoke(root);
            }

            /// <inheritdoc />
            public void close() => cursor.Dispose();

            /// <inheritdoc />
            /// <remarks>
            /// IKVM maps <c>java.lang.AutoCloseable</c> onto <see cref="IDisposable"/>.
            /// </remarks>
            public void Dispose() => close();

        }

    }

}
