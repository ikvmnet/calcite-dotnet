using System;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;
using org.apache.calcite.linq4j;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Carries rows between a linq4j <see cref="Enumerable"/> and a <see cref="ClrDataCursor"/>.
    /// </summary>
    /// <remarks>
    /// <see cref="JavaSequences"/> for the cursor convention: this is the whole of what a converter between
    /// <c>EnumerableConvention</c> and <c>ClrDataCursorConvention</c> does. The rows are not touched — both
    /// conventions ask the same <c>JavaTypeFactory</c> what a field is — and each value is converted rather
    /// than cast at the boundary, for the reason <see cref="JavaSequences.FromJava{TSource}"/> gives.
    /// </remarks>
    static class JavaCursors
    {

        /// <summary>
        /// Opens a cursor over a linq4j sequence.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// A linq4j sub-plan executes at <c>enumerator()</c> — <c>ResultSetEnumerable</c> runs its JDBC
        /// statement there — so the crossing calls it here, at the open, where the rest of the plan's
        /// acquisition happens. The cursor's <see cref="ClrDataCursor.ReadAsync"/> completes synchronously,
        /// because a linq4j <see cref="Enumerator"/> is pulled and cannot be anything else.
        /// </remarks>
        public static ClrDataCursor<TSource> FromJava<TSource>(Enumerable source)
        {
            ArgumentNullException.ThrowIfNull(source);

            return new JavaEnumeratorCursor<TSource>(source.enumerator());
        }

        /// <summary>
        /// <see cref="FromJava{TSource}"/>, as an open that awaits. There is nothing to await, and that is
        /// the honest shape of it: a plan reading a Calcite sub-plan this way is simply not asynchronous
        /// over that part of itself, and cannot be.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static ValueTask<ClrDataCursor<TSource>> FromJavaAsync<TSource>(Enumerable source, CancellationToken cancellationToken)
        {
            return new ValueTask<ClrDataCursor<TSource>>(FromJava<TSource>(source));
        }

        /// <summary>
        /// A cursor over a linq4j <see cref="Enumerator"/>.
        /// </summary>
        sealed class JavaEnumeratorCursor<TSource>(Enumerator enumerator) : ClrDataCursor<TSource>
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
            /// The per-advance token stops rows crossing once it has fired and reaches no further: a table
            /// of Calcite's convention reads <c>DataContext.Variable.CANCEL_FLAG</c> and may be inside
            /// <c>moveNext()</c>. Tying that flag to the statement's token is the data context's job.
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
        /// Reads a plan of the cursor convention as a linq4j sequence, for Calcite's side of a converter
        /// out of it.
        /// </summary>
        /// <param name="plan">The sub-plan, which compiles itself the first time it is run.</param>
        /// <param name="root">The context the query is being run against.</param>
        /// <returns></returns>
        /// <remarks>
        /// The sequence's <c>enumerator()</c> is the plan's synchronous open, because a linq4j
        /// <c>Enumerator</c> is pulled and the generated source calling it cannot await.
        /// </remarks>
        public static Enumerable ToJava(ClrPlan<ClrDataCursor> plan, DataContext root)
        {
            ArgumentNullException.ThrowIfNull(plan);
            ArgumentNullException.ThrowIfNull(root);

            return new CursorEnumerable(plan, root);
        }

        /// <summary>
        /// A linq4j <see cref="Enumerable"/> whose every enumerator is an open of a cursor plan.
        /// </summary>
        sealed class CursorEnumerable(ClrPlan<ClrDataCursor> plan, DataContext root) : AbstractEnumerable
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
        /// linq4j positions before the first row and advances on <c>moveNext</c>, which is what a cursor
        /// does, so the two agree except over <c>reset</c>. That is live — <c>CartesianProductEnumerator</c>
        /// rewinds the inner side once per row of the outer — and a cursor is forward only, so the plan is
        /// opened afresh, which is what the enumerable it stands for would do when asked for a second
        /// enumerator.
        /// </remarks>
        sealed class CursorEnumerator(ClrPlan<ClrDataCursor> plan, DataContext root) : Enumerator
        {

            ClrDataCursor cursor = plan.Invoke(root);

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
            /// <c>AutoCloseable</c> arrives as <see cref="IDisposable"/> under IKVM, and it is the same call.
            /// </remarks>
            public void Dispose() => close();

        }

    }

}
