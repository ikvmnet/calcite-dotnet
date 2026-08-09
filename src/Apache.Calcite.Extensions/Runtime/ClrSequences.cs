using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// Carries a sequence of rows between an <see cref="IEnumerable{T}"/> and an
    /// <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    /// <remarks>
    /// This is the whole of what a converter between <c>ClrEnumerableConvention</c> and
    /// <c>ClrAsyncEnumerableConvention</c> does. The rows are not touched, and there is nothing to touch: the
    /// two conventions share <c>ClrPhysType</c>, so a row of one already is a row of the other. Only the
    /// sequence around it differs.
    ///
    /// <para>The two directions are not the same cost, and the asymmetry is the point.
    /// <see cref="ToAsyncEnumerable{TSource}"/> costs a state machine and no thread; nothing it produces ever
    /// suspends. <see cref="ToEnumerable{TSource}"/> blocks the calling thread once per row, which is the
    /// sync-over-async the asynchronous convention was written to avoid. Both exist because a plan that
    /// cannot be assembled is worse than one that is slow.</para>
    /// </remarks>
    static class ClrSequences
    {

        /// <summary>
        /// Reads an asynchronous sequence as a synchronous one, blocking on each row.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// <b>This blocks a thread per row, and there is no version of it that does not.</b> An
        /// <see cref="IEnumerable{T}"/> has nowhere to suspend, so a caller that wants rows from an
        /// asynchronous plan through this interface waits for them. That is a fact about
        /// <see cref="IEnumerator{T}.MoveNext"/> rather than about this method.
        ///
        /// <para>The token is <see cref="CancellationToken.None"/> because the synchronous convention has no
        /// token to give: a plan of it is a <c>Func&lt;DataContext, IEnumerable&lt;object&gt;&gt;</c>, and the
        /// consumer's only way out is to stop enumerating. Abandoning the sequence disposes the enumerator,
        /// which is what ends the asynchronous plan under it.</para>
        ///
        /// <para><b>The synchronization context is suppressed for the length of each wait</b>, and that is
        /// load bearing rather than tidy. The operators of the asynchronous convention await without
        /// <c>ConfigureAwait(false)</c> — <c>await foreach (var row in source.WithCancellation(token))</c>
        /// continues on the captured context — so a caller that blocked a thread carrying one would wait for
        /// a continuation that can only run on the thread it is blocking. Nulling the context before the call
        /// means there is nothing to capture and the continuation goes to the thread pool. Where there is no
        /// context, which is every thread a query is normally read on, this costs one read.</para>
        /// </remarks>
        public static IEnumerable<TSource> ToEnumerable<TSource>(IAsyncEnumerable<TSource> source)
        {
            ArgumentNullException.ThrowIfNull(source);

            var enumerator = source.GetAsyncEnumerator(CancellationToken.None);

            try
            {
                while (Block(enumerator.MoveNextAsync()))
                    yield return enumerator.Current;
            }
            finally
            {
                Block(enumerator.DisposeAsync());
            }
        }

        /// <summary>
        /// Waits for a <see cref="ValueTask{TResult}"/> the calling thread cannot await.
        /// </summary>
        /// <param name="task"></param>
        /// <returns></returns>
        /// <remarks>
        /// A <see cref="ValueTask{TResult}"/> that has not completed cannot be blocked on directly — its
        /// awaiter is backed by an <see cref="System.Threading.Tasks.Sources.IValueTaskSource{TResult}"/>
        /// that may be recycled the moment it is read once — so it goes through
        /// <see cref="ValueTask{TResult}.AsTask"/>, which allocates. The completed case is the common one and
        /// skips that.
        /// </remarks>
        static bool Block(ValueTask<bool> task)
        {
            if (task.IsCompletedSuccessfully)
                return task.Result;

            var context = SynchronizationContext.Current;
            if (context == null)
                return task.AsTask().GetAwaiter().GetResult();

            SynchronizationContext.SetSynchronizationContext(null);

            try
            {
                return task.AsTask().GetAwaiter().GetResult();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(context);
            }
        }

        /// <summary>
        /// Waits for a <see cref="ValueTask"/> the calling thread cannot await.
        /// </summary>
        /// <param name="task"></param>
        /// <remarks>
        /// <see cref="Block(ValueTask{bool})"/> for the disposal, which has no value. The completed case is
        /// still awaited rather than dropped, because that is what observes a failure.
        /// </remarks>
        static void Block(ValueTask task)
        {
            if (task.IsCompletedSuccessfully)
            {
                task.GetAwaiter().GetResult();
                return;
            }

            var context = SynchronizationContext.Current;
            if (context == null)
            {
                task.AsTask().GetAwaiter().GetResult();
                return;
            }

            SynchronizationContext.SetSynchronizationContext(null);

            try
            {
                task.AsTask().GetAwaiter().GetResult();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(context);
            }
        }

        /// <summary>
        /// Reads a synchronous sequence as an asynchronous one.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>JavaSequences.FromJavaAsync</c> over a .NET sequence rather than a linq4j one, and the same
        /// honest shape: nothing here suspends, because the source is pulled. Producing an asynchronous
        /// sequence that always completes synchronously costs a state machine and no thread — it is not the
        /// sync-over-async of <see cref="ToEnumerable{TSource}"/>, which is a caller blocked waiting. A plan
        /// reading a synchronous sub-plan this way is simply not asynchronous over that part of itself.
        ///
        /// <para>The token is checked per row rather than awaited on, which is the only way an operator that
        /// never suspends can honour one.</para>
        ///
        /// <para>The trailing <c>await</c> is what makes the compiler accept an async iterator that has
        /// nothing to await, as <c>FromJavaAsync</c> does for the same reason.</para>
        /// </remarks>
        public static async IAsyncEnumerable<TSource> ToAsyncEnumerable<TSource>(IEnumerable<TSource> source, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);

            foreach (var row in source)
            {
                cancellationToken.ThrowIfCancellationRequested();

                yield return row;
            }

            await Task.CompletedTask;
        }

    }

}
