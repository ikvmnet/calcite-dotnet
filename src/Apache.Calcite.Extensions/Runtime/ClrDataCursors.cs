using System;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// Carries a cursor between the two ways a plan of the <c>ClrDataCursorConvention</c> can be opened.
    /// </summary>
    /// <remarks>
    /// A node of that convention has two bodies: one composes the opens that acquire synchronously, one the
    /// opens that await their acquisition. The cursor they produce is the same cursor either way — it is
    /// the <em>open</em> that differs, not what is opened — so crossing between the two is cheap in one
    /// direction and a blocked thread in the other, and this is where both are written.
    ///
    /// <para><see cref="Completed{T}"/> is the cheap one: an open that acquired synchronously is already
    /// done, and wrapping it in a completed <see cref="ValueTask{TResult}"/> allocates nothing.
    /// <see cref="Block{T}"/> waits for an open that awaits, on the calling thread, with the
    /// synchronization context suppressed <em>before</em> the open is called and not merely around the
    /// wait — a continuation is captured at the moment of suspension, which is inside the call's synchronous
    /// phase, so suppressing it afterwards is too late. That is why it takes the open as a delegate rather
    /// than the task the open returns. Measured twice on the enumerable convention's crossing, and not
    /// measured a third time.</para>
    ///
    /// <para><b>Internal, and it stays internal</b>, for the reason <c>ClrSequences</c> is: it is what the
    /// convention's own plans are built from, not a toolkit for an adapter.</para>
    /// </remarks>
    static class ClrDataCursors
    {

        /// <summary>
        /// Returns a cursor that was opened synchronously as the result of an open that awaits.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cursor"></param>
        /// <returns></returns>
        /// <remarks>
        /// What a node's default <c>ImplementAsync</c> is over its <c>Implement</c>, and what an operator
        /// that acquires nothing at open answers from its awaiting body. Nothing here suspends and nothing
        /// is promised that cannot be delivered: the cursor's own <c>ReadAsync</c> still awaits wherever a
        /// row has to be waited for.
        /// </remarks>
        public static ValueTask<ClrDataCursor<T>> Completed<T>(ClrDataCursor<T> cursor)
        {
            ArgumentNullException.ThrowIfNull(cursor);

            return new ValueTask<ClrDataCursor<T>>(cursor);
        }

        /// <summary>
        /// Runs an open that awaits and blocks for the cursor it opens.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="open">The open, called here so that the context is suppressed before it starts.</param>
        /// <returns></returns>
        /// <remarks>
        /// <b>This blocks a thread for the whole of the acquisition</b>, and there is no version of it that
        /// does not: a synchronous open has nowhere to suspend. A node whose only real body is the awaiting
        /// one writes its <c>Implement</c> as a delegation through this, and that is a decision made where
        /// it can be read.
        ///
        /// <para>The token is <see cref="CancellationToken.None"/>, because a synchronous open has none to
        /// give.</para>
        /// </remarks>
        public static ClrDataCursor<T> Block<T>(Func<CancellationToken, ValueTask<ClrDataCursor<T>>> open)
        {
            ArgumentNullException.ThrowIfNull(open);

            var context = SynchronizationContext.Current;
            if (context == null)
                return Wait(open(CancellationToken.None));

            SynchronizationContext.SetSynchronizationContext(null);

            try
            {
                return Wait(open(CancellationToken.None));
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(context);
            }
        }

        /// <summary>
        /// Reads an open that awaits a typed cursor as one that awaits the untyped base.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="open"></param>
        /// <returns></returns>
        /// <remarks>
        /// What the awaiting root ends in. The synchronous root converts by reference and needs nothing; a
        /// <see cref="ValueTask{TResult}"/> is invariant, so the awaiting root has to go through one
        /// continuation to change the type parameter. It costs a state machine only when the open actually
        /// suspends.
        /// </remarks>
        public static ValueTask<ClrDataCursor> Untyped<T>(ValueTask<ClrDataCursor<T>> open)
        {
            if (open.IsCompletedSuccessfully)
                return new ValueTask<ClrDataCursor>(open.Result);

            return Awaited(open);

            static async ValueTask<ClrDataCursor> Awaited(ValueTask<ClrDataCursor<T>> open)
            {
                return await open.ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Advances a cursor whose rows have to be awaited for, blocking for the row with the context
        /// suppressed before the advance runs.
        /// </summary>
        /// <param name="cursor"></param>
        /// <returns></returns>
        /// <remarks>
        /// What a cursor over an asynchronous source writes its <see cref="ClrDataCursor.Read"/> as. Every
        /// wait here suppresses the context before the call for the reason the class remarks give.
        /// </remarks>
        internal static bool BlockRead(ClrDataCursor cursor)
        {
            var context = SynchronizationContext.Current;
            if (context == null)
                return Wait(cursor.ReadAsync(CancellationToken.None));

            SynchronizationContext.SetSynchronizationContext(null);

            try
            {
                return Wait(cursor.ReadAsync(CancellationToken.None));
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(context);
            }
        }

        /// <summary>
        /// Disposes something whose disposal has to be awaited for, blocking for it with the context
        /// suppressed before the disposal runs.
        /// </summary>
        /// <param name="disposable"></param>
        internal static void BlockDispose(IAsyncDisposable disposable)
        {
            var context = SynchronizationContext.Current;
            if (context == null)
            {
                Wait(disposable.DisposeAsync());
                return;
            }

            SynchronizationContext.SetSynchronizationContext(null);

            try
            {
                Wait(disposable.DisposeAsync());
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(context);
            }
        }

        /// <summary>
        /// Waits for a <see cref="ValueTask{TResult}"/> the calling thread cannot await.
        /// </summary>
        /// <remarks>
        /// One that has not completed cannot be blocked on directly — its awaiter may be backed by a
        /// recyclable source — so it goes through <see cref="ValueTask{TResult}.AsTask"/>, which allocates.
        /// The completed case is the common one and skips that.
        /// </remarks>
        internal static TResult Wait<TResult>(ValueTask<TResult> task)
        {
            return task.IsCompletedSuccessfully ? task.Result : task.AsTask().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Waits for a <see cref="ValueTask"/> the calling thread cannot await.
        /// </summary>
        /// <remarks>
        /// The completed case is still observed rather than dropped, because that is what surfaces a
        /// failure.
        /// </remarks>
        internal static void Wait(ValueTask task)
        {
            if (task.IsCompletedSuccessfully)
            {
                task.GetAwaiter().GetResult();
                return;
            }

            task.AsTask().GetAwaiter().GetResult();
        }

    }

}
