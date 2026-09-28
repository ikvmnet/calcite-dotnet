using System;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// Converts between a synchronous open and an awaiting open of a <c>ClrCursorConvention</c> plan, and
    /// blocks on asynchronous cursor operations for callers that cannot await.
    /// </summary>
    /// <remarks>
    /// A node of the convention has a body that opens its cursor synchronously and one that awaits the open;
    /// both produce the same cursor. <see cref="Completed{T}"/> turns a synchronous open into an awaiting one
    /// without allocating. <see cref="Block{T}"/>, <see cref="BlockRead"/> and <see cref="BlockDispose"/>
    /// block the calling thread on an awaiting operation.
    ///
    /// <para>Every blocking member clears the synchronization context before it starts the operation, not
    /// just around the wait. A continuation captures the context when the operation first suspends, which
    /// happens inside the call, and a source such as a table's awaiting scan may not use
    /// <c>ConfigureAwait(false)</c>; blocking with a single-threaded context in place would then deadlock.
    /// That is why <see cref="Block{T}"/> takes the open as a delegate rather than the task it returns.</para>
    /// </remarks>
    static class ClrCursors
    {

        /// <summary>
        /// Wraps a cursor that was opened synchronously as the completed result of an awaiting open.
        /// </summary>
        /// <typeparam name="T">The row type.</typeparam>
        /// <param name="cursor">The opened cursor.</param>
        /// <returns>A completed task holding <paramref name="cursor"/>.</returns>
        /// <remarks>
        /// Used by a node's default <c>ImplementAsync</c> and by operators that do no work at open. The
        /// cursor's own <c>ReadAsync</c> still awaits where a row has to be waited for.
        /// </remarks>
        public static ValueTask<IClrCursor<T>> Completed<T>(IClrCursor<T> cursor)
        {
            ArgumentNullException.ThrowIfNull(cursor);

            return new ValueTask<IClrCursor<T>>(cursor);
        }

        /// <summary>
        /// Runs an awaiting open and blocks the calling thread until it has produced its cursor.
        /// </summary>
        /// <typeparam name="T">The row type.</typeparam>
        /// <param name="open">The open. It is called here, after the synchronization context is cleared.</param>
        /// <returns>The opened cursor.</returns>
        /// <remarks>
        /// A node whose only real body is the awaiting one implements its synchronous body through this. The
        /// open is passed <see cref="CancellationToken.None"/>, since a synchronous open has no token.
        /// </remarks>
        public static IClrCursor<T> Block<T>(Func<CancellationToken, ValueTask<IClrCursor<T>>> open)
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
        /// Converts an awaiting open of a typed cursor into an awaiting open of an untyped one.
        /// </summary>
        /// <typeparam name="T">The row type.</typeparam>
        /// <param name="open">The typed open.</param>
        /// <returns>The same open, typed as <see cref="IClrCursor"/>.</returns>
        /// <remarks>
        /// The awaiting root of a plan ends in this. <see cref="ValueTask{TResult}"/> is invariant, so the
        /// conversion needs a continuation; it allocates a state machine only when the open has not already
        /// completed.
        /// </remarks>
        public static ValueTask<IClrCursor> Untyped<T>(ValueTask<IClrCursor<T>> open)
        {
            if (open.IsCompletedSuccessfully)
                return new ValueTask<IClrCursor>(open.Result);

            return Awaited(open);

            static async ValueTask<IClrCursor> Awaited(ValueTask<IClrCursor<T>> open)
            {
                return await open.ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Advances a cursor through its <see cref="IClrCursor.ReadAsync"/>, blocking the calling thread for
        /// the result.
        /// </summary>
        /// <param name="cursor">The cursor to advance.</param>
        /// <returns>What <see cref="IClrCursor.ReadAsync"/> returned.</returns>
        /// <remarks>
        /// A cursor over an asynchronous-only source implements <see cref="ClrCursor.Read"/> with this.
        /// </remarks>
        internal static bool BlockRead(IClrCursor cursor)
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
        /// Disposes an <see cref="IAsyncDisposable"/>, blocking the calling thread until disposal completes.
        /// </summary>
        /// <param name="disposable">The object to dispose.</param>
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
        /// Blocks the calling thread on a <see cref="ValueTask{TResult}"/> and returns its result.
        /// </summary>
        /// <remarks>
        /// An incomplete <see cref="ValueTask{TResult}"/> may be backed by a reusable source that does not
        /// support blocking on its awaiter, so it is converted with <see cref="ValueTask{TResult}.AsTask"/>
        /// first. A completed one is read directly.
        /// </remarks>
        internal static TResult Wait<TResult>(ValueTask<TResult> task)
        {
            return task.IsCompletedSuccessfully ? task.Result : task.AsTask().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Blocks the calling thread on a <see cref="ValueTask"/>.
        /// </summary>
        /// <remarks>
        /// A completed task is still observed, so that a failure is rethrown.
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
