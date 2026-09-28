using System;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// An abstract <see cref="IClrCursor"/>: a forward-only cursor that the reader may advance synchronously
    /// or asynchronously, choosing on each advance.
    /// </summary>
    /// <remarks>
    /// The counterpart of a linq4j <c>Enumerator</c>. Unlike an <c>Enumerator</c> it can suspend, and unlike
    /// an <see cref="System.Collections.Generic.IAsyncEnumerator{T}"/> it takes a cancellation token on each
    /// asynchronous advance rather than once, which is the shape of <c>DbDataReader</c>.
    ///
    /// <para>A cursor is positioned before the first row when it is opened. <see cref="Current"/> is
    /// undefined until an advance has returned <see langword="true"/>, and again after one has returned
    /// <see langword="false"/>. Opening a plan's cursor does the work the plan needs before its first row, as
    /// obtaining a linq4j enumerator does; the first advance reads the first row.</para>
    ///
    /// <para>A derived class implements both <see cref="Read"/> and <see cref="ReadAsync"/>, each advancing
    /// the same state. <see cref="DisposeAsync"/> defaults to <see cref="Dispose"/>; a cursor whose release
    /// can suspend overrides it.</para>
    /// </remarks>
    public abstract class ClrCursor : IClrCursor
    {

        /// <summary>
        /// Gets the row the cursor is positioned on.
        /// </summary>
        /// <remarks>
        /// <see cref="ClrCursor{T}.Current"/> returns the same row typed.
        /// </remarks>
        public object? Current => CurrentObject;

        /// <summary>
        /// Gets the row the cursor is positioned on, for <see cref="Current"/> to answer.
        /// </summary>
        protected abstract object? CurrentObject { get; }

        /// <summary>
        /// Advances to the next row.
        /// </summary>
        /// <returns><see langword="true"/> if the cursor is on a row, <see langword="false"/> if it has
        /// passed the last one.</returns>
        /// <remarks>
        /// The counterpart of <c>Enumerator.moveNext</c>. Where the source can only supply the row
        /// asynchronously, this blocks the calling thread until it arrives.
        /// </remarks>
        public abstract bool Read();

        /// <summary>
        /// Advances to the next row asynchronously.
        /// </summary>
        /// <param name="cancellationToken">The token that cancels this advance. It applies to this call
        /// only and is passed down to the source the advance reads from.</param>
        /// <returns><see langword="true"/> if the cursor is on a row, <see langword="false"/> if it has
        /// passed the last one.</returns>
        public abstract ValueTask<bool> ReadAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Closes the cursor and releases what it holds, whether or not a row was read.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>Enumerator.close</c>. An operator's cursor disposes its input cursors, so
        /// disposing the root cursor disposes the whole plan.
        /// </remarks>
        public abstract void Dispose();

        /// <summary>
        /// Closes the cursor asynchronously.
        /// </summary>
        /// <returns>A task that completes when the cursor is closed.</returns>
        /// <remarks>
        /// Calls <see cref="Dispose"/> and returns a completed task unless overridden. A cursor over an
        /// <see cref="IAsyncDisposable"/> source overrides it.
        /// </remarks>
        public virtual ValueTask DisposeAsync()
        {
            Dispose();

            return default;
        }

    }

    /// <summary>
    /// A <see cref="ClrCursor"/> whose rows are of a known type.
    /// </summary>
    /// <typeparam name="T">The row type, which is the plan's physical row type: an <c>object[]</c>, a
    /// synthetic record, or the boxed value of a one-column result.</typeparam>
    /// <remarks>
    /// A cursor table may derive from this, or implement <see cref="IClrCursor{T}"/> directly.
    /// </remarks>
    public abstract class ClrCursor<T> : ClrCursor, IClrCursor<T>
    {

        /// <summary>
        /// Gets the row the cursor is positioned on.
        /// </summary>
        public new abstract T Current { get; }

        /// <inheritdoc />
        protected sealed override object? CurrentObject => Current;

    }

}
