using System;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// A forward-only cursor over the rows of a plan, advanced synchronously or with await, as the reader
    /// chooses on each advance.
    /// </summary>
    /// <remarks>
    /// What a plan of the <c>ClrDataCursorConvention</c> calling convention opens. The counterpart of a
    /// linq4j <c>Enumerator</c>, with the difference that is the whole point of the convention: linq4j's
    /// <c>moveNext</c> returns a <c>boolean</c> and has nowhere to suspend, and an
    /// <see cref="System.Collections.Generic.IAsyncEnumerator{T}"/> takes its token once at
    /// <c>GetAsyncEnumerator</c> and never again. A cursor carries both advances as members over one
    /// position, so <c>DbDataReader.Read</c> is <see cref="Read"/> and <c>DbDataReader.ReadAsync(token)</c>
    /// is <see cref="ReadAsync"/> with the token that call was given — and nothing between the reader and
    /// the leaf has to decide in advance which it will be asked for.
    ///
    /// <para><b>One cursor, one position, two ways to advance it.</b> A consumer may call either member on
    /// any advance: a row read with <see cref="Read"/> and the next with <see cref="ReadAsync"/> are
    /// consecutive rows of one result. The plan behind a cursor is not built twice for the two members. It
    /// is opened once, and every operator's cursor holds its input cursor and its own state once, with
    /// <see cref="Read"/> and <see cref="ReadAsync"/> each stepping the same fields.</para>
    ///
    /// <para><b>Positioned before the first row when opened</b>, as a linq4j <c>Enumerator</c> and a
    /// <see cref="System.Collections.Generic.IEnumerator{T}"/> both are; <see cref="Current"/> is undefined
    /// until an advance has returned <see langword="true"/>, and undefined again after one has returned
    /// <see langword="false"/>. Opening is not reading: obtaining a cursor runs the plan's acquisition — a
    /// sort drains its input, a leaf executes its statement — exactly as obtaining a linq4j enumerator does,
    /// and the first advance reads the first row.</para>
    ///
    /// <para><b><see cref="Read"/> and <see cref="ReadAsync"/> are both required</b>, because both are the
    /// contract: a cursor that answered one by wrapping the other would block a thread per row or promise
    /// asynchrony it cannot deliver, and either is a decision to be made at the site that makes it, visibly,
    /// rather than by a default. <see cref="Dispose"/> is required and <see cref="DisposeAsync"/> defaults
    /// to it, which is .NET's own shape for the pair; a cursor holding something whose release can await
    /// overrides it.</para>
    /// </remarks>
    public abstract class ClrDataCursor : IDisposable, IAsyncDisposable
    {

        /// <summary>
        /// Gets the row the cursor is positioned on.
        /// </summary>
        /// <remarks>
        /// The typed member is on <see cref="ClrDataCursor{T}"/>; this is the same row as an object, which
        /// is what a reader that knows only the row's runtime shape reads. A row is never a value type — the
        /// physical type boxes what the type factory answers — so nothing is boxed here.
        /// </remarks>
        public object? Current => CurrentObject;

        /// <summary>
        /// Gets the row the cursor is positioned on, for <see cref="Current"/> to answer.
        /// </summary>
        protected abstract object? CurrentObject { get; }

        /// <summary>
        /// Advances to the next row.
        /// </summary>
        /// <returns><see langword="true"/> if there was a row, <see langword="false"/> if the cursor has
        /// passed the last one.</returns>
        /// <remarks>
        /// <c>Enumerator.moveNext</c>. Where the row has to be awaited for — the leaf is a provider that
        /// answers asynchronously and nothing else — this blocks for it, with the synchronization context
        /// suppressed before the wait rather than around it, because the continuation is promised at the
        /// moment of suspension and that moment is inside the call.
        /// </remarks>
        public abstract bool Read();

        /// <summary>
        /// Advances to the next row, awaiting it where it has to be waited for.
        /// </summary>
        /// <param name="cancellationToken">The token for this advance, and this one only.</param>
        /// <returns><see langword="true"/> if there was a row, <see langword="false"/> if the cursor has
        /// passed the last one.</returns>
        /// <remarks>
        /// <see cref="Read"/> with somewhere to suspend. The token is the one the caller gave this call,
        /// handed down through every operator to the leaf, which is what a
        /// <c>DbDataReader.ReadAsync(token)</c> means and what an
        /// <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/> had nowhere to put.
        /// </remarks>
        public abstract ValueTask<bool> ReadAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Closes the cursor and releases what it holds, whether or not a row was ever read.
        /// </summary>
        /// <remarks>
        /// <c>Enumerator.close</c>, and idempotent as that is. An operator's cursor disposes its input
        /// cursors here, so disposing the root disposes the whole plan.
        /// </remarks>
        public abstract void Dispose();

        /// <summary>
        /// Closes the cursor, awaiting any release that has something to await.
        /// </summary>
        /// <remarks>
        /// By default <see cref="Dispose"/>, completed. A cursor whose release can suspend — one over an
        /// <see cref="IAsyncDisposable"/> source — overrides this, and an operator's cursor overrides it to
        /// dispose its inputs the same way.
        /// </remarks>
        public virtual ValueTask DisposeAsync()
        {
            Dispose();

            return default;
        }

    }

    /// <summary>
    /// A <see cref="ClrDataCursor"/> whose rows are of a known type.
    /// </summary>
    /// <typeparam name="T">The row type, which is the plan's physical row type: an <c>object[]</c>, a
    /// synthetic record, or the boxed value of a one-column result.</typeparam>
    /// <remarks>
    /// What every operator of the convention takes and returns, so that a plan is checked as it is built:
    /// a node that hands up a cursor of the wrong row type does not compile. The root hands out the base
    /// class, because the plan's caller reads rows as objects.
    /// </remarks>
    public abstract class ClrDataCursor<T> : ClrDataCursor
    {

        /// <summary>
        /// Gets the row the cursor is positioned on.
        /// </summary>
        public new abstract T Current { get; }

        /// <inheritdoc />
        protected sealed override object? CurrentObject => Current;

    }

}
