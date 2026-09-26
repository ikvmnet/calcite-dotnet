using System;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// A plan of the <see cref="ClrCursorConvention"/> calling convention, ready to open against a
    /// <see cref="DataContext"/>.
    /// </summary>
    /// <remarks>
    /// What <see cref="ClrCursorRelImplementor.ImplementRoot"/> produces, and the counterpart of the
    /// <c>Bindable</c> Calcite's Janino-generated class is: one object per prepared statement, re-openable,
    /// holding the plan and nothing about any execution of it.
    ///
    /// <para><b>Two opens, one cursor.</b> <see cref="Open"/> runs the plan's acquisition synchronously —
    /// a sort drains its input, a leaf executes its statement, on the calling thread — and
    /// <see cref="OpenAsync"/> awaits the same acquisition. What either hands back is a
    /// <see cref="ClrCursor"/> with both <see cref="ClrCursor.Read"/> and
    /// <see cref="ClrCursor.ReadAsync"/> over one position, so a caller chooses how to open and then
    /// chooses again, on every advance, how to read. <c>DbCommand.ExecuteReader</c> is
    /// <see cref="Open"/>, <c>ExecuteReaderAsync(token)</c> is <see cref="OpenAsync"/>, and the reader's two
    /// advances are the cursor's.</para>
    ///
    /// <para>It is the <see cref="IClrCursorBindable"/> a prepared statement carries: the counterpart
    /// of the <c>Bindable</c> Calcite's generated class is, produced at the same point.</para>
    ///
    /// <para><b>Each open is compiled the first time it is called</b>, and not before. The implementor
    /// builds both trees when it implements the root, because both are the plan; compiling is JIT work
    /// that a caller who only ever opens one way should not pay for the other. The two are compiled
    /// independently and at most once each, because a factory is shared by every execution of its
    /// statement.</para>
    /// </remarks>
    public sealed class ClrCursorFactory : IClrCursorBindable
    {

        readonly Expression<Func<DataContext, ClrCursor>> open;
        readonly Expression<Func<DataContext, CancellationToken, ValueTask<ClrCursor>>> openAsync;
        readonly Type elementType;

        Func<DataContext, ClrCursor>? compiledOpen;
        Func<DataContext, CancellationToken, ValueTask<ClrCursor>>? compiledOpenAsync;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="open">The plan as an open that acquires synchronously.</param>
        /// <param name="openAsync">The plan as an open that awaits its acquisition.</param>
        /// <param name="elementType">The CLR type of one row.</param>
        /// <remarks>
        /// Internal, so that <see cref="ClrCursorRelImplementor.ImplementRoot"/> is the only way of
        /// making one: that is where the two trees are required to be two opens of one plan.
        /// </remarks>
        internal ClrCursorFactory(
            Expression<Func<DataContext, ClrCursor>> open,
            Expression<Func<DataContext, CancellationToken, ValueTask<ClrCursor>>> openAsync,
            Type elementType)
        {
            this.open = open ?? throw new ArgumentNullException(nameof(open));
            this.openAsync = openAsync ?? throw new ArgumentNullException(nameof(openAsync));
            this.elementType = elementType ?? throw new ArgumentNullException(nameof(elementType));
        }

        /// <summary>
        /// Gets the plan as an open that acquires synchronously, before it is compiled.
        /// </summary>
        /// <remarks>
        /// The tree, so that a caller can read what the plan is made of — which operators it names, and
        /// that every one of them is a synchronous open — where a compiled delegate says nothing.
        /// </remarks>
        public Expression<Func<DataContext, ClrCursor>> OpenExpression => open;

        /// <summary>
        /// Gets the plan as an open that awaits its acquisition, before it is compiled.
        /// </summary>
        public Expression<Func<DataContext, CancellationToken, ValueTask<ClrCursor>>> OpenAsyncExpression => openAsync;

        /// <summary>
        /// Gets the CLR type of one row.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>Typed.getElementType</c>, and what <c>Meta.CursorFactory.deduce</c> is
        /// given; the same answer whichever way the plan is opened, because what a row is does not depend on
        /// how it was waited for.
        /// </remarks>
        public Type ElementType => elementType;

        /// <summary>
        /// Opens a cursor over the plan's rows, running its acquisition on the calling thread.
        /// </summary>
        /// <param name="root">The context the query reads its schema, parameters and stashed values
        /// from.</param>
        /// <returns>The cursor, positioned before the first row.</returns>
        /// <remarks>
        /// <c>Bindable.bind</c> followed by <c>enumerator()</c>: obtaining the cursor runs the plan, and
        /// reading rows is what comes after. A leaf that can only acquire asynchronously blocks here for
        /// that acquisition; nothing above it does.
        /// </remarks>
        public ClrCursor Open(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            return (compiledOpen ??= open.Compile())(root);
        }

        /// <summary>
        /// Opens a cursor over the plan's rows, awaiting its acquisition.
        /// </summary>
        /// <param name="root">The context the query reads its schema, parameters and stashed values
        /// from.</param>
        /// <param name="cancellationToken">The token for the acquisition. It is this open's and not the
        /// cursor's: each advance takes its own.</param>
        /// <returns>The cursor, positioned before the first row.</returns>
        public ValueTask<ClrCursor> OpenAsync(DataContext root, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(root);

            return (compiledOpenAsync ??= openAsync.Compile())(root, cancellationToken);
        }

    }

}
