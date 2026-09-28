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
    /// Produced by <see cref="ClrCursorRelImplementor.ImplementRoot"/>; the counterpart of the <c>Bindable</c>
    /// that Calcite's generated class implements. It holds the plan and no execution state, so one instance
    /// serves every execution of a prepared statement and can be opened any number of times.
    ///
    /// <para><see cref="Open"/> runs the plan's acquisition on the calling thread (a sort drains its input, a
    /// leaf executes its statement) and <see cref="OpenAsync"/> awaits the same acquisition. Either returns a
    /// cursor offering both <see cref="ClrCursor.Read"/> and <see cref="ClrCursor.ReadAsync"/> over one
    /// position, so the caller chooses how to open and then, on each advance, how to read.</para>
    ///
    /// <para>Each open is compiled on its first call, so a caller that opens only one way does not pay to
    /// compile the other.</para>
    /// </remarks>
    public sealed class ClrCursorFactory : IClrCursorFactory
    {

        readonly Expression<Func<DataContext, IClrCursor>> open;
        readonly Expression<Func<DataContext, CancellationToken, ValueTask<IClrCursor>>> openAsync;
        readonly Type elementType;

        Func<DataContext, IClrCursor>? compiledOpen;
        Func<DataContext, CancellationToken, ValueTask<IClrCursor>>? compiledOpenAsync;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="open">The plan as an open that acquires synchronously.</param>
        /// <param name="openAsync">The plan as an open that awaits its acquisition.</param>
        /// <param name="elementType">The CLR type of one row.</param>
        /// <remarks>
        /// Instances are created by <see cref="ClrCursorRelImplementor.ImplementRoot"/>, which builds both opens
        /// from one plan.
        /// </remarks>
        internal ClrCursorFactory(
            Expression<Func<DataContext, IClrCursor>> open,
            Expression<Func<DataContext, CancellationToken, ValueTask<IClrCursor>>> openAsync,
            Type elementType)
        {
            this.open = open ?? throw new ArgumentNullException(nameof(open));
            this.openAsync = openAsync ?? throw new ArgumentNullException(nameof(openAsync));
            this.elementType = elementType ?? throw new ArgumentNullException(nameof(elementType));
        }

        /// <summary>
        /// Gets the uncompiled expression tree of the open that acquires synchronously.
        /// </summary>
        /// <remarks>
        /// Exposes the operators the plan calls, for inspection.
        /// </remarks>
        public Expression<Func<DataContext, IClrCursor>> OpenExpression => open;

        /// <summary>
        /// Gets the uncompiled expression tree of the open that awaits its acquisition.
        /// </summary>
        public Expression<Func<DataContext, CancellationToken, ValueTask<IClrCursor>>> OpenAsyncExpression => openAsync;

        /// <summary>
        /// Gets the CLR type of one row.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>Typed.getElementType</c>. It is the same whichever way the plan is opened.
        /// </remarks>
        public Type ElementType => elementType;

        /// <summary>
        /// Opens a cursor over the plan's rows, running its acquisition on the calling thread.
        /// </summary>
        /// <param name="root">The context the query reads its schema, parameters and stashed values
        /// from.</param>
        /// <returns>The cursor, positioned before the first row.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="root"/> is null.</exception>
        /// <remarks>
        /// The counterpart of <c>Bindable.bind</c> followed by <c>enumerator()</c>: the plan's acquisition runs
        /// here, before the first row is read. A leaf that can only acquire asynchronously blocks the calling
        /// thread for that acquisition.
        /// </remarks>
        public IClrCursor Open(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            return (compiledOpen ??= open.Compile())(root);
        }

        /// <summary>
        /// Opens a cursor over the plan's rows, awaiting its acquisition.
        /// </summary>
        /// <param name="root">The context the query reads its schema, parameters and stashed values
        /// from.</param>
        /// <param name="cancellationToken">The token for the acquisition only; each
        /// <see cref="ClrCursor.ReadAsync"/> takes its own.</param>
        /// <returns>The cursor, positioned before the first row.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="root"/> is null.</exception>
        public ValueTask<IClrCursor> OpenAsync(DataContext root, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(root);

            return (compiledOpenAsync ??= openAsync.Compile())(root, cancellationToken);
        }

    }

}
