using System.Threading;

using org.apache.calcite;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// The statement's cancellation as a plan of this convention reads it.
    /// </summary>
    /// <remarks>
    /// A <see cref="DataContext"/> is how every per-execution fact reaches a plan: the plan's lambda takes
    /// one, and a generated block names it by a well-known parameter — Calcite's <c>DataContext.ROOT</c>,
    /// which <c>ClrEnumerableRelImplementor</c> binds to the lambda's own. Cancellation travels the same
    /// way and by the same means, which is the point of this class.
    ///
    /// <para><b>Two forms, one context, one source.</b> Calcite's convention reads
    /// <c>DataContext.Variable.CANCEL_FLAG</c>, an <c>AtomicBoolean</c> a table polls between rows, because
    /// a linq4j <c>Enumerator</c> is pulled and has nowhere to suspend. This convention reads
    /// <see cref="CancellationTokenName"/>, a <see cref="CancellationToken"/>, because its operators do suspend and a token
    /// is the thing .NET's asynchronous I/O accepts — <c>DbDataReader.ReadAsync(token)</c> aborts a read in
    /// flight where polling a boolean between rows never could. <c>StatementDataContext</c> puts both in
    /// from one source, so a node reads whichever form it can act on and a caller cancels once.</para>
    ///
    /// <para>The name is this convention's rather than one of Calcite's: <c>DataContext.Variable</c> is a
    /// Java enum and closed, and <c>DataContext.get</c> takes any name.</para>
    ///
    /// <para><b>The token is read once, at the root, and becomes the enumerator's.</b>
    /// <c>ClrEnumerableRelImplementor.ImplementRootAsync</c> wraps the plan in
    /// <see cref="ClrEnumerables.WithCancellation{TSource}"/>, and from there the existing chain carries it:
    /// every operator hands the token it was given at <c>GetAsyncEnumerator</c> to its own source's, down
    /// to the leaf. Threading it through the operators' token <em>parameters</em> instead was written and
    /// does not work — the factory operators, which are most of them, ignore that parameter by
    /// construction, their token arriving at <c>GetAsyncEnumerator</c> — and making all forty-one honour
    /// both would be a linked source per operator per enumeration to duplicate a channel that already
    /// carries it.</para>
    /// </remarks>
    static class ClrDataContexts
    {

        /// <summary>
        /// The name the statement's <see cref="CancellationToken"/> is stashed under, as
        /// <c>DataContext.Variable.CANCEL_FLAG.camelName</c> is the name Calcite's half is stashed under.
        /// </summary>
        public const string CancellationTokenName = "clrCancellationToken";

        /// <summary>
        /// Reads the statement's cancellation out of the context it is executing against.
        /// </summary>
        /// <param name="root"></param>
        /// <returns>The token, or <see cref="CancellationToken.None"/> where the context carries none.</returns>
        /// <remarks>
        /// What the awaiting root reads, once per execution, to wrap the plan with. A
        /// <see cref="DataContext"/> is an SPI a caller may implement, so a context that never heard of this
        /// name answers null and the plan runs uncancelled — the same allowance Calcite's own readers of
        /// <c>CANCEL_FLAG</c> make.
        /// </remarks>
        public static CancellationToken GetCancellationToken(DataContext root)
        {
            if (root is null)
                return CancellationToken.None;

            return root.get(CancellationTokenName) is CancellationToken token ? token : CancellationToken.None;
        }

        /// <summary>
        /// <see cref="GetCancellationToken"/>, which the awaiting root builds its call to.
        /// </summary>
        public static readonly System.Reflection.MethodInfo GetCancellationTokenMethod = typeof(ClrDataContexts).GetMethod(nameof(GetCancellationToken))
            ?? throw new System.InvalidOperationException($"'{nameof(GetCancellationToken)}' is missing.");

    }

}
