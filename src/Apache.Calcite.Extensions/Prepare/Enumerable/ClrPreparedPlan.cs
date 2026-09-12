using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;

namespace Apache.Calcite.Extensions.Prepare.Enumerable
{

    /// <summary>
    /// A planned statement, readable as an <see cref="IEnumerable{T}"/> or as an
    /// <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    /// <remarks>
    /// <b>One plan, two endings.</b> The nodes are the same nodes; what differs is which implementor walks
    /// them, so a statement is no longer prepared for one kind of caller. That is the point of there being
    /// one convention: a connection that asks for rows synchronously and one that awaits them share
    /// everything up to and including the planned tree, and a plan cache keyed on the statement would hold
    /// one entry rather than two.
    ///
    /// <para><b>Each ending is implemented and compiled the first time it is asked for</b>, and not before.
    /// Preparing a statement therefore costs what it always did — most of it is parse, validate and plan;
    /// translation and compilation together were measured at 1 to 4 ms of an 18 to 94 ms prepare — and a
    /// caller that only ever reads one way never pays for the other. The lazies are thread-safe because a
    /// signature is shared: a compiled plan is re-bindable, and two threads reaching first for different
    /// endings must not race.</para>
    /// </remarks>
    sealed class ClrPreparedPlan : IClrBindable, IClrAsyncBindable
    {

        readonly Lazy<IClrBindable> synchronous;
        readonly Lazy<IClrAsyncBindable> asynchronous;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="internalParameters">The values a plan reaches through the
        /// <see cref="DataContext"/>.</param>
        /// <param name="rel">The planned root.</param>
        /// <param name="prefer">How the caller wants a row represented.</param>
        public ClrPreparedPlan(java.util.Map internalParameters, ClrEnumerableRel rel, ClrEnumerablePrefer prefer)
        {
            ArgumentNullException.ThrowIfNull(internalParameters);
            ArgumentNullException.ThrowIfNull(rel);
            ArgumentNullException.ThrowIfNull(prefer);

            synchronous = new Lazy<IClrBindable>(() => ClrEnumerableInterpretable.ToBindable(internalParameters, rel, prefer));
            asynchronous = new Lazy<IClrAsyncBindable>(() => ClrEnumerableInterpretable.ToAsyncBindable(internalParameters, rel, prefer));
        }

        /// <inheritdoc />
        IEnumerable<object> IClrBindable.Bind(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            return synchronous.Value.Bind(root);
        }

        /// <inheritdoc />
        IAsyncEnumerable<object> IClrAsyncBindable.Bind(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            return asynchronous.Value.Bind(root);
        }

        /// <inheritdoc />
        /// <remarks>
        /// What a row is does not depend on which ending is read, so this answers without implementing
        /// either. The synchronous one is built because it is the cheaper of the two to reach and both give
        /// the same answer; a caller asking only this and never binding still pays one implement, which is
        /// what a cursor factory needs anyway.
        /// </remarks>
        public Type ElementType => synchronous.Value.ElementType;

    }

}
