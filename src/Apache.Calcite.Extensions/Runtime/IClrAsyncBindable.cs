using System.Collections.Generic;

using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;

using org.apache.calcite;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// A compiled plan of the <see cref="ClrAsyncEnumerableConvention"/> calling convention, bound to a
    /// <see cref="DataContext"/> when it is run.
    /// </summary>
    /// <remarks>
    /// <see cref="IClrBindable"/> with the sequence swapped. There is no cancellation parameter for the same
    /// reason the leaf SPI has none: a token enters at
    /// <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator"/>, and every operator of the plan reads it from
    /// there.
    /// </remarks>
    public interface IClrAsyncBindable : IClrBindableBase
    {

        /// <summary>
        /// Runs the plan against a <see cref="DataContext"/> and returns its rows.
        /// </summary>
        /// <param name="root">The context the query reads its schema, parameters and stashed values
        /// from.</param>
        /// <returns>The rows, produced as the sequence is enumerated.</returns>
        IAsyncEnumerable<object> Bind(DataContext root);

    }

}
