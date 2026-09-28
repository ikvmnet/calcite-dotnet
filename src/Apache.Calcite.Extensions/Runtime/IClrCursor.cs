using System;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// A forward-only cursor over the rows of a plan, advanced either synchronously or asynchronously.
    /// </summary>
    /// <remarks>
    /// <see cref="Read"/> and <see cref="ReadAsync"/> advance the same position, so a consumer may choose on
    /// every advance which to call; a row read with one and the next row read with the other are consecutive
    /// rows of one result. The token given to <see cref="ReadAsync"/> applies to that advance only and is
    /// passed down to the source the advance reads from.
    ///
    /// <para>A compiled plan returns one from either of its opens, and a cursor table supplies one as a
    /// leaf. <see cref="ClrCursor"/> is an abstract base that implements it.</para>
    /// </remarks>
    public interface IClrCursor : IDisposable, IAsyncDisposable
    {

        /// <summary>
        /// Gets the row the cursor is positioned on.
        /// </summary>
        object? Current { get; }

        /// <summary>
        /// Advances to the next row.
        /// </summary>
        /// <returns><see langword="true"/> if the cursor is on a row; <see langword="false"/> if it has passed
        /// the last one.</returns>
        bool Read();

        /// <summary>
        /// Advances to the next row asynchronously.
        /// </summary>
        /// <param name="cancellationToken">The token that cancels this advance.</param>
        /// <returns><see langword="true"/> if the cursor is on a row; <see langword="false"/> if it has passed
        /// the last one.</returns>
        ValueTask<bool> ReadAsync(CancellationToken cancellationToken);

    }

    /// <summary>
    /// A forward-only cursor over rows of a known type.
    /// </summary>
    /// <typeparam name="T">The type of one row.</typeparam>
    public interface IClrCursor<out T> : IClrCursor
    {

        /// <summary>
        /// Gets the row the cursor is positioned on.
        /// </summary>
        new T Current { get; }

    }

}
