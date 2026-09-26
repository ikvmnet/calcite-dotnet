using System;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// A forward-only cursor over the rows of a plan, advanced synchronously or with await over one position.
    /// </summary>
    /// <remarks>
    /// What a plan of the cursor convention hands back from either of its opens, and what a table of the
    /// cursor SPI hands in. <see cref="Read"/> and <see cref="ReadAsync"/> step the same position, so a
    /// consumer chooses on every advance how to read, and a row read with one and the next with the other
    /// are consecutive rows of one result. The token an advance is given reaches the leaf that advance runs.
    ///
    /// <para><see cref="ClrCursor"/> is the base every cursor of this project derives from, and implements
    /// this; a source that is a cursor already implements this directly.</para>
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
        /// <returns><see langword="true"/> if there is a row; <see langword="false"/> past the last.</returns>
        bool Read();

        /// <summary>
        /// Advances to the next row, awaiting whatever the advance has to wait on.
        /// </summary>
        /// <param name="cancellationToken">The token this advance runs under.</param>
        /// <returns><see langword="true"/> if there is a row; <see langword="false"/> past the last.</returns>
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
