using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents the result of a Calcite execution. Result rows are consumed via
    /// <see cref="ReadAsync(CancellationToken)"/>; <see cref="Current"/> reflects the most recently advanced row.
    /// </summary>
    internal interface ICalciteResult : IDisposable
    {

        /// <summary>
        /// Gets the column metadata for the result set, in column order.
        /// </summary>
        IReadOnlyList<CalciteColumn> Columns { get; }

        /// <summary>
        /// Gets the number of rows affected by the executed statement, when applicable.
        /// </summary>
        long RecordsAffected { get; }

        /// <summary>
        /// Gets the most recently materialized row, or <see langword="null"/> if no row is current.
        /// </summary>
        IReadOnlyList<object?>? Current { get; }

        /// <summary>
        /// Advances to the next row.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns><see langword="true"/> if a row was read; otherwise, <see langword="false"/>.</returns>
        Task<bool> ReadAsync(CancellationToken cancellationToken);

    }

}
