using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Provides a callback that can customize a <see cref="DbCommand"/> before it is executed by the
    /// Calcite ADO.NET adapter.
    /// </summary>
    /// <remarks>
    /// Implement this interface to inject provider-specific settings such as command timeout,
    /// additional parameters, or query hints that cannot be expressed in the SQL text alone.
    /// </remarks>
    public interface DbCommandEnricher
    {

        /// <summary>
        /// Applies customizations to the <paramref name="command"/> before it is executed.
        /// </summary>
        /// <param name="command">The command to customize.</param>
        void Enrich(DbCommand command);

    }

}
