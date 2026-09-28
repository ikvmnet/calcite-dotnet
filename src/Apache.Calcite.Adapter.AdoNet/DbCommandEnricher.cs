using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Prepares a <see cref="DbCommand"/> after its text is set and before it is executed.
    /// </summary>
    /// <remarks>
    /// The adapter uses one to add a pushed-down statement's parameters (see
    /// <see cref="AdoEnumerable.CreateEnricher"/>). The plans the adapter generates pass no enricher of the caller's;
    /// one can be given to <see cref="AdoEnumerable.CreateReader(AdoDataSource, string, org.apache.calcite.linq4j.function.Function1, DbCommandEnricher)"/>,
    /// <see cref="AdoEnumerable.CreateUpdate(AdoDataSource, string, org.apache.calcite.linq4j.function.Function1, DbCommandEnricher)"/>
    /// and <see cref="AdoCursors"/> directly.
    /// </remarks>
    public interface DbCommandEnricher
    {

        /// <summary>
        /// Prepares the command, for example by adding parameters or setting a timeout.
        /// </summary>
        /// <param name="command">The command, with its text set and on an open connection.</param>
        void Enrich(DbCommand command);

    }

}
