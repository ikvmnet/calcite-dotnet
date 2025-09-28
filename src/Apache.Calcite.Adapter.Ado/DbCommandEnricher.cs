using System.Data.Common;

namespace Apache.Calcite.Adapter.Ado
{

    /// <summary>
    /// Functional interface to enrich a <see cref="DbCommand"/>.
    /// </summary>
    public interface DbCommandEnricher
    {

        /// <summary>
        /// Enrichers the <see cref="DbCommand"/>.
        /// </summary>
        /// <param name="command"></param>
        void Enrich(DbCommand command);

    }

}
