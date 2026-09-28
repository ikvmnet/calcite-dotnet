using System.Data;
using System.Data.Common;

using org.apache.calcite.linq4j;
using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// An <see cref="AdoEnumerable"/> that runs a query and yields its rows. Created by
    /// <see cref="AdoEnumerable.CreateReader(AdoDataSource, string)"/> and its overloads.
    /// </summary>
    public class AdoReaderEnumerable : AdoEnumerable
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource">The data source to run the query against.</param>
        /// <param name="sql">The query.</param>
        /// <param name="rowBuilderFactory">Makes the row builder for the reader.</param>
        internal AdoReaderEnumerable(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory) :
            base(dataSource, sql, rowBuilderFactory)
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource">The data source to run the query against.</param>
        /// <param name="sql">The query.</param>
        /// <param name="rowBuilderFactory">Makes the row builder for the reader.</param>
        /// <param name="dbCommandEnricher">Called with each command before it executes.</param>
        internal AdoReaderEnumerable(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory, DbCommandEnricher dbCommandEnricher) :
            base(dataSource, sql, rowBuilderFactory, dbCommandEnricher)
        {

        }

        /// <inheritdoc />
        protected override Enumerator CreateEnumerator(DbConnection connection, DbCommand command)
        {
            try
            {
                return new AdoReaderEnumerator(connection, command, command.ExecuteReader(), RowBuilderFactory);
            }
            catch (DataException e)
            {
                throw new AdoCalciteException("Exception while performing query.", e);
            }
        }

    }

}
