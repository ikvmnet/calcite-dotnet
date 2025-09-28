using System;
using System.Data;
using System.Data.Common;

using org.apache.calcite.linq4j;
using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Implements an enumerable that executes a statement that returns multiple records.
    /// </summary>
    public class AdoReaderEnumerable : AdoEnumerable
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="rowBuilderFactory"></param>
        internal AdoReaderEnumerable(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory) :
            base(dataSource, sql, rowBuilderFactory)
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="rowBuilderFactory"></param>
        /// <param name="dbCommandEnricher"></param>
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
                throw new AdoSchemaException("Exception while performing query.", e);
            }
        }

    }

}
