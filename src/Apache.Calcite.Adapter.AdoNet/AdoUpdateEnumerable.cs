using System;
using System.Data;
using System.Data.Common;

using org.apache.calcite.linq4j;
using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Implements an enumerable that executes a statement and reports a single value for the updated record count.
    /// </summary>
    public class AdoUpdateEnumerable : AdoEnumerable
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="rowBuilderFactory"></param>
        internal AdoUpdateEnumerable(DbDataSource dataSource, string sql, Function1 rowBuilderFactory) :
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
        internal AdoUpdateEnumerable(DbDataSource dataSource, string sql, Function1 rowBuilderFactory, Action<DbCommand> dbCommandEnricher) :
            base(dataSource, sql, rowBuilderFactory, dbCommandEnricher)
        {

        }

        /// <inheritdoc />
        protected override Enumerator CreateEnumerator(DbConnection connection, DbCommand command)
        {
            try
            {
                return Linq4j.singletonEnumerator(new java.lang.Integer(command.ExecuteNonQuery()));
            }
            catch (DataException e)
            {
                throw new AdoSchemaException("Exception while performing query.", e);
            }
            finally
            {
                TryDispose(command);
                TryDispose(connection);
            }
        }

        /// <summary>
        /// Attempts to dispose the instance, ignoring any exceptions.
        /// </summary>
        /// <param name="disposable"></param>
        void TryDispose(IDisposable disposable)
        {
            try
            {
                disposable.Dispose();
            }
            catch
            {

            }
        }

    }

}
