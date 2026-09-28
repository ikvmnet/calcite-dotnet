using System;
using System.Data;
using System.Data.Common;

using org.apache.calcite.linq4j;
using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// An <see cref="AdoEnumerable"/> that runs a statement returning no rows and yields a single row holding the
    /// affected row count as a <see cref="java.lang.Integer"/>. Created by
    /// <see cref="AdoEnumerable.CreateUpdate(AdoDataSource, string)"/> and its overloads.
    /// </summary>
    public class AdoUpdateEnumerable : AdoEnumerable
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource">The data source to run the statement against.</param>
        /// <param name="sql">The statement.</param>
        /// <param name="rowBuilderFactory">Required by the base class and not used.</param>
        internal AdoUpdateEnumerable(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory) :
            base(dataSource, sql, rowBuilderFactory)
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource">The data source to run the statement against.</param>
        /// <param name="sql">The statement.</param>
        /// <param name="rowBuilderFactory">Required by the base class and not used.</param>
        /// <param name="dbCommandEnricher">Called with each command before it executes.</param>
        internal AdoUpdateEnumerable(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory, DbCommandEnricher dbCommandEnricher) :
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
            catch (DbException e)
            {
                throw new AdoCalciteException("Database exception while performing query.", e);
            }
            finally
            {
                TryDispose(command);
                TryDispose(connection);
            }
        }

        /// <summary>
        /// Disposes an object, ignoring any exception it throws.
        /// </summary>
        /// <param name="disposable">The object to dispose.</param>
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
