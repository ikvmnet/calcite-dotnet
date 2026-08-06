using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Reads a query's rows as a .NET sequence.
    /// </summary>
    /// <remarks>
    /// What <see cref="AdoEnumerable"/> does for a plan of Calcite's calling convention, this does for one of
    /// <c>ClrEnumerableConvention</c>: the same connection, command and enricher, and the same row builder,
    /// yielding an <see cref="IEnumerable{T}"/> rather than a linq4j <c>Enumerable</c>. A plan that ends here
    /// reads its rows without a linq4j enumerator between the reader and the operator above it.
    /// </remarks>
    public static class AdoSequences
    {

        /// <summary>
        /// Executes a query and returns its rows.
        /// </summary>
        /// <param name="dataSource">The source to open a connection against.</param>
        /// <param name="sql">The statement to execute.</param>
        /// <param name="rowBuilder">Builds one row from the reader's current position.</param>
        /// <param name="enricher">Fills the command's parameters, or <see langword="null"/> where it has
        /// none.</param>
        /// <returns>The rows, read as the query is enumerated.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="AdoCalciteException">The query could not be executed.</exception>
        public static IEnumerable<TRow> Read<TRow>(AdoDataSource dataSource, string sql, Func<DbDataReader, TRow> rowBuilder, DbCommandEnricher? enricher)
        {
            ArgumentNullException.ThrowIfNull(dataSource);
            ArgumentNullException.ThrowIfNull(sql);
            ArgumentNullException.ThrowIfNull(rowBuilder);

            DbConnection connection;
            DbCommand command;
            DbDataReader reader;

            try
            {
                connection = dataSource.OpenConnection();
                command = connection.CreateCommand();
                command.CommandText = sql;
                enricher?.Enrich(command);
                reader = command.ExecuteReader();
            }
            catch (DbException e)
            {
                throw new AdoCalciteException("Exception while enumerating query.", e);
            }

            return Rows(connection, command, reader, rowBuilder);
        }

        /// <summary>
        /// Yields the reader's rows and closes what was opened for them.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="command"></param>
        /// <param name="reader"></param>
        /// <param name="rowBuilder"></param>
        /// <returns></returns>
        static IEnumerable<TRow> Rows<TRow>(DbConnection connection, DbCommand command, DbDataReader reader, Func<DbDataReader, TRow> rowBuilder)
        {
            using (connection)
            using (command)
            using (reader)
            {
                while (reader.Read())
                    yield return rowBuilder(reader);
            }
        }

    }

}
