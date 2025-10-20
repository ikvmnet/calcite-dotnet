using System;
using System.Data.Common;

using org.apache.calcite.linq4j;
using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Implementation of <see cref="Enumerator"/> that wraps a <see cref="DbDataReader"/> and assumes ownership of a command and connection.
    /// </summary>
    class AdoReaderEnumerator : Enumerator
    {

        readonly DbConnection _connection;
        readonly DbCommand _command;
        readonly DbDataReader _reader;
        readonly Function1 _rowBuilderFactory;
        readonly Function0 _rowBuilder;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="command"></param>
        /// <param name="reader"></param>
        /// <param name="rowBuilderFactory"></param>
        public AdoReaderEnumerator(DbConnection connection, DbCommand command, DbDataReader reader, Function1 rowBuilderFactory)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _command = command ?? throw new ArgumentNullException(nameof(command));
            _reader = reader ?? throw new ArgumentNullException(nameof(reader));
            _rowBuilderFactory = rowBuilderFactory ?? throw new ArgumentNullException(nameof(rowBuilderFactory));
            _rowBuilder = (Function0)_rowBuilderFactory.apply(_reader);
        }

        /// <summary>
        /// Moves to the next row.
        /// </summary>
        /// <returns></returns>
        public bool moveNext()
        {
            return _reader.Read();
        }

        /// <summary>
        /// Gets the current row.
        /// </summary>
        /// <returns></returns>
        public object current()
        {
            return _rowBuilder.apply();
        }

        /// <summary>
        /// Resets the reader.
        /// </summary>
        public void reset()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Closes the instance.
        /// </summary>
        public void close()
        {
            TryDispose(_reader);
            TryDispose(_command);
            TryDispose(_connection);
        }

        /// <summary>
        /// Disposes of the instance.
        /// </summary>
        public void Dispose()
        {
            close();
            GC.SuppressFinalize(this);
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
