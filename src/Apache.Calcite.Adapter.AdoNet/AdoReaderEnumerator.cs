using System;
using System.Data.Common;

using org.apache.calcite.linq4j;
using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// A linq4j <see cref="Enumerator"/> over a <see cref="DbDataReader"/>. It owns the reader, its command and its
    /// connection, and disposes all three when closed.
    /// </summary>
    class AdoReaderEnumerator : Enumerator
    {

        readonly DbConnection _connection;
        readonly DbCommand _command;
        readonly DbDataReader _reader;
        readonly Function1 _rowBuilderFactory;
        readonly Function0 _rowBuilder;

        /// <summary>
        /// Initializes a new instance, creating the row builder from the reader.
        /// </summary>
        /// <param name="connection">The connection the reader was opened on.</param>
        /// <param name="command">The command that produced the reader.</param>
        /// <param name="reader">The reader.</param>
        /// <param name="rowBuilderFactory">A <see cref="Function1"/> from the reader to a <see cref="Function0"/>
        /// that returns the current row.</param>
        public AdoReaderEnumerator(DbConnection connection, DbCommand command, DbDataReader reader, Function1 rowBuilderFactory)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _command = command ?? throw new ArgumentNullException(nameof(command));
            _reader = reader ?? throw new ArgumentNullException(nameof(reader));
            _rowBuilderFactory = rowBuilderFactory ?? throw new ArgumentNullException(nameof(rowBuilderFactory));
            _rowBuilder = (Function0)_rowBuilderFactory.apply(_reader);
        }

        /// <summary>
        /// Advances the reader.
        /// </summary>
        /// <returns>Whether there is a row.</returns>
        public bool moveNext()
        {
            return _reader.Read();
        }

        /// <summary>
        /// Builds the current row. Each call builds it again.
        /// </summary>
        /// <returns>The row.</returns>
        public object current()
        {
            return _rowBuilder.apply();
        }

        /// <summary>
        /// Not supported: a reader cannot be rewound.
        /// </summary>
        /// <exception cref="NotImplementedException">Always.</exception>
        public void reset()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Disposes the reader, the command and the connection, ignoring any exception each throws.
        /// </summary>
        public void close()
        {
            TryDispose(_reader);
            TryDispose(_command);
            TryDispose(_connection);
        }

        /// <summary>
        /// Calls <see cref="close"/>.
        /// </summary>
        public void Dispose()
        {
            close();
            GC.SuppressFinalize(this);
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
