using System;
using System.Data;
using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Manages a transaction over a <see cref="CalciteConnection"/>.
    /// </summary>
    public class CalciteTransaction : DbTransaction
    {

        readonly CalciteConnection _connection;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="isolationLevel"></param>
        internal CalciteTransaction(CalciteConnection connection, IsolationLevel isolationLevel)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));

            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the connection associated to this transaction.
        /// </summary>
        protected override DbConnection DbConnection => _connection;

        /// <inheritdoc />
        public new CalciteConnection Connection => _connection;

        /// <summary>
        /// Gets the isolation level of the current transaction.
        /// </summary>
        public override IsolationLevel IsolationLevel => throw new NotImplementedException();

        /// <summary>
        /// Commmits the current transaction.
        /// </summary>
        /// <exception cref="CalciteDbException"></exception>
        public override void Commit()
        {
            if (_connection.State != ConnectionState.Open)
                throw new CalciteDbException("Connection must be open commit a transaction.");

            throw new NotImplementedException();
        }

        /// <summary>
        /// Rolls back the current transaction.
        /// </summary>
        /// <exception cref="CalciteDbException"></exception>
        public override void Rollback()
        {
            if (_connection.State != ConnectionState.Open)
                throw new CalciteDbException("Connection must be open commit a transaction.");

            throw new NotImplementedException();
        }

#if NET6_0_OR_GREATER
        #region SavePoints

        /// <inheritdoc />
        public override bool SupportsSavepoints => throw new NotImplementedException();

        /// <inheritdoc />
        public override void Save(string savepointName)
        {
            if (_connection.State != ConnectionState.Open)
                throw new CalciteDbException("Connection must be open commit a transaction.");

            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void Rollback(string savepointName)
        {
            if (_connection.State != ConnectionState.Open)
                throw new CalciteDbException("Connection must be open commit a transaction.");

            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void Release(string savepointName)
        {
            if (_connection.State != ConnectionState.Open)
                throw new CalciteDbException("Connection must be open commit a transaction.");

            throw new NotImplementedException();
        }

        #endregion
#endif

    }

}
