using System;
using System.Data;
using System.Data.Common;

using java.sql;

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

            if (_connection._connection is null)
                throw new InvalidOperationException();

            if (_connection._connection.getAutoCommit() == false)
                throw new CalciteDbException("A JDBC transaction is already open.");
            else
            {
                // set isolation level
                _connection._connection.setTransactionIsolation(isolationLevel switch
                {
                    IsolationLevel.Unspecified => java.sql.Connection.TRANSACTION_NONE,
                    IsolationLevel.Chaos => java.sql.Connection.TRANSACTION_NONE,
                    IsolationLevel.ReadUncommitted => java.sql.Connection.TRANSACTION_READ_UNCOMMITTED,
                    IsolationLevel.ReadCommitted => java.sql.Connection.TRANSACTION_READ_COMMITTED,
                    IsolationLevel.RepeatableRead => java.sql.Connection.TRANSACTION_REPEATABLE_READ,
                    IsolationLevel.Serializable => java.sql.Connection.TRANSACTION_SERIALIZABLE,
                    _ => throw new CalciteDbException("Unsupported IsolationLevel for JDBC."),
                });

                // disable auto commit
                _connection._connection.setAutoCommit(false);
            }
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
        public override IsolationLevel IsolationLevel => _connection._connection?.getTransactionIsolation() switch
        {
            java.sql.Connection.TRANSACTION_NONE => IsolationLevel.Unspecified,
            java.sql.Connection.TRANSACTION_READ_COMMITTED => IsolationLevel.ReadCommitted,
            java.sql.Connection.TRANSACTION_READ_UNCOMMITTED => IsolationLevel.ReadUncommitted,
            java.sql.Connection.TRANSACTION_REPEATABLE_READ => IsolationLevel.RepeatableRead,
            java.sql.Connection.TRANSACTION_SERIALIZABLE => IsolationLevel.Serializable,
            null => throw new InvalidOperationException(),
            _ => throw new CalciteDbException($"Unrecognized transaction value.")
        };

        /// <summary>
        /// Commmits the current transaction.
        /// </summary>
        /// <exception cref="CalciteDbException"></exception>
        public override void Commit()
        {
            if (_connection.State != ConnectionState.Open)
                throw new CalciteDbException("Connection must be open commit a transaction.");

            if (_connection._connection is null)
                throw new InvalidOperationException();

            try
            {
                _connection._connection.commit();
                _connection._connection.setAutoCommit(true);
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Rolls back the current transaction.
        /// </summary>
        /// <exception cref="CalciteDbException"></exception>
        public override void Rollback()
        {
            if (_connection.State != ConnectionState.Open)
                throw new CalciteDbException("Connection must be open commit a transaction.");

            if (_connection._connection is null)
                throw new InvalidOperationException();

            try
            {
                _connection._connection.rollback();
                _connection._connection.setAutoCommit(true);
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <inheritdoc />
        public override bool SupportsSavepoints => false;

    }

}
