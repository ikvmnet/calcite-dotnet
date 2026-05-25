using System;
using System.Data;
using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a transaction on a <see cref="CalciteConnection"/>. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// Apache Calcite does not support transactions. <see cref="Commit"/> and <see cref="Rollback"/>
    /// both throw <see cref="NotSupportedException"/>. This class exists to satisfy the ADO.NET
    /// <see cref="DbTransaction"/> contract expected by frameworks that require a non-null transaction.
    /// </remarks>
    public sealed class CalciteTransaction : DbTransaction
    {

        readonly CalciteConnection _connection;
        readonly IsolationLevel _isolationLevel;
        bool _completed;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="isolationLevel"></param>
        /// <exception cref="ArgumentNullException"></exception>
        internal CalciteTransaction(CalciteConnection connection, IsolationLevel isolationLevel)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _isolationLevel = isolationLevel;
        }

        /// <inheritdoc />
        public override IsolationLevel IsolationLevel => _isolationLevel;

        /// <inheritdoc />
        protected override DbConnection DbConnection => _connection;

        /// <inheritdoc />
        public override void Commit()
        {
            ThrowIfCompleted();
            _completed = true;
            throw new NotSupportedException("Commit is not supported by Apache Calcite.");
        }

        /// <inheritdoc />
        public override void Rollback()
        {
            ThrowIfCompleted();
            _completed = true;
            throw new NotSupportedException("Rollback is not supported by Apache Calcite.");
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            _completed = true;
            base.Dispose(disposing);
        }

        void ThrowIfCompleted()
        {
            if (_completed)
                throw new InvalidOperationException("Transaction has already been committed, rolled back, or disposed.");
        }

    }

}
