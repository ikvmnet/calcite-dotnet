using System;
using System.Data;
using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a transaction on a <see cref="CalciteConnection"/>. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// Apache Calcite does not support transactions. <see cref="DbConnection.BeginTransaction()"/> on a
    /// <see cref="CalciteConnection"/> throws <see cref="NotSupportedException"/>, so the provider never hands
    /// out an instance of this class; it is the type <see cref="CalciteCommand"/> and <see cref="CalciteBatch"/>
    /// declare their <c>Transaction</c> property as. <see cref="Commit"/> and <see cref="Rollback"/> both throw
    /// <see cref="NotSupportedException"/>.
    /// </remarks>
    public sealed class CalciteTransaction : DbTransaction
    {

        readonly CalciteConnection _connection;
        readonly IsolationLevel _isolationLevel;
        bool _completed;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="connection">The connection the transaction belongs to.</param>
        /// <param name="isolationLevel">The isolation level <see cref="IsolationLevel"/> reports.</param>
        /// <exception cref="ArgumentNullException"><paramref name="connection"/> is <see langword="null"/>.</exception>
        internal CalciteTransaction(CalciteConnection connection, IsolationLevel isolationLevel)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _isolationLevel = isolationLevel;
        }

        /// <inheritdoc />
        public override IsolationLevel IsolationLevel => _isolationLevel;

        /// <inheritdoc />
        protected override DbConnection DbConnection => _connection;

        /// <summary>
        /// Not supported.
        /// </summary>
        /// <exception cref="NotSupportedException">Always, on the first call.</exception>
        /// <exception cref="InvalidOperationException">The transaction has already been committed, rolled back or disposed.</exception>
        public override void Commit()
        {
            ThrowIfCompleted();
            _completed = true;
            throw new NotSupportedException("Commit is not supported by Apache Calcite.");
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        /// <exception cref="NotSupportedException">Always, on the first call.</exception>
        /// <exception cref="InvalidOperationException">The transaction has already been committed, rolled back or disposed.</exception>
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
