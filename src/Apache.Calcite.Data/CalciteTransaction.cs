using System;
using System.Data;
using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a transaction to be made against an Apache Calcite engine. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// Transactional semantics depend on the underlying engine configuration. For engines that do not
    /// support transactions, <see cref="Commit"/> and <see cref="Rollback"/> simply mark the transaction
    /// as completed without affecting the engine state.
    /// </remarks>
    public sealed class CalciteTransaction : DbTransaction
    {

        readonly CalciteConnection _connection;
        readonly IsolationLevel _isolationLevel;
        bool _completed;

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
        }

        /// <inheritdoc />
        public override void Rollback()
        {
            ThrowIfCompleted();
            _completed = true;
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
