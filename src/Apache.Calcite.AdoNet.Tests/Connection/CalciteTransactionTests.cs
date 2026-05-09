using System;
using System.Data;


using Xunit;

namespace Apache.Calcite.AdoNet.Tests.Connection
{

    public class CalciteTransactionTests
    {

        [Fact]
        public void Commit_should_complete_transaction()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            var tx = (CalciteTransaction)c.BeginTransaction();
            tx.Commit();
            Assert.Throws<InvalidOperationException>(() => tx.Commit());
        }

        [Fact]
        public void Rollback_should_complete_transaction()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            var tx = (CalciteTransaction)c.BeginTransaction();
            tx.Rollback();
            Assert.Throws<InvalidOperationException>(() => tx.Rollback());
        }

        [Fact]
        public void Dispose_should_complete_transaction_silently()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            var tx = (CalciteTransaction)c.BeginTransaction();
            tx.Dispose();
            Assert.Throws<InvalidOperationException>(() => tx.Commit());
        }

        [Theory]
        [InlineData(IsolationLevel.ReadCommitted)]
        [InlineData(IsolationLevel.Serializable)]
        [InlineData(IsolationLevel.Snapshot)]
        public void IsolationLevel_should_be_preserved(IsolationLevel level)
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var tx = c.BeginTransaction(level);
            Assert.Equal(level, tx.IsolationLevel);
        }

    }

}
