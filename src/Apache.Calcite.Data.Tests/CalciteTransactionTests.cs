using System;
using System.Data;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    public class CalciteTransactionTests
    {

        [Fact]
        public void Commit_should_complete_transaction()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            Assert.Throws<NotSupportedException>(() => c.BeginTransaction());
        }

        [Fact]
        public void Rollback_should_complete_transaction()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            Assert.Throws<NotSupportedException>(() => c.BeginTransaction());
        }

        [Fact]
        public void Dispose_should_complete_transaction_silently()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            Assert.Throws<NotSupportedException>(() => c.BeginTransaction());
        }

        [Theory]
        [InlineData(IsolationLevel.ReadCommitted)]
        [InlineData(IsolationLevel.Serializable)]
        [InlineData(IsolationLevel.Snapshot)]
        public void IsolationLevel_should_be_preserved(IsolationLevel level)
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            Assert.Throws<NotSupportedException>(() => c.BeginTransaction(level));
        }

    }

}
