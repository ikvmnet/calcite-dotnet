using System;
using System.Data;


using Xunit;

namespace Apache.Calcite.AdoNet.Tests.Connection
{

    public class CalciteConnectionTests
    {

        [Fact]
        public void Should_start_closed()
        {
            using var c = new CalciteConnection();
            Assert.Equal(ConnectionState.Closed, c.State);
        }

        [Fact]
        public void Should_open_and_close()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            Assert.Equal(ConnectionState.Open, c.State);
            c.Close();
            Assert.Equal(ConnectionState.Closed, c.State);
        }

        [Fact]
        public void Should_reject_connection_string_change_when_open()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            Assert.Throws<InvalidOperationException>(() => c.ConnectionString = TestModels.InlineEmptyModelConnectionString);
        }

        [Fact]
        public void Should_reject_double_open()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            Assert.Throws<InvalidOperationException>(() => c.Open());
        }

        [Fact]
        public void Close_when_already_closed_should_be_noop()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Close();
            Assert.Equal(ConnectionState.Closed, c.State);
        }

        [Fact]
        public void Dispose_should_close_open_connection()
        {
            var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            c.Dispose();
            Assert.Equal(ConnectionState.Closed, c.State);
        }

        [Fact]
        public void ChangeDatabase_should_update_schema()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.ChangeDatabase("NEW_SCHEMA");
            Assert.Equal("NEW_SCHEMA", c.Database);
        }

        [Fact]
        public void DataSource_should_reflect_model()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            Assert.Equal(TestModels.InlineEmptyModelJson, c.DataSource);
        }

        [Fact]
        public void ServerVersion_should_be_non_empty()
        {
            using var c = new CalciteConnection();
            Assert.False(string.IsNullOrWhiteSpace(c.ServerVersion));
        }

        [Fact]
        public void CreateCommand_should_return_command_bound_to_connection()
        {
            using var c = new CalciteConnection();
            using var cmd = c.CreateCommand();
            Assert.Same(c, cmd.Connection);
        }

        [Fact]
        public void BeginTransaction_should_return_transaction_with_isolation_level()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var tx = c.BeginTransaction(IsolationLevel.ReadCommitted);
            Assert.Equal(IsolationLevel.ReadCommitted, tx.IsolationLevel);
        }

    }

}
