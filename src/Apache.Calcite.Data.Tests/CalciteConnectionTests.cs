using System;
using System.Data;

using Xunit;

namespace Apache.Calcite.Data.Tests
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

        [Fact]
        public void Session_should_survive_close_and_reopen()
        {
            // Schema mutations made while open must still be visible after a Close/Open cycle
            // because the Calcite session is retained across state transitions.
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            var rootBefore = c.RootSchema;
            c.Close();
            c.Open();
            // The session was not recreated, so the same SchemaPlus wrapper is returned.
            Assert.Same(rootBefore, c.RootSchema);
        }

        [Fact]
        public void ConnectionString_should_be_locked_after_first_open()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            c.Close();
            // The session already exists; changing the connection string must be rejected even
            // after Close() because the session is kept alive and cannot be reconfigured.
            Assert.Throws<InvalidOperationException>(() => c.ConnectionString = TestModels.InlineEmptyModelConnectionString);
        }

        [Fact]
        public void ConnectionString_can_be_set_before_first_open()
        {
            using var c = new CalciteConnection();
            c.ConnectionString = TestModels.InlineEmptyModelConnectionString;
            // The builder normalizes the connection string (casing, quoting), so we only
            // verify that the assignment was accepted and the value is non-empty.
            Assert.False(string.IsNullOrEmpty(c.ConnectionString));
        }

        [Fact]
        public void RootSchema_should_be_accessible_when_open()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            Assert.NotNull(c.RootSchema);
        }

        [Fact]
        public void RootSchema_should_throw_when_closed()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            Assert.Throws<InvalidOperationException>(() => c.RootSchema);
        }

        [Fact]
        public void Can_reopen_after_close()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            c.Close();
            c.Open();
            Assert.Equal(ConnectionState.Open, c.State);
        }

    }

}
