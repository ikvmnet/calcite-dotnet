using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;


using Xunit;

namespace Apache.Calcite.Data.Tests
{

    public class CalciteCommandTests
    {

        [Fact]
        public void ExecuteScalar_should_return_literal()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES 1";

            var v = cmd.ExecuteScalar();
            Assert.Equal(1, v);
        }

        [Fact]
        public async Task ExecuteScalarAsync_should_return_literal()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES 42";

            var v = await cmd.ExecuteScalarAsync(CancellationToken.None);
            Assert.Equal(42, Convert.ToInt32(v));
        }

        [Fact]
        public void Execute_without_connection_should_throw()
        {
            using var cmd = new CalciteCommand("VALUES 1");
            Assert.Throws<InvalidOperationException>(() => cmd.ExecuteScalar());
        }

        [Fact]
        public void Execute_with_closed_connection_should_throw()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES 1";
            Assert.Throws<InvalidOperationException>(() => cmd.ExecuteScalar());
        }

        [Fact]
        public void CommandText_should_default_to_empty()
        {
            using var cmd = new CalciteCommand();
            Assert.Equal(string.Empty, cmd.CommandText);
        }

        [Fact]
        public void CommandText_should_normalize_null_to_empty()
        {
            using var cmd = new CalciteCommand();
            cmd.CommandText = null!;
            Assert.Equal(string.Empty, cmd.CommandText);
        }

        [Fact]
        public void CommandTimeout_should_reject_negative()
        {
            using var cmd = new CalciteCommand();
            Assert.Throws<ArgumentOutOfRangeException>(() => cmd.CommandTimeout = -1);
        }

        [Fact]
        public void CommandType_should_reject_non_text()
        {
            using var cmd = new CalciteCommand();
            Assert.Throws<NotSupportedException>(() => cmd.CommandType = CommandType.StoredProcedure);
        }

        [Fact]
        public void CommandType_should_accept_text()
        {
            using var cmd = new CalciteCommand();
            cmd.CommandType = CommandType.Text;
            Assert.Equal(CommandType.Text, cmd.CommandType);
        }

        [Fact]
        public void CreateParameter_should_return_calcite_parameter()
        {
            using var cmd = new CalciteCommand();
            var p = cmd.CreateParameter();
            Assert.NotNull(p);
        }

        [Fact]
        public void Cancel_should_be_noop_on_idle_command()
        {
            using var cmd = new CalciteCommand();
            cmd.Cancel();
        }

        [Fact]
        public void Prepare_should_be_noop()
        {
            using var cmd = new CalciteCommand();
            cmd.Prepare();
        }

    }

}
