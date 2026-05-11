using System;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    public class CalciteBatchTests
    {

        [Fact]
        public void ExecuteScalar_should_return_scalar_from_first_command()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var batch = new CalciteBatch(c);
            batch.BatchCommands.Add(new CalciteBatchCommand("VALUES 42"));

            var v = batch.ExecuteScalar();
            Assert.Equal(42, v);
        }

        [Fact]
        public async Task ExecuteScalarAsync_should_return_scalar_from_first_command()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var batch = new CalciteBatch(c);
            batch.BatchCommands.Add(new CalciteBatchCommand("VALUES 99"));

            var v = await batch.ExecuteScalarAsync(CancellationToken.None);
            Assert.Equal(99, v);
        }

        [Fact]
        public void ExecuteScalar_with_multiple_commands_should_return_scalar_from_first()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var batch = new CalciteBatch(c);
            batch.BatchCommands.Add(new CalciteBatchCommand("VALUES 1"));
            batch.BatchCommands.Add(new CalciteBatchCommand("VALUES 2"));

            var v = batch.ExecuteScalar();
            Assert.Equal(1, v);
        }

        [Fact]
        public void ExecuteReader_should_return_rows_from_last_command()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var batch = new CalciteBatch(c);
            batch.BatchCommands.Add(new CalciteBatchCommand("VALUES 7"));

            using var reader = batch.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(7, Convert.ToInt32(reader.GetValue(0)));
        }

        [Fact]
        public async Task ExecuteReaderAsync_should_return_rows_from_last_command()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var batch = new CalciteBatch(c);
            batch.BatchCommands.Add(new CalciteBatchCommand("VALUES 5"));

            using var reader = await batch.ExecuteReaderAsync(CancellationToken.None);
            Assert.True(reader.Read());
            Assert.Equal(5, Convert.ToInt32(reader.GetValue(0)));
        }

        [Fact]
        public void ExecuteScalar_with_empty_batch_should_return_null()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var batch = new CalciteBatch(c);

            var v = batch.ExecuteScalar();
            Assert.Null(v);
        }

        [Fact]
        public void ExecuteReader_with_empty_batch_should_throw()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var batch = new CalciteBatch(c);

            Assert.Throws<InvalidOperationException>(() => batch.ExecuteReader());
        }

        [Fact]
        public void Execute_without_connection_should_throw()
        {
            using var batch = new CalciteBatch();
            batch.BatchCommands.Add(new CalciteBatchCommand("VALUES 1"));

            Assert.Throws<InvalidOperationException>(() => batch.ExecuteScalar());
        }

        [Fact]
        public void CreateBatchCommand_should_return_new_instance()
        {
            using var batch = new CalciteBatch();
            var cmd = batch.CreateBatchCommand();
            Assert.NotNull(cmd);
            Assert.IsType<CalciteBatchCommand>(cmd);
        }

    }

}
