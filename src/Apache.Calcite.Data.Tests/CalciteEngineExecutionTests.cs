using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

using org.apache.calcite.runtime;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// End-to-end tests that exercise the native Calcite engine binding through the ADO.NET surface.
    /// </summary>
    public class CalciteEngineExecutionTests
    {

        [Fact]
        public void Should_execute_arithmetic_expression()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (1 + 2)";
            Assert.Equal(3, Convert.ToInt32(cmd.ExecuteScalar()));
        }

        [Fact]
        public void Should_execute_string_expression()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES ('hello')";
            Assert.Equal("hello", Convert.ToString(cmd.ExecuteScalar()));
        }

        [Fact]
        public void Should_enumerate_multiple_typed_columns()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(x, y)";

            using var r = cmd.ExecuteReader();
            var rows = 0;
            while (r.Read())
            {
                rows++;
                Assert.IsType<int>(r.GetValue(0));
                Assert.IsType<string>(r.GetValue(1));
            }

            Assert.Equal(3, rows);
        }

        [Fact]
        public async Task ExecuteReaderAsync_should_stream_rows()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT * FROM (VALUES (1), (2), (3)) AS t(x)";

            using var r = await cmd.ExecuteReaderAsync(CancellationToken.None);
            var rows = 0;
            while (await r.ReadAsync(CancellationToken.None))
                rows++;

            Assert.Equal(3, rows);
        }

        [Fact]
        public void Should_throw_on_invalid_sql()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "this is not sql";

            Assert.ThrowsAny<Exception>(() => cmd.ExecuteScalar());
        }

        [Fact]
        public void Should_handle_null_literal()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST(NULL AS INTEGER))";

            var v = cmd.ExecuteScalar();
            Assert.True(v is null || v is DBNull);
        }

        [Fact]
        public void Should_close_reader_when_command_behavior_is_close_connection()
        {
            var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES 1";

            using (var r = cmd.ExecuteReader(CommandBehavior.CloseConnection))
            {
                Assert.True(r.Read());
            }
        }

        // --- Bindable (interpreter) execution path ---

        [Fact]
        public void Bindable_scalar_expression_returns_correct_result()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (6 * 7)";
            cmd.RegisterHook(Hook.ENABLE_BINDABLE, true);

            Assert.Equal(42, Convert.ToInt32(cmd.ExecuteScalar()));
        }

        [Fact]
        public void Bindable_string_expression_returns_correct_result()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES ('bindable')";
            cmd.RegisterHook(Hook.ENABLE_BINDABLE, true);

            Assert.Equal("bindable", Convert.ToString(cmd.ExecuteScalar()));
        }

        [Fact]
        public void Bindable_multi_row_query_returns_all_rows()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT * FROM (VALUES (1), (2), (3)) AS t(x)";
            cmd.RegisterHook(Hook.ENABLE_BINDABLE, true);

            using var r = cmd.ExecuteReader();
            var rows = 0;
            while (r.Read())
                rows++;

            Assert.Equal(3, rows);
        }

        [Fact]
        public void Bindable_multi_column_query_returns_typed_columns()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT * FROM (VALUES (10, 'x'), (20, 'y')) AS t(n, s)";
            cmd.RegisterHook(Hook.ENABLE_BINDABLE, true);

            using var r = cmd.ExecuteReader();
            var rows = 0;
            while (r.Read())
            {
                rows++;
                Assert.IsType<int>(r.GetValue(0));
                Assert.IsType<string>(r.GetValue(1));
            }

            Assert.Equal(2, rows);
        }

        [Fact]
        public async Task Bindable_async_reader_streams_rows()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)";
            cmd.RegisterHook(Hook.ENABLE_BINDABLE, true);

            using var r = await cmd.ExecuteReaderAsync(CancellationToken.None);
            var rows = 0;
            while (await r.ReadAsync(CancellationToken.None))
                rows++;

            Assert.Equal(5, rows);
        }

        [Fact]
        public void Bindable_null_literal_is_handled()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST(NULL AS INTEGER))";
            cmd.RegisterHook(Hook.ENABLE_BINDABLE, true);

            var v = cmd.ExecuteScalar();
            Assert.True(v is null || v is DBNull);
        }

        [Fact]
        public void Bindable_set_on_connection_applies_to_all_commands()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.RegisterHook(Hook.ENABLE_BINDABLE, true);
            c.Open();

            using var cmd1 = c.CreateCommand();
            cmd1.CommandText = "VALUES (1)";
            Assert.Equal(1, Convert.ToInt32(cmd1.ExecuteScalar()));

            using var cmd2 = c.CreateCommand();
            cmd2.CommandText = "VALUES (2)";
            Assert.Equal(2, Convert.ToInt32(cmd2.ExecuteScalar()));
        }

        [Fact]
        public void Bindable_connection_and_command_hooks_both_apply()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.RegisterHook(Hook.ENABLE_BINDABLE, true);
            c.Open();

            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (99)";
            cmd.RegisterHook(Hook.ENABLE_BINDABLE, true);

            Assert.Equal(99, Convert.ToInt32(cmd.ExecuteScalar()));
        }

    }

}
