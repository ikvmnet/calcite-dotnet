using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using org.apache.calcite.runtime;

using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// A schema factory that counts how many times a model has been read, per <c>token</c> operand, and keeps
    /// every schema it made so a test can see whether it was disposed. Public with a public parameterless
    /// constructor, which is what a model's <c>factory</c> requires.
    /// </summary>
    public sealed class CountingSchemaFactory : SchemaFactory
    {

        /// <summary>
        /// Builds per token.
        /// </summary>
        public static readonly ConcurrentDictionary<string, int> Builds = new();

        /// <summary>
        /// Schemas made per token, in the order they were made.
        /// </summary>
        public static readonly ConcurrentDictionary<string, ConcurrentQueue<DisposableSchema>> Made = new();

        /// <inheritdoc />
        public Schema create(SchemaPlus parentSchema, string name, java.util.Map operand)
        {
            var token = (string)operand.get("token");
            Builds.AddOrUpdate(token, 1, (_, n) => n + 1);
            var schema = new DisposableSchema();
            Made.GetOrAdd(token, _ => new ConcurrentQueue<DisposableSchema>()).Enqueue(schema);
            return schema;
        }

    }

    /// <summary>
    /// A schema that remembers being disposed, which is how an adapter holding a client would let it go.
    /// </summary>
    public sealed class DisposableSchema : AbstractSchema, IDisposable
    {

        /// <summary>
        /// Whether <see cref="Dispose"/> has been called.
        /// </summary>
        public bool Disposed { get; private set; }

        /// <inheritdoc />
        public void Dispose()
        {
            Disposed = true;
        }

    }

    /// <summary>
    /// What is shared between two connections: the schemas of one data source, keyed by the connection string
    /// for the bare path and held by the caller for the built one, and the root itself only where the built
    /// data source asks for it.
    /// </summary>
    public class CalciteDataSourceSharingTests
    {

        static readonly string Factory = typeof(CountingSchemaFactory).FullName + ", " + typeof(CountingSchemaFactory).Assembly.GetName().Name;

        /// <summary>
        /// A connection string whose model names <see cref="CountingSchemaFactory"/> with a fresh token, so
        /// that the count is this test's alone.
        /// </summary>
        static string ConnectionString(out string token, string extra = "")
        {
            token = Guid.NewGuid().ToString("N");
            return "Model=inline:{\"version\":\"1.0\",\"defaultSchema\":\"S\",\"schemas\":[{\"name\":\"S\",\"type\":\"custom\",\"factory\":\"" + Factory + "\",\"operand\":{\"token\":\"" + token + "\"}}]};Schema=S" + extra;
        }

        const string Ddl = ";parserFactory=org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY";

        /// <summary>
        /// The schema the factory made, as reached through a connection — <c>plus()</c> wraps afresh on
        /// every call, so the wrappers a connection hands out are never the same object and the schema
        /// underneath is what says whether a root is shared.
        /// </summary>
        static DisposableSchema Underlying(CalciteConnection c)
        {
            return (DisposableSchema)((SchemaPlus)c.RootSchema.getSubSchema("S")!).unwrap((java.lang.Class)typeof(DisposableSchema));
        }

        static object? Scalar(CalciteConnection c, string sql)
        {
            using var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteScalar();
        }

        static void NonQuery(CalciteConnection c, string sql)
        {
            using var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        [Fact]
        public void Two_connections_on_one_connection_string_should_share_one_root()
        {
            var cs = ConnectionString(out var token);

            using var a = new CalciteConnection(cs);
            a.Open();
            using var b = new CalciteConnection(cs);
            b.Open();

            Assert.Equal(1, CountingSchemaFactory.Builds[token]);
            Assert.Same(Underlying(a), Underlying(b));
        }

        [Fact]
        public void The_root_should_outlive_the_connection_that_built_it()
        {
            var cs = ConnectionString(out var token);

            using (var a = new CalciteConnection(cs))
                a.Open();

            using var b = new CalciteConnection(cs);
            b.Open();

            Assert.Equal(1, CountingSchemaFactory.Builds[token]);
            Assert.False(CountingSchemaFactory.Made[token].Single().Disposed);
        }

        [Fact]
        public void Pooling_false_should_build_a_root_per_connection_and_dispose_it_with_the_connection()
        {
            var cs = ConnectionString(out var token, ";Pooling=false");

            var a = new CalciteConnection(cs);
            a.Open();
            using var b = new CalciteConnection(cs);
            b.Open();

            Assert.Equal(2, CountingSchemaFactory.Builds[token]);
            Assert.NotSame(Underlying(a), Underlying(b));

            a.Dispose();
            var made = CountingSchemaFactory.Made[token].ToArray();
            Assert.True(made[0].Disposed);
            Assert.False(made[1].Disposed);
        }

        [Fact]
        public void Key_order_and_casing_should_not_make_a_second_root()
        {
            var cs = ConnectionString(out var token);
            var options = new CalciteConnectionStringBuilder(cs);
            var reordered = "schema=" + options.Schema + ";MODEL=" + options.Model;

            using var a = new CalciteConnection(cs);
            a.Open();
            using var b = new CalciteConnection(reordered);
            b.Open();

            Assert.Equal(1, CountingSchemaFactory.Builds[token]);
        }

        [Fact]
        public void A_different_connection_string_should_build_a_root_of_its_own()
        {
            var cs = ConnectionString(out var token);

            using var a = new CalciteConnection(cs);
            a.Open();
            using var b = new CalciteConnection(cs + ";Lex=MYSQL");
            b.Open();

            Assert.Equal(2, CountingSchemaFactory.Builds[token]);
        }

        [Fact]
        public void A_built_data_source_should_not_be_shared_with_the_bare_path()
        {
            var cs = ConnectionString(out var token);

            using var dataSource = new CalciteDataSource(cs);
            using var a = dataSource.OpenConnection();
            using var b = new CalciteConnection(cs);
            b.Open();

            Assert.Equal(2, CountingSchemaFactory.Builds[token]);
            Assert.NotSame(Underlying(a), Underlying(b));
        }

        [Fact]
        public void ClearPool_should_make_the_next_connection_build_again_and_leave_open_ones_alone()
        {
            var cs = ConnectionString(out var token);

            using var a = new CalciteConnection(cs);
            a.Open();
            CalciteConnection.ClearPool(a);
            using var b = new CalciteConnection(cs);
            b.Open();

            Assert.Equal(2, CountingSchemaFactory.Builds[token]);
            Assert.NotSame(Underlying(a), Underlying(b));
            Assert.Equal(2, Convert.ToInt32(Scalar(a, "SELECT 1 + 1")));
            Assert.False(CountingSchemaFactory.Made[token].First().Disposed);
        }

        [Fact]
        public void Disposing_a_data_source_should_dispose_its_schemas_and_refuse_new_connections()
        {
            var schema = new DisposableSchema();
            var dataSource = new CalciteDataSourceBuilder(TestModels.InlineEmptyModelConnectionString)
                .AddSchema("D", schema)
                .Build();

            using (var c = dataSource.OpenConnection())
                Assert.NotNull(c.RootSchema.getSubSchema("D"));

            dataSource.Dispose();

            Assert.True(schema.Disposed);
            Assert.Throws<ObjectDisposedException>(() => dataSource.OpenConnection());
        }

        [Fact]
        public void Clear_on_a_data_source_should_make_the_next_connection_build_again()
        {
            var cs = ConnectionString(out var token);

            using var dataSource = new CalciteDataSource(cs);
            using var a = dataSource.OpenConnection();
            dataSource.Clear();
            using var b = dataSource.OpenConnection();

            Assert.Equal(2, CountingSchemaFactory.Builds[token]);
            Assert.NotSame(Underlying(a), Underlying(b));
        }

        [Fact]
        public void Configure_root_schema_should_run_after_the_model()
        {
            Schema? seen = null;
            using var dataSource = new CalciteDataSourceBuilder(TestModels.InlineEmptyModelConnectionString)
                .ConfigureRootSchema(root => seen = root.getSubSchema("adhoc"))
                .Build();

            using var c = dataSource.OpenConnection();

            Assert.NotNull(seen);
        }

        /// <summary>
        /// The root a connection hands out is typed as the read interface: <c>SchemaPlus</c>, which is what
        /// carries <c>add</c>, is the data source's.
        /// </summary>
        [Fact]
        public void A_connection_should_hand_out_the_root_as_a_schema()
        {
            Assert.Equal(typeof(Schema), typeof(CalciteConnection).GetProperty(nameof(CalciteConnection.RootSchema))!.PropertyType);
        }

        /// <summary>
        /// By default each connection plans against a root of its own, so a table DDL creates stays with the
        /// connection that created it, while it is open and after.
        /// </summary>
        [Fact]
        public void DDL_on_one_connection_should_not_be_visible_on_another()
        {
            var cs = ConnectionString(out var token, Ddl);

            using (var a = new CalciteConnection(cs))
            {
                a.Open();
                NonQuery(a, "CREATE TABLE \"own_t\" (\"id\" INTEGER NOT NULL)");
                NonQuery(a, "INSERT INTO \"own_t\" VALUES (1), (2)");
                Assert.Equal(2, Convert.ToInt32(Scalar(a, "SELECT COUNT(*) FROM \"own_t\"")));

                using var b = new CalciteConnection(cs);
                b.Open();
                Assert.Throws<CalciteException>(() => Scalar(b, "SELECT COUNT(*) FROM \"own_t\""));
            }

            using var c = new CalciteConnection(cs);
            c.Open();
            Assert.Throws<CalciteException>(() => Scalar(c, "SELECT COUNT(*) FROM \"own_t\""));
            Assert.Equal(1, CountingSchemaFactory.Builds[token]);
        }

        /// <summary>
        /// A recursive query registers its working table at its connection's root, and removes it when
        /// closed; on roots of their own, two connections running the same one at once do not meet.
        /// </summary>
        [Fact]
        public async Task Recursive_queries_on_two_connections_should_not_share_a_working_table()
        {
            var cs = ConnectionString(out _);
            const string sql = "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 200) SELECT SUM(n) FROM t";

            var tasks = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
            {
                for (int i = 0; i < 10; i++)
                {
                    using var c = new CalciteConnection(cs);
                    c.Open();
                    Assert.Equal(20100L, Convert.ToInt64(Scalar(c, sql)));
                }
            }));

            await Task.WhenAll(tasks);
        }

        [Fact]
        public void DDL_on_a_shared_root_should_be_visible_on_another_connection()
        {
            using var dataSource = new CalciteDataSourceBuilder(ConnectionString(out _, Ddl)).UseSharedRootSchema().Build();

            using var a = dataSource.OpenConnection();
            NonQuery(a, "CREATE TABLE \"shared_t\" (\"id\" INTEGER NOT NULL)");
            NonQuery(a, "INSERT INTO \"shared_t\" VALUES (1), (2)");

            using var b = dataSource.OpenConnection();

            Assert.Equal(2, Convert.ToInt32(Scalar(b, "SELECT COUNT(*) FROM \"shared_t\"")));
        }

        /// <summary>
        /// A shared root lasts as long as its data source, whether or not a connection is open on it.
        /// </summary>
        [Fact]
        public void DDL_on_a_shared_root_should_outlive_the_connection_that_ran_it()
        {
            using var dataSource = new CalciteDataSourceBuilder(ConnectionString(out _, Ddl)).UseSharedRootSchema().Build();

            using (var a = dataSource.OpenConnection())
                NonQuery(a, "CREATE TABLE \"kept_t\" (\"id\" INTEGER NOT NULL)");

            using var b = dataSource.OpenConnection();

            Assert.Equal(0, Convert.ToInt32(Scalar(b, "SELECT COUNT(*) FROM \"kept_t\"")));
        }

        [Fact]
        public void A_shared_root_should_be_refused_with_pooling_false()
        {
            Assert.Throws<ArgumentException>(() => new CalciteDataSourceBuilder(TestModels.InlineEmptyModelConnectionString + ";Pooling=false").UseSharedRootSchema().Build());
        }

        /// <summary>
        /// Connections used at once over one shared root: each plans with a type factory of its own, so the
        /// grouped aggregates and windows here, which register synthetic types in the factory, do not race,
        /// and the root is only read.
        /// </summary>
        [Fact]
        public async Task Concurrent_connections_on_one_root_should_plan_and_read()
        {
            using var dataSource = new CalciteDataSourceBuilder(ConnectionString(out _, Ddl)).UseSharedRootSchema().Build();

            using (var setup = dataSource.OpenConnection())
            {
                NonQuery(setup, "CREATE TABLE \"conc_t\" (\"g\" INTEGER NOT NULL, \"v\" INTEGER NOT NULL)");
                NonQuery(setup, "INSERT INTO \"conc_t\" VALUES (1, 10), (1, 20), (2, 30), (2, 40), (3, 50)");
            }

            var tasks = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
            {
                for (int i = 0; i < 10; i++)
                {
                    await using var c = await dataSource.OpenConnectionAsync();

                    await using (var cmd = c.CreateCommand())
                    {
                        cmd.CommandText = "SELECT \"g\", COUNT(*), SUM(\"v\") FROM \"conc_t\" GROUP BY \"g\" ORDER BY \"g\"";
                        await using var r = await cmd.ExecuteReaderAsync();
                        var groups = new List<(int, long, int)>();
                        while (await r.ReadAsync())
                            groups.Add((r.GetInt32(0), r.GetInt64(1), r.GetInt32(2)));
                        Assert.Equal([(1, 2L, 30), (2, 2L, 70), (3, 1L, 50)], groups);
                    }

                    await using (var cmd = c.CreateCommand())
                    {
                        cmd.CommandText = "SELECT \"v\", ROW_NUMBER() OVER (PARTITION BY \"g\" ORDER BY \"v\") FROM \"conc_t\" ORDER BY \"v\"";
                        await using var r = await cmd.ExecuteReaderAsync();
                        var rows = new List<(int, long)>();
                        while (await r.ReadAsync())
                            rows.Add((r.GetInt32(0), r.GetInt64(1)));
                        Assert.Equal([(10, 1L), (20, 2L), (30, 1L), (40, 2L), (50, 1L)], rows);
                    }
                }
            }));

            await Task.WhenAll(tasks);
        }

        /// <summary>
        /// The driver's own model step: without a <c>Model</c>, a <c>SchemaType</c> names a factory Calcite
        /// ships, and the schema it makes is named by the <c>Schema</c> key.
        /// </summary>
        [Fact]
        public void SchemaType_should_make_a_schema_named_by_the_Schema_key()
        {
            using var c = new CalciteConnection("SchemaType=MAP;Schema=X;Pooling=false");
            c.Open();

            Assert.NotNull(c.RootSchema.getSubSchema("X"));
            Assert.Equal(2, Convert.ToInt32(Scalar(c, "SELECT 1 + 1")));
        }

        /// <summary>
        /// Without a <c>Model</c>, a <c>SchemaFactory</c> names any factory, and every <c>schema.</c>-prefixed
        /// key is handed to it as an operand.
        /// </summary>
        [Fact]
        public void SchemaFactory_should_make_a_schema_from_the_named_factory()
        {
            var token = Guid.NewGuid().ToString("N");
            using var c = new CalciteConnection("SchemaFactory=\"" + Factory + "\";schema.token=" + token + ";Pooling=false");
            c.Open();

            Assert.Equal(1, CountingSchemaFactory.Builds[token]);
            Assert.NotNull(c.RootSchema.getSubSchema("adhoc"));
        }

        [Fact]
        public void An_evicted_root_should_be_disposed_when_its_last_connection_is()
        {
            var cs = ConnectionString(out var token);

            var a = new CalciteConnection(cs);
            a.Open();
            CalciteConnection.ClearPool(a);

            var schema = CountingSchemaFactory.Made[token].Single();
            Assert.False(schema.Disposed);
            Assert.Equal(2, Convert.ToInt32(Scalar(a, "SELECT 1 + 1")));

            a.Dispose();
            Assert.True(schema.Disposed);
        }

        [Fact]
        public void Clear_should_dispose_the_dropped_root_when_its_last_connection_is()
        {
            var cs = ConnectionString(out var token);

            using var dataSource = new CalciteDataSource(cs);
            var a = dataSource.OpenConnection();
            dataSource.Clear();
            using var b = dataSource.OpenConnection();

            var made = CountingSchemaFactory.Made[token].ToArray();
            Assert.Equal(2, made.Length);
            Assert.False(made[0].Disposed);

            a.Dispose();
            Assert.True(made[0].Disposed);
            Assert.False(made[1].Disposed);
        }

        /// <summary>
        /// A pooled data source that has gone its idle lifetime with no connection open is released and its
        /// schemas disposed, and the next connection builds again.
        /// </summary>
        [Fact]
        public async Task An_idle_data_source_should_be_released_after_its_idle_lifetime()
        {
            var cs = ConnectionString(out var token, ";Connection Idle Lifetime=1;Connection Pruning Interval=1");

            using (var a = new CalciteConnection(cs))
                a.Open();

            var schema = CountingSchemaFactory.Made[token].Single();
            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (schema.Disposed == false && DateTime.UtcNow < deadline)
                await Task.Delay(100);

            Assert.True(schema.Disposed);

            using var b = new CalciteConnection(cs);
            b.Open();
            Assert.Equal(2, CountingSchemaFactory.Builds[token]);
        }

        [Fact]
        public async Task A_data_source_with_a_connection_open_should_not_be_released()
        {
            var cs = ConnectionString(out var token, ";Connection Idle Lifetime=1;Connection Pruning Interval=1");

            using var a = new CalciteConnection(cs);
            a.Open();

            await Task.Delay(3500);

            Assert.False(CountingSchemaFactory.Made[token].Single().Disposed);
            Assert.Equal(2, Convert.ToInt32(Scalar(a, "SELECT 1 + 1")));

            using var b = new CalciteConnection(cs);
            b.Open();
            Assert.Equal(1, CountingSchemaFactory.Builds[token]);
        }

        [Fact]
        public void Pooling_settings_should_be_validated()
        {
            Assert.Throws<ArgumentException>(() => new CalciteDataSource(TestModels.InlineEmptyModelConnectionString + ";Connection Pruning Interval=0"));
            Assert.Throws<ArgumentException>(() => new CalciteDataSource(TestModels.InlineEmptyModelConnectionString + ";Connection Idle Lifetime=5;Connection Pruning Interval=10"));

            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString + ";Connection Pruning Interval=0");
            Assert.Throws<ArgumentException>(() => c.Open());
        }

        /// <summary>
        /// Planning holds a shared root's read lock and DDL wants its write lock, so a DDL statement on one
        /// connection waits for a statement planning on another. The hook fires inside planning, which is
        /// how the test holds a plan open.
        /// </summary>
        [Fact]
        public async Task DDL_on_a_shared_root_should_wait_for_a_statement_planning_on_another_connection()
        {
            using var dataSource = new CalciteDataSourceBuilder(ConnectionString(out _, Ddl)).UseSharedRootSchema().Build();

            using var inPlan = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();

            using var a = dataSource.OpenConnection();
            a.RegisterHook(Hook.PLAN_BEFORE_IMPLEMENTATION, _ =>
            {
                inPlan.Set();
                release.Wait(TimeSpan.FromSeconds(30));
            });

            var planning = Task.Run(() => Scalar(a, "SELECT COUNT(*) FROM (VALUES (1), (2)) AS t(x)"));
            Assert.True(inPlan.Wait(TimeSpan.FromSeconds(30)));

            using var b = dataSource.OpenConnection();
            var ddl = Task.Run(() => NonQuery(b, "CREATE TABLE \"waits_t\" (\"id\" INTEGER NOT NULL)"));

            Assert.False(ddl.Wait(TimeSpan.FromMilliseconds(750)));

            release.Set();
            await planning;
            await ddl;

            Assert.Equal(0, Convert.ToInt32(Scalar(b, "SELECT COUNT(*) FROM \"waits_t\"")));
        }

    }

}
