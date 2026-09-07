using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

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
    /// What is shared between two connections: the root schema of one data source, keyed by the connection
    /// string for the bare path and held by the caller for the built one.
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

        /// <summary>
        /// <c>Synchronous</c> chooses the convention a connection plans into and nothing that is built, so
        /// it is not part of the key — as Npgsql leaves <c>TargetSessionAttributes</c> out of its.
        /// </summary>
        [Fact]
        public void Synchronous_should_not_make_a_second_root()
        {
            var cs = ConnectionString(out var token);

            using var a = new CalciteConnection(cs);
            a.Open();
            using var b = new CalciteConnection(cs + ";Synchronous=true");
            b.Open();

            Assert.Equal(1, CountingSchemaFactory.Builds[token]);
            Assert.Same(Underlying(a), Underlying(b));
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

        [Fact]
        public void DDL_on_one_connection_should_be_visible_on_another()
        {
            var cs = ConnectionString(out _, Ddl);

            using var a = new CalciteConnection(cs);
            a.Open();
            NonQuery(a, "CREATE TABLE \"shared_t\" (\"id\" INTEGER NOT NULL)");
            NonQuery(a, "INSERT INTO \"shared_t\" VALUES (1), (2)");

            using var b = new CalciteConnection(cs);
            b.Open();

            Assert.Equal(2, Convert.ToInt32(Scalar(b, "SELECT COUNT(*) FROM \"shared_t\"")));
        }

        /// <summary>
        /// Connections used at once over one root: each plans with a type factory of its own, so the
        /// grouped aggregates and windows here, which register synthetic types in the factory, do not race,
        /// and the root is only read.
        /// </summary>
        [Fact]
        public async Task Concurrent_connections_on_one_root_should_plan_and_read()
        {
            var cs = ConnectionString(out _, Ddl);

            using (var setup = new CalciteConnection(cs))
            {
                setup.Open();
                NonQuery(setup, "CREATE TABLE \"conc_t\" (\"g\" INTEGER NOT NULL, \"v\" INTEGER NOT NULL)");
                NonQuery(setup, "INSERT INTO \"conc_t\" VALUES (1, 10), (1, 20), (2, 30), (2, 40), (3, 50)");
            }

            var tasks = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
            {
                for (int i = 0; i < 10; i++)
                {
                    await using var c = new CalciteConnection(cs);
                    await c.OpenAsync();

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
        /// And a <c>SchemaFactory</c> names any factory, with every <c>schema.</c>-prefixed key handed to
        /// it as an operand.
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

    }

}
