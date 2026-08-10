using System;
using System.Diagnostics;

using Apache.Calcite.Extensions.Prepare;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel.type;
using org.apache.calcite.runtime;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Tests for the connection's plan cache: association, hits, admission, and invalidation.
    /// </summary>
    public class PlanCacheTests
    {

        readonly Xunit.Abstractions.ITestOutputHelper output;

        public PlanCacheTests(Xunit.Abstractions.ITestOutputHelper output)
        {
            this.output = output;
        }

        static PlanCacheTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.server.ServerDdlExecutor).Assembly);
        }

        static readonly string ServerDdlConnectionString = new CalciteConnectionStringBuilder
        {
            Model = "inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]}",
            ParserFactory = "org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY",
            Schema = "adhoc",
        };

        /// <summary>
        /// An <see cref="IPlanCache"/> that counts what the session asks of it, delegating storage to the
        /// built-in cache. The counters are the observable the tests assert on; the interface is public,
        /// so this is also the proof that a caller-supplied implementation is enough.
        /// </summary>
        sealed class CountingPlanCache : IPlanCache
        {

            readonly LruPlanCache inner;

            public int Gets, Hits, Adds, Removes;
            public PreparedPlan? LastAdded, LastHit;

            public CountingPlanCache(int capacity = 16) => inner = new LruPlanCache(capacity);

            public int Count => inner.Count;

            public PreparedPlan? Get(in PlanCacheKey key)
            {
                Gets++;
                var plan = inner.Get(key);
                if (plan is not null)
                {
                    Hits++;
                    LastHit = plan;
                }

                return plan;
            }

            public void Add(in PlanCacheKey key, PreparedPlan plan)
            {
                Adds++;
                LastAdded = plan;
                inner.Add(key, plan);
            }

            public void Remove(in PlanCacheKey key)
            {
                Removes++;
                inner.Remove(key);
            }

            public void Clear(PlanCacheScope scope) => inner.Clear(scope);

        }

        /// <summary>
        /// A one-row, one-column table whose value tells instances apart.
        /// </summary>
        sealed class ValueTable : AbstractTable, ScannableTable
        {

            readonly int value;

            public ValueTable(int value) => this.value = value;

            public override RelDataType getRowType(RelDataTypeFactory typeFactory) =>
                new RelDataTypeFactory.Builder(typeFactory)
                    .add("VAL", SqlTypeName.INTEGER)
                    .build();

            public org.apache.calcite.linq4j.Enumerable scan(DataContext root) =>
                Linq4j.singletonEnumerable(new object[] { value });

        }

        static CalciteConnection OpenWithCache(CountingPlanCache cache, string? connectionString = null)
        {
            var c = new CalciteConnection(connectionString ?? TestModels.InlineEmptyModelConnectionString);
            c.PlanCacheFactory = CalcitePlanCacheFactory.From(cache);
            c.Open();
            return c;
        }

        [Fact]
        public void Second_execution_of_the_same_text_reuses_the_plan()
        {
            var cache = new CountingPlanCache();
            using var c = OpenWithCache(cache);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT * FROM (VALUES (1), (2), (3)) AS t(x)";

            using (var r = cmd.ExecuteReader())
            {
                var rows = 0;
                while (r.Read())
                    rows++;
                Assert.Equal(3, rows);
            }

            Assert.Equal(1, cache.Adds);

            using (var r = cmd.ExecuteReader())
            {
                var rows = 0;
                while (r.Read())
                    rows++;
                Assert.Equal(3, rows);
            }

            Assert.Equal(1, cache.Adds);
            Assert.Equal(1, cache.Hits);
            Assert.Same(cache.LastAdded, cache.LastHit);
        }

        [Fact]
        public void Different_parameter_values_reuse_the_plan()
        {
            var cache = new CountingPlanCache();
            using var c = OpenWithCache(cache);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT x FROM (VALUES (1), (2), (3)) AS t(x) WHERE x = ?";
            var p = cmd.CreateParameter();
            p.Value = 1;
            cmd.Parameters.Add(p);

            Assert.Equal(1, Convert.ToInt32(cmd.ExecuteScalar()));

            p.Value = 2;
            Assert.Equal(2, Convert.ToInt32(cmd.ExecuteScalar()));

            Assert.Equal(1, cache.Adds);
            Assert.Equal(1, cache.Hits);
        }

        [Fact]
        public void Table_query_reuses_the_plan()
        {
            var cache = new CountingPlanCache();
            using var c = OpenWithCache(cache);
            c.RootSchema.add("CACHED_T", new ValueTable(42));

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT * FROM \"CACHED_T\"";

            Assert.Equal(42, Convert.ToInt32(cmd.ExecuteScalar()));
            Assert.Equal(42, Convert.ToInt32(cmd.ExecuteScalar()));

            Assert.Equal(1, cache.Adds);
            Assert.Equal(1, cache.Hits);
        }

        [Fact]
        public void Replacing_a_table_on_the_schema_invalidates_the_plan()
        {
            var cache = new CountingPlanCache();
            using var c = OpenWithCache(cache);
            c.RootSchema.add("REPLACED_T", new ValueTable(42));

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT * FROM \"REPLACED_T\"";

            Assert.Equal(42, Convert.ToInt32(cmd.ExecuteScalar()));

            // not DDL, so no generation moves — this is exactly the mutation the dependency check exists for
            c.RootSchema.add("REPLACED_T", new ValueTable(43));

            Assert.Equal(43, Convert.ToInt32(cmd.ExecuteScalar()));
            Assert.Equal(1, cache.Removes);
            Assert.Equal(2, cache.Adds);
        }

        [Fact]
        public void A_request_carrying_hooks_bypasses_the_cache()
        {
            var cache = new CountingPlanCache();
            using var c = OpenWithCache(cache);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (6 * 7)";
            cmd.RegisterHook(Hook.ENABLE_BINDABLE, true);

            Assert.Equal(42, Convert.ToInt32(cmd.ExecuteScalar()));
            Assert.Equal(42, Convert.ToInt32(cmd.ExecuteScalar()));

            Assert.Equal(0, cache.Gets);
            Assert.Equal(0, cache.Adds);
        }

        [Fact]
        public void Explain_is_not_cached()
        {
            var cache = new CountingPlanCache();
            using var c = OpenWithCache(cache);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "EXPLAIN PLAN FOR SELECT * FROM (VALUES (1)) AS t(x)";

            Assert.NotNull(cmd.ExecuteScalar());
            Assert.Equal(0, cache.Adds);
        }

        [Fact]
        public void A_shared_cache_partitions_by_connection_and_clears_on_dispose()
        {
            var cache = new CountingPlanCache();

            var c1 = OpenWithCache(cache);
            using var c2 = OpenWithCache(cache);

            using (var cmd1 = c1.CreateCommand())
            {
                cmd1.CommandText = "VALUES (1)";
                cmd1.ExecuteScalar();
            }

            using (var cmd2 = c2.CreateCommand())
            {
                cmd2.CommandText = "VALUES (1)";
                cmd2.ExecuteScalar();
            }

            // one text, two sessions, two entries — plans are never shared, only capacity
            Assert.Equal(2, cache.Adds);
            Assert.Equal(0, cache.Hits);
            Assert.Equal(2, cache.Count);

            c1.Dispose();
            Assert.Equal(1, cache.Count);

            using (var cmd2 = c2.CreateCommand())
            {
                cmd2.CommandText = "VALUES (1)";
                cmd2.ExecuteScalar();
            }

            Assert.Equal(1, cache.Hits);
        }

        [Fact]
        public void PlanCacheSize_connection_string_key_installs_the_builtin_cache()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString + ";PlanCacheSize=8");
            c.Open();

            var cache = Assert.IsType<LruPlanCache>(c.RequireSession().PlanCache);
            Assert.Equal(8, cache.Capacity);

            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (1)";
            cmd.ExecuteScalar();
            cmd.ExecuteScalar();
            Assert.Equal(1, cache.Count);
        }

        [Fact]
        public void Without_a_cache_nothing_is_consulted()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();

            Assert.Null(c.RequireSession().PlanCache);
        }

        [Fact]
        public void The_factory_cannot_change_after_open()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();

            Assert.Throws<InvalidOperationException>(() => c.PlanCacheFactory = CalcitePlanCacheFactory.From(new CountingPlanCache()));
        }

        [Fact]
        public void Prepare_warms_the_cache()
        {
            var cache = new CountingPlanCache();
            using var c = OpenWithCache(cache);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT * FROM (VALUES (1), (2)) AS t(x)";

            cmd.Prepare();
            Assert.Equal(1, cache.Adds);

            Assert.Equal(1, Convert.ToInt32(cmd.ExecuteScalar()));
            Assert.Equal(1, cache.Adds);
            Assert.Equal(1, cache.Hits);
        }

        [Fact]
        public void Prepare_does_not_execute_ddl()
        {
            var cache = new CountingPlanCache();
            using var c = OpenWithCache(cache, ServerDdlConnectionString);

            using (var create = c.CreateCommand())
            {
                create.CommandText = "CREATE TABLE \"adhoc\".\"prepared_ddl\" (\"i\" INTEGER)";
                create.Prepare();
                Assert.Equal(0, cache.Adds);
            }

            // the table must not exist yet: preparing DDL must not have executed it
            using (var probe = c.CreateCommand())
            {
                probe.CommandText = "SELECT * FROM \"adhoc\".\"prepared_ddl\"";
                Assert.ThrowsAny<Exception>(() => probe.ExecuteScalar());
            }

            using (var create = c.CreateCommand())
            {
                create.CommandText = "CREATE TABLE \"adhoc\".\"prepared_ddl\" (\"i\" INTEGER)";
                create.ExecuteNonQuery();
            }

            using (var probe = c.CreateCommand())
            {
                probe.CommandText = "SELECT * FROM \"adhoc\".\"prepared_ddl\"";
                using var r = probe.ExecuteReader();
                Assert.False(r.Read());
            }
        }

        [Fact]
        public void Ddl_moves_the_generation_and_strands_the_old_entry()
        {
            var cache = new CountingPlanCache();
            using var c = OpenWithCache(cache, ServerDdlConnectionString);

            using var select = c.CreateCommand();
            select.CommandText = "SELECT * FROM (VALUES (1)) AS t(x)";
            select.ExecuteScalar();
            Assert.Equal(1, cache.Adds);

            using (var ddl = c.CreateCommand())
            {
                ddl.CommandText = "CREATE SCHEMA IF NOT EXISTS \"strand_test\"";
                ddl.ExecuteNonQuery();
            }

            // the old entry's key is unreachable under the new generation: a fresh plan, no hit
            select.ExecuteScalar();
            Assert.Equal(2, cache.Adds);
            Assert.Equal(0, cache.Hits);
        }

        [Fact]
        public void Synchronous_convention_caches_too()
        {
            var cache = new CountingPlanCache();
            using var c = OpenWithCache(cache, TestModels.InlineEmptyModelConnectionString + ";Synchronous=true");
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT * FROM (VALUES (1), (2)) AS t(x)";

            Assert.Equal(1, Convert.ToInt32(cmd.ExecuteScalar()));
            Assert.Equal(1, Convert.ToInt32(cmd.ExecuteScalar()));

            Assert.Equal(1, cache.Adds);
            Assert.Equal(1, cache.Hits);
        }

        [Fact]
        public void A_data_source_applies_its_factory_to_each_connection()
        {
            var cache = new CountingPlanCache();
            var source = new CalciteDataSource(TestModels.InlineEmptyModelConnectionString)
            {
                PlanCacheFactory = CalcitePlanCacheFactory.From(cache),
            };

            using var c1 = source.OpenConnection();
            using var c2 = source.OpenConnection();

            using (var cmd = c1.CreateCommand())
            {
                cmd.CommandText = "VALUES (1)";
                cmd.ExecuteScalar();
            }

            using (var cmd = c2.CreateCommand())
            {
                cmd.CommandText = "VALUES (1)";
                cmd.ExecuteScalar();
            }

            Assert.Equal(2, cache.Adds);
            Assert.Equal(2, cache.Count);
        }

        [Fact]
        public void A_cache_hit_costs_less_than_planning()
        {
            var cache = new CountingPlanCache(4);
            using var c = OpenWithCache(cache);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT t1.x, t2.y FROM (VALUES (1, 'a'), (2, 'b')) AS t1(x, y) JOIN (VALUES (1, 'a'), (2, 'b')) AS t2(x, y) ON t1.x = t2.x ORDER BY t1.x";

            // populate, then measure hits against a no-cache connection planning the same text
            cmd.ExecuteReader().Dispose();

            var hits = Stopwatch.StartNew();
            for (var i = 0; i < 25; i++)
                cmd.ExecuteReader().Dispose();
            hits.Stop();

            using var uncached = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            uncached.Open();
            using var cmd2 = uncached.CreateCommand();
            cmd2.CommandText = cmd.CommandText;

            var plans = Stopwatch.StartNew();
            for (var i = 0; i < 25; i++)
                cmd2.ExecuteReader().Dispose();
            plans.Stop();

            output.WriteLine($"25 cache hits: {hits.Elapsed.TotalMilliseconds:F1} ms; 25 plans: {plans.Elapsed.TotalMilliseconds:F1} ms.");

            Assert.Equal(25, cache.Hits);
            Assert.True(hits.Elapsed < plans.Elapsed,
                $"25 cache hits took {hits.Elapsed.TotalMilliseconds:F1} ms; 25 plans took {plans.Elapsed.TotalMilliseconds:F1} ms.");
        }

    }

}
