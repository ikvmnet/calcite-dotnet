using Apache.Calcite.Extensions.Prepare;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Tests for <see cref="LruPlanCache"/>'s retention policy: bounded, least-recently-used out first,
    /// and cleared per scope.
    /// </summary>
    [TestClass]
    public class LruPlanCacheTests
    {

        static readonly java.lang.reflect.Type ElementType = (java.lang.Class)typeof(java.lang.Object[]);

        static PlanCacheKey Key(PlanCacheScope scope, string sql, long generation = 0) =>
            new(scope, generation, sql, async: true, ElementType, -1);

        static PreparedPlan Plan(string sql) =>
            new(sql,
                com.google.common.collect.ImmutableList.of(),
                new java.util.LinkedHashMap(),
                null,
                com.google.common.collect.ImmutableList.of(),
                org.apache.calcite.avatica.Meta.CursorFactory.OBJECT,
                null,
                com.google.common.collect.ImmutableList.of(),
                -1,
                null,
                org.apache.calcite.avatica.Meta.StatementType.SELECT);

        [TestMethod]
        public void ShouldStoreAndReturnByKey()
        {
            var cache = new LruPlanCache(2);
            var scope = new PlanCacheScope();
            var plan = Plan("SELECT 1");

            cache.Add(Key(scope, "SELECT 1"), plan);

            cache.Get(Key(scope, "SELECT 1")).Should().BeSameAs(plan);
            cache.Get(Key(scope, "SELECT 2")).Should().BeNull();
        }

        [TestMethod]
        public void ShouldTreatADifferentGenerationAsADifferentKey()
        {
            var cache = new LruPlanCache(2);
            var scope = new PlanCacheScope();

            cache.Add(Key(scope, "SELECT 1", generation: 0), Plan("SELECT 1"));

            cache.Get(Key(scope, "SELECT 1", generation: 1)).Should().BeNull();
        }

        [TestMethod]
        public void ShouldEvictTheLeastRecentlyUsedEntryBeyondCapacity()
        {
            var cache = new LruPlanCache(2);
            var scope = new PlanCacheScope();

            cache.Add(Key(scope, "a"), Plan("a"));
            cache.Add(Key(scope, "b"), Plan("b"));

            // touch a, so b is the one to go
            cache.Get(Key(scope, "a")).Should().NotBeNull();

            cache.Add(Key(scope, "c"), Plan("c"));

            cache.Count.Should().Be(2);
            cache.Get(Key(scope, "a")).Should().NotBeNull();
            cache.Get(Key(scope, "b")).Should().BeNull();
            cache.Get(Key(scope, "c")).Should().NotBeNull();
        }

        [TestMethod]
        public void ShouldReplaceTheEntryWhenAddingTheSameKey()
        {
            var cache = new LruPlanCache(2);
            var scope = new PlanCacheScope();
            var second = Plan("a");

            cache.Add(Key(scope, "a"), Plan("a"));
            cache.Add(Key(scope, "a"), second);

            cache.Count.Should().Be(1);
            cache.Get(Key(scope, "a")).Should().BeSameAs(second);
        }

        [TestMethod]
        public void ShouldDropTheEntryOnRemove()
        {
            var cache = new LruPlanCache(2);
            var scope = new PlanCacheScope();

            cache.Add(Key(scope, "a"), Plan("a"));
            cache.Remove(Key(scope, "a"));

            cache.Count.Should().Be(0);
            cache.Get(Key(scope, "a")).Should().BeNull();
        }

        [TestMethod]
        public void ShouldClearOneScopeAndLeaveTheOther()
        {
            var cache = new LruPlanCache(4);
            var mine = new PlanCacheScope();
            var theirs = new PlanCacheScope();

            cache.Add(Key(mine, "a"), Plan("a"));
            cache.Add(Key(theirs, "a"), Plan("a"));
            cache.Add(Key(mine, "b"), Plan("b"));

            cache.Clear(mine);

            cache.Count.Should().Be(1);
            cache.Get(Key(mine, "a")).Should().BeNull();
            cache.Get(Key(theirs, "a")).Should().NotBeNull();
        }

    }

}
