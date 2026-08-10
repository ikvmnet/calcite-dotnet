using System;

using org.apache.calcite.avatica;
using org.apache.calcite.jdbc;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// The prepare pipeline behind a plan cache.
    /// </summary>
    /// <remarks>
    /// Retention and eviction belong to the <see cref="IPlanCache"/>; everything correctness-shaped is
    /// here, so an implementation of the interface cannot get it wrong. The key carries the scope's
    /// generation, and DDL moves it — DDL executes inside prepare, so the bump is made where its effect is
    /// first visible, read off the returned statement type rather than wired into the pipeline. A DDL text
    /// is never stored, so it can never hit; it is the <em>store</em> that admission guards, because a
    /// statement's kind is not known until it has been parsed, and by then an uncacheable statement has
    /// simply been prepared the ordinary way.
    ///
    /// <para>What the generation cannot see is the schema mutated directly — <c>SchemaPlus.add</c> from
    /// outside the pipeline — and that is what the dependency check covers: a hit is validated by
    /// resolving each recorded table against the live schema and requiring the same instance. The same
    /// check runs before a plan is stored, so a plan whose tables do not resolve stably is never cached at
    /// all rather than cached and evicted on every later lookup.</para>
    ///
    /// <para>Two things are deliberately a caller's problem. Planning-phase hooks are thread-bound and
    /// invisible here, so a caller that has activated any must not come through this path — a cached plan
    /// would neither reflect the hooks nor fire them. And the <c>Prepare.THREAD_EXPAND</c> /
    /// <c>THREAD_INSUBQUERY_THRESHOLD</c> thread-locals are read by the pipeline per prepare; a caller
    /// that varies them between statements of one session would need them in the key, and no caller of
    /// this provider does.</para>
    /// </remarks>
    sealed class CachingPrepare
    {

        readonly IPlanCache cache;
        readonly PlanCacheScope scope;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cache">Where plans are kept.</param>
        /// <param name="scope">The session partition the plans belong to.</param>
        public CachingPrepare(IPlanCache cache, PlanCacheScope scope)
        {
            this.cache = cache ?? throw new ArgumentNullException(nameof(cache));
            this.scope = scope ?? throw new ArgumentNullException(nameof(scope));
        }

        /// <summary>
        /// Gets the session partition the plans belong to.
        /// </summary>
        public PlanCacheScope Scope => scope;

        /// <summary>
        /// Returns a cached plan for the statement, or plans it and offers the result to the cache.
        /// </summary>
        /// <remarks>
        /// <see cref="ClrPrepareImpl.Prepare(CalcitePrepare.Context, string, java.lang.reflect.Type, long, bool)"/>
        /// with the cache in front of it, taking the same arguments because they are the key.
        /// </remarks>
        public PreparedPlan Prepare(CalcitePrepare.Context context, string sql, java.lang.reflect.Type elementType, long maxRowCount, bool async)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(sql);

            var key = new PlanCacheKey(scope, scope.Generation, sql, async, elementType, maxRowCount);

            if (cache.Get(key) is { } hit)
            {
                if (Validate(hit, context))
                    return hit;

                cache.Remove(key);
            }

            var plan = new ClrPrepareImpl().Prepare(context, sql, elementType, maxRowCount, async);

            if (IsDdl(plan.StatementType))
            {
                // the DDL has already taken effect; every key built under the old generation is now
                // unreachable, and this plan-less signature must never be served from the cache — a hit
                // would silently skip the DDL
                scope.BumpGeneration();
                return plan;
            }

            // an EXPLAIN's text renders the plan of one moment, and a plan-less signature has nothing
            // worth a slot; neither earns a store
            if (plan.Bindable is null or ClrExplainBindable)
                return plan;

            if (Validate(plan, context))
                cache.Add(key, plan);

            return plan;
        }

        /// <summary>
        /// Returns whether every table the plan depends on still resolves to the instance it was planned
        /// against.
        /// </summary>
        /// <remarks>
        /// Resolution is by the recorded names, case-sensitively — they are the canonical names resolution
        /// itself answered, not what the statement spelled.
        /// </remarks>
        static bool Validate(PreparedPlan plan, CalcitePrepare.Context context)
        {
            var dependencies = plan.Dependencies;
            if (dependencies is null)
                return false;

            var root = context.getRootSchema();

            foreach (var dependency in dependencies)
            {
                var schema = root;
                for (int i = 0; i < dependency.Path.Length - 1; i++)
                {
                    schema = schema.getSubSchema(dependency.Path[i], true);
                    if (schema is null)
                        return false;
                }

                var entry = schema.getTable(dependency.Path[dependency.Path.Length - 1], true);
                if (entry is null || ReferenceEquals(entry.getTable(), dependency.Table) == false)
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Returns whether <paramref name="t"/> represents a DDL statement type.
        /// </summary>
        static bool IsDdl(Meta.StatementType t) => t.name() switch
        {
            nameof(Meta.StatementType.CREATE) => true,
            nameof(Meta.StatementType.ALTER) => true,
            nameof(Meta.StatementType.DROP) => true,
            nameof(Meta.StatementType.OTHER_DDL) => true,
            _ => false,
        };

    }

}
